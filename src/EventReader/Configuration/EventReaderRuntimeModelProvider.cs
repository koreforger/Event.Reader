using System.Collections.ObjectModel;
using System.Text.RegularExpressions;
using Event.Data;
using Event.Data.Entities;
using Event.Streaming.Processing.Runtime;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Primitives;

namespace EventReader.Configuration;

public interface IEventReaderRuntimeModelProvider
{
    EventReaderRuntimeModel Current { get; }
    long CurrentVersion { get; }
    IReadOnlyList<long> ActiveVersions { get; }
    Task ReloadAsync(CancellationToken ct);
    Task<RuntimeModelGcResult> CollectGarbageAsync(IReadOnlySet<long> activeWorkStoreVersions, CancellationToken ct);
}

public sealed record RuntimeModelGcResult(int VersionsCollected, int VersionsRetained);

public sealed class EventReaderRuntimeModelProvider : IEventReaderRuntimeModelProvider
{
    private readonly IDbContextFactory<EventDataContext> _dbFactory;
    private readonly IConfiguration _configuration;
    private readonly object _lock = new();
    private long _versionCounter;
    private string? _lastKnownModelVersion;
    private EventReaderRuntimeModel _current;
    private readonly Dictionary<long, EventReaderRuntimeModel> _activeVersions = new();

    public EventReaderRuntimeModelProvider(
        IDbContextFactory<EventDataContext> dbFactory,
        IConfiguration configuration)
    {
        _dbFactory = dbFactory ?? throw new ArgumentNullException(nameof(dbFactory));
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));

        _current = EmptyModel(version: 0);
        _activeVersions[0] = _current;

        ChangeToken.OnChange(
            () => _configuration.GetReloadToken(),
            () =>
            {
                var settingsVersion = _configuration["EventReader:ModelVersion"];
                if (settingsVersion == _lastKnownModelVersion)
                {
                    return;
                }

                _ = Task.Run(async () =>
                {
                    try { await ReloadAsync(CancellationToken.None).ConfigureAwait(false); }
                    catch { /* best-effort; operators use ReloadAsync() for guaranteed completion */ }
                });
            });
    }

    public EventReaderRuntimeModel Current
    { get { lock (_lock) { return _current; } } }

    public long CurrentVersion
    { get { lock (_lock) { return _current.Version; } } }

    public IReadOnlyList<long> ActiveVersions
    { get { lock (_lock) { return _activeVersions.Keys.Order().ToArray(); } } }

    public async Task ReloadAsync(CancellationToken ct)
    {
        var newModel = await LoadModelFromDatabaseAsync(ct).ConfigureAwait(false);

        if (!ValidateModel(newModel))
        {
            return;
        }

        var settingsVersion = _configuration["EventReader:ModelVersion"];

        lock (_lock)
        {
            _versionCounter++;
            var versioned = new EventReaderRuntimeModel(
                version: _versionCounter,
                createdUtc: DateTimeOffset.UtcNow,
                sourceSystems: newModel.SourceSystems,
                functionDiscriminatorPaths: newModel.FunctionDiscriminatorPaths,
                clientIdentityPaths: newModel.ClientIdentityPaths,
                functionMatchers: newModel.FunctionMatchers,
                functions: newModel.Functions);

            _activeVersions[_versionCounter] = versioned;
            _current = versioned;
            _lastKnownModelVersion = settingsVersion;
        }
    }

    public Task<RuntimeModelGcResult> CollectGarbageAsync(
        IReadOnlySet<long> activeWorkStoreVersions, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();

        lock (_lock)
        {
            var currentVersion = _current.Version;
            var toRemove = _activeVersions.Keys
                .Where(v => v != currentVersion && !activeWorkStoreVersions.Contains(v))
                .ToList();

            foreach (var v in toRemove) { _activeVersions.Remove(v); }

            return Task.FromResult(new RuntimeModelGcResult(
                VersionsCollected: toRemove.Count,
                VersionsRetained: _activeVersions.Count));
        }
    }

    // -------------------------------------------------------------------------
    // Database loading
    // -------------------------------------------------------------------------

    private async Task<EventReaderRuntimeModel> LoadModelFromDatabaseAsync(CancellationToken ct)
    {
        await using var db = await _dbFactory.CreateDbContextAsync(ct).ConfigureAwait(false);

        var configs = await db.Configs.AsNoTracking().ToListAsync(ct).ConfigureAwait(false);
        var configMap = configs.ToDictionary(c => c.ConfigKey, c => c.ConfigValue, StringComparer.OrdinalIgnoreCase);

        var prefixLength = configMap.TryGetValue("PrefixLength", out var pl)
                           && int.TryParse(pl, out var plv) && plv > 0 ? plv : 10;

        var discriminatorPaths = ParseJsonPaths(configMap, "FunctionDiscriminatorPaths");
        var clientIdentityPaths = ParseJsonPaths(configMap, "ClientIdentityPaths");

        var sourceSystems = await db.SourceSystems.AsNoTracking()
            .Where(ss => ss.IsEnabled)
            .Include(ss => ss.Tags)
            .ToListAsync(ct).ConfigureAwait(false);

        var sourceSystemMap = sourceSystems.ToDictionary(
            ss => ss.SourceSystemId,
            ss => ToSourceSystemDefinition(ss),
            StringComparer.OrdinalIgnoreCase);

        var functions = await db.Functions.AsNoTracking()
            .Where(f => f.IsEnabled)
            .Include(f => f.Rules)
            .Include(f => f.SourceOverrides).ThenInclude(o => o.Rules)
            .ToListAsync(ct).ConfigureAwait(false);

        var functionMap = functions.ToDictionary(f => f.FunctionId, f => ToCompiledFunctionPlan(f));

        var matchers = await db.FunctionMatchers.AsNoTracking()
            .Where(m => functionMap.ContainsKey(m.FunctionId))
            .ToListAsync(ct).ConfigureAwait(false);

        var matcherDefinitions = matchers.Select(ToFunctionMatcherDefinition).ToList();

        var matcherIndex = matcherDefinitions.Count > 0
            ? new FunctionMatcherIndex(matcherDefinitions, prefixLength)
            : FunctionMatcherIndex.Empty;

        return new EventReaderRuntimeModel(
            version: 0,
            createdUtc: DateTimeOffset.UtcNow,
            sourceSystems: new ReadOnlyDictionary<string, SourceSystemDefinition>(sourceSystemMap),
            functionDiscriminatorPaths: discriminatorPaths,
            clientIdentityPaths: clientIdentityPaths,
            functionMatchers: matcherIndex,
            functions: new ReadOnlyDictionary<int, CompiledFunctionPlan>(functionMap));
    }

    // -------------------------------------------------------------------------
    // Mapping helpers
    // -------------------------------------------------------------------------

    private static IReadOnlyList<JsonPathSelector> ParseJsonPaths(
        Dictionary<string, string> configMap, string key)
    {
        if (!configMap.TryGetValue(key, out var raw) || string.IsNullOrWhiteSpace(raw))
            return [];

        return raw.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
            .Where(p => p.StartsWith('$'))
            .Select(p => JsonPathSelector.Create(p))
            .ToList();
    }

    private static SourceSystemDefinition ToSourceSystemDefinition(ErSourceSystem ss) =>
        new(ss.SourceSystemId, ss.KafkaTopic, ss.ProviderName, ss.IsEnabled,
            new ReadOnlyDictionary<string, string>(
                ss.Tags.ToDictionary(t => t.TagKey, t => t.TagValue, StringComparer.OrdinalIgnoreCase)));

    private static CompiledFunctionPlan ToCompiledFunctionPlan(ErFunction f)
    {
        var overrides = f.SourceOverrides.ToDictionary(
            o => o.SourceSystemId,
            o => new SourceSystemFunctionPlan(
                ExtractionScript: new CompiledJexScript(o.ScriptName, o.ScriptVersion),
                RuleSet: new CompiledRuleSet(o.RuleSetVersion,
                    o.Rules.Select(r => new CompiledRule(r.RuleId, r.RuleVersion, r.RuleName)).ToList()),
                OutputRoute: new OutputRoutePlan(o.OutputRouteVersion, o.OutputRouteName, o.OutputTopic),
                FailurePolicy: new FunctionFailurePolicy
                {
                    FailWhenNedbankIdMissing = o.FailWhenNedbankIdMissing,
                    MaxProcessingAttempts = o.MaxProcessingAttempts,
                    MaxOutputAttempts = o.MaxOutputAttempts,
                }),
            StringComparer.OrdinalIgnoreCase);

        var behavior = overrides.Count > 0
            ? new SourceSystemBehavior(new ReadOnlyDictionary<string, SourceSystemFunctionPlan>(overrides))
            : SourceSystemBehavior.Global;

        return new CompiledFunctionPlan(
            FunctionId: f.FunctionId,
            FunctionVersion: f.FunctionVersion,
            Name: f.Name,
            SourceSystemBehavior: behavior,
            ExtractionScript: new CompiledJexScript(f.ScriptName, f.ScriptVersion),
            RuleSet: new CompiledRuleSet(f.RuleSetVersion,
                f.Rules.Select(r => new CompiledRule(r.RuleId, r.RuleVersion, r.RuleName)).ToList()),
            OutputRoute: new OutputRoutePlan(f.OutputRouteVersion, f.OutputRouteName, f.OutputTopic),
            FailurePolicy: new FunctionFailurePolicy
            {
                FailWhenNedbankIdMissing = f.FailWhenNedbankIdMissing,
                MaxProcessingAttempts = f.MaxProcessingAttempts,
                MaxOutputAttempts = f.MaxOutputAttempts,
            });
    }

    private static FunctionMatcherDefinition ToFunctionMatcherDefinition(ErFunctionMatcher m) =>
        new(
            SourceSystemId: string.IsNullOrWhiteSpace(m.SourceSystemId) ? null : m.SourceSystemId,
            FunctionId: m.FunctionId,
            Priority: m.Priority,
            Prefix: m.Prefix,
            Regex: new Regex(m.RegexPattern, RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.IgnoreCase));

    // -------------------------------------------------------------------------
    // Validation
    // -------------------------------------------------------------------------

    private static bool ValidateModel(EventReaderRuntimeModel model)
    {
        if (model.SourceSystems.Count == 0 && model.Functions.Count == 0)
            return true;

        if (model.SourceSystems.Count == 0 && model.Functions.Count > 0)
            return false;

        foreach (var ss in model.SourceSystems.Values)
        {
            if (string.IsNullOrWhiteSpace(ss.SourceSystemId)
                || string.IsNullOrWhiteSpace(ss.KafkaTopic)
                || string.IsNullOrWhiteSpace(ss.ProviderName))
                return false;
        }

        foreach (var matcher in model.FunctionMatchers.Matchers)
        {
            if (!model.Functions.ContainsKey(matcher.FunctionId))
                return false;
        }

        return true;
    }

    // -------------------------------------------------------------------------

    private static EventReaderRuntimeModel EmptyModel(long version) =>
        new(version, DateTimeOffset.UtcNow,
            new ReadOnlyDictionary<string, SourceSystemDefinition>(new Dictionary<string, SourceSystemDefinition>()),
            [], [],
            FunctionMatcherIndex.Empty,
            new ReadOnlyDictionary<int, CompiledFunctionPlan>(new Dictionary<int, CompiledFunctionPlan>()));
}
