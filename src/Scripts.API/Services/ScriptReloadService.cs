using System.Collections.Frozen;
using KoreForge.Scripts.Interfaces;
using KoreForge.Jex;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace EventReader.Scripts.API.Services;

/// <summary>
/// Background service that subscribes to <see cref="IScriptChangeNotification"/> and
/// hot-swaps compiled JEX programs when scripts change.
/// </summary>
internal sealed class ScriptReloadService : IHostedService
{
    private readonly IScriptChangeNotification _changeNotification;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly IJexCompiler _jex;
    private readonly FunctionScriptRepository _assignmentRepo;
    private readonly ILogger<ScriptReloadService> _logger;

    private volatile FrozenDictionary<long, CompiledFunction> _programs = FrozenDictionary<long, CompiledFunction>.Empty;

    public ScriptReloadService(
        IScriptChangeNotification changeNotification,
        IServiceScopeFactory scopeFactory,
        IJexCompiler jex,
        FunctionScriptRepository assignmentRepo,
        ILogger<ScriptReloadService> logger)
    {
        _changeNotification = changeNotification;
        _scopeFactory = scopeFactory;
        _jex = jex;
        _assignmentRepo = assignmentRepo;
        _logger = logger;
    }

    /// <summary>Gets the current compiled programs by FunctionId.</summary>
    public FrozenDictionary<long, CompiledFunction> Programs => _programs;

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _changeNotification.ScriptsChanged += OnScriptsChanged;
        _logger.LogInformation("ScriptReloadService started — listening for script changes");
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _changeNotification.ScriptsChanged -= OnScriptsChanged;
        _logger.LogInformation("ScriptReloadService stopped");
        return Task.CompletedTask;
    }

    /// <summary>
    /// Performs the initial load of all scripts for all functions.
    /// Called during application startup.
    /// </summary>
    public async Task InitialLoadAsync(CancellationToken ct = default)
    {
        using var scope = _scopeFactory.CreateScope();
        var scriptStore = scope.ServiceProvider.GetRequiredService<IScriptStore>();
        var allScripts = await scriptStore.ListAsync(isEnabled: true, ct: ct);
        var compiled = new Dictionary<long, CompiledFunction>();

        foreach (var script in allScripts)
        {
            if (!string.Equals(script.Language, "jex", StringComparison.OrdinalIgnoreCase))
                continue;

            try
            {
                var program = _jex.Compile(script.Content);
                compiled[script.ScriptId] = new CompiledFunction(script.ScriptId, script.Content, program);
                _logger.LogDebug("Compiled script {ScriptId} '{Name}'", script.ScriptId, script.Name);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to compile script {ScriptId} '{Name}' during initial load",
                    script.ScriptId, script.Name);
            }
        }

        _programs = compiled.ToFrozenDictionary();
        _logger.LogInformation("Initial load complete: {Count} JEX scripts compiled", compiled.Count);
    }

    /// <summary>
    /// Retrieves the compiled program for a given script ID, or null if not loaded.
    /// </summary>
    public CompiledFunction? GetCompiledScript(long scriptId)
    {
        return _programs.TryGetValue(scriptId, out var compiled) ? compiled : null;
    }

    private async void OnScriptsChanged(object? sender, ScriptChangeEventArgs e)
    {
        try
        {
            using var scope = _scopeFactory.CreateScope();
            var scriptStore = scope.ServiceProvider.GetRequiredService<IScriptStore>();
            var dict = _programs.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
            var recompiled = 0;

            // Handle changed and added scripts
            var toReload = e.ChangedScriptIds.Concat(e.AddedScriptIds).Distinct().ToList();
            foreach (var scriptId in toReload)
            {
                var script = await scriptStore.GetByIdAsync(scriptId);
                if (script is null || !string.Equals(script.Language, "jex", StringComparison.OrdinalIgnoreCase))
                    continue;

                try
                {
                    var program = _jex.Compile(script.Content);
                    dict[scriptId] = new CompiledFunction(scriptId, script.Content, program);
                    recompiled++;
                    _logger.LogInformation("Recompiled script {ScriptId} '{Name}'", scriptId, script.Name);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to recompile script {ScriptId} '{Name}' — keeping previous version",
                        scriptId, script.Name);
                }
            }

            // Handle deleted scripts
            foreach (var scriptId in e.DeletedScriptIds)
            {
                if (dict.Remove(scriptId))
                {
                    _logger.LogInformation("Removed compiled script {ScriptId}", scriptId);
                }
            }

            _programs = dict.ToFrozenDictionary();
            _logger.LogInformation("Script reload complete: {Recompiled} recompiled, {Deleted} removed",
                recompiled, e.DeletedScriptIds.Count);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing script change event");
        }
    }
}

/// <summary>
/// A compiled JEX program with its source content preserved.
/// </summary>
public sealed record CompiledFunction(long ScriptId, string Script, IJexProgram Program);
