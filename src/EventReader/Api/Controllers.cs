using EventReader.Configuration;
using EventReader.Monitoring;
using Event.Streaming.Processing.Runtime;
using Event.Streaming.Processing.Monitoring;
using Microsoft.Data.SqlClient;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;

namespace EventReader.Api;

[ApiController]
[Route("api/monitoring")]
public sealed class MonitoringController : ControllerBase
{
    private readonly EventReaderMonitoringSnapshot _snapshot;
    private readonly IIncidentStore _incidents;
    private readonly IEventReaderRuntimeOptionsProvider _optionsProvider;

    public MonitoringController(
        EventReaderMonitoringSnapshot snapshot,
        IIncidentStore incidents,
        IEventReaderRuntimeOptionsProvider optionsProvider)
    {
        _snapshot = snapshot;
        _incidents = incidents;
        _optionsProvider = optionsProvider;
    }

    [HttpGet("snapshot")]
    public IActionResult GetSnapshot() => Ok(_snapshot.ApplyRuntimeOptions(_optionsProvider.Current, DateTimeOffset.UtcNow));

    [HttpGet("incidents")]
    public IActionResult GetIncidents([FromQuery] int count = 50) =>
        Ok(_incidents.GetRecent(count));

    [HttpGet("incidents/unresolved")]
    public IActionResult GetUnresolved() =>
        Ok(_incidents.GetUnresolved());
}

[ApiController]
[Route("api/runtime")]
public sealed class RuntimeController : ControllerBase
{
    private static readonly DateTimeOffset StartedAt = DateTimeOffset.UtcNow;
    private readonly EventReaderMonitoringSnapshot _snapshot;
    private readonly IEventReaderRuntimeOptionsProvider _optionsProvider;

    public RuntimeController(
        EventReaderMonitoringSnapshot snapshot,
        IEventReaderRuntimeOptionsProvider optionsProvider)
    {
        _snapshot = snapshot;
        _optionsProvider = optionsProvider;
    }

    [HttpGet("state")]
    public IActionResult GetState()
    {
        var options = _optionsProvider.Current;
        _snapshot.ApplyRuntimeOptions(options, DateTimeOffset.UtcNow);

        return Ok(new
        {
        Application = "EventReader",
        ApplicationRole = options.ApplicationRole,
        InstanceId = options.InstanceId,
        Environment = System.Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT") ?? "Production",
        MachineName = System.Environment.MachineName,
        ProcessId = System.Environment.ProcessId,
        Version = options.Version,
        StartedAt,
        Health = _snapshot.Health.ToString(),
        DiagnosticStage = options.DiagnosticStage.ToString(),
        _snapshot.KafkaMetrics,
        _snapshot.PipelineMetrics,
        _snapshot.SettingsSyncMetrics,
        _snapshot.AppSpecificMetrics,
        });
    }
}

[ApiController]
[Route("api/health")]
public sealed class HealthDetailsController : ControllerBase
{
    private static readonly DateTimeOffset StartedAt = DateTimeOffset.UtcNow;
    private readonly EventReaderMonitoringSnapshot _snapshot;
    private readonly IEventReaderRuntimeOptionsProvider _optionsProvider;

    public HealthDetailsController(
        EventReaderMonitoringSnapshot snapshot,
        IEventReaderRuntimeOptionsProvider optionsProvider)
    {
        _snapshot = snapshot;
        _optionsProvider = optionsProvider;
    }

    [HttpGet("details")]
    public IActionResult GetDetails()
    {
        var options = _optionsProvider.Current;
        _snapshot.ApplyRuntimeOptions(options, DateTimeOffset.UtcNow);
        var healthy = _snapshot.Health is HealthStatus.Healthy or HealthStatus.Unknown;
        return StatusCode(healthy ? StatusCodes.Status200OK : StatusCodes.Status503ServiceUnavailable, new
        {
            Application = "EventReader",
            ApplicationRole = options.ApplicationRole,
            InstanceId = options.InstanceId,
            Environment = _snapshot.Environment,
            MachineName = System.Environment.MachineName,
            ProcessId = System.Environment.ProcessId,
            Version = options.Version,
            BuildTimestamp = StartedAt,
            StartedTimestamp = StartedAt,
            IsHealthy = healthy,
            HealthStatus = _snapshot.Health.ToString(),
        });
    }
}

[ApiController]
[Route("api/discovery")]
public sealed class DiscoveryController : ControllerBase
{
    private static readonly DateTimeOffset StartedAt = DateTimeOffset.UtcNow;
    private readonly EventReaderMonitoringSnapshot _snapshot;
    private readonly IEventReaderRuntimeOptionsProvider _optionsProvider;

    public DiscoveryController(
        EventReaderMonitoringSnapshot snapshot,
        IEventReaderRuntimeOptionsProvider optionsProvider)
    {
        _snapshot = snapshot;
        _optionsProvider = optionsProvider;
    }

    [HttpGet("instances")]
    public IActionResult GetInstances()
    {
        var options = _optionsProvider.Current;
        _snapshot.ApplyRuntimeOptions(options, DateTimeOffset.UtcNow);

        return Ok(new
        {
        App = "EventReader",
        Role = options.ApplicationRole,
        Instances = new[]
        {
            new
            {
                InstanceId = options.InstanceId,
                BaseUrl = $"{Request.Scheme}://{Request.Host}",
                Environment = _snapshot.Environment,
                Version = options.Version,
                StartedTimestamp = StartedAt,
                IsHealthy = _snapshot.Health is HealthStatus.Healthy or HealthStatus.Unknown,
                LastHeartbeat = _snapshot.Timestamp,
            },
        },
        TotalCount = 1,
        });
    }
}

[ApiController]
[Route("api/settings")]
public sealed class SettingsController : ControllerBase
{
    private readonly IConfiguration _config;
    private readonly IEventReaderRuntimeOptionsProvider _optionsProvider;

    public SettingsController(IConfiguration config, IEventReaderRuntimeOptionsProvider optionsProvider)
    {
        _config = config;
        _optionsProvider = optionsProvider;
    }

    [HttpGet]
    public async Task<IActionResult> GetSettings(CancellationToken cancellationToken)
    {
        var applicationId = "EventReader";
        var instanceId = _optionsProvider.Current.InstanceId;
        var rows = await LoadDatabaseSettingsAsync(applicationId, instanceId, cancellationToken).ConfigureAwait(false);
        var effective = rows
            .GroupBy(row => row.Key, StringComparer.OrdinalIgnoreCase)
            .Select(group => group.OrderByDescending(row => ScopePriority(row, applicationId, instanceId)).First())
            .ToDictionary(row => row.Key, row => row, StringComparer.OrdinalIgnoreCase);

        var result = effective
            .Select(row => ToSettingEntry(row.Value, rows, applicationId, instanceId))
            .ToList();

        foreach (var pair in _config.AsEnumerable())
        {
            if (pair.Value is null || effective.ContainsKey(pair.Key) || !IsUsefulConfigKey(pair.Key))
            {
                continue;
            }

            result.Add(new SettingEntryDto(
                pair.Key,
                MaskIfSensitive(pair.Key, pair.Value),
                SectionFromKey(pair.Key),
                "File or environment configuration value",
                null,
                "config",
                "config",
                "application",
                null,
                false,
                false,
                false,
                false,
                false));
        }

        return Ok(result.OrderBy(row => row.Section).ThenBy(row => row.Key));
    }

    [HttpGet("context")]
    public IActionResult GetContext() => Ok(new
    {
        ApplicationId = "EventReader",
        InstanceId = _optionsProvider.Current.InstanceId,
        ClientAppVersion = _optionsProvider.Current.Version,
        IsVersionScopedEnabled = false,
        SupportedVersionKeys = Array.Empty<string>(),
    });

    [HttpPut("{*key}")]
    public async Task<IActionResult> SaveSetting(
        string key,
        [FromBody] SaveSettingRequest request,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(key))
        {
            return BadRequest("Setting key is required.");
        }

        var applicationId = "EventReader";
        var instanceId = string.Equals(request.TargetScope, "instance", StringComparison.OrdinalIgnoreCase)
            ? _optionsProvider.Current.InstanceId
            : null;

        await UpsertSettingAsync(applicationId, instanceId, key, request.Value ?? string.Empty, cancellationToken)
            .ConfigureAwait(false);

        return NoContent();
    }

    [HttpDelete("{*key}")]
    public async Task<IActionResult> DeleteSetting(
        string key,
        [FromQuery] long? id,
        CancellationToken cancellationToken)
    {
        await DeleteSettingAsync(id, "EventReader", _optionsProvider.Current.InstanceId, key, cancellationToken)
            .ConfigureAwait(false);
        return NoContent();
    }

    private async Task<List<DatabaseSettingRow>> LoadDatabaseSettingsAsync(
        string applicationId,
        string instanceId,
        CancellationToken cancellationToken)
    {
        var connectionString = _config.GetConnectionString("KoreForgeSettings")
            ?? _config["KoreForge:Settings:ConnectionString"]
            ?? _config["KoreForgeSettings:ConnectionString"];

        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return [];
        }

        const string sql = """
            SELECT ID, ApplicationId, InstanceId, [Key], [Value], ModifiedDate, [Comment]
            FROM dbo.Settings
            WHERE (ApplicationId IS NULL OR ApplicationId = @applicationId)
              AND (InstanceId IS NULL OR InstanceId = @instanceId)
            """;

        var rows = new List<DatabaseSettingRow>();
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@applicationId", applicationId);
        command.Parameters.AddWithValue("@instanceId", instanceId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            rows.Add(new DatabaseSettingRow(
                reader.GetInt64(0),
                reader.IsDBNull(1) ? null : reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                reader.IsDBNull(5) ? null : reader.GetDateTime(5),
                reader.IsDBNull(6) ? null : reader.GetString(6)));
        }

        return rows;
    }

    private async Task UpsertSettingAsync(
        string applicationId,
        string? instanceId,
        string key,
        string value,
        CancellationToken cancellationToken)
    {
        var connectionString = _config.GetConnectionString("KoreForgeSettings")
            ?? _config["KoreForge:Settings:ConnectionString"]
            ?? _config["KoreForgeSettings:ConnectionString"]
            ?? throw new InvalidOperationException("KoreForgeSettings connection string is required.");

        const string sql = """
            UPDATE dbo.Settings
               SET [Value] = @value,
                   ModifiedBy = 'EventAdminUI',
                   ModifiedDate = SYSUTCDATETIME(),
                   [Comment] = 'Updated from EventAdminUI'
             WHERE ApplicationId = @applicationId
               AND ((InstanceId IS NULL AND @instanceId IS NULL) OR InstanceId = @instanceId)
               AND [Key] = @key;

            IF @@ROWCOUNT = 0
            BEGIN
                INSERT dbo.Settings (ApplicationId, InstanceId, [Key], [Value], IsSecret, ValueEncrypted, CreatedBy, CreatedDate, ModifiedBy, ModifiedDate, [Comment])
                VALUES (@applicationId, @instanceId, @key, @value, 0, 0, 'EventAdminUI', SYSUTCDATETIME(), 'EventAdminUI', SYSUTCDATETIME(), 'Created from EventAdminUI');
            END
            """;

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@applicationId", applicationId);
        command.Parameters.AddWithValue("@instanceId", (object?)instanceId ?? DBNull.Value);
        command.Parameters.AddWithValue("@key", key);
        command.Parameters.AddWithValue("@value", value);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task DeleteSettingAsync(
        long? id,
        string applicationId,
        string instanceId,
        string key,
        CancellationToken cancellationToken)
    {
        var connectionString = _config.GetConnectionString("KoreForgeSettings")
            ?? _config["KoreForge:Settings:ConnectionString"]
            ?? _config["KoreForgeSettings:ConnectionString"]
            ?? throw new InvalidOperationException("KoreForgeSettings connection string is required.");

        var sql = id is not null
            ? "DELETE dbo.Settings WHERE ID = @id AND ApplicationId = @applicationId"
            : """
              DELETE dbo.Settings
               WHERE ApplicationId = @applicationId
                 AND (InstanceId IS NULL OR InstanceId = @instanceId)
                 AND [Key] = @key
              """;

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@id", (object?)id ?? DBNull.Value);
        command.Parameters.AddWithValue("@applicationId", applicationId);
        command.Parameters.AddWithValue("@instanceId", instanceId);
        command.Parameters.AddWithValue("@key", key);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static SettingEntryDto ToSettingEntry(
        DatabaseSettingRow row,
        IReadOnlyList<DatabaseSettingRow> allRows,
        string applicationId,
        string instanceId)
    {
        var scope = EffectiveScope(row, applicationId, instanceId);
        return new SettingEntryDto(
            row.Key,
            MaskIfSensitive(row.Key, row.Value ?? string.Empty),
            SectionFromKey(row.Key),
            row.Comment,
            row.ModifiedDate,
            "database",
            scope,
            "application",
            row.Id,
            row.ApplicationId is not null,
            allRows.Any(x => x.Key.Equals(row.Key, StringComparison.OrdinalIgnoreCase) && x.ApplicationId is null),
            allRows.Any(x => x.Key.Equals(row.Key, StringComparison.OrdinalIgnoreCase) && string.Equals(x.ApplicationId, applicationId, StringComparison.OrdinalIgnoreCase) && x.InstanceId is null),
            allRows.Any(x => x.Key.Equals(row.Key, StringComparison.OrdinalIgnoreCase) && string.Equals(x.ApplicationId, applicationId, StringComparison.OrdinalIgnoreCase) && string.Equals(x.InstanceId, instanceId, StringComparison.OrdinalIgnoreCase)),
            false);
    }

    private static int ScopePriority(DatabaseSettingRow row, string applicationId, string instanceId)
    {
        if (string.Equals(row.ApplicationId, applicationId, StringComparison.OrdinalIgnoreCase) &&
            string.Equals(row.InstanceId, instanceId, StringComparison.OrdinalIgnoreCase))
        {
            return 3;
        }

        if (string.Equals(row.ApplicationId, applicationId, StringComparison.OrdinalIgnoreCase) && row.InstanceId is null)
        {
            return 2;
        }

        return 1;
    }

    private static string EffectiveScope(DatabaseSettingRow row, string applicationId, string instanceId)
    {
        if (string.Equals(row.ApplicationId, applicationId, StringComparison.OrdinalIgnoreCase) &&
            string.Equals(row.InstanceId, instanceId, StringComparison.OrdinalIgnoreCase))
        {
            return "instance";
        }

        if (string.Equals(row.ApplicationId, applicationId, StringComparison.OrdinalIgnoreCase))
        {
            return "application";
        }

        return "global";
    }

    private static bool IsUsefulConfigKey(string key) =>
        key.StartsWith("Kafka:", StringComparison.OrdinalIgnoreCase) ||
        key.StartsWith("EventReader:", StringComparison.OrdinalIgnoreCase) ||
        key.StartsWith("KoreForgeSettings:", StringComparison.OrdinalIgnoreCase) ||
        key.StartsWith("KoreForge:Settings:", StringComparison.OrdinalIgnoreCase) ||
        key.StartsWith("ConnectionStrings:", StringComparison.OrdinalIgnoreCase);

    private static string SectionFromKey(string key)
    {
        var index = key.IndexOf(':', StringComparison.Ordinal);
        return index <= 0 ? key : key[..index];
    }

    private static string MaskIfSensitive(string key, string value)
    {
        if (key.Contains("Password", StringComparison.OrdinalIgnoreCase) ||
            key.Contains("Secret", StringComparison.OrdinalIgnoreCase) ||
            key.Contains("ConnectionString", StringComparison.OrdinalIgnoreCase))
        {
            return string.IsNullOrEmpty(value) ? value : "********";
        }

        return value;
    }

    private sealed record DatabaseSettingRow(
        long Id,
        string? ApplicationId,
        string? InstanceId,
        string Key,
        string? Value,
        DateTime? ModifiedDate,
        string? Comment);

    private sealed record SettingEntryDto(
        string Key,
        string Value,
        string Section,
        string? Description,
        DateTime? LastModified,
        string Source,
        string EffectiveScope,
        string PreferredWriteScope,
        long? EffectiveSettingId,
        bool CanDeleteEffective,
        bool HasGlobalValue,
        bool HasAppOverride,
        bool HasInstanceOverride,
        bool HasInstanceWildcardOverride);

    public sealed record SaveSettingRequest(
        string? Value,
        string? TargetScope,
        string? TargetVersionMode,
        string? ClientAppVersion);
}

[ApiController]
[Route("api/profiles")]
public sealed class ProfilesController : ControllerBase
{
    private readonly IProcessingProfileCatalog _profiles;

    public ProfilesController(IProcessingProfileCatalog profiles)
    {
        _profiles = profiles;
    }

    [HttpGet]
    public IActionResult GetProfiles() => Ok(_profiles.GetActiveProfiles());
}
