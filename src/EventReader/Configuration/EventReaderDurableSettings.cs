using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace EventReader.Configuration;

/// <summary>
/// Durable pipeline runtime settings bound from configuration section "EventReader:Durable".
/// Uses the Microsoft.Extensions.Options pattern with IOptionsMonitor for hot-reload awareness.
///
/// Settings that change at runtime (no restart):
///   - Backpressure thresholds (PauseHighWatermark, ResumeLowWatermark, all Max* backlog/latency limits, MinDiskFreeBytes)
///   - LeaseDuration, IdleDelay, OutputLeaseDuration, OutputIdleDelay (timeouts picked up on next loop iteration)
///   - BatchSize, OutputBatchSize (picked up on next batch)
///   - Retention durations, CleanupInterval, MaxRetryAttempts, Retry delays
///   - Replay settings
///   - CheckpointIntervalMs
///
/// Settings that require application restart:
///   - LogPath, CheckpointPath (FASTER store is initialized once with these paths)
///   - WorkerCount (worker pool cannot dynamically resize)
///   - PrefixLength (changes key-generation behavior)
///   - UsernameResolverType (auth infrastructure)
///   - IndexSizeBuckets, LogPageSizeBits, LogMemorySizeBits (FASTER initialization)
///   - LogicalShardCount (shard topology is fixed at startup)
/// </summary>
public sealed record EventReaderDurableSettings
{
    // ---- FASTER storage (ER-015.1 - Disk and Retention Policy) ----

    public string LogPath { get; init; } = "data/eventreader/faster/work-log";
    public string CheckpointPath { get; init; } = "data/eventreader/faster/checkpoints";
    public int CheckpointIntervalMs { get; init; } = 60_000;
    public long IndexSizeBuckets { get; init; } = 1L << 20;
    public int LogPageSizeBits { get; init; } = 25;
    public int LogMemorySizeBits { get; init; } = 28;

    // ---- Worker pool (ER-012.1) ----

    public int WorkerCount { get; init; } = 32;
    public int LogicalShardCount { get; init; } = 1024;
    public TimeSpan LeaseDuration { get; init; } = TimeSpan.FromMinutes(5);
    public int BatchSize { get; init; } = 10;
    public TimeSpan IdleDelay { get; init; } = TimeSpan.FromMilliseconds(100);

    // ---- Output publisher (ER-012.1) ----

    public int OutputBatchSize { get; init; } = 100;
    public TimeSpan OutputLeaseDuration { get; init; } = TimeSpan.FromMinutes(5);
    public TimeSpan OutputIdleDelay { get; init; } = TimeSpan.FromMilliseconds(100);

    // ---- Backpressure thresholds (ER-012.2 - hot-reloadable) ----

    public double PauseHighWatermark { get; init; } = 1.0;
    public double ResumeLowWatermark { get; init; } = 0.5;
    public int MaxClassifiedBacklog { get; init; } = 100_000;
    public int MaxShardBacklog { get; init; } = 10_000;
    public int MaxReadyToOutputBacklog { get; init; } = 10_000;
    public double MaxOutputPublishLatencyMs { get; init; } = 10_000;
    public double MaxEnqueueLatencyMs { get; init; } = 5_000;
    public double MaxStoreLatencyMs { get; init; } = 5_000;
    public long MinDiskFreeBytes { get; init; } = 1_000_000_000;

    // ---- Prefix length ----

    public int PrefixLength { get; init; } = 3;

    // ---- Username resolver settings ----

    public string UsernameResolverType { get; init; } = "claims";
    public string UsernameClaim { get; init; } = "sub";

    // ---- Retry settings ----

    public int MaxRetryAttempts { get; init; } = 3;
    public TimeSpan RetryInitialDelay { get; init; } = TimeSpan.FromSeconds(1);
    public TimeSpan RetryMaxDelay { get; init; } = TimeSpan.FromMinutes(5);

    // ---- Replay settings ----

    public TimeSpan ReplayDefaultFrom { get; init; } = TimeSpan.FromHours(1);
    public int ReplayMaxBatchSize { get; init; } = 1000;

    // ---- Retention policy (ER-015.1) ----

    public TimeSpan CompletedRecordRetention { get; init; } = TimeSpan.FromDays(7);
    public TimeSpan FailedRecordRetention { get; init; } = TimeSpan.FromDays(30);
    public int CheckpointRetentionCount { get; init; } = 3;
    public TimeSpan CleanupInterval { get; init; } = TimeSpan.FromHours(1);

    // ---- Disk sizing guidance (ER-015.1) ----

    /// <summary>
    /// Estimated disk usage per day in bytes.
    /// Formula: (messagesPerSecond * averagePayloadBytes * 86400 seconds/day) * 1.2 overhead factor.
    /// Multiply by CompletedRecordRetention days for sizing the FASTER log volume.
    /// </summary>
    public static long EstimatedDiskBytesPerDay(long messagesPerSecond, int averagePayloadBytes) =>
        (long)(messagesPerSecond * (long)averagePayloadBytes * 86_400 * 1.2);

    /// <summary>
    /// Estimated total disk needed based on throughput and retention settings.
    /// </summary>
    public long EstimatedTotalDiskBytes(long messagesPerSecond, int averagePayloadBytes) =>
        (long)(EstimatedDiskBytesPerDay(messagesPerSecond, averagePayloadBytes)
               * CompletedRecordRetention.TotalDays * 1.1); // +10% for checkpoints and failed records

    // ---- Restart-required settings annotation for hot-reload awareness ----

    public static readonly IReadOnlySet<string> RestartRequiredSettings = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        nameof(LogPath),
        nameof(CheckpointPath),
        nameof(WorkerCount),
        nameof(LogicalShardCount),
        nameof(PrefixLength),
        nameof(UsernameResolverType),
        nameof(IndexSizeBuckets),
        nameof(LogPageSizeBits),
        nameof(LogMemorySizeBits),
    };
}

/// <summary>
/// Validates <see cref="EventReaderDurableSettings"/> before they are applied.
/// Registered via <c>services.AddSingleton&lt;IValidateOptions&lt;EventReaderDurableSettings&gt;&gt;</c>.
/// </summary>
public sealed class EventReaderSettingsValidator : IValidateOptions<EventReaderDurableSettings>
{
    public ValidateOptionsResult Validate(string? name, EventReaderDurableSettings options)
    {
        var errors = new List<string>();

        if (options.WorkerCount <= 0)
            errors.Add("WorkerCount must be positive.");
        if (options.LogicalShardCount <= 0)
            errors.Add("LogicalShardCount must be positive.");
        if (options.BatchSize <= 0)
            errors.Add("BatchSize must be positive.");
        if (options.OutputBatchSize <= 0)
            errors.Add("OutputBatchSize must be positive.");
        if (options.PauseHighWatermark is < 0 or > 1)
            errors.Add("PauseHighWatermark must be between 0.0 and 1.0.");
        if (options.ResumeLowWatermark is < 0 or > 1)
            errors.Add("ResumeLowWatermark must be between 0.0 and 1.0.");
        if (options.PauseHighWatermark < options.ResumeLowWatermark)
            errors.Add("PauseHighWatermark must be >= ResumeLowWatermark.");
        if (options.CompletedRecordRetention <= TimeSpan.Zero)
            errors.Add("CompletedRecordRetention must be positive.");
        if (options.FailedRecordRetention <= TimeSpan.Zero)
            errors.Add("FailedRecordRetention must be positive.");
        if (options.CheckpointRetentionCount <= 0)
            errors.Add("CheckpointRetentionCount must be at least 1.");
        if (options.CleanupInterval <= TimeSpan.Zero)
            errors.Add("CleanupInterval must be positive.");
        if (options.MaxRetryAttempts < 0)
            errors.Add("MaxRetryAttempts must be non-negative.");
        if (options.MaxClassifiedBacklog < 0)
            errors.Add("MaxClassifiedBacklog must be non-negative.");
        if (options.MaxShardBacklog < 0)
            errors.Add("MaxShardBacklog must be non-negative.");
        if (options.MinDiskFreeBytes < 0)
            errors.Add("MinDiskFreeBytes must be non-negative.");

        return errors.Count > 0
            ? ValidateOptionsResult.Fail(errors)
            : ValidateOptionsResult.Success;
    }
}

/// <summary>
/// Listens for changes to <see cref="EventReaderDurableSettings"/> via <see cref="IOptionsMonitor{T}"/>
/// and logs which settings changed. Warns when restart-required settings are modified.
/// </summary>
public sealed class EventReaderSettingsChangeMonitor : IDisposable
{
    private readonly IOptionsMonitor<EventReaderDurableSettings> _monitor;
    private readonly ILogger<EventReaderSettingsChangeMonitor> _log;
    private readonly IDisposable _subscription = null!;
    private EventReaderDurableSettings _previous;

    public EventReaderSettingsChangeMonitor(
        IOptionsMonitor<EventReaderDurableSettings> monitor,
        ILogger<EventReaderSettingsChangeMonitor> logger)
    {
        _monitor = monitor;
        _log = logger;
        _previous = monitor.CurrentValue;
        _subscription = monitor.OnChange(OnSettingsChanged)!;
    }

    public EventReaderDurableSettings Current => _monitor.CurrentValue;

    private void OnSettingsChanged(EventReaderDurableSettings next, string? name)
    {
        var previous = _previous;
        _previous = next;

        var restartRequired = new List<string>();
        var changed = new List<string>();

        Compare(previous, next, restartRequired, changed);

        _log.LogInformation("EventReader durable settings reloaded ({Count} runtime, {RestartCount} restart-required changes)",
            changed.Count, restartRequired.Count);

        if (changed.Count > 0)
        {
            _log.LogInformation("Hot-reloaded settings: {Changes}", string.Join(", ", changed));
        }

        if (restartRequired.Count > 0)
        {
            _log.LogWarning(
                "Settings requiring application restart were modified: {Settings}. Current values are not applied until restart.",
                string.Join(", ", restartRequired));
        }
    }

    private static void Compare(
        EventReaderDurableSettings old,
        EventReaderDurableSettings next,
        List<string> restartRequired,
        List<string> changed)
    {
        CompareValue(nameof(EventReaderDurableSettings.LogPath), old.LogPath, next.LogPath, restartRequired, changed);
        CompareValue(nameof(EventReaderDurableSettings.CheckpointPath), old.CheckpointPath, next.CheckpointPath, restartRequired, changed);
        CompareValue(nameof(EventReaderDurableSettings.CheckpointIntervalMs), old.CheckpointIntervalMs, next.CheckpointIntervalMs, restartRequired, changed);
        CompareValue(nameof(EventReaderDurableSettings.IndexSizeBuckets), old.IndexSizeBuckets, next.IndexSizeBuckets, restartRequired, changed);
        CompareValue(nameof(EventReaderDurableSettings.LogPageSizeBits), old.LogPageSizeBits, next.LogPageSizeBits, restartRequired, changed);
        CompareValue(nameof(EventReaderDurableSettings.LogMemorySizeBits), old.LogMemorySizeBits, next.LogMemorySizeBits, restartRequired, changed);
        CompareValue(nameof(EventReaderDurableSettings.WorkerCount), old.WorkerCount, next.WorkerCount, restartRequired, changed);
        CompareValue(nameof(EventReaderDurableSettings.LogicalShardCount), old.LogicalShardCount, next.LogicalShardCount, restartRequired, changed);
        CompareValue(nameof(EventReaderDurableSettings.LeaseDuration), old.LeaseDuration, next.LeaseDuration, restartRequired, changed);
        CompareValue(nameof(EventReaderDurableSettings.BatchSize), old.BatchSize, next.BatchSize, restartRequired, changed);
        CompareValue(nameof(EventReaderDurableSettings.IdleDelay), old.IdleDelay, next.IdleDelay, restartRequired, changed);
        CompareValue(nameof(EventReaderDurableSettings.OutputBatchSize), old.OutputBatchSize, next.OutputBatchSize, restartRequired, changed);
        CompareValue(nameof(EventReaderDurableSettings.OutputLeaseDuration), old.OutputLeaseDuration, next.OutputLeaseDuration, restartRequired, changed);
        CompareValue(nameof(EventReaderDurableSettings.OutputIdleDelay), old.OutputIdleDelay, next.OutputIdleDelay, restartRequired, changed);
        CompareValue(nameof(EventReaderDurableSettings.PauseHighWatermark), old.PauseHighWatermark, next.PauseHighWatermark, restartRequired, changed);
        CompareValue(nameof(EventReaderDurableSettings.ResumeLowWatermark), old.ResumeLowWatermark, next.ResumeLowWatermark, restartRequired, changed);
        CompareValue(nameof(EventReaderDurableSettings.MaxClassifiedBacklog), old.MaxClassifiedBacklog, next.MaxClassifiedBacklog, restartRequired, changed);
        CompareValue(nameof(EventReaderDurableSettings.MaxShardBacklog), old.MaxShardBacklog, next.MaxShardBacklog, restartRequired, changed);
        CompareValue(nameof(EventReaderDurableSettings.MaxReadyToOutputBacklog), old.MaxReadyToOutputBacklog, next.MaxReadyToOutputBacklog, restartRequired, changed);
        CompareValue(nameof(EventReaderDurableSettings.MaxOutputPublishLatencyMs), old.MaxOutputPublishLatencyMs, next.MaxOutputPublishLatencyMs, restartRequired, changed);
        CompareValue(nameof(EventReaderDurableSettings.MaxEnqueueLatencyMs), old.MaxEnqueueLatencyMs, next.MaxEnqueueLatencyMs, restartRequired, changed);
        CompareValue(nameof(EventReaderDurableSettings.MaxStoreLatencyMs), old.MaxStoreLatencyMs, next.MaxStoreLatencyMs, restartRequired, changed);
        CompareValue(nameof(EventReaderDurableSettings.MinDiskFreeBytes), old.MinDiskFreeBytes, next.MinDiskFreeBytes, restartRequired, changed);
        CompareValue(nameof(EventReaderDurableSettings.PrefixLength), old.PrefixLength, next.PrefixLength, restartRequired, changed);
        CompareValue(nameof(EventReaderDurableSettings.UsernameResolverType), old.UsernameResolverType, next.UsernameResolverType, restartRequired, changed);
        CompareValue(nameof(EventReaderDurableSettings.MaxRetryAttempts), old.MaxRetryAttempts, next.MaxRetryAttempts, restartRequired, changed);
        CompareValue(nameof(EventReaderDurableSettings.RetryInitialDelay), old.RetryInitialDelay, next.RetryInitialDelay, restartRequired, changed);
        CompareValue(nameof(EventReaderDurableSettings.RetryMaxDelay), old.RetryMaxDelay, next.RetryMaxDelay, restartRequired, changed);
        CompareValue(nameof(EventReaderDurableSettings.ReplayDefaultFrom), old.ReplayDefaultFrom, next.ReplayDefaultFrom, restartRequired, changed);
        CompareValue(nameof(EventReaderDurableSettings.ReplayMaxBatchSize), old.ReplayMaxBatchSize, next.ReplayMaxBatchSize, restartRequired, changed);
        CompareValue(nameof(EventReaderDurableSettings.CompletedRecordRetention), old.CompletedRecordRetention, next.CompletedRecordRetention, restartRequired, changed);
        CompareValue(nameof(EventReaderDurableSettings.FailedRecordRetention), old.FailedRecordRetention, next.FailedRecordRetention, restartRequired, changed);
        CompareValue(nameof(EventReaderDurableSettings.CheckpointRetentionCount), old.CheckpointRetentionCount, next.CheckpointRetentionCount, restartRequired, changed);
        CompareValue(nameof(EventReaderDurableSettings.CleanupInterval), old.CleanupInterval, next.CleanupInterval, restartRequired, changed);
    }

    private static void CompareValue<T>(
        string propertyName,
        T oldValue,
        T newValue,
        List<string> restartRequired,
        List<string> changed) where T : notnull
    {
        if (!EqualityComparer<T>.Default.Equals(oldValue, newValue))
        {
            if (EventReaderDurableSettings.RestartRequiredSettings.Contains(propertyName))
            {
                restartRequired.Add(propertyName);
            }
            else
            {
                changed.Add(propertyName);
            }
        }
    }

    public void Dispose() => _subscription.Dispose();
}
