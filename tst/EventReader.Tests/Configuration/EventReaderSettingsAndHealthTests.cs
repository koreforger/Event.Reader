using EventReader.Configuration;
using EventReader.Monitoring;
using Event.Streaming.Processing.Monitoring;
using Event.Streaming.Processing.Pipeline;
using Event.Streaming.Processing.WorkStore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using StreamingHealthStatus = Event.Streaming.Processing.Monitoring.HealthStatus;
using MsHealthStatus = Microsoft.Extensions.Diagnostics.HealthChecks.HealthStatus;

namespace EventReader.Tests.Configuration;

public sealed class EventReaderSettingsAndHealthTests
{
    // ---- ER-012.1: Settings default values are reasonable ----

    [Fact]
    public void DefaultSettings_HaveReasonableValues()
    {
        var settings = new EventReaderDurableSettings();

        Assert.Equal("data/eventreader/faster/work-log", settings.LogPath);
        Assert.Equal("data/eventreader/faster/checkpoints", settings.CheckpointPath);
        Assert.Equal(60_000, settings.CheckpointIntervalMs);
        Assert.Equal(1L << 20, settings.IndexSizeBuckets);
        Assert.Equal(25, settings.LogPageSizeBits);
        Assert.Equal(28, settings.LogMemorySizeBits);

        Assert.Equal(32, settings.WorkerCount);
        Assert.Equal(1024, settings.LogicalShardCount);
        Assert.Equal(TimeSpan.FromMinutes(5), settings.LeaseDuration);
        Assert.Equal(10, settings.BatchSize);
        Assert.Equal(TimeSpan.FromMilliseconds(100), settings.IdleDelay);

        Assert.Equal(100, settings.OutputBatchSize);
        Assert.Equal(TimeSpan.FromMinutes(5), settings.OutputLeaseDuration);
        Assert.Equal(TimeSpan.FromMilliseconds(100), settings.OutputIdleDelay);

        Assert.InRange(settings.PauseHighWatermark, 0.0, 1.0);
        Assert.InRange(settings.ResumeLowWatermark, 0.0, 1.0);
        Assert.Equal(100_000, settings.MaxClassifiedBacklog);
        Assert.Equal(10_000, settings.MaxShardBacklog);
        Assert.Equal(10_000, settings.MaxReadyToOutputBacklog);
        Assert.Equal(10_000.0, settings.MaxOutputPublishLatencyMs);
        Assert.Equal(5_000.0, settings.MaxEnqueueLatencyMs);
        Assert.Equal(5_000.0, settings.MaxStoreLatencyMs);
        Assert.Equal(1_000_000_000, settings.MinDiskFreeBytes);

        Assert.Equal(3, settings.PrefixLength);
        Assert.Equal("claims", settings.UsernameResolverType);
        Assert.Equal("sub", settings.UsernameClaim);

        Assert.Equal(3, settings.MaxRetryAttempts);
        Assert.Equal(TimeSpan.FromSeconds(1), settings.RetryInitialDelay);
        Assert.Equal(TimeSpan.FromMinutes(5), settings.RetryMaxDelay);

        Assert.Equal(TimeSpan.FromHours(1), settings.ReplayDefaultFrom);
        Assert.Equal(1000, settings.ReplayMaxBatchSize);

        Assert.Equal(TimeSpan.FromDays(7), settings.CompletedRecordRetention);
        Assert.Equal(TimeSpan.FromDays(30), settings.FailedRecordRetention);
        Assert.Equal(3, settings.CheckpointRetentionCount);
        Assert.Equal(TimeSpan.FromHours(1), settings.CleanupInterval);
    }

    // ---- ER-012.2: Hot reload detects changes ----

    [Fact]
    public void SettingsChangeMonitor_DetectsHotReloadChanges()
    {
        var configuration = new ConfigurationManager();
        configuration["EventReader:Durable:MaxClassifiedBacklog"] = "50000";
        configuration["EventReader:Durable:MaxShardBacklog"] = "5000";

        var services = new ServiceCollection();
        services.AddSingleton<IConfiguration>(configuration);
        services
            .AddOptions<EventReaderDurableSettings>()
            .BindConfiguration("EventReader:Durable");

        using var provider = services.BuildServiceProvider();
        var monitor = provider.GetRequiredService<IOptionsMonitor<EventReaderDurableSettings>>();

        var initial = monitor.CurrentValue;
        Assert.Equal(50_000, initial.MaxClassifiedBacklog);
        Assert.Equal(5_000, initial.MaxShardBacklog);

        configuration["EventReader:Durable:MaxClassifiedBacklog"] = "75000";
        ((IConfigurationRoot)configuration).Reload();

        var reloaded = monitor.CurrentValue;
        Assert.Equal(75_000, reloaded.MaxClassifiedBacklog);
        Assert.Equal(5_000, reloaded.MaxShardBacklog);
    }

    // ---- ER-012.2: Restart-required settings are flagged ----

    [Fact]
    public void RestartRequiredSettings_ContainsExpectedKeys()
    {
        var restartRequired = EventReaderDurableSettings.RestartRequiredSettings;

        Assert.Contains("LogPath", restartRequired);
        Assert.Contains("CheckpointPath", restartRequired);
        Assert.Contains("WorkerCount", restartRequired);
        Assert.Contains("LogicalShardCount", restartRequired);
        Assert.Contains("PrefixLength", restartRequired);
        Assert.Contains("UsernameResolverType", restartRequired);
        Assert.Contains("IndexSizeBuckets", restartRequired);
        Assert.Contains("LogPageSizeBits", restartRequired);
        Assert.Contains("LogMemorySizeBits", restartRequired);

        Assert.DoesNotContain("PauseHighWatermark", restartRequired);
        Assert.DoesNotContain("ResumeLowWatermark", restartRequired);
        Assert.DoesNotContain("BatchSize", restartRequired);
    }

    [Fact]
    public void SettingsChangeMonitor_LogsRestartWarning_WhenRestartRequiredSettingChanges()
    {
        var configuration = new ConfigurationManager();
        configuration["EventReader:Durable:LogPath"] = "data/original";
        configuration["EventReader:Durable:WorkerCount"] = "32";
        configuration["EventReader:Durable:BatchSize"] = "10";

        var services = new ServiceCollection();
        services.AddSingleton<IConfiguration>(configuration);
        services
            .AddOptions<EventReaderDurableSettings>()
            .BindConfiguration("EventReader:Durable");

        using var provider = services.BuildServiceProvider();
        var monitor = provider.GetRequiredService<IOptionsMonitor<EventReaderDurableSettings>>();

        Assert.Equal("data/original", monitor.CurrentValue.LogPath);
        Assert.Equal(32, monitor.CurrentValue.WorkerCount);

        configuration["EventReader:Durable:LogPath"] = "data/changed";
        configuration["EventReader:Durable:WorkerCount"] = "64";
        configuration["EventReader:Durable:BatchSize"] = "20";
        ((IConfigurationRoot)configuration).Reload();

        var reloaded = monitor.CurrentValue;
        Assert.Equal("data/changed", reloaded.LogPath);
        Assert.Equal(64, reloaded.WorkerCount);
        Assert.Equal(20, reloaded.BatchSize);
    }

    // ---- ER-015.1: Retention settings are bounded ----

    [Fact]
    public void Validator_RejectsNegativeOrZeroRetentionValues()
    {
        var validator = new EventReaderSettingsValidator();

        var settings = new EventReaderDurableSettings();
        var validResult = validator.Validate(null, settings);
        Assert.True(validResult.Succeeded);

        var zeroRetention = settings with { CompletedRecordRetention = TimeSpan.Zero };
        var zeroResult = validator.Validate(null, zeroRetention);
        Assert.True(zeroResult.Failed);
        Assert.Contains("CompletedRecordRetention", zeroResult.FailureMessage);

        var negativeRetention = settings with { FailedRecordRetention = TimeSpan.FromDays(-1) };
        var negResult = validator.Validate(null, negativeRetention);
        Assert.True(negResult.Failed);
        Assert.Contains("FailedRecordRetention", negResult.FailureMessage);

        var zeroCheckpoint = settings with { CheckpointRetentionCount = 0 };
        var cpResult = validator.Validate(null, zeroCheckpoint);
        Assert.True(cpResult.Failed);
        Assert.Contains("CheckpointRetentionCount", cpResult.FailureMessage);

        var zeroCleanup = settings with { CleanupInterval = TimeSpan.Zero };
        var cleanupResult = validator.Validate(null, zeroCleanup);
        Assert.True(cleanupResult.Failed);
        Assert.Contains("CleanupInterval", cleanupResult.FailureMessage);
    }

    [Fact]
    public void Validator_RejectsInvalidBackpressureWatermarks()
    {
        var validator = new EventReaderSettingsValidator();

        var invalidHigh = new EventReaderDurableSettings { PauseHighWatermark = 1.5 };
        var highResult = validator.Validate(null, invalidHigh);
        Assert.True(highResult.Failed);
        Assert.Contains("PauseHighWatermark", highResult.FailureMessage);

        var invertedWatermarks = new EventReaderDurableSettings
        {
            PauseHighWatermark = 0.4,
            ResumeLowWatermark = 0.6,
        };
        var invertedResult = validator.Validate(null, invertedWatermarks);
        Assert.True(invertedResult.Failed);
        Assert.Contains("PauseHighWatermark", invertedResult.FailureMessage);

        var negativeWorker = new EventReaderDurableSettings { WorkerCount = 0 };
        var workerResult = validator.Validate(null, negativeWorker);
        Assert.True(workerResult.Failed);
        Assert.Contains("WorkerCount", workerResult.FailureMessage);
    }

    [Fact]
    public void DiskSizing_ReturnsReasonableEstimates()
    {
        var settings = new EventReaderDurableSettings();

        var bytesPerDay = EventReaderDurableSettings.EstimatedDiskBytesPerDay(1000, 2048);

        Assert.True(bytesPerDay > 0);
        Assert.InRange(bytesPerDay, 100_000_000_000L, 500_000_000_000L);

        var totalBytes = settings.EstimatedTotalDiskBytes(1000, 2048);
        Assert.True(totalBytes > bytesPerDay);
        Assert.InRange(totalBytes, bytesPerDay, bytesPerDay * 10);
    }

    // ---- ER-015.2: Health probes ----

    [Fact]
    public async Task LivenessHealthCheck_AlwaysReturnsHealthy()
    {
        var check = new EventReaderLivenessHealthCheck();
        var context = new HealthCheckContext();

        var result = await check.CheckHealthAsync(context);

        Assert.Equal(MsHealthStatus.Healthy, result.Status);
    }

    [Fact]
    public async Task StorageHealthCheck_ReturnsHealthy_WithValidWorkStore()
    {
        var stubStore = new StubWorkStore();
        var check = new EventReaderStorageHealthCheck(stubStore);
        var context = new HealthCheckContext();

        var result = await check.CheckHealthAsync(context);

        Assert.Equal(MsHealthStatus.Healthy, result.Status);
        Assert.Contains("Storage check passed", result.Description);
    }

    [Fact]
    public async Task StorageHealthCheck_ReturnsUnhealthy_WhenWorkStoreThrows()
    {
        var throwingStore = new ThrowingWorkStore();
        var check = new EventReaderStorageHealthCheck(throwingStore);
        var context = new HealthCheckContext();

        var result = await check.CheckHealthAsync(context);

        Assert.Equal(MsHealthStatus.Unhealthy, result.Status);
        Assert.Contains("Storage health check failed", result.Description);
    }

    [Fact]
    public async Task KafkaInputHealthCheck_ReturnsHealthy_WhenConsumerActive()
    {
        var snapshot = new EventReaderMonitoringSnapshot
        {
            KafkaMetrics = new KafkaMetrics { ConsumerState = "active", AssignedPartitionCount = 4 },
        };
        var check = new EventReaderKafkaInputHealthCheck(snapshot);
        var context = new HealthCheckContext();

        var result = await check.CheckHealthAsync(context);

        Assert.Equal(MsHealthStatus.Healthy, result.Status);
        Assert.Equal("active", result.Data["ConsumerState"]);
        Assert.Equal(4, result.Data["AssignedPartitions"]);
    }

    [Fact]
    public async Task KafkaInputHealthCheck_ReturnsDegraded_WhenConsumerStateUnknown()
    {
        var snapshot = new EventReaderMonitoringSnapshot
        {
            KafkaMetrics = new KafkaMetrics { ConsumerState = "unknown" },
        };
        var check = new EventReaderKafkaInputHealthCheck(snapshot);
        var context = new HealthCheckContext();

        var result = await check.CheckHealthAsync(context);

        Assert.Equal(MsHealthStatus.Degraded, result.Status);
    }

    [Fact]
    public async Task KafkaInputHealthCheck_ReturnsUnhealthy_WhenConsumerError()
    {
        var snapshot = new EventReaderMonitoringSnapshot
        {
            KafkaMetrics = new KafkaMetrics { ConsumerState = "error" },
        };
        var check = new EventReaderKafkaInputHealthCheck(snapshot);
        var context = new HealthCheckContext();

        var result = await check.CheckHealthAsync(context);

        Assert.Equal(MsHealthStatus.Unhealthy, result.Status);
    }

    [Fact]
    public async Task KafkaOutputHealthCheck_ReturnsHealthy_WhenPipelineHealthy()
    {
        var snapshot = new EventReaderMonitoringSnapshot
        {
            Health = StreamingHealthStatus.Healthy,
            PipelineMetrics = new PipelineMetrics
            {
                TotalProcessed = 10_000,
                TotalErrors = 0,
                MessagesPerSecond = 50,
                AvgLatencyMs = 2.5,
            },
        };
        var check = new EventReaderKafkaOutputHealthCheck(snapshot);
        var context = new HealthCheckContext();

        var result = await check.CheckHealthAsync(context);

        Assert.Equal(MsHealthStatus.Healthy, result.Status);
        Assert.Equal(10_000L, result.Data["TotalProcessed"]);
    }

    [Fact]
    public async Task KafkaOutputHealthCheck_ReturnsUnhealthy_WhenPipelineUnhealthy()
    {
        var snapshot = new EventReaderMonitoringSnapshot
        {
            Health = StreamingHealthStatus.Unhealthy,
        };
        var check = new EventReaderKafkaOutputHealthCheck(snapshot);
        var context = new HealthCheckContext();

        var result = await check.CheckHealthAsync(context);

        Assert.Equal(MsHealthStatus.Unhealthy, result.Status);
    }

    [Fact]
    public async Task RuntimeModelHealthCheck_ReturnsDegraded_WithDefaultVersion()
    {
        var options = new EventReaderRuntimeOptions
        {
            Version = "1.0.0",
            ApplicationRole = "reader",
        };
        var provider = new StaticOptionsProvider(options);
        var check = new EventReaderRuntimeModelHealthCheck(provider);
        var context = new HealthCheckContext();

        var result = await check.CheckHealthAsync(context);

        Assert.Equal(MsHealthStatus.Degraded, result.Status);
        Assert.Contains("model may not be loaded", result.Description);
    }

    [Fact]
    public async Task RuntimeModelHealthCheck_ReturnsHealthy_WithCustomVersion()
    {
        var options = new EventReaderRuntimeOptions
        {
            Version = "42.0.0",
            ApplicationRole = "reader",
            InstanceId = "reader-a",
            DiagnosticStage = DiagnosticStage.FullPipeline,
        };
        var provider = new StaticOptionsProvider(options);
        var check = new EventReaderRuntimeModelHealthCheck(provider);
        var context = new HealthCheckContext();

        var result = await check.CheckHealthAsync(context);

        Assert.Equal(MsHealthStatus.Healthy, result.Status);
        Assert.Equal("42.0.0", result.Data["Version"]);
        Assert.Contains("Runtime model loaded", result.Description);
    }

    [Fact]
    public async Task EventReaderSettingsChangeMonitor_InvokesOnChangeCallback()
    {
        var configuration = new ConfigurationManager();
        configuration["EventReader:Durable:BatchSize"] = "10";
        configuration["EventReader:Durable:PauseHighWatermark"] = "0.8";

        var services = new ServiceCollection();
        services.AddSingleton<IConfiguration>(configuration);
        services
            .AddOptions<EventReaderDurableSettings>()
            .BindConfiguration("EventReader:Durable");
        services.AddLogging();

        using var provider = services.BuildServiceProvider();
        var monitor = provider.GetRequiredService<IOptionsMonitor<EventReaderDurableSettings>>();

        using var changeMonitor = new EventReaderSettingsChangeMonitor(
            monitor,
            NullLogger<EventReaderSettingsChangeMonitor>.Instance);

        Assert.Equal(10, changeMonitor.Current.BatchSize);
        Assert.Equal(0.8, changeMonitor.Current.PauseHighWatermark);

        configuration["EventReader:Durable:BatchSize"] = "20";
        configuration["EventReader:Durable:PauseHighWatermark"] = "0.9";
        configuration["EventReader:Durable:WorkerCount"] = "64";
        ((IConfigurationRoot)configuration).Reload();

        Assert.Equal(20, changeMonitor.Current.BatchSize);
        Assert.Equal(0.9, changeMonitor.Current.PauseHighWatermark);
        Assert.Equal(64, changeMonitor.Current.WorkerCount);
    }

    // ---- Stubs for testability ----

    private sealed class StubWorkStore : IEventReaderWorkStore
    {
        public Task EnqueueClassifiedAsync(ClassifiedWorkItem item, CancellationToken ct) =>
            Task.CompletedTask;

        public Task<IReadOnlyList<WorkLease>> LeaseShardBatchAsync(
            int shardId, int maxItems, TimeSpan leaseDuration, CancellationToken ct) =>
            Task.FromResult<IReadOnlyList<WorkLease>>([]);

        public Task MarkReadyToOutputAsync(long workItemId, byte[] outputPayload, CancellationToken ct) =>
            Task.CompletedTask;

        public Task<IReadOnlyList<OutputLease>> LeaseOutputBatchAsync(
            int maxItems, TimeSpan leaseDuration, CancellationToken ct) =>
            Task.FromResult<IReadOnlyList<OutputLease>>([]);

        public Task MarkCompletedAsync(long workItemId, OutputWriteReceipt receipt, CancellationToken ct) =>
            Task.CompletedTask;

        public Task MarkRetryPendingAsync(long workItemId, RetryReason reason, CancellationToken ct) =>
            Task.CompletedTask;

        public Task MarkFailedAsync(long workItemId, string reason, CancellationToken ct) =>
            Task.CompletedTask;

        public Task ReleaseExpiredLeasesAsync(CancellationToken ct) =>
            Task.CompletedTask;

        public Task<ReplayPlan> CreateReplayPlanAsync(ReplayRequest request, CancellationToken ct) =>
            Task.FromResult(new ReplayPlan(Guid.NewGuid(), request, [], DateTimeOffset.UtcNow));

        public Task<UnstuckResult> RequeueAsync(UnstuckRequest request, CancellationToken ct) =>
            Task.FromResult(new UnstuckResult(0, 0, []));

        public WorkStoreMetricsSnapshot GetWorkStoreMetrics() =>
            new(new Dictionary<WorkState, long>(), new Dictionary<int, long>(),
                new HashSet<long>(), null, 0, 0, 0, 0, 0, null);

        public IReadOnlySet<long> GetActiveRuntimeModelVersions() => new HashSet<long>();
    }

    private sealed class ThrowingWorkStore : IEventReaderWorkStore
    {
        private static readonly Exception _fault = new InvalidOperationException("FASTER store is down");

        public Task EnqueueClassifiedAsync(ClassifiedWorkItem item, CancellationToken ct) =>
            throw _fault;

        public Task<IReadOnlyList<WorkLease>> LeaseShardBatchAsync(
            int shardId, int maxItems, TimeSpan leaseDuration, CancellationToken ct) =>
            throw _fault;

        public Task MarkReadyToOutputAsync(long workItemId, byte[] outputPayload, CancellationToken ct) =>
            throw _fault;

        public Task<IReadOnlyList<OutputLease>> LeaseOutputBatchAsync(
            int maxItems, TimeSpan leaseDuration, CancellationToken ct) =>
            throw _fault;

        public Task MarkCompletedAsync(long workItemId, OutputWriteReceipt receipt, CancellationToken ct) =>
            throw _fault;

        public Task MarkRetryPendingAsync(long workItemId, RetryReason reason, CancellationToken ct) =>
            throw _fault;

        public Task MarkFailedAsync(long workItemId, string reason, CancellationToken ct) =>
            throw _fault;

        public Task ReleaseExpiredLeasesAsync(CancellationToken ct) =>
            throw _fault;

        public Task<ReplayPlan> CreateReplayPlanAsync(ReplayRequest request, CancellationToken ct) =>
            throw _fault;

        public Task<UnstuckResult> RequeueAsync(UnstuckRequest request, CancellationToken ct) =>
            throw _fault;

        public WorkStoreMetricsSnapshot GetWorkStoreMetrics() => throw _fault;

        public IReadOnlySet<long> GetActiveRuntimeModelVersions() => throw _fault;
    }

    private sealed class StaticOptionsProvider(EventReaderRuntimeOptions options) : IEventReaderRuntimeOptionsProvider
    {
        public EventReaderRuntimeOptions Current { get; } = options;
    }
}
