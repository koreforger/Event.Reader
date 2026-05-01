using EventReader.Monitoring;
using Event.Streaming.Processing.Monitoring;
using Event.Streaming.Processing.WorkStore;
using KF.Web.HealthChecks;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using KfHealthStatus = Event.Streaming.Processing.Monitoring.HealthStatus;

namespace EventReader.Configuration;

/// <summary>
/// Liveness probe — tagged "live". Returns Healthy if the process is running.
/// This is a lightweight check with no external I/O.
/// Mapped to GET /health/live by <c>MapKfHealthEndpoints()</c>.
/// </summary>
public sealed class EventReaderLivenessHealthCheck : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken ct = default)
    {
        return Task.FromResult(HealthCheckResult.Healthy("EventReader process is alive"));
    }
}

/// <summary>
/// Storage health check — tagged "ready". Checks FASTER disk free, log size, and store connectivity.
/// Mapped to GET /health/ready by <c>MapKfHealthEndpoints()</c>.
/// </summary>
public sealed class EventReaderStorageHealthCheck : IHealthCheck
{
    private readonly IEventReaderWorkStore _workStore;

    public EventReaderStorageHealthCheck(IEventReaderWorkStore workStore)
    {
        _workStore = workStore;
    }

    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken ct = default)
    {
        try
        {
            if (_workStore is FasterEventReaderWorkStore fasterStore)
            {
                var detailed = fasterStore.GetDetailedMetrics();
                if (detailed.DiskFreeBytes <= 0)
                {
                    return HealthCheckResult.Unhealthy(
                        "FASTER storage disk is full or unavailable",
                        data: new Dictionary<string, object>
                        {
                            ["DiskFreeBytes"] = detailed.DiskFreeBytes,
                            ["DiskTotalBytes"] = detailed.DiskTotalBytes,
                        });
                }

                var data = new Dictionary<string, object>
                {
                    ["DiskFreeBytes"] = detailed.DiskFreeBytes,
                    ["DiskTotalBytes"] = detailed.DiskTotalBytes,
                    ["DiskUsedPercent"] = detailed.DiskTotalBytes > 0
                        ? Math.Round(100.0 * (detailed.DiskTotalBytes - detailed.DiskFreeBytes) / detailed.DiskTotalBytes, 1)
                        : 0.0,
                    ["LastCheckpointTime"] = detailed.LastCheckpointTime?.ToString("O") ?? "never",
                    ["AverageEnqueueLatencyMs"] = Math.Round(detailed.AverageEnqueueLatencyMs, 2),
                };

                return HealthCheckResult.Healthy("FASTER storage is healthy", data: data);
            }

            // For non-FASTER stores, verify basic connectivity
            await _workStore.ReleaseExpiredLeasesAsync(ct).ConfigureAwait(false);

            return HealthCheckResult.Healthy("Storage check passed", data: new Dictionary<string, object>
            {
                ["StoreType"] = _workStore.GetType().Name,
            });
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy("Storage health check failed", ex);
        }
    }
}

/// <summary>
/// Kafka input health check — tagged "ready". Verifies the Kafka consumer is connected and active.
/// Mapped to GET /health/ready by <c>MapKfHealthEndpoints()</c>.
/// </summary>
public sealed class EventReaderKafkaInputHealthCheck : IHealthCheck
{
    private readonly EventReaderMonitoringSnapshot _snapshot;

    public EventReaderKafkaInputHealthCheck(EventReaderMonitoringSnapshot snapshot)
    {
        _snapshot = snapshot;
    }

    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken ct = default)
    {
        var kafkaMetrics = _snapshot.KafkaMetrics;
        var consumerState = kafkaMetrics.ConsumerState ?? "unknown";
        var data = new Dictionary<string, object>
        {
            ["ConsumerState"] = consumerState,
            ["AssignedPartitions"] = kafkaMetrics.AssignedPartitionCount,
            ["TotalLag"] = kafkaMetrics.TotalLag,
        };

        if (consumerState.Equals("active", StringComparison.OrdinalIgnoreCase))
        {
            return Task.FromResult(HealthCheckResult.Healthy(
                $"Kafka consumer is active with {kafkaMetrics.AssignedPartitionCount} partitions",
                data: data));
        }

        if (consumerState.Equals("unknown", StringComparison.OrdinalIgnoreCase))
        {
            return Task.FromResult(HealthCheckResult.Degraded(
                "Kafka consumer state is unknown (may still be starting up)",
                data: data));
        }

        return Task.FromResult(HealthCheckResult.Unhealthy(
            $"Kafka consumer is {consumerState}",
            data: data));
    }
}

/// <summary>
/// Kafka output health check — tagged "ready". Verifies the output publisher is healthy.
/// Mapped to GET /health/ready by <c>MapKfHealthEndpoints()</c>.
/// </summary>
public sealed class EventReaderKafkaOutputHealthCheck : IHealthCheck
{
    private readonly EventReaderMonitoringSnapshot _snapshot;

    public EventReaderKafkaOutputHealthCheck(EventReaderMonitoringSnapshot snapshot)
    {
        _snapshot = snapshot;
    }

    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken ct = default)
    {
        var pipeline = _snapshot.PipelineMetrics;
        var data = new Dictionary<string, object>
        {
            ["TotalProcessed"] = pipeline.TotalProcessed,
            ["TotalErrors"] = pipeline.TotalErrors,
            ["MessagesPerSecond"] = pipeline.MessagesPerSecond,
            ["AvgLatencyMs"] = Math.Round(pipeline.AvgLatencyMs, 2),
        };

        if (_snapshot.Health == Event.Streaming.Processing.Monitoring.HealthStatus.Unhealthy)
        {
            return Task.FromResult(HealthCheckResult.Unhealthy(
                "Output publisher is unhealthy — pipeline errors detected",
                data: data));
        }

        if (_snapshot.Health == Event.Streaming.Processing.Monitoring.HealthStatus.Degraded)
        {
            return Task.FromResult(HealthCheckResult.Degraded(
                "Output publisher is degraded — check pipeline error rates",
                data: data));
        }

        return Task.FromResult(HealthCheckResult.Healthy(
            "Output publisher is healthy",
            data: data));
    }
}

/// <summary>
/// Runtime model health check — tagged "ready". Verifies the current runtime model is loaded.
/// Mapped to GET /health/ready by <c>MapKfHealthEndpoints()</c>.
/// </summary>
public sealed class EventReaderRuntimeModelHealthCheck : IHealthCheck
{
    private readonly IEventReaderRuntimeOptionsProvider _optionsProvider;

    public EventReaderRuntimeModelHealthCheck(IEventReaderRuntimeOptionsProvider optionsProvider)
    {
        _optionsProvider = optionsProvider;
    }

    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken ct = default)
    {
        var options = _optionsProvider.Current;
        var data = new Dictionary<string, object>
        {
            ["Version"] = options.Version ?? "unknown",
            ["ApplicationRole"] = options.ApplicationRole ?? "reader",
            ["InstanceId"] = options.InstanceId ?? "unknown",
            ["DiagnosticStage"] = options.DiagnosticStage.ToString(),
        };

        if (string.IsNullOrEmpty(options.Version) || options.Version == "1.0.0")
        {
            return Task.FromResult(HealthCheckResult.Degraded(
                "Runtime model version is default — model may not be loaded",
                data: data));
        }

        return Task.FromResult(HealthCheckResult.Healthy(
            $"Runtime model loaded (version {options.Version})",
            data: data));
    }
}

/// <summary>
/// Health check registration extensions for EventReader.
/// Call <c>services.AddEventReaderHealthChecks()</c> in Program.cs after AddHealthChecks().
/// </summary>
public static class EventReaderHealthCheckExtensions
{
    /// <summary>
    /// Registers all EventReader health checks with the appropriate tags.
    /// </summary>
    public static IHealthChecksBuilder AddEventReaderHealthChecks(this IHealthChecksBuilder builder)
    {
        builder.AddCheck<EventReaderLivenessHealthCheck>(
            "eventreader-liveness",
            tags: [HealthTags.Live]);

        builder.AddCheck<EventReaderStorageHealthCheck>(
            "eventreader-storage",
            tags: [HealthTags.Ready]);

        builder.AddCheck<EventReaderKafkaInputHealthCheck>(
            "eventreader-kafka-input",
            tags: [HealthTags.Ready, HealthTags.Kafka]);

        builder.AddCheck<EventReaderKafkaOutputHealthCheck>(
            "eventreader-kafka-output",
            tags: [HealthTags.Ready, HealthTags.Kafka]);

        builder.AddCheck<EventReaderRuntimeModelHealthCheck>(
            "eventreader-runtime-model",
            tags: [HealthTags.Ready]);

        return builder;
    }
}
