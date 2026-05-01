using EventReader.Scripts.API.Models;

namespace EventReader.Scripts.API.Services;

/// <summary>
/// Default incident telemetry provider used when the host does not register a runtime-backed implementation.
/// Returns empty snapshots so dashboard pages can load without hard failures.
/// </summary>
internal sealed class NoOpIncidentTelemetryProvider : IIncidentTelemetryProvider
{
    public Task<IReadOnlyList<ConsumerPartitionLag>> GetPartitionLagAsync(CancellationToken ct = default)
    {
        return Task.FromResult<IReadOnlyList<ConsumerPartitionLag>>(Array.Empty<ConsumerPartitionLag>());
    }

    public Task<IReadOnlyList<RebalanceEvent>> GetRecentRebalancesAsync(CancellationToken ct = default)
    {
        return Task.FromResult<IReadOnlyList<RebalanceEvent>>(Array.Empty<RebalanceEvent>());
    }

    public Task<IReadOnlyList<DlqTopicSnapshot>> GetDlqSnapshotsAsync(CancellationToken ct = default)
    {
        return Task.FromResult<IReadOnlyList<DlqTopicSnapshot>>(Array.Empty<DlqTopicSnapshot>());
    }

    public Task<ConsumerHealthSnapshot?> GetConsumerHealthAsync(CancellationToken ct = default)
    {
        return Task.FromResult<ConsumerHealthSnapshot?>(new ConsumerHealthSnapshot
        {
            CapturedAtUtc = DateTime.UtcNow,
            TotalWorkers = 0,
            ActiveWorkers = 0,
            PausedWorkers = 0,
            FailedWorkers = 0,
            Workers = Array.Empty<ConsumerWorkerHealth>(),
        });
    }
}
