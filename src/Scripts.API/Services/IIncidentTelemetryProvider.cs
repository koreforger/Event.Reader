using EventReader.Scripts.API.Models;

namespace EventReader.Scripts.API.Services;

/// <summary>
/// Read-only incident telemetry surface consumed by dashboard endpoints.
/// Host applications can provide a custom implementation backed by Kafka runtime state.
/// </summary>
public interface IIncidentTelemetryProvider
{
    Task<IReadOnlyList<ConsumerPartitionLag>> GetPartitionLagAsync(CancellationToken ct = default);

    Task<IReadOnlyList<RebalanceEvent>> GetRecentRebalancesAsync(CancellationToken ct = default);

    Task<IReadOnlyList<DlqTopicSnapshot>> GetDlqSnapshotsAsync(CancellationToken ct = default);

    Task<ConsumerHealthSnapshot?> GetConsumerHealthAsync(CancellationToken ct = default);
}
