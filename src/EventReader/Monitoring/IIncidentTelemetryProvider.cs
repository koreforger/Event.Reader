namespace EventReader.Monitoring;

public interface IIncidentTelemetryProvider
{
    Task<IReadOnlyList<ConsumerPartitionLag>> GetPartitionLagAsync(CancellationToken ct = default);
    Task<IReadOnlyList<RebalanceEvent>> GetRecentRebalancesAsync(CancellationToken ct = default);
    Task<IReadOnlyList<DlqTopicSnapshot>> GetDlqSnapshotsAsync(CancellationToken ct = default);
    Task<ConsumerHealthSnapshot?> GetConsumerHealthAsync(CancellationToken ct = default);
}
