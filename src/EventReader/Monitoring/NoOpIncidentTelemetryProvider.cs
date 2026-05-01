namespace EventReader.Monitoring;

internal sealed class NoOpIncidentTelemetryProvider : IIncidentTelemetryProvider
{
    public Task<IReadOnlyList<ConsumerPartitionLag>> GetPartitionLagAsync(CancellationToken ct = default) =>
        Task.FromResult<IReadOnlyList<ConsumerPartitionLag>>(Array.Empty<ConsumerPartitionLag>());

    public Task<IReadOnlyList<RebalanceEvent>> GetRecentRebalancesAsync(CancellationToken ct = default) =>
        Task.FromResult<IReadOnlyList<RebalanceEvent>>(Array.Empty<RebalanceEvent>());

    public Task<IReadOnlyList<DlqTopicSnapshot>> GetDlqSnapshotsAsync(CancellationToken ct = default) =>
        Task.FromResult<IReadOnlyList<DlqTopicSnapshot>>(Array.Empty<DlqTopicSnapshot>());

    public Task<ConsumerHealthSnapshot?> GetConsumerHealthAsync(CancellationToken ct = default) =>
        Task.FromResult<ConsumerHealthSnapshot?>(new ConsumerHealthSnapshot
        {
            CapturedAtUtc = DateTime.UtcNow,
            TotalWorkers = 0,
            ActiveWorkers = 0,
            PausedWorkers = 0,
            FailedWorkers = 0,
            Workers = Array.Empty<ConsumerWorkerHealth>(),
        });
}
