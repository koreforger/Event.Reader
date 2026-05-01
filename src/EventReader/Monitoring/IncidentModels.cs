namespace EventReader.Monitoring;

public sealed record ConsumerPartitionLag
{
    public required string ConsumerGroupId { get; init; }
    public required string TopicName { get; init; }
    public required int Partition { get; init; }
    public required long EndOffset { get; init; }
    public required long CommittedOffset { get; init; }
    public required long Lag { get; init; }
    public required bool IsHealthy { get; init; }
    public DateTime? EndOffsetTimestampUtc { get; init; }
}

public sealed record RebalanceEvent
{
    public required string EventType { get; init; }
    public required DateTime OccurredAtUtc { get; init; }
    public int? WorkerId { get; init; }
    public int PartitionCount { get; init; }
    public string? Details { get; init; }
}

public sealed record DlqTopicSnapshot
{
    public required string TopicName { get; init; }
    public required long BacklogCount { get; init; }
    public required double IngressRatePerMinute { get; init; }
    public DateTime? LastMessageAtUtc { get; init; }
    public IReadOnlyList<string> SampleMessages { get; init; } = Array.Empty<string>();
}

public sealed record ConsumerWorkerHealth
{
    public required int WorkerId { get; init; }
    public required string State { get; init; }
    public required int BacklogSize { get; init; }
    public required int BacklogCapacity { get; init; }
    public required double BacklogUtilization { get; init; }
    public DateTime? LastConsumedAtUtc { get; init; }
}

public sealed record ConsumerHealthSnapshot
{
    public required DateTime CapturedAtUtc { get; init; }
    public required int TotalWorkers { get; init; }
    public required int ActiveWorkers { get; init; }
    public required int PausedWorkers { get; init; }
    public required int FailedWorkers { get; init; }
    public IReadOnlyList<ConsumerWorkerHealth> Workers { get; init; } = Array.Empty<ConsumerWorkerHealth>();
}
