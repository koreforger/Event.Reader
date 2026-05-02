using Confluent.Kafka;
using EventReader.Configuration;
using KoreForge.Kafka.Consumer.Abstractions;
using Event.Streaming.Processing.Monitoring;

namespace EventReader.Monitoring;

/// <summary>
/// Adapts EventReader runtime counters to the dashboard incident telemetry contract.
/// Rebalance events are fed by librdkafka partition-assignment callbacks through
/// IKafkaConsumerLifecycleObserver, not by polling the Kafka admin API.
/// </summary>
public sealed class EventReaderIncidentTelemetryProvider : IIncidentTelemetryProvider, IKafkaConsumerLifecycleObserver
{
    private const int MaxRebalanceEvents = 500;

    private readonly EventReaderMonitoringSnapshot _snapshot;
    private readonly EventReaderMetricsAccumulator _metrics;
    private readonly IEventReaderRuntimeOptionsProvider _optionsProvider;
    private readonly IConfiguration _configuration;
    private readonly object _gate = new();
    private readonly List<RebalanceEvent> _rebalanceEvents = [];
    private readonly Dictionary<int, HashSet<TopicPartition>> _workerAssignments = [];

    public EventReaderIncidentTelemetryProvider(
        EventReaderMonitoringSnapshot snapshot,
        EventReaderMetricsAccumulator metrics,
        IEventReaderRuntimeOptionsProvider optionsProvider,
        IConfiguration configuration)
    {
        _snapshot = snapshot;
        _metrics = metrics;
        _optionsProvider = optionsProvider;
        _configuration = configuration;
    }

    public Task<IReadOnlyList<ConsumerPartitionLag>> GetPartitionLagAsync(CancellationToken ct = default)
    {
        var kafka = _snapshot.KafkaMetrics;
        var topicName = GetInputTopicName();
        var groupId = GetConsumerGroupId();

        if (kafka.LagPerPartition.Count > 0)
        {
            var partitionCount = Math.Max(1, kafka.LagPerPartition.Count);
            var messagesPerPartition = Math.Max(0, _metrics.TotalMessagesReceived / partitionCount);
            IReadOnlyList<ConsumerPartitionLag> rows = kafka.LagPerPartition
                .OrderBy(pair => pair.Key)
                .Select(pair =>
                {
                    var lag = Math.Max(0, pair.Value);
                    var endOffset = Math.Max(messagesPerPartition, lag);
                    var committedOffset = Math.Max(0, endOffset - lag);

                    return new ConsumerPartitionLag
                    {
                        ConsumerGroupId = groupId,
                        TopicName = topicName,
                        Partition = pair.Key,
                        EndOffset = endOffset,
                        CommittedOffset = committedOffset,
                        Lag = lag,
                        IsHealthy = lag == 0,
                        EndOffsetTimestampUtc = _metrics.LastMessageReceivedAtUtc?.UtcDateTime,
                    };
                })
                .ToArray();

            return Task.FromResult(rows);
        }

        var received = _metrics.TotalMessagesReceived;
        var committed = Math.Min(_metrics.TotalProcessed, received);
        var syntheticLag = Math.Max(0, received - committed);

        IReadOnlyList<ConsumerPartitionLag> fallback =
        [
            new ConsumerPartitionLag
            {
                ConsumerGroupId = groupId,
                TopicName = topicName,
                Partition = 0,
                EndOffset = received,
                CommittedOffset = committed,
                Lag = syntheticLag,
                IsHealthy = syntheticLag == 0,
                EndOffsetTimestampUtc = _metrics.LastMessageReceivedAtUtc?.UtcDateTime,
            },
        ];

        return Task.FromResult(fallback);
    }

    public Task<IReadOnlyList<RebalanceEvent>> GetRecentRebalancesAsync(CancellationToken ct = default)
    {
        lock (_gate)
        {
            return Task.FromResult<IReadOnlyList<RebalanceEvent>>(
                _rebalanceEvents
                    .OrderByDescending(e => e.OccurredAtUtc)
                    .Take(100)
                    .ToArray());
        }
    }

    public Task<IReadOnlyList<DlqTopicSnapshot>> GetDlqSnapshotsAsync(CancellationToken ct = default)
    {
        var topicName = _optionsProvider.Current.DlqTopic;
        if (string.IsNullOrWhiteSpace(topicName))
        {
            topicName = "stream.dlq";
        }

        IReadOnlyList<DlqTopicSnapshot> snapshots =
        [
            new DlqTopicSnapshot
            {
                TopicName = topicName,
                BacklogCount = _metrics.DlqWritten,
                IngressRatePerMinute = 0,
                LastMessageAtUtc = _metrics.DlqWritten > 0 ? _metrics.LastMessageReceivedAtUtc?.UtcDateTime : null,
                SampleMessages = Array.Empty<string>(),
            },
        ];

        return Task.FromResult(snapshots);
    }

    public Task<ConsumerHealthSnapshot?> GetConsumerHealthAsync(CancellationToken ct = default)
    {
        var workerCount = GetConfiguredWorkerCount();
        var totalLag = Math.Max(0, _snapshot.KafkaMetrics.TotalLag);
        var backlogCapacity = Math.Max(1, GetConfiguredBatchCapacity());
        var state = GetWorkerState();
        var activeWorkers = state == "active" ? workerCount : 0;
        var failedWorkers = state == "failed" ? workerCount : 0;
        var pausedWorkers = state == "paused" ? workerCount : 0;

        var workers = Enumerable.Range(1, workerCount)
            .Select(workerId => new ConsumerWorkerHealth
            {
                WorkerId = workerId,
                State = state,
                BacklogSize = (int)Math.Min(int.MaxValue, totalLag),
                BacklogCapacity = backlogCapacity,
                BacklogUtilization = Math.Min(1, totalLag / (double)backlogCapacity),
                LastConsumedAtUtc = _metrics.LastProcessedAtUtc?.UtcDateTime,
            })
            .ToArray();

        var snapshot = new ConsumerHealthSnapshot
        {
            CapturedAtUtc = DateTime.UtcNow,
            TotalWorkers = workerCount,
            ActiveWorkers = activeWorkers,
            PausedWorkers = pausedWorkers,
            FailedWorkers = failedWorkers,
            Workers = workers,
        };

        return Task.FromResult<ConsumerHealthSnapshot?>(snapshot);
    }

    public void OnPartitionsAssigned(KafkaConsumerLifecycleEvent lifecycleEvent)
    {
        lock (_gate)
        {
            _workerAssignments[lifecycleEvent.WorkerId] = lifecycleEvent.Partitions.ToHashSet();
            AddRebalanceEvent(
                "assigned",
                lifecycleEvent.OccurredAtUtc.UtcDateTime,
                lifecycleEvent.WorkerId,
                lifecycleEvent.Partitions.Count,
                $"Worker {lifecycleEvent.WorkerId} assigned {BuildPartitionPreview(lifecycleEvent.Partitions)}.");
            UpdateKafkaMetricsSnapshot("active", lifecycleEvent.OccurredAtUtc);
        }
    }

    public void OnPartitionsRevoked(KafkaConsumerLifecycleEvent lifecycleEvent)
    {
        lock (_gate)
        {
            _workerAssignments.Remove(lifecycleEvent.WorkerId);
            AddRebalanceEvent(
                "revoked",
                lifecycleEvent.OccurredAtUtc.UtcDateTime,
                lifecycleEvent.WorkerId,
                lifecycleEvent.Partitions.Count,
                $"Worker {lifecycleEvent.WorkerId} revoked {BuildPartitionPreview(lifecycleEvent.Partitions)}.");
            UpdateKafkaMetricsSnapshot("rebalancing", lifecycleEvent.OccurredAtUtc);
        }
    }

    private string GetWorkerState()
    {
        if (_snapshot.Health is HealthStatus.Unhealthy)
        {
            return "failed";
        }

        return _snapshot.KafkaMetrics.ConsumerState.ToLowerInvariant() switch
        {
            "paused" => "paused",
            "stopped" => "stopped",
            "faulted" => "failed",
            _ => "active",
        };
    }

    private string GetInputTopicName() =>
        _configuration.GetSection("Kafka:Profiles:Default:Topics")
            .GetChildren()
            .Select(section => section.Value)
            .FirstOrDefault(value => !string.IsNullOrWhiteSpace(value))
        ?? "raw.events";

    private string GetConsumerGroupId() =>
        _configuration["Kafka:Profiles:Default:ConfluentOptions:group.id"]
        ?? "event-reader";

    private int GetConfiguredWorkerCount() =>
        TryReadPositiveInt("Kafka:Profiles:Default:ExtendedConsumer:ConsumerCount", 1);

    private int GetConfiguredBatchCapacity()
    {
        var workers = GetConfiguredWorkerCount();
        var maxBatchSize = TryReadPositiveInt("Kafka:Profiles:Default:ExtendedConsumer:MaxBatchSize", 100);
        var maxInFlight = TryReadPositiveInt("Kafka:Profiles:Default:ExtendedConsumer:MaxInFlightBatches", 1);
        return workers * maxBatchSize * maxInFlight;
    }

    private int TryReadPositiveInt(string key, int fallback) =>
        int.TryParse(_configuration[key], out var value) && value > 0 ? value : fallback;

    private void AddRebalanceEvent(
        string eventType,
        DateTime occurredAtUtc,
        int workerId,
        int partitionCount,
        string details)
    {
        _rebalanceEvents.Add(new RebalanceEvent
        {
            EventType = eventType,
            OccurredAtUtc = occurredAtUtc,
            WorkerId = workerId,
            PartitionCount = partitionCount,
            Details = details,
        });

        if (_rebalanceEvents.Count > MaxRebalanceEvents)
        {
            _rebalanceEvents.RemoveRange(0, _rebalanceEvents.Count - MaxRebalanceEvents);
        }
    }

    private void UpdateKafkaMetricsSnapshot(string state, DateTimeOffset eventTime)
    {
        var assignedPartitions = _workerAssignments.Values
            .SelectMany(partitions => partitions)
            .Distinct()
            .OrderBy(partition => partition.Topic)
            .ThenBy(partition => partition.Partition.Value)
            .ToArray();

        var totalLag = Math.Max(0, _metrics.TotalMessagesReceived - _metrics.TotalProcessed);
        var lagPerPartition = BuildLagPerPartition(assignedPartitions, totalLag);

        _snapshot.KafkaMetrics = new KafkaMetrics
        {
            ConsumerState = state,
            AssignedPartitionCount = assignedPartitions.Length,
            TotalLag = totalLag,
            LagPerPartition = lagPerPartition,
            LastRebalance = eventTime,
            RebalanceCount = _rebalanceEvents.Count,
        };
    }

    private static IReadOnlyDictionary<int, long> BuildLagPerPartition(
        IReadOnlyList<TopicPartition> partitions,
        long totalLag)
    {
        if (partitions.Count == 0)
        {
            return new Dictionary<int, long>();
        }

        var lagPerPartition = new Dictionary<int, long>();
        for (var i = 0; i < partitions.Count; i++)
        {
            lagPerPartition[partitions[i].Partition.Value] = i == 0 ? totalLag : 0;
        }

        return lagPerPartition;
    }

    private static string BuildPartitionPreview(IEnumerable<TopicPartition> partitions)
    {
        const int maxPartitionsInPreview = 6;
        var normalized = partitions
            .Select(partition => $"{partition.Topic}:{partition.Partition.Value}")
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(partition => partition, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        if (normalized.Length == 0)
        {
            return "no partitions";
        }

        if (normalized.Length <= maxPartitionsInPreview)
        {
            return string.Join(", ", normalized);
        }

        return $"{string.Join(", ", normalized.Take(maxPartitionsInPreview))} (+{normalized.Length - maxPartitionsInPreview} more)";
    }
}
