using Confluent.Kafka;
using EventReader.Configuration;
using EventReader.Monitoring;
using KoreForge.Kafka.Consumer.Abstractions;
using Microsoft.Extensions.Configuration;

namespace EventReader.Tests;

public sealed class EventReaderIncidentTelemetryProviderTests
{
    [Fact]
    public async Task AssignedAndRevokedCallbacks_CreateRebalanceEvents()
    {
        var provider = CreateProvider();
        var assigned = new[]
        {
            new TopicPartition("raw.events", new Partition(0)),
            new TopicPartition("raw.events", new Partition(1)),
        };

        provider.OnPartitionsAssigned(new KafkaConsumerLifecycleEvent(1, assigned, DateTimeOffset.UtcNow));
        provider.OnPartitionsRevoked(new KafkaConsumerLifecycleEvent(1, assigned, DateTimeOffset.UtcNow.AddSeconds(1)));

        var rebalances = await provider.GetRecentRebalancesAsync();

        Assert.Equal(2, rebalances.Count);
        Assert.Equal("revoked", rebalances[0].EventType);
        Assert.Equal("assigned", rebalances[1].EventType);
        Assert.Equal(1, rebalances[0].WorkerId);
        Assert.Equal(2, rebalances[0].PartitionCount);
    }

    [Fact]
    public void AssignedCallback_UpdatesKafkaMetricsSnapshot()
    {
        var snapshot = new EventReaderMonitoringSnapshot();
        var metrics = new EventReaderMetricsAccumulator();
        metrics.RecordBatch(10, DateTimeOffset.UtcNow);
        metrics.RecordProcessed(TimeSpan.FromMilliseconds(1), DateTimeOffset.UtcNow);
        var provider = CreateProvider(snapshot, metrics);

        provider.OnPartitionsAssigned(new KafkaConsumerLifecycleEvent(
            1,
            [new TopicPartition("raw.events", new Partition(0))],
            DateTimeOffset.UtcNow));

        Assert.Equal("active", snapshot.KafkaMetrics.ConsumerState);
        Assert.Equal(1, snapshot.KafkaMetrics.AssignedPartitionCount);
        Assert.Equal(9, snapshot.KafkaMetrics.TotalLag);
        Assert.Equal(1, snapshot.KafkaMetrics.RebalanceCount);
    }

    private static EventReaderIncidentTelemetryProvider CreateProvider(
        EventReaderMonitoringSnapshot? snapshot = null,
        EventReaderMetricsAccumulator? metrics = null)
    {
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Kafka:Profiles:Default:ConfluentOptions:group.id"] = "event-reader-tests",
                ["Kafka:Profiles:Default:Topics:0"] = "raw.events",
                ["Kafka:Profiles:Default:ExtendedConsumer:ConsumerCount"] = "1",
                ["Kafka:Profiles:Default:ExtendedConsumer:MaxBatchSize"] = "100",
            })
            .Build();

        return new EventReaderIncidentTelemetryProvider(
            snapshot ?? new EventReaderMonitoringSnapshot(),
            metrics ?? new EventReaderMetricsAccumulator(),
            new StaticOptionsProvider(),
            config);
    }

    private sealed class StaticOptionsProvider : IEventReaderRuntimeOptionsProvider
    {
        public EventReaderRuntimeOptions Current { get; } = new()
        {
            DlqTopic = "stream.dlq",
            InstanceId = "reader-test",
            Version = "1.0.0",
        };
    }
}
