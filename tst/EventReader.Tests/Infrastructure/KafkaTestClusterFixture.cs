using System.Linq;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Testcontainers.Kafka;

namespace EventReader.Tests.Infrastructure;

public sealed class KafkaTestClusterFixture : IAsyncLifetime
{
    private readonly KafkaContainer _kafkaContainer;

    public KafkaTestClusterFixture()
    {
        _kafkaContainer = new KafkaBuilder()
            .WithImage("confluentinc/cp-kafka:7.6.1")
            .Build();
    }

    public string BootstrapServers => _kafkaContainer.GetBootstrapAddress();

    public async Task InitializeAsync()
    {
        await _kafkaContainer.StartAsync().ConfigureAwait(false);
        await WaitUntilReadyAsync().ConfigureAwait(false);
    }

    public Task DisposeAsync() => _kafkaContainer.DisposeAsync().AsTask();

    public async Task CreateTopicAsync(string topicName, int partitions = 1)
    {
        using var admin = BuildAdminClient();
        try
        {
            await admin.CreateTopicsAsync([
                new TopicSpecification
                {
                    Name = topicName,
                    NumPartitions = partitions,
                    ReplicationFactor = 1,
                }
            ]).ConfigureAwait(false);
        }
        catch (CreateTopicsException ex) when (ex.Results.Any(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
        {
        }
    }

    public async Task ProduceJsonAsync(string topicName, IEnumerable<string> payloads)
    {
        using var producer = new ProducerBuilder<byte[], byte[]>(new ProducerConfig
        {
            BootstrapServers = BootstrapServers,
            Acks = Acks.All,
        }).Build();

        foreach (var payload in payloads)
        {
            await producer.ProduceAsync(topicName, new Message<byte[], byte[]>
            {
                Value = System.Text.Encoding.UTF8.GetBytes(payload),
            }).ConfigureAwait(false);
        }

        producer.Flush(TimeSpan.FromSeconds(10));
    }

    public async Task ProduceJsonAsync(
        string topicName,
        int messageCount,
        Func<int, string> payloadFactory,
        int batchSize = 5_000)
    {
        ArgumentNullException.ThrowIfNull(payloadFactory);

        using var producer = new ProducerBuilder<byte[], byte[]>(new ProducerConfig
        {
            BootstrapServers = BootstrapServers,
            Acks = Acks.All,
        }).Build();

        var pending = new List<Task<DeliveryResult<byte[], byte[]>>>(batchSize);

        for (var index = 0; index < messageCount; index++)
        {
            pending.Add(producer.ProduceAsync(topicName, new Message<byte[], byte[]>
            {
                Value = System.Text.Encoding.UTF8.GetBytes(payloadFactory(index)),
            }));

            if (pending.Count < batchSize)
            {
                continue;
            }

            await Task.WhenAll(pending).ConfigureAwait(false);
            pending.Clear();
        }

        if (pending.Count > 0)
        {
            await Task.WhenAll(pending).ConfigureAwait(false);
        }

        producer.Flush(TimeSpan.FromSeconds(30));
    }

    private IAdminClient BuildAdminClient()
    {
        return new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = BootstrapServers,
        }).Build();
    }

    private async Task WaitUntilReadyAsync()
    {
        using var admin = BuildAdminClient();
        var deadline = DateTime.UtcNow.AddMinutes(1);

        while (true)
        {
            try
            {
                _ = admin.GetMetadata(TimeSpan.FromSeconds(2));
                return;
            }
            catch
            {
                if (DateTime.UtcNow > deadline)
                {
                    throw;
                }

                await Task.Delay(TimeSpan.FromSeconds(1)).ConfigureAwait(false);
            }
        }
    }
}

[CollectionDefinition(CollectionName)]
public sealed class KafkaClusterCollection : ICollectionFixture<KafkaTestClusterFixture>
{
    public const string CollectionName = "event-reader-kafka";
}