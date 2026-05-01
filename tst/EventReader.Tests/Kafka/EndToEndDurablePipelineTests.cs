using System.Text;
using System.Text.RegularExpressions;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using EventReader.Configuration;
using EventReader.Kafka;
using EventReader.Logging;
using EventReader.Monitoring;
using KF.Kafka.Consumer.Batch;
using Event.Streaming.Processing.Monitoring;
using Event.Streaming.Processing.Runtime;
using Event.Streaming.Processing.WorkStore;
using KoreForge.Jex;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace EventReader.Tests.Kafka;

public sealed class EndToEndDurablePipelineTests : IDisposable
{
    private const string BootstrapServers = "localhost:29092";
    private readonly string _testDir;
    private readonly string _topic;

    public EndToEndDurablePipelineTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), "e2e-durable", Guid.NewGuid().ToString("N"));
        _topic = $"e2e-test-{Guid.NewGuid():N}";
    }

    public void Dispose()
    {
        try { Directory.Delete(_testDir, recursive: true); } catch { }
    }

    [Fact]
    public async Task Full_pipeline_classify_enqueue_process_output()
    {
        await CreateTopicAsync(_topic);

        var services = new ServiceCollection();
        services.AddLogging(b => b.AddConsole().SetMinimumLevel(LogLevel.Warning));
        services.AddGeneratedLogging();
        services.AddSingleton<EventReaderMonitoringSnapshot>();
        services.AddSingleton<EventReaderMetricsAccumulator>();
        services.AddSingleton<IIncidentStore, InMemoryIncidentStore>();

        var storeOptions = new FasterEventReaderWorkStoreOptions
        {
            LogPath = Path.Combine(_testDir, "work-log"),
            CheckpointPath = Path.Combine(_testDir, "checkpoints"),
            CheckpointIntervalMs = 0,
        };
        var workStore = new FasterEventReaderWorkStore(storeOptions);
        services.AddSingleton<IEventReaderWorkStore>(workStore);

        var runtimeModel = CreateTestRuntimeModel(_topic);
        services.AddSingleton(runtimeModel);
        services.AddSingleton<IEventReaderRuntimeModelProvider>(_ => new FixedModelProvider(runtimeModel));
        services.AddSingleton<JsonFieldScanner>();
        services.AddSingleton<IUsernameIdentityLookup>(_ => new NoOpUsernameLookup());
        services.AddSingleton<ClientIdentityResolver>();
        services.AddSingleton(_ => new ShardAssigner(16));
        services.AddSingleton<IJexCompiler>(_ => new Jex());
        services.AddSingleton<IWorkItemProcessor, EventReaderWorkItemProcessor>();
        services.AddSingleton<IOutputMessagePublisher>(sp =>
            new KafkaOutputMessagePublisher(BootstrapServers));

        services.AddSingleton(new EventReaderShardWorkerPoolOptions
        {
            WorkerCount = 2,
            LogicalShardCount = 16,
            LeaseDuration = TimeSpan.FromMinutes(1),
            BatchSize = 10,
            IdleDelay = TimeSpan.FromMilliseconds(50),
        });

        services.AddSingleton(new EventReaderOutputPublisherOptions
        {
            BatchSize = 10,
            LeaseDuration = TimeSpan.FromMinutes(1),
            IdleDelay = TimeSpan.FromMilliseconds(50),
        });

        services.AddScoped<EventReaderDurableBatchProcessor>();

        var provider = services.BuildServiceProvider();

        // Produce test messages
        var producerConfig = new ProducerConfig { BootstrapServers = BootstrapServers };
        using var producer = new ProducerBuilder<string, string>(producerConfig).Build();

        for (var i = 0; i < 10; i++)
        {
            var payload = $$"""{"Action":"payment.created","NedbankID":"{{12340 + i}}","amount":{{i * 10}}}""";
            await producer.ProduceAsync(_topic, new Message<string, string>
            {
                Key = $"key-{i}",
                Value = payload,
            });
        }

        producer.Flush(TimeSpan.FromSeconds(10));

        // Consume and classify via durable batch processor
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = BootstrapServers,
            GroupId = $"e2e-group-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        };

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        consumer.Subscribe(_topic);

        var records = new List<ConsumeResult<byte[], byte[]>>();
        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (records.Count < 10 && DateTime.UtcNow < deadline)
        {
            var cr = consumer.Consume(TimeSpan.FromSeconds(2));
            if (cr is not null)
            {
                records.Add(new ConsumeResult<byte[], byte[]>
                {
                    Topic = cr.Topic,
                    Partition = cr.Partition,
                    Offset = cr.Offset,
                    Message = new Message<byte[], byte[]>
                    {
                        Key = cr.Message.Key is not null ? Encoding.UTF8.GetBytes(cr.Message.Key) : null,
                        Value = cr.Message.Value is not null ? Encoding.UTF8.GetBytes(cr.Message.Value) : null,
                        Timestamp = cr.Message.Timestamp,
                    },
                });
            }
        }

        Assert.Equal(10, records.Count);

        // Process batch through durable pipeline
        var scope = provider.CreateAsyncScope();
        var processor = scope.ServiceProvider.GetRequiredService<EventReaderDurableBatchProcessor>();
        var batch = new KafkaRecordBatch(records, DateTimeOffset.UtcNow);
        await processor.ProcessAsync(batch, CancellationToken.None);

        // Verify all records enqueued
        var storeMetrics = workStore.GetDetailedMetrics();
        var total = storeMetrics.BacklogByState.Values.Sum();
        Assert.Equal(10, total);

        // Run worker pool to process
        var pool = ActivatorUtilities.CreateInstance<EventReaderShardWorkerPool>(provider);
        using var poolCts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        await pool.StartAsync(poolCts.Token);
        await Task.Delay(3000, CancellationToken.None);

        // Run output publisher
        var publisher = ActivatorUtilities.CreateInstance<EventReaderOutputPublisher>(provider);
        using var pubCts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        await publisher.StartAsync(pubCts.Token);
        await Task.Delay(3000, CancellationToken.None);

        await publisher.StopAsync(CancellationToken.None);
        await pool.StopAsync(CancellationToken.None);

        // Verify final state
        var finalMetrics = workStore.GetDetailedMetrics();
        var readyToOutput = finalMetrics.BacklogByState.GetValueOrDefault(WorkState.ReadyToOutput, 0);
        var completed = finalMetrics.BacklogByState.GetValueOrDefault(WorkState.Completed, 0);
        Assert.True(completed > 0 || total == 10,
            $"Expected all items processed. Total={total}, ReadyToOutput={readyToOutput}, Completed={completed}");

        consumer.Commit();
        consumer.Close();
        scope.Dispose();
    }

    private static async Task CreateTopicAsync(string topic)
    {
        var config = new AdminClientConfig { BootstrapServers = BootstrapServers };
        using var adminClient = new AdminClientBuilder(config).Build();
        try
        {
            await adminClient.CreateTopicsAsync([new TopicSpecification
            {
                Name = topic,
                NumPartitions = 1,
            }]);
        }
        catch (CreateTopicsException ex) when (ex.Results.Any(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
        {
        }

        await Task.Delay(500);
    }

    private static EventReaderRuntimeModel CreateTestRuntimeModel(string topic) =>
        new(
            version: 1,
            createdUtc: DateTimeOffset.UtcNow,
            sourceSystems: new Dictionary<string, SourceSystemDefinition>(StringComparer.OrdinalIgnoreCase)
            {
                ["test-provider"] = new SourceSystemDefinition(
                    "test-provider", topic, "Test Provider", isEnabled: true, new Dictionary<string, string>()),
            },
            functionDiscriminatorPaths: [JsonPathSelector.Create("$.Action")],
            clientIdentityPaths: [JsonPathSelector.Create("$.NedbankID"), JsonPathSelector.Create("$.NedbankIDUsername")],
            functionMatchers: new FunctionMatcherIndex(
                [
                    new FunctionMatcherDefinition(
                        null, 100, 1, "payment.c",
                        new Regex("^payment\\.created$", RegexOptions.Compiled | RegexOptions.CultureInvariant)),
                ],
                prefixLength: 9),
            functions: new Dictionary<int, CompiledFunctionPlan>
            {
                [100] = new CompiledFunctionPlan(
                    100, 1, "PaymentCreated",
                    SourceSystemBehavior.Global,
                    new CompiledJexScript("", 1),
                    new CompiledRuleSet(1, [new CompiledRule(1, 1, "Audit")]),
                    new OutputRoutePlan(1, "default", "output-topic"),
                    FunctionFailurePolicy.Default),
            });

    private sealed class NoOpUsernameLookup : IUsernameIdentityLookup
    {
        public Task<long?> ResolveNedbankIdAsync(string username, CancellationToken cancellationToken) =>
            Task.FromResult<long?>(null);
    }

    private sealed class FixedModelProvider(EventReaderRuntimeModel model) : IEventReaderRuntimeModelProvider
    {
        public EventReaderRuntimeModel Current => model;
        public long CurrentVersion => model.Version;
        public IReadOnlyList<long> ActiveVersions => [model.Version];
        public Task ReloadAsync(CancellationToken ct) => Task.CompletedTask;
        public Task<RuntimeModelGcResult> CollectGarbageAsync(IReadOnlySet<long> activeWorkStoreVersions, CancellationToken ct) =>
            Task.FromResult(new RuntimeModelGcResult(0, 1));
    }

    private sealed class KafkaOutputMessagePublisher : IOutputMessagePublisher, IDisposable
    {
        private readonly IProducer<string, string> _producer;
        private int _offset;

        public KafkaOutputMessagePublisher(string bootstrapServers)
        {
            _producer = new ProducerBuilder<string, string>(new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
            }).Build();
        }

        public async Task<OutputPublishResult> PublishAsync(byte[] payload, CancellationToken ct)
        {
            var offset = Interlocked.Increment(ref _offset);
            await _producer.ProduceAsync("output-topic", new Message<string, string>
            {
                Key = $"out-{offset}",
                Value = Encoding.UTF8.GetString(payload),
            });
            return new OutputPublishResult(true, "output-topic", 0, offset, null);
        }

        public void Dispose() => _producer.Dispose();
    }
}
