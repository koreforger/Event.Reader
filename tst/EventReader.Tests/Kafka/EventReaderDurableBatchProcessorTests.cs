using System.Text;
using System.Text.RegularExpressions;
using Confluent.Kafka;
using EventReader.Configuration;
using EventReader.Kafka;
using EventReader.Logging;
using EventReader.Monitoring;
using KF.Kafka.Consumer.Batch;
using Event.Streaming.Processing.Monitoring;
using Event.Streaming.Processing.Runtime;
using Event.Streaming.Processing.WorkStore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace EventReader.Tests.Kafka;

public sealed class EventReaderDurableBatchProcessorTests : IDisposable
{
    private readonly string _testDir;

    public EventReaderDurableBatchProcessorTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), "durable-proc-tests", Guid.NewGuid().ToString("N"));
    }

    public void Dispose()
    {
        try { Directory.Delete(_testDir, recursive: true); } catch { }
    }

    [Fact]
    public async Task Classifies_and_enqueues_valid_record()
    {
        var (processor, workStore) = CreateProcessor();

        var batch = CreateBatch("""{"Action":"payment.created","NedbankID":"12345"}""");
        await processor.ProcessAsync(batch, CancellationToken.None);

        var metrics = workStore.GetMetrics();
        Assert.True(metrics.LastWorkItemId >= 1);
    }

    [Fact]
    public async Task Enqueue_is_idempotent_across_duplicate_batch_delivery()
    {
        var (processor, workStore) = CreateProcessor();

        var batch = CreateBatch("""{"Action":"payment.created","NedbankID":"12345"}""");
        await processor.ProcessAsync(batch, CancellationToken.None);

        var firstId = workStore.GetMetrics().LastWorkItemId;

        await processor.ProcessAsync(batch, CancellationToken.None);

        Assert.Equal(firstId, workStore.GetMetrics().LastWorkItemId);
    }

    [Fact]
    public async Task Unclassified_record_does_not_enqueue()
    {
        var (processor, workStore) = CreateProcessor();

        var batch = CreateBatch("""{"Action":"unknown.event","NedbankID":"12345"}""");
        await processor.ProcessAsync(batch, CancellationToken.None);

        Assert.Equal(0, workStore.GetMetrics().LastWorkItemId);
    }

    [Fact]
    public async Task Action_is_used_as_discriminator_even_when_identity_fields_appear_first()
    {
        var (processor, workStore) = CreateProcessor();

        var batch = CreateBatch("""{"NedbankID":"12345","NedbankIDUsername":"alice","Action":"payment.created"}""");
        await processor.ProcessAsync(batch, CancellationToken.None);

        Assert.Equal(1, workStore.GetMetrics().LastWorkItemId);
    }

    [Fact]
    public async Task Empty_payload_does_not_enqueue()
    {
        var (processor, workStore) = CreateProcessor();

        var batch = new KafkaRecordBatch(
            [
                new ConsumeResult<byte[], byte[]>
                {
                    Topic = "test-topic",
                    Partition = new Partition(0),
                    Offset = new Offset(1),
                    Message = new Message<byte[], byte[]>
                    {
                        Value = [],
                        Timestamp = new Timestamp(DateTime.UtcNow),
                    },
                },
            ],
            DateTimeOffset.UtcNow);

        await processor.ProcessAsync(batch, CancellationToken.None);

        Assert.Equal(0, workStore.GetMetrics().LastWorkItemId);
    }

    [Fact]
    public async Task Missing_topic_mapping_does_not_enqueue()
    {
        var (processor, workStore) = CreateProcessor();

        var batch = new KafkaRecordBatch(
            [
                new ConsumeResult<byte[], byte[]>
                {
                    Topic = "unknown-topic",
                    Partition = new Partition(0),
                    Offset = new Offset(1),
                    Message = new Message<byte[], byte[]>
                    {
                        Value = Encoding.UTF8.GetBytes("""{"Action":"payment.created"}"""),
                        Timestamp = new Timestamp(DateTime.UtcNow),
                    },
                },
            ],
            DateTimeOffset.UtcNow);

        await processor.ProcessAsync(batch, CancellationToken.None);

        Assert.Equal(0, workStore.GetMetrics().LastWorkItemId);
    }

    [Fact]
    public async Task Shard_is_assigned_from_nedbank_id()
    {
        var (processor, workStore) = CreateProcessor();

        var batch = CreateBatch("""{"Action":"payment.created","NedbankID":"12345"}""");
        await processor.ProcessAsync(batch, CancellationToken.None);

        var shardId = new ShardAssigner(1024).Assign(12345).ShardId;

        var leases = await workStore.LeaseShardBatchAsync(
            shardId,
            10,
            TimeSpan.FromMinutes(5),
            CancellationToken.None);

        Assert.Single(leases);
        Assert.Equal(shardId, leases[0].ShardId);
    }

    [Fact]
    public async Task Multiple_records_in_batch_all_enqueue()
    {
        var (processor, workStore) = CreateProcessor();

        var batch = new KafkaRecordBatch(
            [
                CreateConsumeResult("""{"Action":"payment.created","NedbankID":"111"}""", offset: 1),
                CreateConsumeResult("""{"Action":"payment.created","NedbankID":"222"}""", offset: 2),
                CreateConsumeResult("""{"Action":"payment.created","NedbankID":"333"}""", offset: 3),
            ],
            DateTimeOffset.UtcNow);

        await processor.ProcessAsync(batch, CancellationToken.None);

        Assert.Equal(3, workStore.GetMetrics().LastWorkItemId);
    }

    [Fact]
    public async Task Invalid_json_is_handled_gracefully_without_enqueue()
    {
        var (processor, workStore) = CreateProcessor();

        var batch = new KafkaRecordBatch(
            [
                CreateConsumeResult("""{"Action":"payment.created","NedbankID":"111"}""", offset: 1),
                CreateConsumeResult("invalid json {{{", offset: 2),
            ],
            DateTimeOffset.UtcNow);

        await processor.ProcessAsync(batch, CancellationToken.None);

        Assert.Equal(1, workStore.GetMetrics().LastWorkItemId);
    }

    [Fact]
    public async Task Model_swap_is_picked_up_on_next_record_without_restart()
    {
        var (processor, workStore, provider) = CreateProcessorWithProvider();

        // First record: classified under initial model (function ID 100)
        await processor.ProcessAsync(CreateBatch("""{"Action":"payment.created","NedbankID":"111"}""", offset: 1), CancellationToken.None);
        Assert.Equal(1, workStore.GetMetrics().LastWorkItemId);

        // Swap to an empty model (no source systems)
        provider.SetModel(new EventReaderRuntimeModel(
            version: 2,
            createdUtc: DateTimeOffset.UtcNow,
            sourceSystems: new Dictionary<string, SourceSystemDefinition>(StringComparer.OrdinalIgnoreCase),
            functionDiscriminatorPaths: [],
            clientIdentityPaths: [],
            functionMatchers: FunctionMatcherIndex.Empty,
            functions: new Dictionary<int, CompiledFunctionPlan>()));

        // Second record: should miss (no source systems in new model)
        await processor.ProcessAsync(CreateBatch("""{"Action":"payment.created","NedbankID":"222"}""", offset: 2), CancellationToken.None);

        // Still only 1 work item — the second was dropped by the new empty model
        Assert.Equal(1, workStore.GetMetrics().LastWorkItemId);
    }

    private (EventReaderDurableBatchProcessor processor, FasterEventReaderWorkStore store) CreateProcessor()
    {
        var (proc, store, _) = CreateProcessorWithProvider();
        return (proc, store);
    }

    private (EventReaderDurableBatchProcessor processor, FasterEventReaderWorkStore store, StubRuntimeModelProvider provider) CreateProcessorWithProvider()
    {
        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddConsole());
        services.AddGeneratedLogging();
        services.AddSingleton<EventReaderMonitoringSnapshot>();
        services.AddSingleton<EventReaderMetricsAccumulator>();
        services.AddSingleton<IIncidentStore, InMemoryIncidentStore>();

        var options = new FasterEventReaderWorkStoreOptions
        {
            LogPath = Path.Combine(_testDir, "work-log"),
            CheckpointPath = Path.Combine(_testDir, "checkpoints"),
            CheckpointIntervalMs = 0,
        };

        var workStore = new FasterEventReaderWorkStore(options);
        services.AddSingleton<IEventReaderWorkStore>(workStore);

        var runtimeModel = CreateTestRuntimeModel();
        var stub = new StubRuntimeModelProvider(runtimeModel);
        services.AddSingleton<IEventReaderRuntimeModelProvider>(stub);

        services.AddSingleton<JsonFieldScanner>();
        services.AddSingleton<IUsernameIdentityLookup>(_ => new NoOpUsernameLookup());
        services.AddSingleton<ClientIdentityResolver>();
        services.AddSingleton(_ => new ShardAssigner(1024));

        services.AddScoped<EventReaderDurableBatchProcessor>();

        var provider = services.BuildServiceProvider();
        var processor = provider.GetRequiredService<EventReaderDurableBatchProcessor>();

        return (processor, workStore, stub);
    }

    private static KafkaRecordBatch CreateBatch(string json, long offset = 1) =>
        new([CreateConsumeResult(json, offset)], DateTimeOffset.UtcNow);

    private static ConsumeResult<byte[], byte[]> CreateConsumeResult(string json, long offset) =>
        new()
        {
            Topic = "test-topic",
            Partition = new Partition(0),
            Offset = new Offset(offset),
            Message = new Message<byte[], byte[]>
            {
                Value = Encoding.UTF8.GetBytes(json),
                Timestamp = new Timestamp(DateTime.UtcNow),
            },
        };

    private static EventReaderRuntimeModel CreateTestRuntimeModel() =>
        new(
            version: 1,
            createdUtc: DateTimeOffset.UtcNow,
            sourceSystems: new Dictionary<string, SourceSystemDefinition>(StringComparer.OrdinalIgnoreCase)
            {
                ["test-provider"] = new SourceSystemDefinition(
                    "test-provider",
                    "test-topic",
                    "Test Provider",
                    isEnabled: true,
                    new Dictionary<string, string>()),
            },
            functionDiscriminatorPaths: [JsonPathSelector.Create("$.Action")],
            clientIdentityPaths: [JsonPathSelector.Create("$.NedbankID"), JsonPathSelector.Create("$.NedbankIDUsername")],
            functionMatchers: new FunctionMatcherIndex(
                [
                    new FunctionMatcherDefinition(
                        null,
                        100,
                        1,
                        "payment.c",
                        new Regex("^payment\\.created$", RegexOptions.Compiled | RegexOptions.CultureInvariant)),
                ],
                prefixLength: 9),
            functions: new Dictionary<int, CompiledFunctionPlan>
            {
                [100] = new CompiledFunctionPlan(
                    100,
                    1,
                    "PaymentCreated",
                    SourceSystemBehavior.Global,
                    new CompiledJexScript("extract", 1),
                    new CompiledRuleSet(1, []),
                    new OutputRoutePlan(1, "default", "output-topic"),
                    FunctionFailurePolicy.Default),
            });

    private sealed class NoOpUsernameLookup : IUsernameIdentityLookup
    {
        public Task<long?> ResolveNedbankIdAsync(string username, CancellationToken cancellationToken) =>
            Task.FromResult<long?>(null);
    }

    private sealed class StubRuntimeModelProvider : IEventReaderRuntimeModelProvider
    {
        private EventReaderRuntimeModel _current;

        public StubRuntimeModelProvider(EventReaderRuntimeModel model) => _current = model;

        public EventReaderRuntimeModel Current => _current;
        public long CurrentVersion => _current.Version;
        public IReadOnlyList<long> ActiveVersions => [_current.Version];

        public void SetModel(EventReaderRuntimeModel model) => _current = model;

        public Task ReloadAsync(CancellationToken ct) => Task.CompletedTask;

        public Task<RuntimeModelGcResult> CollectGarbageAsync(
            IReadOnlySet<long> activeWorkStoreVersions, CancellationToken ct) =>
            Task.FromResult(new RuntimeModelGcResult(0, 1));
    }
}
