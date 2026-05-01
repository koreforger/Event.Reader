using System.Text;
using System.Text.RegularExpressions;
using EventReader.Kafka;
using EventReader.Logging;
using EventReader.Monitoring;
using Event.Streaming.Processing.Monitoring;
using Event.Streaming.Processing.Runtime;
using Event.Streaming.Processing.WorkStore;
using KoreForge.Jex;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace EventReader.Tests.Kafka;

public sealed class EventReaderShardWorkerPoolTests : IDisposable
{
    private readonly string _testDir;

    public EventReaderShardWorkerPoolTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), "worker-pool-tests", Guid.NewGuid().ToString("N"));
    }

    public void Dispose()
    {
        try { Directory.Delete(_testDir, recursive: true); } catch { }
    }

    [Fact]
    public async Task Worker_pool_processes_enqueued_items()
    {
        var (pool, workStore) = CreatePool(workerCount: 2, logicalShardCount: 4);

        var item = CreateClassifiedWorkItem(shardId: 2);
        await workStore.EnqueueClassifiedAsync(item, CancellationToken.None);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await pool.StartAsync(cts.Token);

        await Task.Delay(500, CancellationToken.None);

        var leases = await workStore.LeaseShardBatchAsync(
            2, 10, TimeSpan.FromMinutes(5), CancellationToken.None);

        Assert.Empty(leases);

        var outputLeases = await workStore.LeaseOutputBatchAsync(
            10, TimeSpan.FromMinutes(5), CancellationToken.None);

        Assert.NotEmpty(outputLeases);

        await pool.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Worker_respects_shard_boundaries()
    {
        var (pool, workStore) = CreatePool(workerCount: 2, logicalShardCount: 4);

        var item = CreateClassifiedWorkItem(shardId: 0);
        await workStore.EnqueueClassifiedAsync(item, CancellationToken.None);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await pool.StartAsync(cts.Token);

        await Task.Delay(500, CancellationToken.None);

        var shard0Leases = await workStore.LeaseShardBatchAsync(0, 10, TimeSpan.FromMinutes(5), CancellationToken.None);
        Assert.Empty(shard0Leases);

        var outputLeases = await workStore.LeaseOutputBatchAsync(10, TimeSpan.FromMinutes(5), CancellationToken.None);
        Assert.NotEmpty(outputLeases);

        await pool.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Processor_extracts_and_builds_output()
    {
        var provider = CreateServiceProvider();
        var processor = provider.GetRequiredService<IWorkItemProcessor>();

        var lease = new WorkLease(
            42,
            7,
            "owner",
            DateTimeOffset.UtcNow.AddMinutes(5),
            1,
            CreateClassifiedWorkItem());

        var result = await processor.ProcessAsync(lease, CancellationToken.None);

        Assert.True(result.Success);
        Assert.NotNull(result.OutputPayload);
    }

    [Fact]
    public async Task Processor_output_contains_source_identity()
    {
        var provider = CreateServiceProvider();
        var processor = provider.GetRequiredService<IWorkItemProcessor>();

        var lease = new WorkLease(
            42,
            7,
            "owner",
            DateTimeOffset.UtcNow.AddMinutes(5),
            1,
            CreateClassifiedWorkItem());

        var result = await processor.ProcessAsync(lease, CancellationToken.None);

        var json = Encoding.UTF8.GetString(result.OutputPayload!);
        Assert.Contains("42", json);
        Assert.Contains("test-provider", json);
        Assert.Contains("test-topic", json);
        Assert.Contains("100", json);
    }

    [Fact]
    public async Task Processor_rule_outputs_are_appended()
    {
        var provider = CreateServiceProvider();
        var processor = provider.GetRequiredService<IWorkItemProcessor>();

        var lease = new WorkLease(
            42,
            7,
            "owner",
            DateTimeOffset.UtcNow.AddMinutes(5),
            1,
            CreateClassifiedWorkItem());

        var result = await processor.ProcessAsync(lease, CancellationToken.None);
        var json = Encoding.UTF8.GetString(result.OutputPayload!);

        Assert.Contains("\"output\"", json);
    }

    private (EventReaderShardWorkerPool pool, FasterEventReaderWorkStore store) CreatePool(
        int workerCount, int logicalShardCount)
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.AddConsole());
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

        services.AddSingleton(CreateTestRuntimeModel());
        services.AddSingleton<IJexCompiler>(_ => new Jex());
        services.AddSingleton<IWorkItemProcessor, EventReaderWorkItemProcessor>();
        services.AddSingleton(new EventReaderShardWorkerPoolOptions
        {
            WorkerCount = workerCount,
            LogicalShardCount = logicalShardCount,
            LeaseDuration = TimeSpan.FromMinutes(1),
            BatchSize = 10,
            IdleDelay = TimeSpan.FromMilliseconds(50),
        });
        services.AddSingleton<EventReaderShardWorkerPool>();

        var provider = services.BuildServiceProvider();
        var pool = provider.GetRequiredService<EventReaderShardWorkerPool>();
        return (pool, workStore);
    }

    private ServiceProvider CreateServiceProvider()
    {
        var services = new ServiceCollection();
        services.AddSingleton(CreateTestRuntimeModel());
        services.AddSingleton<IJexCompiler>(_ => new Jex());
        services.AddSingleton<IWorkItemProcessor, EventReaderWorkItemProcessor>();
        return services.BuildServiceProvider();
    }

    private static ClassifiedWorkItem CreateClassifiedWorkItem(int shardId = 7) =>
        new(
            0,
            new KafkaSourceIdentity("test-provider", "test-topic", 0, 42, DateTimeOffset.UtcNow),
            100,
            12345,
            shardId,
            1,
            1,
            1,
            1,
            1,
            Encoding.UTF8.GetBytes("""{"Action":"payment.created","amount":42}"""),
            DateTimeOffset.UtcNow);

    private static EventReaderRuntimeModel CreateTestRuntimeModel() =>
        new(
            version: 1,
            createdUtc: DateTimeOffset.UtcNow,
            sourceSystems: new Dictionary<string, SourceSystemDefinition>(StringComparer.OrdinalIgnoreCase)
            {
                ["test-provider"] = new SourceSystemDefinition(
                    "test-provider", "test-topic", "Test Provider", true, new Dictionary<string, string>()),
            },
            functionDiscriminatorPaths: [JsonPathSelector.Create("$.Action")],
            clientIdentityPaths: [JsonPathSelector.Create("$.NedbankID")],
            functionMatchers: FunctionMatcherIndex.Empty,
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
}
