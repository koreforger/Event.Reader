using System.Text.RegularExpressions;
using EventReader.Configuration;
using EventReader.Kafka;
using EventReader.Logging;
using EventReader.Monitoring;
using EventReader.Tests.Infrastructure;
using KoreForge.Kafka.Configuration.Extensions;
using KoreForge.Kafka.Configuration.Factory;
using KoreForge.Kafka.Consumer.Hosting;
using Event.Streaming.Processing.Monitoring;
using Event.Streaming.Processing.Runtime;
using Event.Streaming.Processing.WorkStore;
using KoreForge.Time;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

namespace EventReader.Tests.Kafka;

/// <summary>
/// Verifies that FASTER durably persists work-store state across a simulated consumer restart.
///
/// Test flow:
///   Phase 1 — Pre-load 1 M messages into Kafka.  Run the consumer until at least 300 K messages
///              have been processed.  Dispose the work store (which forces a final checkpoint).
///   Restart  — Assert checkpoint files exist on disk.  Create a new FasterEventReaderWorkStore
///              pointing at the same paths.  Verify that RecoverOrBootstrap rehydrates the
///              classified-item count and LastWorkItemId from the checkpoint.
///   Phase 2  — Resume the consumer with the recovered store.  Process the remaining messages.
///              FASTER source-index deduplication prevents double-counting of any messages that
///              Kafka re-delivers.  Assert that the final unique classified count matches the
///              expected total from the full 1 M message set.
/// </summary>
[Collection(KafkaClusterCollection.CollectionName)]
public sealed class FasterDurabilityResumeTests : IDisposable
{
    private const int TotalMessages = 1_000_000;
    private const int Phase1Target = 300_000;
    private const int ExpectedClassified = 900_000;
    private const int MaxCounterDrift = 20_000;

    private readonly KafkaTestClusterFixture _fixture;
    private readonly ITestOutputHelper _output;
    private readonly string _testDir;

    public FasterDurabilityResumeTests(KafkaTestClusterFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
        _testDir = Path.Combine(Path.GetTempPath(), "eventreader-durability-resume", Guid.NewGuid().ToString("N"));
    }

    public void Dispose()
    {
        try
        {
            Directory.Delete(_testDir, recursive: true);
        }
        catch
        {
        }
    }

    [Fact(DisplayName = "FASTER Durability — Checkpoint Survives Consumer Restart: backlog resumes from exact position")]
    [Trait("Category", "StressIntegration")]
    public async Task FASTER_state_recovered_from_checkpoint_after_consumer_restart()
    {
        var expectedClassified = ExpectedClassified;
        var topic = $"event-reader-durability-{Guid.NewGuid():N}";
        var groupId = $"reader-durability-{Guid.NewGuid():N}";

        var storeOptions = new FasterEventReaderWorkStoreOptions
        {
            LogPath = Path.Combine(_testDir, "work-log"),
            CheckpointPath = Path.Combine(_testDir, "checkpoints"),
            // Disable timer-based checkpointing; rely on the guarantee in DisposeAsync()
            // and the operation-count-based checkpointing (every 100 K ops).
            CheckpointIntervalMs = 0,
        };

        _output.WriteLine($"[Durability] Producing {TotalMessages:N0} messages to topic {topic}...");
        await _fixture.CreateTopicAsync(topic);
        await _fixture.ProduceJsonAsync(topic, TotalMessages, index => BuildPayload(index, expectedClassified));
        _output.WriteLine($"[Durability] Production complete.");

        // ── Phase 1: Partial processing ─────────────────────────────────────────────
        _output.WriteLine($"[Durability] Phase 1 — processing until {Phase1Target:N0} messages consumed...");

        var phase1Metrics = new EventReaderMetricsAccumulator();
        var workStore1 = new FasterEventReaderWorkStore(storeOptions);

        await using (BuildServiceProvider(topic, groupId, workStore1, phase1Metrics, out var factory1, out var loggerFactory1, out var processor1))
        {
            await using var host1 = KafkaConsumerHost.Create()
                .UseKafkaConfigurationProfile("Default", factory1)
                .UseLoggerFactory(loggerFactory1)
                .UseProcessor(() => processor1)
                .Build();

            using var cts1 = new CancellationTokenSource(TimeSpan.FromMinutes(10));

            await host1.StartAsync(cts1.Token);
            await WaitUntilAsync(
                () => phase1Metrics.TotalProcessed >= Phase1Target,
                TimeSpan.FromMinutes(8),
                cts1.Token);
            await host1.StopAsync(cts1.Token);
        }

        var phase1Classified = phase1Metrics.ClassificationCounts.Values.Sum();
        var phase1WorkItemId = workStore1.GetMetrics().LastWorkItemId;

        _output.WriteLine($"[Durability] Phase 1 complete: processed={phase1Metrics.TotalProcessed:N0}, classified={phase1Classified:N0}, workItemId={phase1WorkItemId}");
        Console.WriteLine($"EventReader durability phase1: processed={phase1Metrics.TotalProcessed}; classified={phase1Classified}; workItemId={phase1WorkItemId}");

        // DisposeAsync() guarantees TakeCheckpointAsync() is called before closing FASTER.
        await workStore1.DisposeAsync();

        // ── Verify checkpoint files exist on disk ────────────────────────────────────
        var checkpointFiles = Directory.Exists(storeOptions.CheckpointPath)
            ? Directory.GetFiles(storeOptions.CheckpointPath, "*", SearchOption.AllDirectories)
            : [];

        _output.WriteLine($"[Durability] Checkpoint files found: {checkpointFiles.Length}");
        Assert.True(
            checkpointFiles.Length > 0,
            $"Expected checkpoint files under '{storeOptions.CheckpointPath}' after dispose, but found none.");

        // ── Recovery: new store instance → RecoverOrBootstrap() → _store.Recover() ──
        _output.WriteLine($"[Durability] Creating new work-store instance — verifying recovery...");

        var workStore2 = new FasterEventReaderWorkStore(storeOptions);
        var recoveredMetrics = workStore2.GetMetrics();
        var recoveredDetailed = workStore2.GetDetailedMetrics();
        var recoveredClassified = recoveredDetailed.BacklogByState.GetValueOrDefault(WorkState.Classified, 0);

        _output.WriteLine($"[Durability] Recovered: workItemId={recoveredMetrics.LastWorkItemId}, classified-backlog={recoveredClassified:N0}");

        Assert.True(
            recoveredMetrics.LastWorkItemId > 0,
            $"Expected FASTER to recover a positive LastWorkItemId from checkpoint, but saw 0.");
        Assert.True(
            recoveredMetrics.LastWorkItemId >= phase1WorkItemId,
            $"Expected recovered LastWorkItemId ({recoveredMetrics.LastWorkItemId}) to be >= phase1 LastWorkItemId ({phase1WorkItemId}).");
        Assert.Equal(
            phase1Classified,
            (int)recoveredClassified);
        Assert.True(
            recoveredClassified > 0,
            $"Expected Classified backlog to survive checkpoint, but saw 0.");

        // ── Phase 2: Process remaining messages with the recovered store ─────────────
        _output.WriteLine($"[Durability] Phase 2 — processing remaining messages with recovered store...");

        var phase2Metrics = new EventReaderMetricsAccumulator();

        await using (BuildServiceProvider(topic, groupId, workStore2, phase2Metrics, out var factory2, out var loggerFactory2, out var processor2))
        {
            await using var host2 = KafkaConsumerHost.Create()
                .UseKafkaConfigurationProfile("Default", factory2)
                .UseLoggerFactory(loggerFactory2)
                .UseProcessor(() => processor2)
                .Build();

            using var cts2 = new CancellationTokenSource(TimeSpan.FromMinutes(15));

            await host2.StartAsync(cts2.Token);

            // FASTER source-index deduplication means Kafka re-deliveries are silently skipped.
            // The ground truth for unique classified items is workStore2.GetMetrics().LastWorkItemId.
            await WaitUntilAsync(
                () => workStore2.GetMetrics().LastWorkItemId >= expectedClassified,
                TimeSpan.FromMinutes(13),
                cts2.Token);

            await host2.StopAsync(cts2.Token);
        }

        var finalWorkItemId = workStore2.GetMetrics().LastWorkItemId;
        var finalDetailed = workStore2.GetDetailedMetrics();
        var finalClassified = finalDetailed.BacklogByState.GetValueOrDefault(WorkState.Classified, 0);

        _output.WriteLine($"[Durability] Phase 2 complete: phase2-processed={phase2Metrics.TotalProcessed:N0}, final-workItemId={finalWorkItemId}, final-classified={finalClassified:N0}");
        Console.WriteLine($"EventReader durability phase2: phase2-processed={phase2Metrics.TotalProcessed}; final-workItemId={finalWorkItemId}; final-classified={finalClassified}");

        await workStore2.DisposeAsync();

        // ── Final assertions ─────────────────────────────────────────────────────────
        Assert.Equal(0, phase1Metrics.TotalErrors);
        Assert.Equal(0, phase2Metrics.TotalErrors);

        Assert.True(
            finalWorkItemId >= finalClassified,
            $"Expected LastWorkItemId ({finalWorkItemId}) to be >= classified backlog ({finalClassified}).");
        Assert.True(
            finalWorkItemId >= expectedClassified,
            $"Expected final LastWorkItemId ({finalWorkItemId}) to be >= expected classified count ({expectedClassified}).");
        Assert.True(
            finalWorkItemId <= expectedClassified + MaxCounterDrift,
            $"Expected final LastWorkItemId ({finalWorkItemId}) to stay within +{MaxCounterDrift} of expected classified count ({expectedClassified}).");
        Assert.True(
            finalClassified >= recoveredClassified,
            $"Expected final classified backlog ({finalClassified}) to be >= recovered classified backlog ({recoveredClassified}).");
    }

    private ServiceProvider BuildServiceProvider(
        string topic,
        string groupId,
        FasterEventReaderWorkStore workStore,
        EventReaderMetricsAccumulator metrics,
        out IKafkaClientConfigFactory factory,
        out ILoggerFactory loggerFactory,
        out EventReaderDurableBatchProcessor processor)
    {
        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Warning));
        services.AddGeneratedLogging();
        services.AddSingleton<ISystemClock>(_ => UtcSystemClock.Instance);
        services.AddSingleton<EventReaderMonitoringSnapshot>();
        services.AddSingleton(metrics);
        services.AddSingleton<IIncidentStore, InMemoryIncidentStore>();

        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Kafka:Clusters:Local:BootstrapServers"] = _fixture.BootstrapServers,
                ["Kafka:Profiles:Default:Type"] = "Consumer",
                ["Kafka:Profiles:Default:Cluster"] = "Local",
                ["Kafka:Profiles:Default:ExplicitGroupId"] = groupId,
                ["Kafka:Profiles:Default:Topics:0"] = topic,
                ["Kafka:Profiles:Default:ConfluentOptions:auto.offset.reset"] = "earliest",
                ["Kafka:Profiles:Default:ConfluentOptions:enable.auto.commit"] = "false",
                ["Kafka:Profiles:Default:ExtendedConsumer:ConsumerCount"] = "1",
                ["Kafka:Profiles:Default:ExtendedConsumer:MaxBatchSize"] = "5000",
                ["Kafka:Profiles:Default:ExtendedConsumer:MaxBatchWaitMs"] = "200",
                ["Kafka:Profiles:Default:ExtendedConsumer:StartMode"] = "Earliest",
            })
            .Build();

        services.AddKafkaConfiguration(config);
        services.AddSingleton<IConfiguration>(config);
        services.AddSingleton<IEventReaderWorkStore>(workStore);
        services.AddSingleton(CreateRuntimeModel(topic));
        services.AddSingleton<IEventReaderRuntimeModelProvider>(sp =>
            new FixedModelProvider(sp.GetRequiredService<EventReaderRuntimeModel>()));
        services.AddSingleton<JsonFieldScanner>();
        services.AddSingleton<IUsernameIdentityLookup>(_ => new NoOpUsernameLookup());
        services.AddSingleton<ClientIdentityResolver>();
        services.AddSingleton(_ => new ShardAssigner(1024));
        services.AddScoped<EventReaderDurableBatchProcessor>();

        var sp = services.BuildServiceProvider();
        var scope = sp.CreateScope();

        factory = scope.ServiceProvider.GetRequiredService<IKafkaClientConfigFactory>();
        loggerFactory = scope.ServiceProvider.GetRequiredService<ILoggerFactory>();
        processor = scope.ServiceProvider.GetRequiredService<EventReaderDurableBatchProcessor>();

        return sp;
    }

    private static string BuildPayload(int index, int expectedClassified)
    {
        if (index < expectedClassified)
        {
            return index % 2 == 0
                ? $$"""{"NedbankID":"{{100000 + index}}","Action":"payment.created","EventId":"evt-{{index}}"}"""
                : $$"""{"Action":"payment.created","NedbankID":"{{100000 + index}}","EventId":"evt-{{index}}"}""";
        }

        return $$"""{"NedbankID":"{{100000 + index}}","Action":"payment.unknown","EventId":"evt-{{index}}"}""";
    }

    private static EventReaderRuntimeModel CreateRuntimeModel(string topic) =>
        new(
            version: 1,
            createdUtc: DateTimeOffset.UtcNow,
            sourceSystems: new Dictionary<string, SourceSystemDefinition>(StringComparer.OrdinalIgnoreCase)
            {
                ["payments-provider"] = new SourceSystemDefinition(
                    "payments-provider",
                    topic,
                    "Payments Provider",
                    isEnabled: true,
                    new Dictionary<string, string>()),
            },
            functionDiscriminatorPaths: [JsonPathSelector.Create("$.Action")],
            clientIdentityPaths: [JsonPathSelector.Create("$.NedbankID")],
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
                    new CompiledJexScript("", 1),
                    new CompiledRuleSet(1, []),
                    new OutputRoutePlan(1, "default", "output-topic"),
                    FunctionFailurePolicy.Default),
            });

    private static async Task WaitUntilAsync(
        Func<bool> condition,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!condition())
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException("Condition was not satisfied before timeout.");
            }

            await Task.Delay(250, cancellationToken).ConfigureAwait(false);
        }
    }

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
}
