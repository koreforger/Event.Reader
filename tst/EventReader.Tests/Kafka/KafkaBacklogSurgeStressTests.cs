using System.Diagnostics;
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

[Collection(KafkaClusterCollection.CollectionName)]
public sealed class KafkaBacklogSurgeStressTests : IDisposable
{
    private const int DefaultMessageCount = 10_000_000;
    private const int SampleIntervalMs = 5_000;

    private readonly KafkaTestClusterFixture _fixture;
    private readonly ITestOutputHelper _output;
    private readonly string _testDir;

    public KafkaBacklogSurgeStressTests(KafkaTestClusterFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
        _testDir = Path.Combine(Path.GetTempPath(), "eventreader-surge-stress", Guid.NewGuid().ToString("N"));
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

    [Fact(DisplayName = "Backlog Surge (10M) — Resource Monitoring: FASTER disk, RAM, and CPU behaviour under maximum load")]
    [Trait("Category", "StressIntegration")]
    public async Task Surge_consumer_tracks_resource_usage_across_10m_messages()
    {
        var totalMessages = ResolveMessageCount();
        var expectedClassified = (int)(totalMessages * 0.9);
        var topic = $"event-reader-surge-{Guid.NewGuid():N}";

        _output.WriteLine($"[Surge] Producing {totalMessages:N0} messages to topic {topic}...");
        await _fixture.CreateTopicAsync(topic);
        await _fixture.ProduceJsonAsync(topic, totalMessages, index => BuildPayload(index, expectedClassified));
        _output.WriteLine($"[Surge] Production complete. Starting consumer...");

        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Warning));
        services.AddGeneratedLogging();
        services.AddSingleton<ISystemClock>(_ => UtcSystemClock.Instance);
        services.AddSingleton<EventReaderMonitoringSnapshot>();
        services.AddSingleton<EventReaderMetricsAccumulator>();
        services.AddSingleton<IIncidentStore, InMemoryIncidentStore>();

        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Kafka:Clusters:Local:BootstrapServers"] = _fixture.BootstrapServers,
                ["Kafka:Profiles:Default:Type"] = "Consumer",
                ["Kafka:Profiles:Default:Cluster"] = "Local",
                ["Kafka:Profiles:Default:ExplicitGroupId"] = $"reader-surge-stress-{Guid.NewGuid():N}",
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

        var storeOptions = new FasterEventReaderWorkStoreOptions
        {
            LogPath = Path.Combine(_testDir, "work-log"),
            CheckpointPath = Path.Combine(_testDir, "checkpoints"),
            CheckpointIntervalMs = 30_000,
        };

        var workStore = new FasterEventReaderWorkStore(storeOptions);
        services.AddSingleton<IEventReaderWorkStore>(workStore);
        services.AddSingleton(CreateRuntimeModel(topic));
        services.AddSingleton<IEventReaderRuntimeModelProvider>(sp =>
            new FixedModelProvider(sp.GetRequiredService<EventReaderRuntimeModel>()));
        services.AddSingleton<JsonFieldScanner>();
        services.AddSingleton<IUsernameIdentityLookup>(_ => new NoOpUsernameLookup());
        services.AddSingleton<ClientIdentityResolver>();
        services.AddSingleton(_ => new ShardAssigner(1024));
        services.AddScoped<EventReaderDurableBatchProcessor>();

        await using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var processor = scope.ServiceProvider.GetRequiredService<EventReaderDurableBatchProcessor>();
        var factory = scope.ServiceProvider.GetRequiredService<IKafkaClientConfigFactory>();
        var loggerFactory = scope.ServiceProvider.GetRequiredService<ILoggerFactory>();
        var metrics = scope.ServiceProvider.GetRequiredService<EventReaderMetricsAccumulator>();

        await using var host = KafkaConsumerHost.Create()
            .UseKafkaConfigurationProfile("Default", factory)
            .UseLoggerFactory(loggerFactory)
            .UseProcessor(() => processor)
            .Build();

        // ── Resource sampler ─────────────────────────────────────────────────────────
        var snapshots = new List<ResourceSnapshot>();
        var samplerCts = new CancellationTokenSource();
        var samplerTask = Task.Run(async () =>
        {
            var proc = Process.GetCurrentProcess();
            var prevCpuTime = proc.TotalProcessorTime;
            var prevWall = DateTime.UtcNow;

            while (!samplerCts.Token.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(SampleIntervalMs, samplerCts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }

                proc.Refresh();
                var nowCpuTime = proc.TotalProcessorTime;
                var nowWall = DateTime.UtcNow;

                var wallMs = Math.Max(1, (nowWall - prevWall).TotalMilliseconds);
                var cpuPercent = (nowCpuTime - prevCpuTime).TotalMilliseconds / wallMs / Environment.ProcessorCount * 100.0;
                prevCpuTime = nowCpuTime;
                prevWall = nowWall;

                var fasterLogMb = GetDirectorySizeBytes(storeOptions.LogPath) / (1024.0 * 1024);
                var fasterCkptMb = GetDirectorySizeBytes(storeOptions.CheckpointPath) / (1024.0 * 1024);
                var ramMb = proc.WorkingSet64 / (1024 * 1024);
                var processedNow = metrics.TotalProcessed;
                var diskFreeGb = 0.0;

                try
                {
                    diskFreeGb = workStore.GetDetailedMetrics().DiskFreeBytes / (1024.0 * 1024 * 1024);
                }
                catch
                {
                }

                var snap = new ResourceSnapshot(
                    Time: DateTimeOffset.UtcNow,
                    ProcessedCount: processedNow,
                    WorkingSetMb: ramMb,
                    FasterLogMb: fasterLogMb,
                    FasterCheckpointMb: fasterCkptMb,
                    DiskFreeGb: diskFreeGb,
                    CpuPercent: cpuPercent);

                snapshots.Add(snap);
                Console.WriteLine(
                    $"RESOURCE SAMPLE: t={snap.Time:HH:mm:ss} " +
                    $"processed={snap.ProcessedCount:N0} " +
                    $"ram={snap.WorkingSetMb}MB " +
                    $"faster-log={snap.FasterLogMb:F1}MB " +
                    $"disk-free={snap.DiskFreeGb:F2}GB " +
                    $"cpu={snap.CpuPercent:F1}%");
            }
        });

        // ── Run ──────────────────────────────────────────────────────────────────────
        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(30));
        var stopwatch = Stopwatch.StartNew();

        await host.StartAsync(cts.Token);
        await WaitUntilAsync(() => metrics.TotalProcessed >= totalMessages, TimeSpan.FromMinutes(28), cts.Token);
        stopwatch.Stop();
        await host.StopAsync(cts.Token);

        await samplerCts.CancelAsync();
        await samplerTask;

        // ── Report ───────────────────────────────────────────────────────────────────
        var classifiedCount = metrics.ClassificationCounts.Values.Sum();
        var accuracy = classifiedCount / (double)totalMessages;
        var rate = metrics.TotalProcessed / Math.Max(1d, stopwatch.Elapsed.TotalSeconds);
        var detailedMetrics = workStore.GetDetailedMetrics();
        var finalFasterLogMb = GetDirectorySizeBytes(storeOptions.LogPath) / (1024.0 * 1024);

        _output.WriteLine($"[Surge] Processed {metrics.TotalProcessed:N0} messages in {stopwatch.Elapsed.TotalSeconds:F2}s => {rate:F2} msg/s");
        _output.WriteLine($"[Surge] Classified {classifiedCount:N0}; accuracy {accuracy:P2}; unclassified={metrics.Unclassified:N0}");
        _output.WriteLine($"[Surge] FASTER log size at end: {finalFasterLogMb:F1} MB");
        _output.WriteLine($"[Surge] FASTER BacklogByState: {string.Join(", ", detailedMetrics.BacklogByState.Select(kv => $"{kv.Key}={kv.Value:N0}"))}");

        if (snapshots.Count > 0)
        {
            var peakRam = snapshots.Max(s => s.WorkingSetMb);
            var peakCpu = snapshots.Max(s => s.CpuPercent);
            var firstLogMb = snapshots.First().FasterLogMb;
            var lastLogMb = snapshots.Last().FasterLogMb;
            var logGrowthMb = lastLogMb - firstLogMb;

            _output.WriteLine($"[Surge] Peak RAM: {peakRam} MB  |  Peak CPU: {peakCpu:F1}%");
            _output.WriteLine($"[Surge] FASTER log growth: {firstLogMb:F1} MB → {lastLogMb:F1} MB (+{logGrowthMb:F1} MB)");
            _output.WriteLine($"[Surge] Resource samples ({snapshots.Count}):");
            foreach (var s in snapshots)
            {
                _output.WriteLine($"  {s.Time:HH:mm:ss}  processed={s.ProcessedCount,12:N0}  ram={s.WorkingSetMb,6}MB  log={s.FasterLogMb,8:F1}MB  cpu={s.CpuPercent,5:F1}%  disk-free={s.DiskFreeGb:F2}GB");
            }

            // FINDING: FASTER uses an append-only log — completed items remain on disk.
            // Log growth is expected and proportional to record count × average record size.
            // To reclaim disk space, explicit log compaction (FasterKV.Log.Compact) would be required.
            // This test surfaces the growth rate so capacity planning can be data-driven.
            Console.WriteLine($"EventReader surge rate: {rate:F2} msg/s; peak-ram={peakRam}MB; faster-log-growth={logGrowthMb:F1}MB; classified={classifiedCount}; accuracy={accuracy:P2}");
        }

        // ── Assertions ───────────────────────────────────────────────────────────────
        Assert.Equal(totalMessages, metrics.TotalProcessed);
        Assert.Equal(0, metrics.TotalErrors);
        Assert.True(
            accuracy >= 0.9,
            $"Expected at least 90% classification accuracy but saw {accuracy:P2}.");
        Assert.Equal(classifiedCount, detailedMetrics.BacklogByState.GetValueOrDefault(WorkState.Classified, 0));

        if (snapshots.Count > 0)
        {
            var peakRamMb = snapshots.Max(s => s.WorkingSetMb);
            // FASTER caps in-memory pages at LogMemorySizeBits=28 (256 MB).
            // Total process working set for a test runner + FASTER + Kafka producer should comfortably fit in 4 GB.
            Assert.True(
                peakRamMb < 4096,
                $"Peak working set {peakRamMb} MB exceeded 4 GB — possible memory leak.");
        }
    }

    private static int ResolveMessageCount()
    {
        var raw = Environment.GetEnvironmentVariable("EVENTREADER_SURGE_COUNT");
        return int.TryParse(raw, out var parsed) && parsed > 0
            ? parsed
            : DefaultMessageCount;
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

    private static long GetDirectorySizeBytes(string path)
    {
        if (!Directory.Exists(path))
        {
            return 0;
        }

        return Directory.GetFiles(path, "*", SearchOption.AllDirectories)
            .Sum(f =>
            {
                try { return new FileInfo(f).Length; }
                catch { return 0L; }
            });
    }

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

    private sealed record ResourceSnapshot(
        DateTimeOffset Time,
        long ProcessedCount,
        long WorkingSetMb,
        double FasterLogMb,
        double FasterCheckpointMb,
        double DiskFreeGb,
        double CpuPercent);

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
