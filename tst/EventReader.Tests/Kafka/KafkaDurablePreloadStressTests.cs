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
public sealed class KafkaDurablePreloadStressTests : IDisposable
{
    private const int DefaultMessageCount = 500_000;
    private readonly KafkaTestClusterFixture _fixture;
    private readonly ITestOutputHelper _output;
    private readonly string _testDir;

    public KafkaDurablePreloadStressTests(KafkaTestClusterFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
        _testDir = Path.Combine(Path.GetTempPath(), "eventreader-durable-stress", Guid.NewGuid().ToString("N"));
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

    [Fact(DisplayName = "Durable Preload Stress — Full Pipeline: pre-loaded Kafka messages classified and durably tracked")]
    [Trait("Category", "Stress")]
    public async Task Durable_consumer_processes_preloaded_messages_and_reports_rate()
    {
        var totalMessages = ResolveMessageCount();
        var expectedClassified = (int)(totalMessages * 0.9);
        var topic = $"event-reader-durable-{Guid.NewGuid():N}";

        await _fixture.CreateTopicAsync(topic);
        await _fixture.ProduceJsonAsync(topic, totalMessages, index => BuildPayload(index, expectedClassified));

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
                ["Kafka:Profiles:Default:ExplicitGroupId"] = $"reader-durable-stress-{Guid.NewGuid():N}",
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
            CheckpointIntervalMs = 0,
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

        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(10));
        var stopwatch = Stopwatch.StartNew();

        await host.StartAsync(cts.Token);
        await WaitUntilAsync(() => metrics.TotalProcessed >= totalMessages, TimeSpan.FromMinutes(8), cts.Token);
        stopwatch.Stop();
        await host.StopAsync(cts.Token);

        var classifiedCount = metrics.ClassificationCounts.Values.Sum();
        var accuracy = classifiedCount / (double)totalMessages;
        var rate = metrics.TotalProcessed / Math.Max(1d, stopwatch.Elapsed.TotalSeconds);
        var detailedMetrics = workStore.GetDetailedMetrics();

        _output.WriteLine($"Processed {metrics.TotalProcessed} messages in {stopwatch.Elapsed.TotalSeconds:F2}s => {rate:F2} msg/s");
        _output.WriteLine($"Classified {classifiedCount} messages => accuracy {accuracy:P2}; unclassified={metrics.Unclassified}");
        Console.WriteLine($"EventReader durable preload rate: {rate:F2} msg/s; classified={classifiedCount}; accuracy={accuracy:P2}; unclassified={metrics.Unclassified}");

        Assert.Equal(totalMessages, metrics.TotalProcessed);
        Assert.Equal(0, metrics.TotalErrors);
        Assert.True(classifiedCount >= expectedClassified,
            $"Expected at least {expectedClassified} classified messages but saw {classifiedCount}.");
        Assert.True(accuracy >= 0.9,
            $"Expected at least 90% classification accuracy but saw {accuracy:P2}.");
        Assert.Equal(totalMessages - classifiedCount, metrics.Unclassified);
        Assert.Equal(classifiedCount, workStore.GetMetrics().LastWorkItemId);
        Assert.Equal(classifiedCount, detailedMetrics.BacklogByState.GetValueOrDefault(WorkState.Classified, 0));
    }

    private static int ResolveMessageCount()
    {
        var raw = Environment.GetEnvironmentVariable("EVENTREADER_PRELOAD_STRESS_COUNT");
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