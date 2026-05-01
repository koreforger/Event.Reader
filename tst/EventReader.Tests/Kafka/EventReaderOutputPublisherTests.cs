using System.Text;
using EventReader.Kafka;
using Event.Streaming.Processing.WorkStore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace EventReader.Tests.Kafka;

public sealed class EventReaderOutputPublisherTests : IDisposable
{
    private readonly string _testDir;
    private readonly List<IDisposable> _disposables = new();

    public EventReaderOutputPublisherTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), "output-publisher-tests", Guid.NewGuid().ToString("N"));
    }

    public void Dispose()
    {
        foreach (var d in _disposables)
        {
            try { d.Dispose(); } catch { }
        }

        try { Directory.Delete(_testDir, recursive: true); } catch { }
    }

    [Fact]
    public async Task Successful_publish_moves_item_to_completed()
    {
        var workStore = CreateWorkStore(nameof(Successful_publish_moves_item_to_completed));
        var publisher = new TestOutputPublisher(success: true);
        var outputPublisher = BuildPublisher(workStore, publisher);

        await EnqueueAndMarkReadyToOutputAsync(workStore, shardId: 0, sourceOffset: 1);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await outputPublisher.StartAsync(cts.Token);
        await Task.Delay(300, CancellationToken.None);

        var outputLeases = await workStore.LeaseOutputBatchAsync(
            10, TimeSpan.FromMinutes(5), CancellationToken.None);
        Assert.Empty(outputLeases);

        var metrics = outputPublisher.GetMetrics();
        Assert.Equal(1, metrics.PublishAttempts);
        Assert.Equal(1, metrics.PublishSuccesses);
        Assert.Equal(0, metrics.PublishFailures);
        Assert.Equal(0, metrics.Retries);
        Assert.True(metrics.AverageLatencyMs > 0);
        Assert.NotNull(metrics.LastPublishUtc);

        cts.Cancel();
        await outputPublisher.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Failed_publish_marks_item_for_retry()
    {
        var workStore = CreateWorkStore(nameof(Failed_publish_marks_item_for_retry));
        var publisher = new TestOutputPublisher(success: false, error: "Kafka broker unavailable");
        var outputPublisher = BuildPublisher(workStore, publisher);

        await EnqueueAndMarkReadyToOutputAsync(workStore, shardId: 0, sourceOffset: 10);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await outputPublisher.StartAsync(cts.Token);
        await Task.Delay(300, CancellationToken.None);

        var outputLeases = await workStore.LeaseOutputBatchAsync(
            10, TimeSpan.FromMinutes(5), CancellationToken.None);
        Assert.Empty(outputLeases);

        var metrics = outputPublisher.GetMetrics();
        Assert.Equal(1, metrics.PublishAttempts);
        Assert.Equal(0, metrics.PublishSuccesses);
        Assert.Equal(1, metrics.PublishFailures);
        Assert.Equal(1, metrics.Retries);

        cts.Cancel();
        await outputPublisher.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Idle_when_no_items_ready()
    {
        var workStore = CreateWorkStore(nameof(Idle_when_no_items_ready));
        var publisher = new TestOutputPublisher(success: true);
        var outputPublisher = BuildPublisher(workStore, publisher);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await outputPublisher.StartAsync(cts.Token);
        await Task.Delay(200, CancellationToken.None);

        var metrics = outputPublisher.GetMetrics();
        Assert.Equal(0, metrics.PublishAttempts);
        Assert.Equal(0, metrics.PublishSuccesses);
        Assert.Equal(0, metrics.PublishFailures);

        cts.Cancel();
        await outputPublisher.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Batch_processes_multiple_items()
    {
        var workStore = CreateWorkStore(nameof(Batch_processes_multiple_items));
        var publisher = new TestOutputPublisher(success: true);
        var outputPublisher = BuildPublisher(workStore, publisher);

        const int itemCount = 3;
        for (var i = 0; i < itemCount; i++)
        {
            await EnqueueAndMarkReadyToOutputAsync(workStore, shardId: 0, sourceOffset: 100 + i);
        }

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await outputPublisher.StartAsync(cts.Token);
        await Task.Delay(500, CancellationToken.None);

        var outputLeases = await workStore.LeaseOutputBatchAsync(
            10, TimeSpan.FromMinutes(5), CancellationToken.None);
        Assert.Empty(outputLeases);

        var metrics = outputPublisher.GetMetrics();
        Assert.Equal(itemCount, metrics.PublishAttempts);
        Assert.Equal(itemCount, metrics.PublishSuccesses);
        Assert.Equal(0, metrics.PublishFailures);

        cts.Cancel();
        await outputPublisher.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Graceful_shutdown()
    {
        var workStore = CreateWorkStore(nameof(Graceful_shutdown));
        var publisher = new TestOutputPublisher(success: true);
        var outputPublisher = BuildPublisher(workStore, publisher);

        using var cts = new CancellationTokenSource();
        await outputPublisher.StartAsync(cts.Token);
        await Task.Delay(100, CancellationToken.None);

        cts.Cancel();

        using var stopCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await outputPublisher.StopAsync(stopCts.Token);

        Assert.True(true);
    }

    private sealed class TestOutputPublisher : IOutputMessagePublisher
    {
        private readonly bool _success;
        private readonly string? _error;

        public TestOutputPublisher(bool success = true, string? error = null)
        {
            _success = success;
            _error = error;
        }

        public Task<OutputPublishResult> PublishAsync(byte[] payload, CancellationToken ct)
        {
            if (_success)
            {
                return Task.FromResult(new OutputPublishResult(true, "test-topic", 0, 42, null));
            }

            return Task.FromResult(new OutputPublishResult(false, null, 0, 0, _error ?? "Simulated failure"));
        }
    }

    private FasterEventReaderWorkStore CreateWorkStore(string name)
    {
        var logPath = Path.Combine(_testDir, name, "work-log");
        var checkpointPath = Path.Combine(_testDir, name, "checkpoints");

        var options = new FasterEventReaderWorkStoreOptions
        {
            LogPath = logPath,
            CheckpointPath = checkpointPath,
            CheckpointIntervalMs = 0,
        };

        var store = new FasterEventReaderWorkStore(options);
        _disposables.Add(store);
        return store;
    }

    private EventReaderOutputPublisher BuildPublisher(
        IEventReaderWorkStore workStore,
        IOutputMessagePublisher publisher)
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.AddConsole().SetMinimumLevel(LogLevel.Debug));
        var provider = services.BuildServiceProvider();
        _disposables.Add(provider);

        var scopeFactory = provider.GetRequiredService<IServiceScopeFactory>();
        var logger = provider.GetRequiredService<ILogger<EventReaderOutputPublisher>>();

        var options = new EventReaderOutputPublisherOptions
        {
            BatchSize = 10,
            LeaseDuration = TimeSpan.FromMinutes(1),
            IdleDelay = TimeSpan.FromMilliseconds(50),
        };

        return new EventReaderOutputPublisher(workStore, publisher, logger, scopeFactory, options);
    }

    private static async Task<long> EnqueueAndMarkReadyToOutputAsync(
        FasterEventReaderWorkStore workStore,
        int shardId,
        long sourceOffset = 42)
    {
        var item = new ClassifiedWorkItem(
            0,
            new KafkaSourceIdentity("test-provider", "test-topic", 0, sourceOffset, DateTimeOffset.UtcNow),
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

        await workStore.EnqueueClassifiedAsync(item, CancellationToken.None);

        var leases = await workStore.LeaseShardBatchAsync(
            shardId, 1, TimeSpan.FromMinutes(5), CancellationToken.None);
        Assert.NotEmpty(leases);

        var outputPayload = Encoding.UTF8.GetBytes("""{"output":"test-result"}""");
        await workStore.MarkReadyToOutputAsync(leases[0].WorkItemId, outputPayload, CancellationToken.None);

        return leases[0].WorkItemId;
    }
}
