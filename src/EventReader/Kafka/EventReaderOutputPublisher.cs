using System.Diagnostics;
using Event.Streaming.Processing.WorkStore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace EventReader.Kafka;

public interface IOutputMessagePublisher
{
    Task<OutputPublishResult> PublishAsync(byte[] payload, CancellationToken ct);
}

public sealed record OutputPublishResult(bool Success, string? Topic, int Partition, long Offset, string? Error);

public sealed record EventReaderOutputPublisherOptions
{
    public int BatchSize { get; init; } = 100;
    public TimeSpan LeaseDuration { get; init; } = TimeSpan.FromMinutes(5);
    public TimeSpan IdleDelay { get; init; } = TimeSpan.FromMilliseconds(100);
}

public sealed record OutputPublisherMetrics(
    long PublishAttempts,
    long PublishSuccesses,
    long PublishFailures,
    long Retries,
    double AverageLatencyMs,
    double P95LatencyMs,
    double P99LatencyMs,
    DateTimeOffset? LastPublishUtc);

public sealed class EventReaderOutputPublisher : BackgroundService
{
    private readonly IEventReaderWorkStore _workStore;
    private readonly IOutputMessagePublisher _publisher;
    private readonly ILogger<EventReaderOutputPublisher> _log;
    private readonly int _batchSize;
    private readonly TimeSpan _leaseDuration;
    private readonly TimeSpan _idleDelay;

    private long _publishAttempts;
    private long _publishSuccesses;
    private long _publishFailures;
    private long _retries;
    private long _latencyTotalTicks;
    private long _lastPublishTimestamp;
    private readonly Lock _metricsLock = new();
    private readonly Queue<long> _latencySamples = new();
    private const int MaxLatencySamples = 2048;

    public EventReaderOutputPublisher(
        IEventReaderWorkStore workStore,
        IOutputMessagePublisher publisher,
        ILogger<EventReaderOutputPublisher> logger,
        IServiceScopeFactory scopeFactory,
        EventReaderOutputPublisherOptions options)
    {
        _workStore = workStore ?? throw new ArgumentNullException(nameof(workStore));
        _publisher = publisher ?? throw new ArgumentNullException(nameof(publisher));
        _log = logger ?? throw new ArgumentNullException(nameof(logger));
        _batchSize = options.BatchSize;
        _leaseDuration = options.LeaseDuration;
        _idleDelay = options.IdleDelay;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _log.LogInformation("EventReader output publisher starting with batch size {BatchSize}", _batchSize);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var leases = await _workStore
                    .LeaseOutputBatchAsync(_batchSize, _leaseDuration, stoppingToken)
                    .ConfigureAwait(false);

                if (leases.Count == 0)
                {
                    await Task.Delay(_idleDelay, stoppingToken).ConfigureAwait(false);
                    continue;
                }

                foreach (var lease in leases)
                {
                    stoppingToken.ThrowIfCancellationRequested();
                    await ProcessLeaseAsync(lease, stoppingToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "EventReader output publisher loop error");
                await Task.Delay(_idleDelay, stoppingToken).ConfigureAwait(false);
            }
        }

        _log.LogInformation("EventReader output publisher stopped");
    }

    private async Task ProcessLeaseAsync(OutputLease lease, CancellationToken ct)
    {
        Interlocked.Increment(ref _publishAttempts);
        var sw = Stopwatch.StartNew();

        try
        {
            var result = await _publisher.PublishAsync(lease.OutputPayload, ct).ConfigureAwait(false);
            sw.Stop();
            RecordLatency(sw.ElapsedTicks);

            if (result.Success)
            {
                Interlocked.Increment(ref _publishSuccesses);

                var receipt = new OutputWriteReceipt(
                    result.Topic ?? "default",
                    result.Partition,
                    result.Offset,
                    DateTimeOffset.UtcNow,
                    null);

                await _workStore.MarkCompletedAsync(lease.WorkItemId, receipt, ct).ConfigureAwait(false);

                _log.LogInformation(
                    "EventReader output publisher published work item {WorkItemId} to {Topic} partition {Partition} offset {Offset}",
                    lease.WorkItemId, receipt.Topic, receipt.Partition, receipt.Offset);
            }
            else
            {
                Interlocked.Increment(ref _publishFailures);
                Interlocked.Increment(ref _retries);

                var retryReason = new RetryReason(
                    "Output",
                    result.Error ?? "Publish failed",
                    DateTimeOffset.UtcNow.AddSeconds(10),
                    WorkState.ReadyToOutput);

                await _workStore.MarkRetryPendingAsync(lease.WorkItemId, retryReason, ct).ConfigureAwait(false);

                _log.LogWarning(
                    "EventReader output publisher failed for work item {WorkItemId}: {Error}",
                    lease.WorkItemId, result.Error);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            sw.Stop();
            RecordLatency(sw.ElapsedTicks);
            Interlocked.Increment(ref _publishFailures);

            _log.LogError(ex, "EventReader output publisher exception for work item {WorkItemId}", lease.WorkItemId);

            try
            {
                await _workStore.MarkFailedAsync(lease.WorkItemId, ex.Message, CancellationToken.None)
                    .ConfigureAwait(false);
            }
            catch
            {
            }
        }
    }

    private void RecordLatency(long ticks)
    {
        ticks = Math.Max(0, ticks);
        Interlocked.Add(ref _latencyTotalTicks, ticks);
        Interlocked.Exchange(ref _lastPublishTimestamp, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

        lock (_metricsLock)
        {
            _latencySamples.Enqueue(ticks);
            while (_latencySamples.Count > MaxLatencySamples)
            {
                _latencySamples.Dequeue();
            }
        }
    }

    public OutputPublisherMetrics GetMetrics()
    {
        var attempts = Interlocked.Read(ref _publishAttempts);
        var successes = Interlocked.Read(ref _publishSuccesses);
        var failures = Interlocked.Read(ref _publishFailures);
        var retries = Interlocked.Read(ref _retries);
        var totalTicks = Interlocked.Read(ref _latencyTotalTicks);
        var lastTimestamp = Interlocked.Read(ref _lastPublishTimestamp);

        var avgMs = attempts > 0
            ? TimeSpan.FromTicks(totalTicks / attempts).TotalMilliseconds
            : 0;

        long[] samples;
        lock (_metricsLock)
        {
            samples = _latencySamples.ToArray();
        }

        var p95Ms = PercentileMs(samples, 0.95);
        var p99Ms = PercentileMs(samples, 0.99);

        return new OutputPublisherMetrics(
            attempts,
            successes,
            failures,
            retries,
            avgMs,
            p95Ms,
            p99Ms,
            lastTimestamp > 0 ? DateTimeOffset.FromUnixTimeMilliseconds(lastTimestamp) : null);
    }

    private static double PercentileMs(long[] samples, double percentile)
    {
        if (samples.Length == 0)
        {
            return 0;
        }

        Array.Sort(samples);
        var index = (int)Math.Ceiling(percentile * samples.Length) - 1;
        index = Math.Clamp(index, 0, samples.Length - 1);
        return TimeSpan.FromTicks(samples[index]).TotalMilliseconds;
    }
}
