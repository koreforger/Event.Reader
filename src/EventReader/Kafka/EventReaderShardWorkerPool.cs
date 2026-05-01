using EventReader.Monitoring;
using Event.Streaming.Processing.WorkStore;
using Microsoft.Extensions.Logging;

namespace EventReader.Kafka;

public sealed class EventReaderShardWorkerPool : BackgroundService
{
    private readonly int _workerCount;
    private readonly int _logicalShardCount;
    private readonly TimeSpan _leaseDuration;
    private readonly int _batchSize;
    private readonly TimeSpan _idleDelay;
    private readonly IEventReaderWorkStore _workStore;
    private readonly IWorkItemProcessor _processor;
    private readonly EventReaderMetricsAccumulator _metrics;
    private readonly ILogger<EventReaderShardWorkerPool> _log;

    public EventReaderShardWorkerPool(
        EventReaderShardWorkerPoolOptions options,
        IEventReaderWorkStore workStore,
        IWorkItemProcessor processor,
        EventReaderMetricsAccumulator metrics,
        ILogger<EventReaderShardWorkerPool> log)
    {
        _workerCount = options.WorkerCount;
        _logicalShardCount = options.LogicalShardCount;
        _leaseDuration = options.LeaseDuration;
        _batchSize = options.BatchSize;
        _idleDelay = options.IdleDelay;
        _workStore = workStore;
        _processor = processor;
        _metrics = metrics;
        _log = log;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _log.LogInformation("Shard worker pool starting: {WorkerCount} workers, {LogicalShardCount} shards",
            _workerCount, _logicalShardCount);

        var workers = new Task[_workerCount];
        for (var i = 0; i < _workerCount; i++)
        {
            var workerIndex = i;
            workers[i] = RunWorkerAsync(workerIndex, stoppingToken);
        }

        await Task.WhenAll(workers).ConfigureAwait(false);

        _log.LogInformation("Shard worker pool stopped");
    }

    private async Task RunWorkerAsync(int workerIndex, CancellationToken ct)
    {
        _log.LogInformation("Shard worker {WorkerIndex} started", workerIndex);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                var leased = 0;
                for (var shardId = 0; shardId < _logicalShardCount; shardId++)
                {
                    if (shardId % _workerCount != workerIndex)
                    {
                        continue;
                    }

                    ct.ThrowIfCancellationRequested();

                    var leases = await _workStore
                        .LeaseShardBatchAsync(shardId, _batchSize, _leaseDuration, ct)
                        .ConfigureAwait(false);

                    foreach (var lease in leases)
                    {
                        ct.ThrowIfCancellationRequested();

                        try
                        {
                            var result = await _processor.ProcessAsync(lease, ct).ConfigureAwait(false);

                            if (result.Success && result.OutputPayload is not null)
                            {
                                await _workStore.MarkReadyToOutputAsync(
                                    lease.WorkItemId, result.OutputPayload, ct).ConfigureAwait(false);
                            }
                            else if (result.ShouldRetry)
                            {
                                var retryReason = new RetryReason(
                                    "Processing",
                                    result.ErrorMessage ?? "Unknown processing error",
                                    DateTimeOffset.UtcNow.AddSeconds(10),
                                    WorkState.Classified);

                                await _workStore.MarkRetryPendingAsync(
                                    lease.WorkItemId, retryReason, ct).ConfigureAwait(false);
                            }
                            else
                            {
                                await _workStore.MarkFailedAsync(
                                    lease.WorkItemId,
                                    result.ErrorMessage ?? "Processing failed",
                                    ct).ConfigureAwait(false);
                            }

                            _log.LogInformation("Worker {WorkerIndex} processed item {WorkItemId}", workerIndex, lease.WorkItemId);
                        }
                        catch (Exception ex) when (ex is not OperationCanceledException)
                        {
                            _log.LogError(ex, "Worker {WorkerIndex} failed item {WorkItemId}", workerIndex, lease.WorkItemId);

                            try
                            {
                                await _workStore.MarkFailedAsync(
                                    lease.WorkItemId, ex.Message, CancellationToken.None).ConfigureAwait(false);
                            }
                            catch
                            {
                            }
                        }

                        leased++;
                    }

                    if (leases.Count > 0)
                    {
                        _metrics.RecordProcessed(TimeSpan.Zero, DateTimeOffset.UtcNow);
                    }
                }

                if (leased == 0)
                {
                    await Task.Delay(_idleDelay, ct).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "Worker {WorkerIndex} loop error", workerIndex);
                await Task.Delay(_idleDelay, ct).ConfigureAwait(false);
            }
        }

        _log.LogInformation("Shard worker {WorkerIndex} stopped", workerIndex);
    }
}

public sealed record EventReaderShardWorkerPoolOptions
{
    public int WorkerCount { get; init; } = 32;
    public int LogicalShardCount { get; init; } = 1024;
    public TimeSpan LeaseDuration { get; init; } = TimeSpan.FromMinutes(5);
    public int BatchSize { get; init; } = 10;
    public TimeSpan IdleDelay { get; init; } = TimeSpan.FromMilliseconds(100);
}
