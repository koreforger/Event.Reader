using Event.Streaming.Processing.WorkStore;
using Microsoft.Extensions.Logging;

namespace EventReader.Kafka;

public sealed class EventReaderBackpressureController
{
    private readonly BackpressureConfig _config;
    private readonly ILogger<EventReaderBackpressureController> _log;
    private volatile bool _isPaused;

    public EventReaderBackpressureController(
        ILogger<EventReaderBackpressureController> log,
        BackpressureConfig? config = null)
    {
        _log = log;
        _config = config ?? new BackpressureConfig();
    }

    public bool IsPaused => _isPaused;

    public bool ShouldPause(FasterEventReaderWorkStoreDetailedMetrics metrics)
    {
        if (_isPaused)
            return false;

        var signals = EvaluateSignals(metrics, _config);

        if (signals.Any(s => s.Value >= _config.PauseHighWatermark))
        {
            _isPaused = true;
            foreach (var (name, value) in signals.Where(s => s.Value >= _config.PauseHighWatermark))
            {
                _log.LogWarning(
                    "Backpressure PAUSE: signal {SignalName} at {SignalValue:F2} exceeds high watermark {HighWatermark:F2}",
                    name, value, _config.PauseHighWatermark);
            }

            return true;
        }

        return false;
    }

    public bool ShouldResume(FasterEventReaderWorkStoreDetailedMetrics metrics)
    {
        if (!_isPaused)
            return false;

        var signals = EvaluateSignals(metrics, _config);

        if (signals.All(s => s.Value <= _config.ResumeLowWatermark))
        {
            _isPaused = false;
            _log.LogInformation(
                "Backpressure RESUME: all signals below low watermark {LowWatermark:F2}",
                _config.ResumeLowWatermark);
            return true;
        }

        return false;
    }

    internal static Dictionary<string, double> EvaluateSignals(
        FasterEventReaderWorkStoreDetailedMetrics metrics,
        BackpressureConfig? config = null)
    {
        config ??= new BackpressureConfig();
        var signals = new Dictionary<string, double>();

        var classifiedCount = metrics.BacklogByState.TryGetValue(WorkState.Classified, out var c) ? c : 0;
        signals["FASTER.ClassifiedBacklog"] = config.MaxClassifiedBacklog > 0
            ? classifiedCount / (double)config.MaxClassifiedBacklog
            : 0;

        var maxShard = metrics.BacklogByShard.Count > 0 ? metrics.BacklogByShard.Values.Max() : 0L;
        signals["PerShard.MaxBacklog"] = config.MaxShardBacklog > 0
            ? maxShard / (double)config.MaxShardBacklog
            : 0;

        var readyCount = metrics.BacklogByState.TryGetValue(WorkState.ReadyToOutput, out var r) ? r : 0;
        signals["ReadyToOutput.Backlog"] = config.MaxReadyToOutputBacklog > 0
            ? readyCount / (double)config.MaxReadyToOutputBacklog
            : 0;

        signals["PublishLatency"] = config.MaxOutputPublishLatencyMs > 0
            ? metrics.AverageOutputLeaseLatencyMs / config.MaxOutputPublishLatencyMs
            : 0;

        signals["EnqueueLatency"] = config.MaxEnqueueLatencyMs > 0
            ? metrics.AverageEnqueueLatencyMs / config.MaxEnqueueLatencyMs
            : 0;

        signals["StoreLatency"] = config.MaxStoreLatencyMs > 0
            ? metrics.AverageShardLeaseLatencyMs / config.MaxStoreLatencyMs
            : 0;

        if (metrics.DiskTotalBytes > 0 && config.MinDiskFreeBytes > 0)
        {
            signals["DiskUsage"] = metrics.DiskFreeBytes < config.MinDiskFreeBytes ? 1.0 : 0.0;
        }
        else
        {
            signals["DiskUsage"] = 0;
        }

        return signals;
    }
}

public sealed record BackpressureConfig
{
    public double PauseHighWatermark { get; init; } = 1.0;
    public double ResumeLowWatermark { get; init; } = 0.5;

    public int MaxClassifiedBacklog { get; init; } = 100_000;
    public int MaxShardBacklog { get; init; } = 10_000;
    public int MaxReadyToOutputBacklog { get; init; } = 10_000;
    public double MaxOutputPublishLatencyMs { get; init; } = 10_000;
    public double MaxEnqueueLatencyMs { get; init; } = 5_000;
    public double MaxStoreLatencyMs { get; init; } = 5_000;
    public long MinDiskFreeBytes { get; init; } = 1_000_000_000;
}
