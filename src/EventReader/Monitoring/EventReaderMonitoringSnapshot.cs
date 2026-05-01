using Event.Streaming.Processing.Monitoring;
using EventReader.Configuration;

namespace EventReader.Monitoring;

/// <summary>
/// EventReader runtime monitoring snapshot returned by /api/monitoring/snapshot.
/// Implements IMonitoringSnapshot for the shared monitoring contract.
/// </summary>
public sealed class EventReaderMonitoringSnapshot : IMonitoringSnapshot
{
    // IMonitoringSnapshot identity
    public string Application { get; set; } = "EventReader";
    public string Instance { get; set; } = "1";
    public string Environment { get; set; } = System.Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT") ?? "Production";
    public DateTimeOffset Timestamp { get; set; } = DateTimeOffset.UtcNow;
    public string Version { get; set; } = "1.0.0";
    public HealthStatus Health { get; set; } = HealthStatus.Unknown;

    // IMonitoringSnapshot metrics
    public KafkaMetrics KafkaMetrics { get; set; } = new();
    public PipelineMetrics PipelineMetrics { get; set; } = new();
    public SettingsSyncMetrics SettingsSyncMetrics { get; set; } = new();
    public IReadOnlyDictionary<string, object> AppSpecificMetrics { get; set; }
        = new Dictionary<string, object>();

    // EventReader-specific fields (not in IMonitoringSnapshot)
    public string SeekMode { get; set; } = "None";
    public string DiagnosticStage { get; set; } = "FullPipeline";
    public bool StopBoundaryActive { get; set; }
    public long StopBoundaryMessagesSkipped { get; set; }

    public EventReaderMonitoringSnapshot ApplyRuntimeOptions(
        EventReaderRuntimeOptions options,
        DateTimeOffset capturedAt)
    {
        Instance = options.InstanceId;
        Version = options.Version;
        Timestamp = capturedAt;
        DiagnosticStage = options.DiagnosticStage.ToString();

        var appMetrics = new Dictionary<string, object>(AppSpecificMetrics, StringComparer.OrdinalIgnoreCase)
        {
            ["DiagnosticStage"] = DiagnosticStage,
        };
        AppSpecificMetrics = appMetrics;

        return this;
    }
}

/// <summary>
/// Mutable accumulator for EventReader pipeline metrics.
/// Updated concurrently by batch processor workers.
/// </summary>
public sealed class EventReaderMetricsAccumulator
{
    private const int MaxLatencySamples = 2048;

    private long _processed;
    private long _errors;
    private long _stopBoundarySkipped;
    private long _unclassified;
    private long _parseErrors;
    private long _batches;
    private long _messagesReceived;
    private long _dlqWritten;
    private long _latencyCount;
    private long _latencyTotalTicks;
    private long _lastProcessedUnixMs;
    private long _lastMessageReceivedUnixMs;
    private readonly Dictionary<string, long> _classificationCounts = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, long> _routeAttempts = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, long> _routeSuccesses = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, long> _routeFailures = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, long> _routeSkipped = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, long> _errorsByCategory = new(StringComparer.OrdinalIgnoreCase);
    private readonly Queue<long> _latencySamples = new();
    private readonly Lock _lock = new();

    public void RecordProcessed() => Interlocked.Increment(ref _processed);

    public void RecordProcessed(TimeSpan latency, DateTimeOffset processedAtUtc)
    {
        RecordProcessed();
        RecordLatency(latency);
        Interlocked.Exchange(ref _lastProcessedUnixMs, processedAtUtc.ToUnixTimeMilliseconds());
    }

    public void RecordError() => RecordError("General");
    public void RecordError(string category)
    {
        Interlocked.Increment(ref _errors);
        lock (_lock)
        {
            _errorsByCategory.TryGetValue(category, out var current);
            _errorsByCategory[category] = current + 1;
        }
    }

    public void RecordStopBoundarySkip() => Interlocked.Increment(ref _stopBoundarySkipped);
    public void RecordUnclassified() => Interlocked.Increment(ref _unclassified);
    public void RecordParseError() => Interlocked.Increment(ref _parseErrors);
    public void RecordDlqWritten() => Interlocked.Increment(ref _dlqWritten);
    public void RecordBatch(int count)
    {
        Interlocked.Increment(ref _batches);
        Interlocked.Add(ref _messagesReceived, count);
    }

    public void RecordBatch(int count, DateTimeOffset receivedAtUtc)
    {
        RecordBatch(count);
        if (count > 0)
        {
            Interlocked.Exchange(ref _lastMessageReceivedUnixMs, receivedAtUtc.ToUnixTimeMilliseconds());
        }
    }

    public void RecordClassified(string profile)
    {
        lock (_lock)
        {
            _classificationCounts.TryGetValue(profile, out var current);
            _classificationCounts[profile] = current + 1;
        }
    }

    public void RecordRouteAttempt(string routeName)
    {
        lock (_lock)
        {
            _routeAttempts.TryGetValue(routeName, out var current);
            _routeAttempts[routeName] = current + 1;
        }
    }

    public void RecordRouteSuccess(string routeName) => IncrementRoute(_routeSuccesses, routeName);
    public void RecordRouteFailure(string routeName) => IncrementRoute(_routeFailures, routeName);
    public void RecordRouteSkipped(string routeName) => IncrementRoute(_routeSkipped, routeName);

    private void IncrementRoute(Dictionary<string, long> target, string routeName)
    {
        lock (_lock)
        {
            target.TryGetValue(routeName, out var current);
            target[routeName] = current + 1;
        }
    }

    public long TotalProcessed => Interlocked.Read(ref _processed);
    public long TotalErrors => Interlocked.Read(ref _errors);
    public long StopBoundarySkipped => Interlocked.Read(ref _stopBoundarySkipped);
    public long Unclassified => Interlocked.Read(ref _unclassified);
    public long ParseErrors => Interlocked.Read(ref _parseErrors);
    public long TotalBatches => Interlocked.Read(ref _batches);
    public long TotalMessagesReceived => Interlocked.Read(ref _messagesReceived);
    public long DlqWritten => Interlocked.Read(ref _dlqWritten);
    public double AverageLatencyMs
    {
        get
        {
            var count = Interlocked.Read(ref _latencyCount);
            if (count == 0)
            {
                return 0;
            }

            var ticks = Interlocked.Read(ref _latencyTotalTicks);
            return TimeSpan.FromTicks(ticks / count).TotalMilliseconds;
        }
    }

    public double P95LatencyMs => PercentileLatencyMs(0.95);
    public double P99LatencyMs => PercentileLatencyMs(0.99);
    public DateTimeOffset? LastProcessedAtUtc => UnixMsToDateTimeOffset(Interlocked.Read(ref _lastProcessedUnixMs));
    public DateTimeOffset? LastMessageReceivedAtUtc => UnixMsToDateTimeOffset(Interlocked.Read(ref _lastMessageReceivedUnixMs));
    public IReadOnlyDictionary<string, long> ClassificationCounts
    {
        get
        {
            lock (_lock) return new Dictionary<string, long>(_classificationCounts);
        }
    }

    public IReadOnlyDictionary<string, long> RouteAttempts
    {
        get
        {
            lock (_lock) return new Dictionary<string, long>(_routeAttempts);
        }
    }

    public IReadOnlyDictionary<string, long> RouteSuccesses
    {
        get
        {
            lock (_lock) return new Dictionary<string, long>(_routeSuccesses);
        }
    }

    public IReadOnlyDictionary<string, long> RouteFailures
    {
        get
        {
            lock (_lock) return new Dictionary<string, long>(_routeFailures);
        }
    }

    public IReadOnlyDictionary<string, long> RouteSkipped
    {
        get
        {
            lock (_lock) return new Dictionary<string, long>(_routeSkipped);
        }
    }

    private void RecordLatency(TimeSpan latency)
    {
        var ticks = Math.Max(0, latency.Ticks);
        Interlocked.Increment(ref _latencyCount);
        Interlocked.Add(ref _latencyTotalTicks, ticks);

        lock (_lock)
        {
            _latencySamples.Enqueue(ticks);
            while (_latencySamples.Count > MaxLatencySamples)
            {
                _latencySamples.Dequeue();
            }
        }
    }

    private double PercentileLatencyMs(double percentile)
    {
        long[] samples;
        lock (_lock)
        {
            if (_latencySamples.Count == 0)
            {
                return 0;
            }

            samples = _latencySamples.ToArray();
        }

        Array.Sort(samples);
        var index = (int)Math.Ceiling(percentile * samples.Length) - 1;
        index = Math.Clamp(index, 0, samples.Length - 1);
        return TimeSpan.FromTicks(samples[index]).TotalMilliseconds;
    }

    private static DateTimeOffset? UnixMsToDateTimeOffset(long unixMs) =>
        unixMs <= 0 ? null : DateTimeOffset.FromUnixTimeMilliseconds(unixMs);

    public IReadOnlyDictionary<string, long> ErrorCountsByCategory
    {
        get
        {
            lock (_lock) return new Dictionary<string, long>(_errorsByCategory);
        }
    }
}
