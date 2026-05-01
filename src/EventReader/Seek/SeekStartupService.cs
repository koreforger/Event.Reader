using Confluent.Kafka;
using EventReader.Logging;
using Event.Streaming.In.Seek;

namespace EventReader.Seek;

/// <summary>
/// Applied once at consumer startup: repositions all assigned partitions
/// according to <see cref="ConsumerSeekOptions"/> before the first batch is consumed.
/// </summary>
public sealed class SeekStartupService
{
    private readonly SeekLogger<SeekStartupService> _log;

    public SeekStartupService(SeekLogger<SeekStartupService> log) => _log = log;

    public void ApplySeek<TKey, TValue>(
        IConsumer<TKey, TValue> consumer,
        ConsumerSeekOptions opts,
        IEnumerable<TopicPartition> assignedPartitions,
        SeekBoundaryState boundaryState)
    {
        if (opts.Mode == SeekMode.None)
        {
            _log.Mode.None.LogDebug("Seek mode is None — using profile StartMode");
            return;
        }

        var partitions = assignedPartitions.ToList();

        if (opts.Mode is SeekMode.FromOffset or SeekMode.Range)
            ApplyOffsetSeek(consumer, opts, partitions, boundaryState);
        else if (opts.Mode == SeekMode.FromTimestamp)
            ApplyTimestampSeek(consumer, opts, partitions, boundaryState, isRange: false);
    }

    private void ApplyOffsetSeek<TKey, TValue>(
        IConsumer<TKey, TValue> consumer,
        ConsumerSeekOptions opts,
        List<TopicPartition> partitions,
        SeekBoundaryState boundaryState)
    {
        if (opts.TryParseStartAsOffset(out var startOffset))
        {
            foreach (var tp in partitions)
            {
                consumer.Seek(new TopicPartitionOffset(tp, new Offset(startOffset)));
                _log.Applied.LogInformation(
                    "Seek applied — topic: {Topic}, partition: {Partition}, offset: {Offset}",
                    tp.Topic, tp.Partition.Value, startOffset);
            }
            if (opts.Mode == SeekMode.Range)
                ConfigureStopBoundary(opts, partitions, boundaryState, consumer);
        }
        else
        {
            ApplyTimestampSeek(consumer, opts, partitions, boundaryState, isRange: opts.Mode == SeekMode.Range);
        }
    }

    private void ApplyTimestampSeek<TKey, TValue>(
        IConsumer<TKey, TValue> consumer,
        ConsumerSeekOptions opts,
        List<TopicPartition> partitions,
        SeekBoundaryState boundaryState,
        bool isRange)
    {
        if (!opts.TryParseStartAsTimestamp(out var startTs))
        {
            _log.Failed.LogError(
                "SeekFromTimestamp failed — cannot parse start value: {Start}",
                opts.StartOffsetOrTimestamp);
            return;
        }

        var epochMs = startTs.ToUnixTimeMilliseconds();
        var timestampedPartitions = partitions
            .Select(tp => new TopicPartitionTimestamp(tp, new Timestamp(epochMs, TimestampType.CreateTime)))
            .ToList();

        var results = consumer.OffsetsForTimes(timestampedPartitions, TimeSpan.FromSeconds(15));

        foreach (var r in results)
        {
            var targetOffset = r.Offset == Offset.Unset ? Offset.End : r.Offset;
            consumer.Seek(new TopicPartitionOffset(r.TopicPartition, targetOffset));
            _log.Timestamp.Resolved.LogInformation(
                "Seek timestamp resolved — topic: {Topic}, partition: {Partition}, timestamp: {Timestamp}, offset: {Offset}",
                r.TopicPartition.Topic, r.TopicPartition.Partition.Value, startTs, targetOffset);
        }

        _log.Applied.LogInformation("Seek from timestamp applied — timestamp: {Timestamp}", startTs);

        if (isRange)
            ConfigureStopBoundary(opts, partitions, boundaryState, consumer);
    }

    private void ConfigureStopBoundary<TKey, TValue>(
        ConsumerSeekOptions opts,
        List<TopicPartition> partitions,
        SeekBoundaryState boundaryState,
        IConsumer<TKey, TValue> consumer)
    {
        if (!opts.HasStopBoundary) return;

        if (opts.TryParseStopAsOffset(out var stopOffset))
        {
            boundaryState.SetStopOffset(stopOffset);
            _log.Range.Configured.LogInformation("Seek range stop offset configured — offset: {Offset}", stopOffset);
        }
        else if (opts.TryParseStopAsTimestamp(out var stopTs))
        {
            var epochMs = stopTs.ToUnixTimeMilliseconds();
            var timestampedPartitions = partitions
                .Select(tp => new TopicPartitionTimestamp(tp, new Timestamp(epochMs, TimestampType.CreateTime)))
                .ToList();
            var results = consumer.OffsetsForTimes(timestampedPartitions, TimeSpan.FromSeconds(15));
            var stopOffsets = results.ToDictionary(
                r => r.TopicPartition.Partition.Value,
                r => r.Offset == Offset.Unset ? long.MaxValue : r.Offset.Value);
            boundaryState.SetStopOffsets(stopOffsets);
            _log.Range.Configured.LogInformation("Seek range stop timestamp configured — timestamp: {Timestamp}", stopTs);
        }
    }
}

/// <summary>
/// Holds the stop boundary for Range-mode seeks.
/// The batch processor checks this after each message to know when to stop.
/// </summary>
public sealed class SeekBoundaryState
{
    private long? _stopOffset;
    private IReadOnlyDictionary<int, long>? _stopOffsets;

    public bool HasStopBoundary => _stopOffset.HasValue || _stopOffsets is not null;

    public void SetStopOffset(long offset) => _stopOffset = offset;

    public void SetStopOffsets(IReadOnlyDictionary<int, long> offsets) => _stopOffsets = offsets;

    public bool HasPassed(int partition, long offset)
    {
        if (_stopOffset.HasValue)
            return offset >= _stopOffset.Value;
        if (_stopOffsets is not null && _stopOffsets.TryGetValue(partition, out var partStop))
            return offset >= partStop;
        return false;
    }
}
