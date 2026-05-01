using EventReader.Monitoring;

namespace EventReader.Tests;

public sealed class EventReaderMetricsAccumulatorTests
{
    [Fact]
    public void InitialCountsAreZero()
    {
        var acc = new EventReaderMetricsAccumulator();
        Assert.Equal(0, acc.TotalProcessed);
        Assert.Equal(0, acc.TotalErrors);
        Assert.Equal(0, acc.StopBoundarySkipped);
    }

    [Fact]
    public void RecordProcessed_IncrementsProcessed()
    {
        var acc = new EventReaderMetricsAccumulator();
        acc.RecordProcessed();
        acc.RecordProcessed();
        Assert.Equal(2, acc.TotalProcessed);
        Assert.Equal(0, acc.TotalErrors);
    }

    [Fact]
    public void RecordProcessed_WithLatency_RecordsLatencyAndLastProcessedTimestamp()
    {
        var acc = new EventReaderMetricsAccumulator();
        var processedAt = DateTimeOffset.Parse("2026-04-28T05:00:00Z");

        acc.RecordProcessed(TimeSpan.FromMilliseconds(10), processedAt);
        acc.RecordProcessed(TimeSpan.FromMilliseconds(30), processedAt.AddSeconds(1));

        Assert.Equal(2, acc.TotalProcessed);
        Assert.Equal(20, acc.AverageLatencyMs);
        Assert.Equal(30, acc.P95LatencyMs);
        Assert.Equal(30, acc.P99LatencyMs);
        Assert.Equal(processedAt.AddSeconds(1), acc.LastProcessedAtUtc);
    }

    [Fact]
    public void RecordBatch_WithTimestamp_RecordsLastMessageReceivedTimestamp()
    {
        var acc = new EventReaderMetricsAccumulator();
        var receivedAt = DateTimeOffset.Parse("2026-04-28T05:00:00Z");

        acc.RecordBatch(3, receivedAt);

        Assert.Equal(1, acc.TotalBatches);
        Assert.Equal(3, acc.TotalMessagesReceived);
        Assert.Equal(receivedAt, acc.LastMessageReceivedAtUtc);
    }

    [Fact]
    public void RecordError_IncrementsErrors()
    {
        var acc = new EventReaderMetricsAccumulator();
        acc.RecordError();
        Assert.Equal(1, acc.TotalErrors);
        Assert.Equal(0, acc.TotalProcessed);
    }

    [Fact]
    public void RecordStopBoundarySkip_IncrementsSkipped()
    {
        var acc = new EventReaderMetricsAccumulator();
        acc.RecordStopBoundarySkip();
        Assert.Equal(1, acc.StopBoundarySkipped);
    }

    [Fact]
    public void IndependentCounters_DoNotInterfere()
    {
        var acc = new EventReaderMetricsAccumulator();
        acc.RecordProcessed();
        acc.RecordError();
        acc.RecordStopBoundarySkip();
        Assert.Equal(1, acc.TotalProcessed);
        Assert.Equal(1, acc.TotalErrors);
        Assert.Equal(1, acc.StopBoundarySkipped);
    }
}
