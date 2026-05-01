using EventReader.Seek;

namespace EventReader.Tests;

public sealed class SeekBoundaryStateTests
{
    [Fact]
    public void HasStopBoundary_Initially_IsFalse()
    {
        var state = new SeekBoundaryState();
        Assert.False(state.HasStopBoundary);
    }

    [Fact]
    public void SetStopOffset_SetsHasStopBoundaryTrue()
    {
        var state = new SeekBoundaryState();
        state.SetStopOffset(500L);
        Assert.True(state.HasStopBoundary);
    }

    [Fact]
    public void SetStopOffsets_SetsHasStopBoundaryTrue()
    {
        var state = new SeekBoundaryState();
        state.SetStopOffsets(new Dictionary<int, long> { [0] = 100L });
        Assert.True(state.HasStopBoundary);
    }

    [Fact]
    public void HasPassed_WithSingleOffset_ReturnsTrueAtBoundary()
    {
        var state = new SeekBoundaryState();
        state.SetStopOffset(100L);
        Assert.False(state.HasPassed(0, 99L));
        Assert.True(state.HasPassed(0, 100L));
        Assert.True(state.HasPassed(0, 101L));
    }

    [Fact]
    public void HasPassed_WithPerPartitionOffsets_ReturnsTruePerPartition()
    {
        var state = new SeekBoundaryState();
        state.SetStopOffsets(new Dictionary<int, long> { [0] = 100L, [1] = 200L });
        Assert.False(state.HasPassed(0, 99L));
        Assert.True(state.HasPassed(0, 100L));
        Assert.False(state.HasPassed(1, 199L));
        Assert.True(state.HasPassed(1, 200L));
    }

    [Fact]
    public void HasPassed_WithNoStopBoundary_ReturnsFalse()
    {
        var state = new SeekBoundaryState();
        Assert.False(state.HasPassed(0, long.MaxValue));
    }

    [Fact]
    public void HasPassed_WithPerPartitionOffsets_UnknownPartitionReturnsFalse()
    {
        var state = new SeekBoundaryState();
        state.SetStopOffsets(new Dictionary<int, long> { [0] = 100L });
        Assert.False(state.HasPassed(99, 999L));
    }
}
