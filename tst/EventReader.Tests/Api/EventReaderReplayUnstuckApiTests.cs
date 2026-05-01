using System.Collections.Concurrent;
using EventReader.Api;
using Event.Streaming.Processing.WorkStore;
using Microsoft.AspNetCore.Mvc;

namespace EventReader.Tests;

public sealed class EventReaderReplayUnstuckApiTests
{
    [Fact]
    public async Task ListReplaySessions_ReturnsEmptyInitially()
    {
        ReplayController.ClearSessions();
        var store = new TestEventReaderWorkStore();
        var controller = new ReplayController(store);

        var result = Assert.IsType<OkObjectResult>(controller.ListSessions());
        var sessions = Assert.IsAssignableFrom<IReadOnlyList<ReplaySession>>(result.Value);
        Assert.Empty(sessions);
    }

    [Fact]
    public async Task CreateReplay_ReturnsSessionWithId()
    {
        var store = new TestEventReaderWorkStore();
        var controller = new ReplayController(store);
        var request = new ReplayRequest(
            ReplayMode.FromKafka,
            ["input-topic"],
            ["provider-a-payments"],
            DateTimeOffset.UtcNow.AddHours(-1),
            DateTimeOffset.UtcNow,
            false,
            [123, 456],
            "Replay failed processing window");

        var result = Assert.IsType<OkObjectResult>(await controller.CreateReplay(request, CancellationToken.None));
        var session = Assert.IsType<ReplaySession>(result.Value);

        Assert.NotEqual(Guid.Empty, session.ReplayId);
        Assert.Equal("Created", session.Status);
        Assert.Same(request, session.Request);
    }

    [Fact]
    public async Task CancelReplay_MarksSessionCancelled()
    {
        var store = new TestEventReaderWorkStore();
        var controller = new ReplayController(store);
        var request = new ReplayRequest(
            ReplayMode.FromKafka,
            ["input-topic"],
            ["provider-a-payments"],
            null,
            null,
            false,
            [1],
            "Test replay");

        var createResult = Assert.IsType<OkObjectResult>(await controller.CreateReplay(request, CancellationToken.None));
        var session = Assert.IsType<ReplaySession>(createResult.Value);

        var cancelResult = Assert.IsType<OkObjectResult>(controller.CancelSession(session.ReplayId));
        var cancelled = Assert.IsType<ReplaySession>(cancelResult.Value);

        Assert.Equal("Cancelled", cancelled.Status);
        Assert.Equal(session.ReplayId, cancelled.ReplayId);
    }

    [Fact]
    public async Task ListWorkItems_WithFilter_ReturnsItems()
    {
        var store = new TestEventReaderWorkStore();
        var controller = new WorkController(store);

        var result = Assert.IsType<OkObjectResult>(controller.ListWork(
            state: WorkState.Failed,
            functionId: null,
            shardId: null,
            sourceTopic: null,
            sourcePartition: null,
            sourceOffsetMin: null,
            sourceOffsetMax: null,
            workItemIdMin: null,
            workItemIdMax: null,
            olderThanSeconds: null));

        Assert.NotNull(result.Value);
    }

    [Fact]
    public async Task RequeueFailedWork_DelegatesToStore()
    {
        var store = new TestEventReaderWorkStore();
        var controller = new WorkController(store);
        var request = new WorkIdsRequest(
            [1001, 1002, 1003],
            "test-user",
            "Testing manual requeue");

        var result = Assert.IsType<OkObjectResult>(await controller.RequeueWork(request, CancellationToken.None));
        var unstuckResult = Assert.IsType<UnstuckResult>(result.Value);

        Assert.Equal(3, unstuckResult.RequestedCount);
    }

    [Fact]
    public async Task ReleaseExpiredLeases_DelegatesToStore()
    {
        var store = new TestEventReaderWorkStore();
        var controller = new WorkController(store);

        var result = Assert.IsType<OkObjectResult>(await controller.ReleaseExpiredLeases(CancellationToken.None));

        Assert.True(store.ReleaseExpiredLeasesCalled);
    }

    private sealed class TestEventReaderWorkStore : IEventReaderWorkStore
    {
        public bool ReleaseExpiredLeasesCalled { get; private set; }

        public Task EnqueueClassifiedAsync(ClassifiedWorkItem item, CancellationToken ct)
        {
            return Task.CompletedTask;
        }

        public Task<IReadOnlyList<WorkLease>> LeaseShardBatchAsync(int shardId, int maxItems, TimeSpan leaseDuration, CancellationToken ct)
        {
            return Task.FromResult<IReadOnlyList<WorkLease>>([]);
        }

        public Task MarkReadyToOutputAsync(long workItemId, byte[] outputPayload, CancellationToken ct)
        {
            return Task.CompletedTask;
        }

        public Task<IReadOnlyList<OutputLease>> LeaseOutputBatchAsync(int maxItems, TimeSpan leaseDuration, CancellationToken ct)
        {
            return Task.FromResult<IReadOnlyList<OutputLease>>([]);
        }

        public Task MarkCompletedAsync(long workItemId, OutputWriteReceipt receipt, CancellationToken ct)
        {
            return Task.CompletedTask;
        }

        public Task MarkRetryPendingAsync(long workItemId, RetryReason reason, CancellationToken ct)
        {
            return Task.CompletedTask;
        }

        public Task MarkFailedAsync(long workItemId, string reason, CancellationToken ct)
        {
            return Task.CompletedTask;
        }

        public Task ReleaseExpiredLeasesAsync(CancellationToken ct)
        {
            ReleaseExpiredLeasesCalled = true;
            return Task.CompletedTask;
        }

        public Task<ReplayPlan> CreateReplayPlanAsync(ReplayRequest request, CancellationToken ct)
        {
            var plan = new ReplayPlan(
                Guid.NewGuid(),
                request,
                [],
                DateTimeOffset.UtcNow);
            return Task.FromResult(plan);
        }

        public Task<UnstuckResult> RequeueAsync(UnstuckRequest request, CancellationToken ct)
        {
            var result = new UnstuckResult(
                request.WorkItemIds.Count,
                0,
                []);
            return Task.FromResult(result);
        }

        public WorkStoreMetricsSnapshot GetWorkStoreMetrics() =>
            new(new Dictionary<WorkState, long>(), new Dictionary<int, long>(),
                new HashSet<long>(), null, 0, 0, 0, 0, 0, null);

        public IReadOnlySet<long> GetActiveRuntimeModelVersions() => new HashSet<long>();
    }
}
