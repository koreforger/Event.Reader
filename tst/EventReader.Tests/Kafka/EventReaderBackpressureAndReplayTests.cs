using EventReader.Kafka;
using Event.Streaming.In.Seek;
using Event.Streaming.Processing.WorkStore;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace EventReader.Tests.Kafka;

public sealed class EventReaderBackpressureAndReplayTests
{
    [Fact]
    public void Backpressure_pauses_when_backlog_is_high()
    {
        var controller = new EventReaderBackpressureController(
            NullLogger<EventReaderBackpressureController>.Instance,
            new BackpressureConfig
            {
                PauseHighWatermark = 1.0,
                MaxClassifiedBacklog = 100,
            });

        var metrics = CreateMetrics(classifiedCount: 150);

        var shouldPause = controller.ShouldPause(metrics);

        Assert.True(shouldPause);
        Assert.True(controller.IsPaused);
    }

    [Fact]
    public void Backpressure_resumes_when_backlog_drops_below_low_watermark()
    {
        var controller = new EventReaderBackpressureController(
            NullLogger<EventReaderBackpressureController>.Instance,
            new BackpressureConfig
            {
                PauseHighWatermark = 1.0,
                ResumeLowWatermark = 0.5,
                MaxClassifiedBacklog = 100,
            });

        var highMetrics = CreateMetrics(classifiedCount: 150);
        controller.ShouldPause(highMetrics);
        Assert.True(controller.IsPaused);

        var lowMetrics = CreateMetrics(classifiedCount: 20);

        var shouldResume = controller.ShouldResume(lowMetrics);

        Assert.True(shouldResume);
        Assert.False(controller.IsPaused);
    }

    [Fact]
    public void Backpressure_does_not_pause_when_no_signals_above_threshold()
    {
        var controller = new EventReaderBackpressureController(
            NullLogger<EventReaderBackpressureController>.Instance,
            new BackpressureConfig
            {
                PauseHighWatermark = 1.0,
                MaxClassifiedBacklog = 10_000,
                MaxShardBacklog = 10_000,
                MaxReadyToOutputBacklog = 10_000,
            });

        var metrics = CreateMetrics(classifiedCount: 50);

        var shouldPause = controller.ShouldPause(metrics);

        Assert.False(shouldPause);
        Assert.False(controller.IsPaused);
    }

    [Fact]
    public async Task Replay_from_failed_creates_plan_and_tracks_progress()
    {
        var workStore = new TestWorkStore();
        var orchestrator = new EventReaderReplayOrchestrator(
            workStore,
            NullLogger<EventReaderReplayOrchestrator>.Instance);

        var request = new ReplayRequest(
            ReplayMode.FromFailed,
            ["test-topic"],
            ["test-source"],
            null,
            null,
            false,
            [100],
            "reprocess failures");

        var session = await orchestrator.StartReplayAsync(request, "test-user", CancellationToken.None);

        Assert.Equal("Running", session.State);
        Assert.Equal("test-user", session.RequestedBy);
        Assert.Equal(ReplayMode.FromFailed, session.Request.Mode);

        var result = await orchestrator.RequeueFailedAsync(
            session,
            [1001, 1002, 1003],
            CancellationToken.None);

        Assert.Equal(3, result.RequestedCount);
        Assert.True(orchestrator.ActiveSessions.ContainsKey(session.ReplayId));
    }

    [Fact]
    public async Task Replay_sessions_are_trackable()
    {
        var workStore = new TestWorkStore();
        var orchestrator = new EventReaderReplayOrchestrator(
            workStore,
            NullLogger<EventReaderReplayOrchestrator>.Instance);

        var session = await orchestrator.StartReplayAsync(
            new ReplayRequest(
                ReplayMode.FromKafka,
                ["topic-a"],
                ["source-a"],
                DateTimeOffset.UtcNow.AddHours(-1),
                null,
                false,
                [],
                "audit replay"),
            "operator-1",
            CancellationToken.None);

        Assert.Single(orchestrator.ActiveSessions);
        Assert.Contains(session.ReplayId, orchestrator.ActiveSessions.Keys);

        orchestrator.UpdateProgress(session.ReplayId, recordsScanned: 100, recordsEnqueued: 95);
        var updated = orchestrator.ActiveSessions[session.ReplayId];
        Assert.Equal(100, updated.RecordsScanned);
        Assert.Equal(95, updated.RecordsEnqueued);

        orchestrator.CompleteSession(session.ReplayId);
        var completed = orchestrator.ActiveSessions[session.ReplayId];
        Assert.Equal("Completed", completed.State);
    }

    [Fact]
    public void Multiple_backpressure_signals_combine_correctly()
    {
        var controller = new EventReaderBackpressureController(
            NullLogger<EventReaderBackpressureController>.Instance,
            new BackpressureConfig
            {
                PauseHighWatermark = 1.0,
                ResumeLowWatermark = 0.5,
                MaxClassifiedBacklog = 100,
                MaxShardBacklog = 100,
                MaxReadyToOutputBacklog = 100,
            });

        var moderateMetrics = CreateMetrics(
            classifiedCount: 80,
            shardBacklogs: new Dictionary<int, long> { [0] = 70, [1] = 60 },
            readyCount: 50);

        Assert.False(controller.ShouldPause(moderateMetrics));

        var mixedMetrics = CreateMetrics(
            classifiedCount: 80,
            shardBacklogs: new Dictionary<int, long> { [0] = 120, [1] = 30 },
            readyCount: 50);

        Assert.True(controller.ShouldPause(mixedMetrics));
        Assert.True(controller.IsPaused);

        var resumeMetrics = CreateMetrics(
            classifiedCount: 20,
            shardBacklogs: new Dictionary<int, long> { [0] = 10, [1] = 5 },
            readyCount: 5);

        Assert.True(controller.ShouldResume(resumeMetrics));
        Assert.False(controller.IsPaused);
    }

    [Fact]
    public void Create_seek_options_generates_correct_seek_modes()
    {
        var workStore = new TestWorkStore();
        var orchestrator = new EventReaderReplayOrchestrator(
            workStore,
            NullLogger<EventReaderReplayOrchestrator>.Instance);

        var rangeRequest = new ReplayRequest(
            ReplayMode.FromKafka,
            ["t1"],
            ["s1"],
            DateTimeOffset.Parse("2026-04-27T10:00:00Z"),
            DateTimeOffset.Parse("2026-04-27T11:00:00Z"),
            false,
            [],
            "range replay");

        var rangeOptions = orchestrator.CreateSeekOptions(rangeRequest);
        Assert.Equal(SeekMode.Range, rangeOptions.Mode);
        Assert.NotNull(rangeOptions.StartOffsetOrTimestamp);
        Assert.NotNull(rangeOptions.StopOffsetOrTimestamp);

        var timestampRequest = new ReplayRequest(
            ReplayMode.FromKafka,
            ["t1"],
            ["s1"],
            DateTimeOffset.Parse("2026-04-27T10:00:00Z"),
            null,
            false,
            [],
            "timestamp replay");

        var timestampOptions = orchestrator.CreateSeekOptions(timestampRequest);
        Assert.Equal(SeekMode.FromTimestamp, timestampOptions.Mode);
        Assert.NotNull(timestampOptions.StartOffsetOrTimestamp);
        Assert.Null(timestampOptions.StopOffsetOrTimestamp);

        var noneRequest = new ReplayRequest(
            ReplayMode.FromKafka,
            ["t1"],
            ["s1"],
            null,
            null,
            false,
            [],
            "none replay");

        var noneOptions = orchestrator.CreateSeekOptions(noneRequest);
        Assert.Equal(SeekMode.None, noneOptions.Mode);
    }

    private static FasterEventReaderWorkStoreDetailedMetrics CreateMetrics(
        long classifiedCount = 0,
        IReadOnlyDictionary<int, long>? shardBacklogs = null,
        long readyCount = 0,
        double avgEnqueueMs = 0,
        double avgShardLeaseMs = 0,
        double avgOutputLeaseMs = 0,
        long diskFree = 10_000_000_000,
        long diskTotal = 100_000_000_000)
    {
        var stateCounts = new Dictionary<WorkState, long>
        {
            [WorkState.Classified] = classifiedCount,
            [WorkState.Processing] = 0,
            [WorkState.ReadyToOutput] = readyCount,
            [WorkState.Publishing] = 0,
            [WorkState.Completed] = 0,
            [WorkState.RetryPending] = 0,
            [WorkState.Failed] = 0,
            [WorkState.Suppressed] = 0,
        };

        return new FasterEventReaderWorkStoreDetailedMetrics(
            stateCounts,
            shardBacklogs ?? new Dictionary<int, long>(),
            new HashSet<long>(),
            null,
            diskFree,
            diskTotal,
            avgEnqueueMs,
            avgShardLeaseMs,
            avgOutputLeaseMs,
            null);
    }

    private sealed class TestWorkStore : IEventReaderWorkStore
    {
        private readonly List<long> _enqueued = [];

        public Task EnqueueClassifiedAsync(ClassifiedWorkItem item, CancellationToken ct)
        {
            _enqueued.Add(item.WorkItemId);
            return Task.CompletedTask;
        }

        public Task<IReadOnlyList<WorkLease>> LeaseShardBatchAsync(
            int shardId, int maxItems, TimeSpan leaseDuration, CancellationToken ct) =>
            Task.FromResult<IReadOnlyList<WorkLease>>([]);

        public Task MarkReadyToOutputAsync(
            long workItemId, byte[] outputPayload, CancellationToken ct) =>
            Task.CompletedTask;

        public Task<IReadOnlyList<OutputLease>> LeaseOutputBatchAsync(
            int maxItems, TimeSpan leaseDuration, CancellationToken ct) =>
            Task.FromResult<IReadOnlyList<OutputLease>>([]);

        public Task MarkCompletedAsync(
            long workItemId, OutputWriteReceipt receipt, CancellationToken ct) =>
            Task.CompletedTask;

        public Task MarkRetryPendingAsync(
            long workItemId, RetryReason reason, CancellationToken ct) =>
            Task.CompletedTask;

        public Task MarkFailedAsync(
            long workItemId, string reason, CancellationToken ct) =>
            Task.CompletedTask;

        public Task ReleaseExpiredLeasesAsync(CancellationToken ct) =>
            Task.CompletedTask;

        public Task<ReplayPlan> CreateReplayPlanAsync(
            ReplayRequest request, CancellationToken ct) =>
            Task.FromResult(new ReplayPlan(Guid.NewGuid(), request, [], DateTimeOffset.UtcNow));

        public Task<UnstuckResult> RequeueAsync(
            UnstuckRequest request, CancellationToken ct) =>
            Task.FromResult(new UnstuckResult(
                request.WorkItemIds.Count,
                request.WorkItemIds.Count,
                []));

        public WorkStoreMetricsSnapshot GetWorkStoreMetrics() =>
            new(new Dictionary<WorkState, long>(), new Dictionary<int, long>(),
                new HashSet<long>(), null, 0, 0, 0, 0, 0, null);

        public IReadOnlySet<long> GetActiveRuntimeModelVersions() => new HashSet<long>();
    }
}
