using System.Collections.Concurrent;
using Event.Streaming.In.Seek;
using Event.Streaming.Processing.WorkStore;
using Microsoft.Extensions.Logging;

namespace EventReader.Kafka;

public sealed class EventReaderReplayOrchestrator
{
    private readonly IEventReaderWorkStore _workStore;
    private readonly ILogger<EventReaderReplayOrchestrator> _log;
    private readonly ConcurrentDictionary<Guid, ReplaySession> _sessions = new();

    public EventReaderReplayOrchestrator(
        IEventReaderWorkStore workStore,
        ILogger<EventReaderReplayOrchestrator> log)
    {
        _workStore = workStore;
        _log = log;
    }

    public IReadOnlyDictionary<Guid, ReplaySession> ActiveSessions => _sessions;

    public async Task<ReplaySession> StartReplayAsync(
        ReplayRequest request,
        string requestedBy,
        CancellationToken ct)
    {
        var replayId = Guid.NewGuid();
        var session = new ReplaySession(
            replayId,
            requestedBy,
            request,
            DateTimeOffset.UtcNow,
            State: "Running",
            RecordsScanned: 0,
            RecordsEnqueued: 0,
            RecordsCompleted: 0,
            RecordsFailed: 0);

        _sessions[replayId] = session;

        switch (request.Mode)
        {
            case ReplayMode.FromKafka:
                _log.LogInformation(
                    "Replay session {ReplayId} mode=FromKafka topics={Topics} started by {RequestedBy}",
                    replayId, string.Join(",", request.Topics), requestedBy);
                break;

            case ReplayMode.FromStore:
                _log.LogInformation(
                    "Replay session {ReplayId} mode=FromStore started by {RequestedBy}",
                    replayId, requestedBy);
                var plan = await _workStore.CreateReplayPlanAsync(request, ct).ConfigureAwait(false);
                _sessions[replayId] = session with { RecordsScanned = plan.WorkItemIds.Count };
                break;

            case ReplayMode.FromFailed:
                _log.LogInformation(
                    "Replay session {ReplayId} mode=FromFailed started by {RequestedBy}",
                    replayId, requestedBy);
                break;
        }

        return _sessions[replayId];
    }

    public async Task<UnstuckResult> RequeueFailedAsync(
        ReplaySession session,
        IReadOnlyList<long> failedWorkItemIds,
        CancellationToken ct)
    {
        var request = new UnstuckRequest(
            failedWorkItemIds,
            session.RequestedBy,
            session.Request.Reason);

        var result = await _workStore.RequeueAsync(request, ct).ConfigureAwait(false);

        var requested = failedWorkItemIds.Count;
        _sessions[session.ReplayId] = session with
        {
            RecordsScanned = session.RecordsScanned + requested,
            RecordsEnqueued = session.RecordsEnqueued + result.MovedCount,
            RecordsFailed = session.RecordsFailed + result.SkippedWorkItemIds.Count,
        };

        _log.LogInformation(
            "Replay session {ReplayId} requeue: {Requested} requested, {Moved} moved, {Skipped} skipped",
            session.ReplayId, requested, result.MovedCount, result.SkippedWorkItemIds.Count);

        return result;
    }

    public ConsumerSeekOptions CreateSeekOptions(ReplayRequest request)
    {
        if (request.FromUtc.HasValue && request.ToUtc.HasValue)
        {
            return new ConsumerSeekOptions
            {
                Mode = SeekMode.Range,
                StartOffsetOrTimestamp = request.FromUtc.Value.ToString("O"),
                StopOffsetOrTimestamp = request.ToUtc.Value.ToString("O"),
            };
        }

        if (request.FromUtc.HasValue)
        {
            return new ConsumerSeekOptions
            {
                Mode = SeekMode.FromTimestamp,
                StartOffsetOrTimestamp = request.FromUtc.Value.ToString("O"),
            };
        }

        return new ConsumerSeekOptions { Mode = SeekMode.None };
    }

    public void UpdateProgress(
        Guid replayId,
        long recordsScanned = 0,
        long recordsEnqueued = 0,
        long recordsCompleted = 0,
        long recordsFailed = 0)
    {
        if (_sessions.TryGetValue(replayId, out var session))
        {
            _sessions[replayId] = session with
            {
                RecordsScanned = session.RecordsScanned + recordsScanned,
                RecordsEnqueued = session.RecordsEnqueued + recordsEnqueued,
                RecordsCompleted = session.RecordsCompleted + recordsCompleted,
                RecordsFailed = session.RecordsFailed + recordsFailed,
            };
        }
    }

    public void CompleteSession(Guid replayId)
    {
        if (_sessions.TryGetValue(replayId, out var session))
        {
            _sessions[replayId] = session with
            {
                State = session.RecordsFailed > 0 ? "PartiallyFailed" : "Completed",
            };
        }
    }

    public void FailSession(Guid replayId, string reason)
    {
        if (_sessions.TryGetValue(replayId, out var session))
        {
            _sessions[replayId] = session with { State = $"Failed: {reason}" };
        }
    }
}

public sealed record ReplaySession(
    Guid ReplayId,
    string RequestedBy,
    ReplayRequest Request,
    DateTimeOffset StartedUtc,
    string State,
    long RecordsScanned,
    long RecordsEnqueued,
    long RecordsCompleted,
    long RecordsFailed);
