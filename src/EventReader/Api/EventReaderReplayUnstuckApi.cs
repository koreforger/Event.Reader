using System.Collections.Concurrent;
using Event.Streaming.Processing.WorkStore;
using Microsoft.AspNetCore.Mvc;

namespace EventReader.Api;

[ApiController]
[Route("api/eventreader/replay")]
public sealed class ReplayController : ControllerBase
{
    private static readonly ConcurrentDictionary<Guid, ReplaySession> _sessions = new();
    private readonly IEventReaderWorkStore _workStore;

    public static void ClearSessions() => _sessions.Clear();

    public ReplayController(IEventReaderWorkStore workStore)
    {
        _workStore = workStore;
    }

    [HttpGet]
    public IActionResult ListSessions()
    {
        var sessions = _sessions.Values
            .OrderByDescending(s => s.CreatedUtc)
            .ToList();
        return Ok(sessions);
    }

    [HttpPost]
    public async Task<IActionResult> CreateReplay(
        [FromBody] ReplayRequest request,
        CancellationToken cancellationToken)
    {
        var plan = await _workStore.CreateReplayPlanAsync(request, cancellationToken).ConfigureAwait(false);
        var session = new ReplaySession
        {
            ReplayId = plan.ReplayId,
            Request = request,
            Status = "Created",
            CreatedUtc = plan.CreatedUtc,
            UpdatedUtc = plan.CreatedUtc,
        };
        _sessions[plan.ReplayId] = session;
        return Ok(session);
    }

    [HttpGet("{replayId:guid}")]
    public IActionResult GetSession(Guid replayId)
    {
        if (_sessions.TryGetValue(replayId, out var session))
        {
            return Ok(session);
        }

        return NotFound(new { replayId, Message = "Replay session not found." });
    }

    [HttpPost("{replayId:guid}/cancel")]
    public IActionResult CancelSession(Guid replayId)
    {
        if (!_sessions.TryGetValue(replayId, out var session))
        {
            return NotFound(new { replayId, Message = "Replay session not found." });
        }

        if (session.Status is "Cancelled" or "Completed")
        {
            return Conflict(new { replayId, session.Status, Message = $"Session is already {session.Status}." });
        }

        session.Status = "Cancelled";
        session.UpdatedUtc = DateTimeOffset.UtcNow;
        return Ok(session);
    }
}

[ApiController]
[Route("api/eventreader/work")]
public sealed class WorkController : ControllerBase
{
    private readonly IEventReaderWorkStore _workStore;
    private readonly FasterEventReaderWorkStore? _fasterStore;

    public WorkController(IEventReaderWorkStore workStore)
    {
        _workStore = workStore;
        _fasterStore = workStore as FasterEventReaderWorkStore;
    }

    [HttpGet]
    public IActionResult ListWork(
        [FromQuery] WorkState? state,
        [FromQuery] int? functionId,
        [FromQuery] int? shardId,
        [FromQuery] string? sourceTopic,
        [FromQuery] int? sourcePartition,
        [FromQuery] long? sourceOffsetMin,
        [FromQuery] long? sourceOffsetMax,
        [FromQuery] long? workItemIdMin,
        [FromQuery] long? workItemIdMax,
        [FromQuery] int? olderThanSeconds)
    {
        if (_fasterStore is null)
        {
            return Ok(new { Items = Array.Empty<object>(), TotalCount = 0 });
        }

        var metrics = _fasterStore.GetDetailedMetrics();
        var items = new List<WorkItemSummary>();

        if (state.HasValue)
        {
            metrics.BacklogByState.TryGetValue(state.Value, out var count);
            items.Add(new WorkItemSummary
            {
                State = state.Value.ToString(),
                Count = count,
            });
        }
        else
        {
            foreach (var (ws, count) in metrics.BacklogByState)
            {
                items.Add(new WorkItemSummary
                {
                    State = ws.ToString(),
                    Count = count,
                });
            }
        }

        if (shardId.HasValue)
        {
            metrics.BacklogByShard.TryGetValue(shardId.Value, out var shardCount);
            foreach (var item in items)
            {
                item.ShardCount = shardCount;
            }
        }

        return Ok(new
        {
            Items = items,
            TotalCount = items.Sum(i => i.Count),
            metrics.OldestUnfinishedAge,
        });
    }

    [HttpGet("{workItemId:long}")]
    public IActionResult InspectWork(long workItemId)
    {
        if (_fasterStore is null)
        {
            return NotFound(new { workItemId, Message = "Work store does not support item-level inspection." });
        }

        var metrics = _fasterStore.GetDetailedMetrics();
        return Ok(new
        {
            workItemId,
            Message = "Detailed item inspection is not available via the work store API.",
            StoreMetrics = new
            {
                BacklogByState = metrics.BacklogByState,
                BacklogByShard = metrics.BacklogByShard,
                metrics.OldestUnfinishedAge,
                metrics.AverageEnqueueLatencyMs,
                metrics.AverageShardLeaseLatencyMs,
                metrics.AverageOutputLeaseLatencyMs,
                metrics.LastCheckpointTime,
            },
        });
    }

    [HttpPost("requeue")]
    public async Task<IActionResult> RequeueWork(
        [FromBody] WorkIdsRequest request,
        CancellationToken cancellationToken)
    {
        var unstuckRequest = new UnstuckRequest(
            request.WorkItemIds,
            request.RequestedBy ?? "api",
            request.Reason ?? "Manual requeue");

        var result = await _workStore.RequeueAsync(unstuckRequest, cancellationToken).ConfigureAwait(false);
        return Ok(result);
    }

    [HttpPost("skip")]
    public async Task<IActionResult> SkipWork(
        [FromBody] WorkIdsRequest request,
        CancellationToken cancellationToken)
    {
        var unstuckRequest = new UnstuckRequest(
            request.WorkItemIds,
            request.RequestedBy ?? "api",
            request.Reason ?? "Manual skip");

        var result = await _workStore.RequeueAsync(unstuckRequest, cancellationToken).ConfigureAwait(false);
        return Ok(result);
    }

    [HttpPost("retry-output")]
    public async Task<IActionResult> RetryOutput(
        [FromBody] WorkIdsRequest request,
        CancellationToken cancellationToken)
    {
        var unstuckRequest = new UnstuckRequest(
            request.WorkItemIds,
            request.RequestedBy ?? "api",
            request.Reason ?? "Manual retry-output");

        var result = await _workStore.RequeueAsync(unstuckRequest, cancellationToken).ConfigureAwait(false);
        return Ok(result);
    }

    [HttpPost("release-expired-leases")]
    public async Task<IActionResult> ReleaseExpiredLeases(CancellationToken cancellationToken)
    {
        await _workStore.ReleaseExpiredLeasesAsync(cancellationToken).ConfigureAwait(false);
        return Ok(new { Message = "Expired leases released." });
    }
}

public sealed class ReplaySession
{
    public Guid ReplayId { get; init; }
    public ReplayRequest? Request { get; init; }
    public string Status { get; set; } = "Created";
    public long RecordsScanned { get; set; }
    public long RecordsEnqueued { get; set; }
    public long RecordsCompleted { get; set; }
    public long RecordsFailed { get; set; }
    public DateTimeOffset CreatedUtc { get; init; }
    public DateTimeOffset UpdatedUtc { get; set; }
}

public sealed class WorkItemSummary
{
    public string State { get; init; } = "";
    public long Count { get; set; }
    public long ShardCount { get; set; }
}

public sealed record WorkIdsRequest(
    IReadOnlyList<long> WorkItemIds,
    string? RequestedBy,
    string? Reason);
