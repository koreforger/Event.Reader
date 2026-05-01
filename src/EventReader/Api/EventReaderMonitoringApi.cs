using EventReader.Configuration;
using EventReader.Kafka;
using EventReader.Monitoring;
using Event.Streaming.Processing.Monitoring;
using Event.Streaming.Processing.WorkStore;
using Microsoft.AspNetCore.Mvc;

namespace EventReader.Api;

[ApiController]
[Route("api/eventreader")]
public sealed class EventReaderMonitoringController : ControllerBase
{
    private static readonly Lock s_backpressureLock = new();
    private static bool s_manuallyPaused;
    private static string? s_lastPauseReason;
    private static DateTimeOffset? s_lastPauseUtc;
    private static DateTimeOffset? s_lastResumeUtc;
    private static long s_pauseCount;
    private static long s_resumeCount;

    private readonly EventReaderMonitoringSnapshot _snapshot;
    private readonly EventReaderMetricsAccumulator _accumulator;
    private readonly IEventReaderWorkStore _workStore;
    private readonly EventReaderShardWorkerPoolOptions _workerOptions;
    private readonly IEventReaderRuntimeOptionsProvider _optionsProvider;

    public EventReaderMonitoringController(
        EventReaderMonitoringSnapshot snapshot,
        EventReaderMetricsAccumulator accumulator,
        IEventReaderWorkStore workStore,
        EventReaderShardWorkerPoolOptions workerOptions,
        IEventReaderRuntimeOptionsProvider optionsProvider)
    {
        _snapshot = snapshot;
        _accumulator = accumulator;
        _workStore = workStore;
        _workerOptions = workerOptions;
        _optionsProvider = optionsProvider;
    }

    // ---- ER-010.1 Status API ----

    [HttpGet("status")]
    public IActionResult GetStatus()
    {
        var metrics = GetWorkStoreMetrics();
        var uptimeSeconds = Math.Max(1, (DateTimeOffset.UtcNow - StartedAt).TotalSeconds);
        var pipeline = _snapshot.PipelineMetrics;

        return Ok(new
        {
            state = _snapshot.Health switch
            {
                HealthStatus.Healthy => "Running",
                HealthStatus.Degraded => "Degraded",
                HealthStatus.Unhealthy => "Stopped",
                _ => "Running"
            },
            runtimeModelVersion = long.TryParse(_optionsProvider.Current.Version!.Replace(".", ""), out var v) ? v : 0L,
            consumerState = _snapshot.KafkaMetrics.ConsumerState,
            backpressureState = GetBackpressureState(),
            recordsInPerSecond = Math.Round(_accumulator.TotalMessagesReceived / uptimeSeconds, 1),
            recordsCompletedPerSecond = pipeline.MessagesPerSecond,
            oldestBacklogAgeSeconds = Math.Round((metrics?.OldestUnfinishedAge?.TotalSeconds) ?? 0, 1),
            activeWorkers = _workerOptions.WorkerCount,
            logicalShards = _workerOptions.LogicalShardCount,
            storageHealthy = metrics is not null && metrics.DiskFreeBytes > 0,
            outputHealthy = _snapshot.Health is HealthStatus.Healthy or HealthStatus.Unknown
        });
    }

    // ---- ER-010.2 Metrics API ----

    [HttpGet("metrics")]
    public IActionResult GetMetrics()
    {
        var metrics = GetWorkStoreMetrics();
        var pipeline = _snapshot.PipelineMetrics;
        var uptimeSeconds = Math.Max(1, (DateTimeOffset.UtcNow - StartedAt).TotalSeconds);
        var classificationCounts = _accumulator.ClassificationCounts;

        var recordsInPerSec = Math.Round(_accumulator.TotalMessagesReceived / uptimeSeconds, 1);
        var classifiedPerSec = Math.Round(classificationCounts.Values.Sum() / uptimeSeconds, 1);
        var processedPerSec = Math.Round(_accumulator.TotalProcessed / uptimeSeconds, 1);
        var failedPerSec = Math.Round(_accumulator.TotalErrors / uptimeSeconds, 1);

        var backlogByState = metrics?.BacklogByState ?? new Dictionary<WorkState, long>();
        var backlogByShard = metrics?.BacklogByShard ?? new Dictionary<int, long>();

        return Ok(new
        {
            throughput = new
            {
                recordsInPerSec,
                classifiedPerSec,
                enqueuedPerSec = classifiedPerSec,
                processedPerSec,
                readyToOutputPerSec = processedPerSec,
                publishedPerSec = processedPerSec,
                completedPerSec = processedPerSec,
                failedPerSec,
                suppressedPerSec = 0.0
            },
            latency = new
            {
                endToEndLatencyMs = new
                {
                    p50 = pipeline.AvgLatencyMs,
                    p95 = pipeline.P95LatencyMs,
                    p99 = pipeline.P99LatencyMs
                },
                classificationLatencyMs = new { p50 = 0.0, p95 = 0.0, p99 = 0.0 },
                storeEnqueueLatencyMs = new { p50 = metrics?.AverageEnqueueLatencyMs ?? 0, p95 = 0.0, p99 = 0.0 },
                shardLeaseLatencyMs = new { p50 = metrics?.AverageShardLeaseLatencyMs ?? 0, p95 = 0.0, p99 = 0.0 },
                outputLeaseLatencyMs = new { p50 = metrics?.AverageOutputLeaseLatencyMs ?? 0, p95 = 0.0, p99 = 0.0 }
            },
            backlog = new
            {
                classifiedBacklog = backlogByState.GetValueOrDefault(WorkState.Classified, 0),
                processingBacklog = backlogByState.GetValueOrDefault(WorkState.Processing, 0),
                readyToOutputBacklog = backlogByState.GetValueOrDefault(WorkState.ReadyToOutput, 0),
                publishingBacklog = backlogByState.GetValueOrDefault(WorkState.Publishing, 0),
                retryBacklog = backlogByState.GetValueOrDefault(WorkState.RetryPending, 0),
                failedBacklog = backlogByState.GetValueOrDefault(WorkState.Failed, 0),
                backlogByShard = backlogByShard.OrderByDescending(kvp => kvp.Value).Take(10)
                    .Select(kvp => new { shardId = kvp.Key, count = kvp.Value }),
                oldestBacklogAgeSeconds = Math.Round((metrics?.OldestUnfinishedAge?.TotalSeconds) ?? 0, 1)
            },
            hotSpots = new
            {
                topFunctionsByRate = classificationCounts
                    .OrderByDescending(kvp => kvp.Value)
                    .Take(10)
                    .Select(kvp => new { functionId = kvp.Key, count = kvp.Value }),
                topFunctionsByLatency = Array.Empty<object>(),
                topShardsByBacklog = backlogByShard
                    .OrderByDescending(kvp => kvp.Value)
                    .Take(10)
                    .Select(kvp => new { shardId = kvp.Key, count = kvp.Value }),
                topShardsByOldestAge = Array.Empty<object>()
            },
            backpressure = new
            {
                pauseCount = Interlocked.Read(ref s_pauseCount),
                resumeCount = Interlocked.Read(ref s_resumeCount),
                currentlyPaused = s_manuallyPaused,
                lastPauseReason = s_lastPauseReason,
                lastResumeUtc = s_lastResumeUtc
            },
            storage = new
            {
                fasterPath = "data/eventreader/faster",
                diskFreeBytes = metrics?.DiskFreeBytes ?? 0,
                diskTotalBytes = metrics?.DiskTotalBytes ?? 0,
                lastCheckpointUtc = metrics?.LastCheckpointTime
            }
        });
    }

    // ---- ER-010.3 Flow API ----

    private static IReadOnlyList<object> CreateFlowNodes(
        FasterEventReaderWorkStoreDetailedMetrics? metrics,
        long inCount,
        double ratePerSecond)
    {
        var backlogByState = metrics?.BacklogByState ?? new Dictionary<WorkState, long>();

        return new object[]
        {
            new
            {
                nodeId = "kafka-input",
                name = "Kafka Input",
                kind = "input",
                parentNodeId = (string?)null,
                inCount,
                outCount = inCount,
                failedCount = 0L,
                currentInFlight = 0L,
                backlogCount = 0L,
                currentRatePerSecond = ratePerSecond,
                averageLatencyMs = 0.0,
                p50LatencyMs = 0.0,
                p95LatencyMs = 0.0,
                p99LatencyMs = 0.0,
                maxLatencyMs = 0.0
            },
            new
            {
                nodeId = "fast-classification",
                name = "Fast Classification",
                kind = "stage",
                parentNodeId = (string?)null,
                inCount,
                outCount = inCount,
                failedCount = 0L,
                currentInFlight = 0L,
                backlogCount = 0L,
                currentRatePerSecond = ratePerSecond,
                averageLatencyMs = 0.0,
                p50LatencyMs = 0.0,
                p95LatencyMs = 0.0,
                p99LatencyMs = 0.0,
                maxLatencyMs = 0.0
            },
            new
            {
                nodeId = "durable-enqueue",
                name = "Durable Enqueue",
                kind = "stage",
                parentNodeId = (string?)null,
                inCount,
                outCount = inCount,
                failedCount = 0L,
                currentInFlight = 0L,
                backlogCount = backlogByState.GetValueOrDefault(WorkState.Classified, 0),
                currentRatePerSecond = ratePerSecond,
                averageLatencyMs = metrics?.AverageEnqueueLatencyMs ?? 0,
                p50LatencyMs = 0.0,
                p95LatencyMs = 0.0,
                p99LatencyMs = 0.0,
                maxLatencyMs = 0.0
            },
            new
            {
                nodeId = "shard-processing",
                name = "Shard Processing",
                kind = "stage",
                parentNodeId = (string?)null,
                inCount,
                outCount = inCount,
                failedCount = 0L,
                currentInFlight = 0L,
                backlogCount = backlogByState.GetValueOrDefault(WorkState.Processing, 0),
                currentRatePerSecond = ratePerSecond,
                averageLatencyMs = 0.0,
                p50LatencyMs = 0.0,
                p95LatencyMs = 0.0,
                p99LatencyMs = 0.0,
                maxLatencyMs = 0.0
            },
            new
            {
                nodeId = "output-preparation",
                name = "Output Preparation",
                kind = "stage",
                parentNodeId = (string?)null,
                inCount,
                outCount = inCount,
                failedCount = 0L,
                currentInFlight = 0L,
                backlogCount = backlogByState.GetValueOrDefault(WorkState.ReadyToOutput, 0),
                currentRatePerSecond = ratePerSecond,
                averageLatencyMs = 0.0,
                p50LatencyMs = 0.0,
                p95LatencyMs = 0.0,
                p99LatencyMs = 0.0,
                maxLatencyMs = 0.0
            },
            new
            {
                nodeId = "kafka-output",
                name = "Kafka Output",
                kind = "stage",
                parentNodeId = (string?)null,
                inCount,
                outCount = inCount,
                failedCount = 0L,
                currentInFlight = 0L,
                backlogCount = backlogByState.GetValueOrDefault(WorkState.Publishing, 0),
                currentRatePerSecond = ratePerSecond,
                averageLatencyMs = metrics?.AverageOutputLeaseLatencyMs ?? 0,
                p50LatencyMs = 0.0,
                p95LatencyMs = 0.0,
                p99LatencyMs = 0.0,
                maxLatencyMs = 0.0
            },
            new
            {
                nodeId = "completed",
                name = "Completed",
                kind = "sink",
                parentNodeId = (string?)null,
                inCount,
                outCount = inCount,
                failedCount = 0L,
                currentInFlight = 0L,
                backlogCount = 0L,
                currentRatePerSecond = ratePerSecond,
                averageLatencyMs = 0.0,
                p50LatencyMs = 0.0,
                p95LatencyMs = 0.0,
                p99LatencyMs = 0.0,
                maxLatencyMs = 0.0
            }
        };
    }

    private static IReadOnlyList<object> CreateFlowEdges(long count, double rate)
    {
        return new object[]
        {
            new { fromNodeId = "kafka-input", toNodeId = "fast-classification", name = "raw records", totalTransferred = count, currentRatePerSecond = rate },
            new { fromNodeId = "fast-classification", toNodeId = "durable-enqueue", name = "classified records", totalTransferred = count, currentRatePerSecond = rate },
            new { fromNodeId = "durable-enqueue", toNodeId = "shard-processing", name = "enqueued work", totalTransferred = count, currentRatePerSecond = rate },
            new { fromNodeId = "shard-processing", toNodeId = "output-preparation", name = "processed work", totalTransferred = count, currentRatePerSecond = rate },
            new { fromNodeId = "output-preparation", toNodeId = "kafka-output", name = "ready output", totalTransferred = count, currentRatePerSecond = rate },
            new { fromNodeId = "kafka-output", toNodeId = "completed", name = "published records", totalTransferred = count, currentRatePerSecond = rate }
        };
    }

    [HttpGet("flow")]
    public IActionResult GetFlow()
    {
        var metrics = GetWorkStoreMetrics();
        var processed = _accumulator.TotalProcessed;
        var uptimeSeconds = Math.Max(1, (DateTimeOffset.UtcNow - StartedAt).TotalSeconds);
        var rate = Math.Round(processed / uptimeSeconds, 1);

        return Ok(new
        {
            graphId = "eventreader-main",
            name = "EventReader",
            generatedAt = DateTimeOffset.UtcNow,
            nodes = CreateFlowNodes(metrics, processed, rate),
            edges = CreateFlowEdges(processed, rate)
        });
    }

    [HttpGet("flow/nodes/{nodeId}")]
    public IActionResult GetFlowNode(string nodeId)
    {
        var metrics = GetWorkStoreMetrics();
        var processed = _accumulator.TotalProcessed;
        var uptimeSeconds = Math.Max(1, (DateTimeOffset.UtcNow - StartedAt).TotalSeconds);
        var rate = Math.Round(processed / uptimeSeconds, 1);

        var allNodes = CreateFlowNodes(metrics, processed, rate);
        var node = allNodes.FirstOrDefault(n => GetNodeId(n) == nodeId);
        if (node is null)
            return NotFound(new { error = "Node not found" });

        var children = GetChildrenForNode(nodeId, metrics, processed, rate);

        return Ok(new
        {
            node,
            children
        });
    }

    [HttpGet("flow/nodes/{nodeId}/children")]
    public IActionResult GetFlowNodeChildren(string nodeId)
    {
        var metrics = GetWorkStoreMetrics();
        var processed = _accumulator.TotalProcessed;
        var uptimeSeconds = Math.Max(1, (DateTimeOffset.UtcNow - StartedAt).TotalSeconds);
        var rate = Math.Round(processed / uptimeSeconds, 1);

        var allNodes = CreateFlowNodes(metrics, processed, rate);
        var exists = allNodes.Any(n => GetNodeId(n) == nodeId);
        if (!exists)
            return NotFound(new { error = "Node not found" });

        return Ok(new
        {
            children = GetChildrenForNode(nodeId, metrics, processed, rate)
        });
    }

    [HttpGet("flow/bottlenecks")]
    public IActionResult GetBottlenecks()
    {
        var metrics = GetWorkStoreMetrics();

        var bottlenecks = new List<object>();
        if (metrics is not null)
        {
            var backlogByState = metrics.BacklogByState;
            if (backlogByState.GetValueOrDefault(WorkState.Processing, 0) > 1000)
            {
                bottlenecks.Add(new
                {
                    nodeId = "shard-processing",
                    reason = "High processing backlog",
                    severity = "warning",
                    currentRatePerSecond = 0.0,
                    p95LatencyMs = 0.0,
                    upstreamBacklog = backlogByState[WorkState.Processing]
                });
            }
            if (backlogByState.GetValueOrDefault(WorkState.Publishing, 0) > 1000)
            {
                bottlenecks.Add(new
                {
                    nodeId = "kafka-output",
                    reason = "High publishing backlog",
                    severity = "warning",
                    currentRatePerSecond = 0.0,
                    p95LatencyMs = 0.0,
                    upstreamBacklog = backlogByState[WorkState.Publishing]
                });
            }
            if (metrics.OldestUnfinishedAge?.TotalSeconds > 60)
            {
                bottlenecks.Add(new
                {
                    nodeId = "durable-enqueue",
                    reason = "Oldest unfinished work is stale",
                    severity = "warning",
                    currentRatePerSecond = 0.0,
                    p95LatencyMs = 0.0,
                    upstreamBacklog = 0L
                });
            }
        }

        return Ok(new
        {
            generatedAt = DateTimeOffset.UtcNow,
            bottlenecks
        });
    }

    // ---- ER-010.4 Shard and Work APIs ----

    [HttpGet("shards")]
    public IActionResult GetShards()
    {
        var metrics = GetWorkStoreMetrics();
        var backlogByShard = metrics?.BacklogByShard ?? new Dictionary<int, long>();

        var shards = backlogByShard
            .Select(kvp => new
            {
                shardId = kvp.Key,
                backlogCount = kvp.Value
            })
            .OrderByDescending(s => s.backlogCount)
            .ToList();

        return Ok(new
        {
            shards,
            totalShards = _workerOptions.LogicalShardCount,
            activeShards = shards.Count(s => s.backlogCount > 0)
        });
    }

    [HttpGet("work")]
    public IActionResult GetWork(
        [FromQuery] string? state,
        [FromQuery] int? functionId,
        [FromQuery] int? shardId,
        [FromQuery] string? topic)
    {
        var metrics = GetWorkStoreMetrics();
        var backlogByState = metrics?.BacklogByState ?? new Dictionary<WorkState, long>();

        var items = new List<object>();
        foreach (var kvp in backlogByState)
        {
            if (kvp.Value == 0) continue;
            if (state is not null && !string.Equals(kvp.Key.ToString(), state, StringComparison.OrdinalIgnoreCase)) continue;

            items.Add(new
            {
                state = kvp.Key.ToString(),
                count = kvp.Value,
                shardId = shardId,
                functionId = functionId,
                topic = topic
            });
        }

        return Ok(new
        {
            items,
            totalCount = items.Sum(i => ((dynamic)i).count)
        });
    }

    [HttpGet("work/{workItemId:long}")]
    public IActionResult GetWorkItem(long workItemId)
    {
        var metrics = GetWorkStoreMetrics();
        return Ok(new
        {
            workItemId,
            state = "Unknown",
            shardId = 0,
            functionId = 0,
            leaseOwnerId = (string?)null,
            leaseExpiresUtc = (DateTimeOffset?)null,
            processingAttempt = 0,
            outputAttempt = 0,
            createdUtc = (DateTimeOffset?)null,
            updatedUtc = (DateTimeOffset?)null,
            lastError = (string?)null,
            storageHealthy = metrics is not null && metrics.DiskFreeBytes > 0
        });
    }

    // ---- ER-010.5 Backpressure API ----

    [HttpGet("backpressure")]
    public IActionResult GetBackpressure()
    {
        lock (s_backpressureLock)
        {
            return Ok(new
            {
                state = GetBackpressureState(),
                manuallyPaused = s_manuallyPaused,
                lastPauseReason = s_lastPauseReason,
                lastPauseUtc = s_lastPauseUtc,
                lastResumeUtc = s_lastResumeUtc,
                pauseCount = Interlocked.Read(ref s_pauseCount),
                resumeCount = Interlocked.Read(ref s_resumeCount)
            });
        }
    }

    [HttpPost("backpressure/pause")]
    public IActionResult Pause([FromQuery] string? reason)
    {
        lock (s_backpressureLock)
        {
            s_manuallyPaused = true;
            s_lastPauseReason = reason ?? "Manual pause requested";
            s_lastPauseUtc = DateTimeOffset.UtcNow;
            Interlocked.Increment(ref s_pauseCount);
        }

        return Ok(new
        {
            state = "Paused",
            message = "Backpressure paused manually",
            reason = s_lastPauseReason
        });
    }

    [HttpPost("backpressure/resume")]
    public IActionResult Resume()
    {
        lock (s_backpressureLock)
        {
            s_manuallyPaused = false;
            s_lastResumeUtc = DateTimeOffset.UtcNow;
            Interlocked.Increment(ref s_resumeCount);
        }

        return Ok(new
        {
            state = "Open",
            message = "Backpressure resumed"
        });
    }

    // ---- Helpers ----

    private static readonly DateTimeOffset StartedAt = DateTimeOffset.UtcNow;

    private static string GetBackpressureState() => s_manuallyPaused ? "Paused" : "Open";

    private FasterEventReaderWorkStoreDetailedMetrics? GetWorkStoreMetrics()
    {
        if (_workStore is FasterEventReaderWorkStore fasterStore)
            return fasterStore.GetDetailedMetrics();
        return null;
    }

    private static string GetNodeId(object node)
    {
        var type = node.GetType();
        var prop = type.GetProperty("nodeId");
        return (string)prop!.GetValue(node)!;
    }

    private static IReadOnlyList<object> GetChildrenForNode(
        string nodeId,
        FasterEventReaderWorkStoreDetailedMetrics? metrics,
        long processed,
        double rate)
    {
        return nodeId switch
        {
            "fast-classification" => new object[]
            {
                new { nodeId = "scan-json", name = "Scan JSON", kind = "child", parentNodeId = nodeId,
                    inCount = processed, outCount = processed, failedCount = 0L, currentInFlight = 0L, backlogCount = 0L,
                    currentRatePerSecond = rate, averageLatencyMs = 0.0, p50LatencyMs = 0.0, p95LatencyMs = 0.0, p99LatencyMs = 0.0, maxLatencyMs = 0.0 },
                new { nodeId = "resolve-source", name = "Resolve Source System", kind = "child", parentNodeId = nodeId,
                    inCount = processed, outCount = processed, failedCount = 0L, currentInFlight = 0L, backlogCount = 0L,
                    currentRatePerSecond = rate, averageLatencyMs = 0.0, p50LatencyMs = 0.0, p95LatencyMs = 0.0, p99LatencyMs = 0.0, maxLatencyMs = 0.0 },
                new { nodeId = "resolve-function", name = "Resolve FunctionID", kind = "child", parentNodeId = nodeId,
                    inCount = processed, outCount = processed, failedCount = 0L, currentInFlight = 0L, backlogCount = 0L,
                    currentRatePerSecond = rate, averageLatencyMs = 0.0, p50LatencyMs = 0.0, p95LatencyMs = 0.0, p99LatencyMs = 0.0, maxLatencyMs = 0.0 },
                new { nodeId = "resolve-nid", name = "Resolve NID", kind = "child", parentNodeId = nodeId,
                    inCount = processed, outCount = processed, failedCount = 0L, currentInFlight = 0L, backlogCount = 0L,
                    currentRatePerSecond = rate, averageLatencyMs = 0.0, p50LatencyMs = 0.0, p95LatencyMs = 0.0, p99LatencyMs = 0.0, maxLatencyMs = 0.0 },
                new { nodeId = "assign-shard", name = "Assign Shard", kind = "child", parentNodeId = nodeId,
                    inCount = processed, outCount = processed, failedCount = 0L, currentInFlight = 0L, backlogCount = 0L,
                    currentRatePerSecond = rate, averageLatencyMs = 0.0, p50LatencyMs = 0.0, p95LatencyMs = 0.0, p99LatencyMs = 0.0, maxLatencyMs = 0.0 }
            },
            "shard-processing" => new object[]
            {
                new { nodeId = "lease-work", name = "Lease Work", kind = "child", parentNodeId = nodeId,
                    inCount = processed, outCount = processed, failedCount = 0L, currentInFlight = 0L, backlogCount = 0L,
                    currentRatePerSecond = rate, averageLatencyMs = metrics?.AverageShardLeaseLatencyMs ?? 0, p50LatencyMs = 0.0, p95LatencyMs = 0.0, p99LatencyMs = 0.0, maxLatencyMs = 0.0 },
                new { nodeId = "parse-full-json", name = "Parse Full JSON", kind = "child", parentNodeId = nodeId,
                    inCount = processed, outCount = processed, failedCount = 0L, currentInFlight = 0L, backlogCount = 0L,
                    currentRatePerSecond = rate, averageLatencyMs = 0.0, p50LatencyMs = 0.0, p95LatencyMs = 0.0, p99LatencyMs = 0.0, maxLatencyMs = 0.0 },
                new { nodeId = "run-extraction", name = "Run Extraction Script", kind = "child", parentNodeId = nodeId,
                    inCount = processed, outCount = processed, failedCount = 0L, currentInFlight = 0L, backlogCount = 0L,
                    currentRatePerSecond = rate, averageLatencyMs = 0.0, p50LatencyMs = 0.0, p95LatencyMs = 0.0, p99LatencyMs = 0.0, maxLatencyMs = 0.0 },
                new { nodeId = "run-rules", name = "Run Rules", kind = "child", parentNodeId = nodeId,
                    inCount = processed, outCount = processed, failedCount = 0L, currentInFlight = 0L, backlogCount = 0L,
                    currentRatePerSecond = rate, averageLatencyMs = 0.0, p50LatencyMs = 0.0, p95LatencyMs = 0.0, p99LatencyMs = 0.0, maxLatencyMs = 0.0 },
                new { nodeId = "build-output", name = "Build Output Object", kind = "child", parentNodeId = nodeId,
                    inCount = processed, outCount = processed, failedCount = 0L, currentInFlight = 0L, backlogCount = 0L,
                    currentRatePerSecond = rate, averageLatencyMs = 0.0, p50LatencyMs = 0.0, p95LatencyMs = 0.0, p99LatencyMs = 0.0, maxLatencyMs = 0.0 },
                new { nodeId = "mark-ready", name = "Mark ReadyToOutput", kind = "child", parentNodeId = nodeId,
                    inCount = processed, outCount = processed, failedCount = 0L, currentInFlight = 0L, backlogCount = 0L,
                    currentRatePerSecond = rate, averageLatencyMs = 0.0, p50LatencyMs = 0.0, p95LatencyMs = 0.0, p99LatencyMs = 0.0, maxLatencyMs = 0.0 }
            },
            "kafka-output" => new object[]
            {
                new { nodeId = "lease-output", name = "Lease Output", kind = "child", parentNodeId = nodeId,
                    inCount = processed, outCount = processed, failedCount = 0L, currentInFlight = 0L, backlogCount = 0L,
                    currentRatePerSecond = rate, averageLatencyMs = metrics?.AverageOutputLeaseLatencyMs ?? 0, p50LatencyMs = 0.0, p95LatencyMs = 0.0, p99LatencyMs = 0.0, maxLatencyMs = 0.0 },
                new { nodeId = "build-kafka-message", name = "Build Kafka Message", kind = "child", parentNodeId = nodeId,
                    inCount = processed, outCount = processed, failedCount = 0L, currentInFlight = 0L, backlogCount = 0L,
                    currentRatePerSecond = rate, averageLatencyMs = 0.0, p50LatencyMs = 0.0, p95LatencyMs = 0.0, p99LatencyMs = 0.0, maxLatencyMs = 0.0 },
                new { nodeId = "publish-kafka-message", name = "Publish Kafka Message", kind = "child", parentNodeId = nodeId,
                    inCount = processed, outCount = processed, failedCount = 0L, currentInFlight = 0L, backlogCount = 0L,
                    currentRatePerSecond = rate, averageLatencyMs = 0.0, p50LatencyMs = 0.0, p95LatencyMs = 0.0, p99LatencyMs = 0.0, maxLatencyMs = 0.0 },
                new { nodeId = "mark-completed", name = "Mark Completed", kind = "child", parentNodeId = nodeId,
                    inCount = processed, outCount = processed, failedCount = 0L, currentInFlight = 0L, backlogCount = 0L,
                    currentRatePerSecond = rate, averageLatencyMs = 0.0, p50LatencyMs = 0.0, p95LatencyMs = 0.0, p99LatencyMs = 0.0, maxLatencyMs = 0.0 }
            },
            _ => Array.Empty<object>()
        };
    }
}
