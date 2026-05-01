using System.Text.Json;
using EventReader.Api;
using EventReader.Configuration;
using EventReader.Kafka;
using EventReader.Monitoring;
using Event.Streaming.Processing.Monitoring;
using Event.Streaming.Processing.WorkStore;
using Microsoft.AspNetCore.Mvc;

namespace EventReader.Tests.Api;

public sealed class EventReaderMonitoringApiTests
{
    [Fact]
    public void Status_ReturnsExpectedFields()
    {
        var controller = CreateController();

        var result = Assert.IsType<OkObjectResult>(controller.GetStatus());
        using var doc = JsonDocument.Parse(JsonSerializer.Serialize(result.Value));
        var root = doc.RootElement;

        Assert.True(root.TryGetProperty("state", out _));
        Assert.True(root.TryGetProperty("runtimeModelVersion", out _));
        Assert.True(root.TryGetProperty("consumerState", out _));
        Assert.True(root.TryGetProperty("backpressureState", out _));
        Assert.True(root.TryGetProperty("recordsInPerSecond", out _));
        Assert.True(root.TryGetProperty("recordsCompletedPerSecond", out _));
        Assert.True(root.TryGetProperty("oldestBacklogAgeSeconds", out _));
        Assert.True(root.TryGetProperty("activeWorkers", out _));
        Assert.True(root.TryGetProperty("logicalShards", out _));
        Assert.True(root.TryGetProperty("storageHealthy", out _));
        Assert.True(root.TryGetProperty("outputHealthy", out _));
    }

    [Fact]
    public void Metrics_ReturnsExpectedSections()
    {
        var controller = CreateController();

        var result = Assert.IsType<OkObjectResult>(controller.GetMetrics());
        using var doc = JsonDocument.Parse(JsonSerializer.Serialize(result.Value));
        var root = doc.RootElement;

        Assert.True(root.TryGetProperty("throughput", out var throughput));
        Assert.True(root.TryGetProperty("latency", out _));
        Assert.True(root.TryGetProperty("backlog", out _));
        Assert.True(root.TryGetProperty("hotSpots", out _));
        Assert.True(root.TryGetProperty("backpressure", out _));
        Assert.True(root.TryGetProperty("storage", out _));

        Assert.True(throughput.TryGetProperty("recordsInPerSec", out _));
        Assert.True(throughput.TryGetProperty("processedPerSec", out _));
    }

    [Fact]
    public void Flow_ReturnsGraphWithNodesAndEdges()
    {
        var controller = CreateController();

        var result = Assert.IsType<OkObjectResult>(controller.GetFlow());
        using var doc = JsonDocument.Parse(JsonSerializer.Serialize(result.Value));
        var root = doc.RootElement;

        Assert.Equal("eventreader-main", root.GetProperty("graphId").GetString());
        Assert.Equal("EventReader", root.GetProperty("name").GetString());

        var nodes = root.GetProperty("nodes");
        Assert.True(nodes.GetArrayLength() >= 5);
        var firstNode = nodes[0];
        Assert.True(firstNode.TryGetProperty("nodeId", out _));
        Assert.True(firstNode.TryGetProperty("inCount", out _));
        Assert.True(firstNode.TryGetProperty("currentRatePerSecond", out _));

        var edges = root.GetProperty("edges");
        Assert.True(edges.GetArrayLength() >= 3);
        var firstEdge = edges[0];
        Assert.True(firstEdge.TryGetProperty("fromNodeId", out _));
        Assert.True(firstEdge.TryGetProperty("toNodeId", out _));
        Assert.True(firstEdge.TryGetProperty("totalTransferred", out _));
    }

    [Fact]
    public void FlowNode_ReturnsNodeWithChildren()
    {
        var controller = CreateController();

        var result = Assert.IsType<OkObjectResult>(controller.GetFlowNode("shard-processing"));
        using var doc = JsonDocument.Parse(JsonSerializer.Serialize(result.Value));
        var root = doc.RootElement;

        var node = root.GetProperty("node");
        Assert.Equal("shard-processing", node.GetProperty("nodeId").GetString());

        var children = root.GetProperty("children");
        Assert.True(children.GetArrayLength() >= 3);
        var firstChild = children[0];
        Assert.True(firstChild.TryGetProperty("nodeId", out _));
    }

    [Fact]
    public void Shards_ReturnsShardData()
    {
        var controller = CreateController();

        var result = Assert.IsType<OkObjectResult>(controller.GetShards());
        using var doc = JsonDocument.Parse(JsonSerializer.Serialize(result.Value));
        var root = doc.RootElement;

        Assert.True(root.TryGetProperty("shards", out _));
        Assert.True(root.TryGetProperty("totalShards", out _));
        Assert.True(root.TryGetProperty("activeShards", out _));

        Assert.Equal(1024, root.GetProperty("totalShards").GetInt32());
    }

    [Fact]
    public void Backpressure_PauseAndResume_Cycle()
    {
        var controller = CreateController();

        var initial = Assert.IsType<OkObjectResult>(controller.GetBackpressure());
        var initialDoc = JsonDocument.Parse(JsonSerializer.Serialize(initial.Value));
        Assert.Equal("Open", initialDoc.RootElement.GetProperty("state").GetString());

        var pauseResult = Assert.IsType<OkObjectResult>(controller.Pause("test reason"));
        var pauseDoc = JsonDocument.Parse(JsonSerializer.Serialize(pauseResult.Value));
        Assert.Equal("Paused", pauseDoc.RootElement.GetProperty("state").GetString());

        var afterPause = Assert.IsType<OkObjectResult>(controller.GetBackpressure());
        var afterDoc = JsonDocument.Parse(JsonSerializer.Serialize(afterPause.Value));
        Assert.Equal("Paused", afterDoc.RootElement.GetProperty("state").GetString());
        Assert.Equal("test reason", afterDoc.RootElement.GetProperty("lastPauseReason").GetString());

        var resumeResult = Assert.IsType<OkObjectResult>(controller.Resume());
        var resumeDoc = JsonDocument.Parse(JsonSerializer.Serialize(resumeResult.Value));
        Assert.Equal("Open", resumeDoc.RootElement.GetProperty("state").GetString());

        var afterResume = Assert.IsType<OkObjectResult>(controller.GetBackpressure());
        var afterResumeDoc = JsonDocument.Parse(JsonSerializer.Serialize(afterResume.Value));
        Assert.Equal("Open", afterResumeDoc.RootElement.GetProperty("state").GetString());
        Assert.True(afterResumeDoc.RootElement.GetProperty("pauseCount").GetInt64() >= 1);
        Assert.True(afterResumeDoc.RootElement.GetProperty("resumeCount").GetInt64() >= 1);
    }

    [Fact]
    public void FlowNode_UnknownNode_ReturnsNotFound()
    {
        var controller = CreateController();

        var result = controller.GetFlowNode("nonexistent-node");
        Assert.IsType<NotFoundObjectResult>(result);
    }

    [Fact]
    public void FlowBottlenecks_ReturnsCandidates()
    {
        var controller = CreateController();

        var result = Assert.IsType<OkObjectResult>(controller.GetBottlenecks());
        using var doc = JsonDocument.Parse(JsonSerializer.Serialize(result.Value));
        var root = doc.RootElement;

        Assert.True(root.TryGetProperty("generatedAt", out _));
        Assert.True(root.TryGetProperty("bottlenecks", out _));
    }

    private static EventReaderMonitoringController CreateController()
    {
        var snapshot = new EventReaderMonitoringSnapshot
        {
            Health = HealthStatus.Healthy,
            KafkaMetrics = new KafkaMetrics { ConsumerState = "active" },
            PipelineMetrics = new PipelineMetrics
            {
                TotalProcessed = 100,
                TotalErrors = 5,
                AvgLatencyMs = 1.5,
                P95LatencyMs = 4.2,
                P99LatencyMs = 8.1,
                MessagesPerSecond = 42.0
            }
        };

        var accumulator = new EventReaderMetricsAccumulator();
        accumulator.RecordBatch(100, DateTimeOffset.UtcNow);
        accumulator.RecordProcessed(TimeSpan.FromMilliseconds(1), DateTimeOffset.UtcNow);

        var options = new EventReaderRuntimeOptions
        {
            InstanceId = "reader-test",
            Version = "42.0.0",
            ApplicationRole = "reader",
        };

        return new EventReaderMonitoringController(
            snapshot,
            accumulator,
            new StubWorkStore(),
            new EventReaderShardWorkerPoolOptions
            {
                WorkerCount = 32,
                LogicalShardCount = 1024
            },
            new StaticOptionsProvider(options));
    }

    private sealed class StubWorkStore : IEventReaderWorkStore
    {
        public Task EnqueueClassifiedAsync(ClassifiedWorkItem item, CancellationToken ct) =>
            Task.CompletedTask;

        public Task<IReadOnlyList<WorkLease>> LeaseShardBatchAsync(
            int shardId, int maxItems, TimeSpan leaseDuration, CancellationToken ct) =>
            Task.FromResult<IReadOnlyList<WorkLease>>(Array.Empty<WorkLease>());

        public Task MarkReadyToOutputAsync(long workItemId, byte[] outputPayload, CancellationToken ct) =>
            Task.CompletedTask;

        public Task<IReadOnlyList<OutputLease>> LeaseOutputBatchAsync(
            int maxItems, TimeSpan leaseDuration, CancellationToken ct) =>
            Task.FromResult<IReadOnlyList<OutputLease>>(Array.Empty<OutputLease>());

        public Task MarkCompletedAsync(long workItemId, OutputWriteReceipt receipt, CancellationToken ct) =>
            Task.CompletedTask;

        public Task MarkRetryPendingAsync(long workItemId, RetryReason reason, CancellationToken ct) =>
            Task.CompletedTask;

        public Task MarkFailedAsync(long workItemId, string reason, CancellationToken ct) =>
            Task.CompletedTask;

        public Task ReleaseExpiredLeasesAsync(CancellationToken ct) =>
            Task.CompletedTask;

        public Task<ReplayPlan> CreateReplayPlanAsync(ReplayRequest request, CancellationToken ct) =>
            Task.FromResult(new ReplayPlan(Guid.NewGuid(), request, Array.Empty<long>(), DateTimeOffset.UtcNow));

        public Task<UnstuckResult> RequeueAsync(UnstuckRequest request, CancellationToken ct) =>
            Task.FromResult(new UnstuckResult(0, 0, Array.Empty<long>()));

        public WorkStoreMetricsSnapshot GetWorkStoreMetrics() =>
            new(new Dictionary<WorkState, long>(), new Dictionary<int, long>(),
                new HashSet<long>(), null, 0, 0, 0, 0, 0, null);

        public IReadOnlySet<long> GetActiveRuntimeModelVersions() => new HashSet<long>();
    }

    private sealed class StaticOptionsProvider(EventReaderRuntimeOptions options) : IEventReaderRuntimeOptionsProvider
    {
        public EventReaderRuntimeOptions Current { get; } = options;
    }
}
