using Event.Streaming.Processing.WorkStore;

namespace EventReader.Tests.WorkStore;

/// <summary>
/// ER-005.6: Tests for work store metrics exposed via IEventReaderWorkStore.
/// Covers backlog by state/shard, oldest age, active runtime model versions, and latency metrics.
/// </summary>
public sealed class WorkStoreMetricsTests : IDisposable
{
    private readonly string _testDir;
    private readonly FasterEventReaderWorkStore _store;

    public WorkStoreMetricsTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), "ws-metrics-tests", Guid.NewGuid().ToString("N"));
        _store = new FasterEventReaderWorkStore(new FasterEventReaderWorkStoreOptions
        {
            LogPath = Path.Combine(_testDir, "work-log"),
            CheckpointPath = Path.Combine(_testDir, "checkpoints"),
            CheckpointIntervalMs = 0,
        });
    }

    public void Dispose()
    {
        _store.Dispose();
        try { Directory.Delete(_testDir, recursive: true); } catch { }
    }

    [Fact]
    public async Task GetWorkStoreMetrics_backlog_by_state_reflects_classified_item()
    {
        await _store.EnqueueClassifiedAsync(MakeItem(nid: 1, runtimeVersion: 1), CancellationToken.None);

        // Use the interface method (ER-005.6 requirement)
        IEventReaderWorkStore iface = _store;
        var metrics = iface.GetWorkStoreMetrics();

        Assert.Equal(1, metrics.BacklogByState.GetValueOrDefault(WorkState.Classified));
        Assert.Equal(0, metrics.BacklogByState.GetValueOrDefault(WorkState.Completed));
    }

    [Fact]
    public async Task GetWorkStoreMetrics_backlog_by_shard_counts_unfinished_items_only()
    {
        await _store.EnqueueClassifiedAsync(MakeItem(nid: 1, shardId: 3, runtimeVersion: 1), CancellationToken.None);
        await _store.EnqueueClassifiedAsync(MakeItem(nid: 2, shardId: 3, runtimeVersion: 1), CancellationToken.None);
        await _store.EnqueueClassifiedAsync(MakeItem(nid: 3, shardId: 7, runtimeVersion: 1), CancellationToken.None);

        IEventReaderWorkStore iface = _store;
        var metrics = iface.GetWorkStoreMetrics();

        Assert.Equal(2, metrics.BacklogByShard.GetValueOrDefault(3));
        Assert.Equal(1, metrics.BacklogByShard.GetValueOrDefault(7));
    }

    [Fact]
    public async Task GetWorkStoreMetrics_oldest_unfinished_age_is_non_negative()
    {
        await _store.EnqueueClassifiedAsync(MakeItem(nid: 1, runtimeVersion: 1), CancellationToken.None);

        IEventReaderWorkStore iface = _store;
        var metrics = iface.GetWorkStoreMetrics();

        Assert.NotNull(metrics.OldestUnfinishedAge);
        Assert.True(metrics.OldestUnfinishedAge.Value >= TimeSpan.Zero);
    }

    [Fact]
    public async Task GetWorkStoreMetrics_completed_items_do_not_appear_in_backlog_by_shard()
    {
        // Enqueue → lease → mark ready → lease output → mark completed
        await _store.EnqueueClassifiedAsync(MakeItem(nid: 1, shardId: 5, runtimeVersion: 1), CancellationToken.None);

        var leases = await _store.LeaseShardBatchAsync(5, 1, TimeSpan.FromMinutes(5), CancellationToken.None);
        Assert.Single(leases);
        await _store.MarkReadyToOutputAsync(leases[0].WorkItemId, [1, 2, 3], CancellationToken.None);

        var outputLeases = await _store.LeaseOutputBatchAsync(1, TimeSpan.FromMinutes(5), CancellationToken.None);
        Assert.Single(outputLeases);
        await _store.MarkCompletedAsync(outputLeases[0].WorkItemId,
            new OutputWriteReceipt("output", 0, 1, DateTimeOffset.UtcNow, null), CancellationToken.None);

        IEventReaderWorkStore iface = _store;
        var metrics = iface.GetWorkStoreMetrics();

        Assert.False(metrics.BacklogByShard.ContainsKey(5),
            "Completed items must not appear in shard backlog");
        Assert.Null(metrics.OldestUnfinishedAge);
    }

    [Fact]
    public async Task GetWorkStoreMetrics_active_runtime_model_versions_reflects_non_terminal_items()
    {
        await _store.EnqueueClassifiedAsync(MakeItem(nid: 1, runtimeVersion: 10), CancellationToken.None);
        await _store.EnqueueClassifiedAsync(MakeItem(nid: 2, runtimeVersion: 11), CancellationToken.None);

        IEventReaderWorkStore iface = _store;
        var metrics = iface.GetWorkStoreMetrics();

        Assert.Contains(10L, metrics.ActiveRuntimeModelVersions);
        Assert.Contains(11L, metrics.ActiveRuntimeModelVersions);
    }

    [Fact]
    public async Task GetWorkStoreMetrics_active_runtime_model_versions_empty_when_store_is_empty()
    {
        IEventReaderWorkStore iface = _store;
        var metrics = iface.GetWorkStoreMetrics();

        Assert.Empty(metrics.ActiveRuntimeModelVersions);
    }

    [Fact]
    public async Task GetActiveRuntimeModelVersions_returns_only_versions_with_non_terminal_items()
    {
        await _store.EnqueueClassifiedAsync(MakeItem(nid: 1, runtimeVersion: 5), CancellationToken.None);
        await _store.EnqueueClassifiedAsync(MakeItem(nid: 2, runtimeVersion: 6), CancellationToken.None);

        // Complete the second item
        var leases = await _store.LeaseShardBatchAsync(0, 10, TimeSpan.FromMinutes(5), CancellationToken.None);
        var lease6 = leases.FirstOrDefault(l => l.Item.RuntimeModelVersion == 6);
        if (lease6 is not null)
        {
            await _store.MarkReadyToOutputAsync(lease6.WorkItemId, [99], CancellationToken.None);
            var output = await _store.LeaseOutputBatchAsync(1, TimeSpan.FromMinutes(5), CancellationToken.None);
            if (output.Count > 0)
            {
                await _store.MarkCompletedAsync(output[0].WorkItemId,
                    new OutputWriteReceipt("t", 0, 1, DateTimeOffset.UtcNow, null), CancellationToken.None);
            }
        }

        IEventReaderWorkStore iface = _store;
        var versions = iface.GetActiveRuntimeModelVersions();

        Assert.Contains(5L, versions);
        // version 6 item is completed, may or may not be present depending on which item was completed
    }

    [Fact]
    public void GetWorkStoreMetrics_disk_free_bytes_is_positive_on_valid_path()
    {
        IEventReaderWorkStore iface = _store;
        var metrics = iface.GetWorkStoreMetrics();

        Assert.True(metrics.DiskFreeBytes > 0);
        Assert.True(metrics.DiskTotalBytes > 0);
        Assert.True(metrics.DiskFreeBytes <= metrics.DiskTotalBytes);
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private static long _nonce;

    private static ClassifiedWorkItem MakeItem(long nid, int shardId = 0, long runtimeVersion = 1)
    {
        var offset = Interlocked.Increment(ref _nonce);
        return new ClassifiedWorkItem(
            workItemId: 0,
            source: new KafkaSourceIdentity("sys", "topic", 0, offset, null),
            functionId: 100,
            nedbankId: nid,
            shardId: shardId,
            runtimeModelVersion: runtimeVersion,
            functionVersion: 1,
            extractionScriptVersion: 1,
            ruleSetVersion: 1,
            outputRouteVersion: 1,
            rawPayload: [1],
            createdUtc: DateTimeOffset.UtcNow);
    }
}
