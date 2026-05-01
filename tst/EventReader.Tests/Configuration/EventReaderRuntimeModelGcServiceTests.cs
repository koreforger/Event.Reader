using Event.Data.Entities;
using EventReader.Configuration;
using Event.Streaming.Processing.WorkStore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;

namespace EventReader.Tests.Configuration;

/// <summary>
/// ER-002.3: Proves the GC background service collects stale runtime model versions
/// and retains versions still referenced by active (non-terminal) work items.
/// </summary>
public sealed class EventReaderRuntimeModelGcServiceTests : IDisposable
{
    private readonly string _testDir;
    private readonly FasterEventReaderWorkStore _store;

    public EventReaderRuntimeModelGcServiceTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), "gc-svc-tests", Guid.NewGuid().ToString("N"));
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
    public async Task GcService_collects_version_not_referenced_by_any_work_item()
    {
        var provider = await CreateProviderAsync();

        var version1 = provider.CurrentVersion;
        Assert.Equal(1, version1);

        // Advance to version 2 — no work items exist, so version1 is orphaned
        await provider.ReloadAsync(CancellationToken.None);
        var version2 = provider.CurrentVersion;
        Assert.Equal(2, version2);
        Assert.Contains(version1, provider.ActiveVersions);

        await RunGcServiceCycle(provider);

        Assert.DoesNotContain(version1, provider.ActiveVersions);
        Assert.Contains(version2, provider.ActiveVersions);
    }

    [Fact]
    public async Task GcService_retains_version_referenced_by_classified_work_item()
    {
        var provider = await CreateProviderAsync();
        var version1 = provider.CurrentVersion;

        // Enqueue a work item under version1
        await _store.EnqueueClassifiedAsync(new ClassifiedWorkItem(
            workItemId: 0,
            source: new KafkaSourceIdentity("sys", "topic", 0, 1, null),
            functionId: 100,
            nedbankId: 9999,
            shardId: 0,
            runtimeModelVersion: version1,
            functionVersion: 1,
            extractionScriptVersion: 1,
            ruleSetVersion: 1,
            outputRouteVersion: 1,
            rawPayload: [42],
            createdUtc: DateTimeOffset.UtcNow), CancellationToken.None);

        // Advance to version 2
        await provider.ReloadAsync(CancellationToken.None);
        Assert.NotEqual(version1, provider.CurrentVersion);

        await RunGcServiceCycle(provider);

        // version1 must survive: a Classified work item still holds a reference
        Assert.Contains(version1, provider.ActiveVersions);
    }

    [Fact]
    public async Task GcService_never_collects_current_version()
    {
        var provider = await CreateProviderAsync();
        var version1 = provider.CurrentVersion;

        // No version advance — run GC against empty work store
        await RunGcServiceCycle(provider);

        // Single version that is also current must survive
        Assert.Contains(version1, provider.ActiveVersions);
    }

    // helpers

    private async Task RunGcServiceCycle(IEventReaderRuntimeModelProvider provider)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var svc = new EventReaderRuntimeModelGcService(
            provider,
            _store,
            NullLogger<EventReaderRuntimeModelGcService>.Instance,
            interval: TimeSpan.FromMilliseconds(30));

        await svc.StartAsync(cts.Token);
        await Task.Delay(120); // let at least one cycle run
        await cts.CancelAsync();
        try { await svc.StopAsync(CancellationToken.None); } catch (OperationCanceledException) { }
    }

    private static async Task<EventReaderRuntimeModelProvider> CreateProviderAsync()
    {
        var factory = new InMemoryDbContextFactory();
        await EventReaderTestSeed.SeedGcTestDataAsync(factory);
        var provider = new EventReaderRuntimeModelProvider(factory, new ConfigurationManager());
        await provider.ReloadAsync(CancellationToken.None); // warm up to v1
        return provider;
    }
}
