using Event.Data.Entities;
using EventReader.Configuration;
using Microsoft.Extensions.Configuration;

namespace EventReader.Tests.Configuration;

public sealed class EventReaderRuntimeModelProviderTests
{
    // helpers

    private static EventReaderRuntimeModelProvider CreateProvider(InMemoryDbContextFactory factory) =>
        new(factory, new ConfigurationManager());

    // construction

    [Fact]
    public void Ctor_StartsWithEmptyModel_VersionZero()
    {
        var factory = new InMemoryDbContextFactory();
        var provider = CreateProvider(factory);

        Assert.Equal(0, provider.CurrentVersion);
        Assert.Empty(provider.Current.SourceSystems);
        Assert.Empty(provider.Current.Functions);
        Assert.Single(provider.ActiveVersions);
    }

    // loading

    [Fact]
    public async Task ReloadAsync_LoadsDataFromDatabase()
    {
        var factory = new InMemoryDbContextFactory();
        await EventReaderTestSeed.SeedDefaultAsync(factory);
        var provider = CreateProvider(factory);

        await provider.ReloadAsync(CancellationToken.None);

        var model = provider.Current;
        Assert.Equal(1, provider.CurrentVersion);
        Assert.Single(model.SourceSystems);
        Assert.Equal("ss-1", model.SourceSystems["ss-1"].SourceSystemId);
        Assert.Equal("topic.source", model.SourceSystems["ss-1"].KafkaTopic);
        Assert.Equal(2, model.FunctionDiscriminatorPaths.Count);
        Assert.Single(model.ClientIdentityPaths);
        Assert.Single(model.FunctionMatchers.Matchers);
        Assert.Single(model.Functions);
        Assert.True(model.Functions.ContainsKey(100));
    }

    [Fact]
    public async Task ReloadAsync_EmptyDatabase_ProducesValidEmptyModel()
    {
        var factory = new InMemoryDbContextFactory();
        var provider = CreateProvider(factory);

        await provider.ReloadAsync(CancellationToken.None);

        Assert.Equal(1, provider.CurrentVersion);
        Assert.Empty(provider.Current.SourceSystems);
        Assert.Empty(provider.Current.Functions);
    }

    // version advancement

    [Fact]
    public async Task ReloadAsync_SuccessfulReload_IncrementsVersionAndSwapsModel()
    {
        var factory = new InMemoryDbContextFactory();
        await EventReaderTestSeed.SeedDefaultAsync(factory);
        var provider = CreateProvider(factory);

        await provider.ReloadAsync(CancellationToken.None);
        var original = provider.Current;
        Assert.Equal(1, provider.CurrentVersion);

        await provider.ReloadAsync(CancellationToken.None);
        var reloaded = provider.Current;

        Assert.NotSame(original, reloaded);
        Assert.Equal(2, provider.CurrentVersion);
        Assert.Equal(1, original.Version);
        Assert.Equal(2, reloaded.Version);
        Assert.Contains(1L, provider.ActiveVersions);
        Assert.Contains(2L, provider.ActiveVersions);
    }

    // validation

    [Fact]
    public async Task ReloadAsync_FunctionsWithoutSourceSystems_KeepsOldModel()
    {
        var factory = new InMemoryDbContextFactory();
        await EventReaderTestSeed.SeedDefaultAsync(factory);
        var provider = CreateProvider(factory);

        await provider.ReloadAsync(CancellationToken.None);
        var original = provider.Current;
        Assert.Equal(1, provider.CurrentVersion);

        await using (var db = await factory.CreateDbContextAsync())
        {
            var sourceSystems = db.SourceSystems.ToList();
            db.SourceSystems.RemoveRange(sourceSystems);
            await db.SaveChangesAsync();
        }

        await provider.ReloadAsync(CancellationToken.None);

        Assert.Same(original, provider.Current);
        Assert.Equal(1, provider.CurrentVersion);
        Assert.DoesNotContain(2L, provider.ActiveVersions);
    }

    // snapshot isolation

    [Fact]
    public async Task ReloadAsync_AtomicModelSwap_OldSnapshotRemainsValid()
    {
        var factory = new InMemoryDbContextFactory();
        await EventReaderTestSeed.SeedDefaultAsync(factory);
        var provider = CreateProvider(factory);

        await provider.ReloadAsync(CancellationToken.None);
        var snapshotBefore = provider.Current;
        Assert.Single(snapshotBefore.SourceSystems);

        await provider.ReloadAsync(CancellationToken.None);
        var snapshotAfter = provider.Current;

        Assert.Equal(2, snapshotAfter.Version);
        Assert.Equal(1, snapshotBefore.Version);
        Assert.Single(snapshotBefore.SourceSystems);
        Assert.Equal("ss-1", snapshotBefore.SourceSystems["ss-1"].SourceSystemId);
    }

    // garbage collection

    [Fact]
    public async Task CollectGarbageAsync_RemovesUnreferencedVersions_KeepsCurrentAndReferenced()
    {
        var factory = new InMemoryDbContextFactory();
        await EventReaderTestSeed.SeedDefaultAsync(factory);
        var provider = CreateProvider(factory);

        await provider.ReloadAsync(CancellationToken.None); // v1
        await provider.ReloadAsync(CancellationToken.None); // v2
        await provider.ReloadAsync(CancellationToken.None); // v3

        Assert.Equal(3, provider.CurrentVersion);

        var result = await provider.CollectGarbageAsync(new HashSet<long> { 2 }, CancellationToken.None);

        Assert.Equal(2, result.VersionsCollected); // v0, v1
        Assert.Equal(2, result.VersionsRetained);  // v2, v3
        Assert.DoesNotContain(0L, provider.ActiveVersions);
        Assert.DoesNotContain(1L, provider.ActiveVersions);
        Assert.Contains(2L, provider.ActiveVersions);
        Assert.Contains(3L, provider.ActiveVersions);
    }

    [Fact]
    public async Task CollectGarbageAsync_CurrentVersionIsNeverCollected()
    {
        var factory = new InMemoryDbContextFactory();
        await EventReaderTestSeed.SeedDefaultAsync(factory);
        var provider = CreateProvider(factory);

        await provider.ReloadAsync(CancellationToken.None); // v1
        await provider.ReloadAsync(CancellationToken.None); // v2
        await provider.ReloadAsync(CancellationToken.None); // v3

        var result = await provider.CollectGarbageAsync(new HashSet<long>(), CancellationToken.None);

        Assert.Equal(3, result.VersionsCollected);
        Assert.Equal(1, result.VersionsRetained);
        Assert.Single(provider.ActiveVersions);
        Assert.Contains(3L, provider.ActiveVersions);
    }

    [Fact]
    public async Task CollectGarbageAsync_KeepsAllVersionsReferencedByWorkItems()
    {
        var factory = new InMemoryDbContextFactory();
        await EventReaderTestSeed.SeedDefaultAsync(factory);
        var provider = CreateProvider(factory);

        await provider.ReloadAsync(CancellationToken.None); // v1
        await provider.ReloadAsync(CancellationToken.None); // v2
        await provider.ReloadAsync(CancellationToken.None); // v3
        await provider.ReloadAsync(CancellationToken.None); // v4
        await provider.ReloadAsync(CancellationToken.None); // v5

        Assert.Equal(5, provider.CurrentVersion);

        var result = await provider.CollectGarbageAsync(new HashSet<long> { 1, 3, 4 }, CancellationToken.None);

        Assert.Equal(2, result.VersionsCollected); // v0, v2
        Assert.Equal(4, result.VersionsRetained);  // v1, v3, v4, v5
        Assert.DoesNotContain(0L, provider.ActiveVersions);
        Assert.DoesNotContain(2L, provider.ActiveVersions);
        Assert.Contains(1L, provider.ActiveVersions);
        Assert.Contains(3L, provider.ActiveVersions);
        Assert.Contains(4L, provider.ActiveVersions);
        Assert.Contains(5L, provider.ActiveVersions);
    }
}
