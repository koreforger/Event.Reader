using Event.Data;
using Event.Data.Entities;
using Microsoft.EntityFrameworkCore;

namespace EventReader.Tests.Configuration;

/// <summary>
/// In-memory EF DbContextFactory for use in provider unit tests.
/// Each factory instance points to an isolated named in-memory database.
/// </summary>
internal sealed class InMemoryDbContextFactory : IDbContextFactory<EventDataContext>
{
    private readonly DbContextOptions<EventDataContext> _options;

    public InMemoryDbContextFactory(string? dbName = null) =>
        _options = new DbContextOptionsBuilder<EventDataContext>()
            .UseInMemoryDatabase(dbName ?? Guid.NewGuid().ToString("N"))
            .Options;

    public EventDataContext CreateDbContext() => new(_options);

    public Task<EventDataContext> CreateDbContextAsync(CancellationToken ct = default) =>
        Task.FromResult(new EventDataContext(_options));
}

/// <summary>
/// Standard seed data used across provider tests.
/// </summary>
internal static class EventReaderTestSeed
{
    public static async Task SeedDefaultAsync(
        InMemoryDbContextFactory factory,
        int functionVersion = 1,
        CancellationToken ct = default)
    {
        await using var db = await factory.CreateDbContextAsync(ct);

        db.Configs.AddRange([
            new ErConfig { ConfigKey = "PrefixLength", ConfigValue = "10" },
            new ErConfig { ConfigKey = "FunctionDiscriminatorPaths", ConfigValue = "$.type,$.eventType" },
            new ErConfig { ConfigKey = "ClientIdentityPaths", ConfigValue = "$.clientId" },
        ]);

        db.SourceSystems.Add(new ErSourceSystem
        {
            SourceSystemId = "ss-1",
            KafkaTopic = "topic.source",
            ProviderName = "prov-default",
            IsEnabled = true,
            Tags =
            [
                new ErSourceSystemTag { SourceSystemId = "ss-1", TagKey = "env", TagValue = "prod" },
            ],
        });

        db.Functions.Add(new ErFunction
        {
            FunctionId = 100,
            FunctionVersion = functionVersion,
            Name = "ProcessAlpha",
            IsEnabled = true,
            ScriptName = "extract-alpha",
            ScriptVersion = 1,
            RuleSetVersion = 1,
            OutputRouteVersion = 1,
            OutputRouteName = "default-route",
            OutputTopic = "output.topic",
            FailWhenNedbankIdMissing = false,
            MaxProcessingAttempts = 3,
            MaxOutputAttempts = 3,
            Rules =
            [
                new ErFunctionRule { FunctionId = 100, RuleId = 1, RuleVersion = 1, RuleName = "validate-alpha" },
            ],
        });

        db.FunctionMatchers.Add(new ErFunctionMatcher
        {
            SourceSystemId = null,
            FunctionId = 100,
            Priority = 10,
            Prefix = "alpha",
            RegexPattern = "^alpha.*",
        });

        await db.SaveChangesAsync(ct);
    }

    public static async Task SeedGcTestDataAsync(
        InMemoryDbContextFactory factory,
        CancellationToken ct = default)
    {
        await using var db = await factory.CreateDbContextAsync(ct);

        db.Configs.Add(new ErConfig { ConfigKey = "PrefixLength", ConfigValue = "9" });

        db.SourceSystems.Add(new ErSourceSystem
        {
            SourceSystemId = "sys",
            KafkaTopic = "topic",
            ProviderName = "Sys",
            IsEnabled = true,
        });

        db.Functions.Add(new ErFunction
        {
            FunctionId = 100,
            FunctionVersion = 1,
            Name = "Pay",
            IsEnabled = true,
            ScriptName = "pay-script",
            ScriptVersion = 1,
            RuleSetVersion = 1,
            OutputRouteVersion = 1,
            OutputRouteName = "pay-route",
            OutputTopic = "out.topic",
        });

        db.FunctionMatchers.Add(new ErFunctionMatcher
        {
            FunctionId = 100,
            Priority = 1,
            Prefix = "p",
            RegexPattern = "^pay$",
        });

        await db.SaveChangesAsync(ct);
    }
}
