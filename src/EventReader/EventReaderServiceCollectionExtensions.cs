using Event.Data;
using EventReader.Configuration;
using EventReader.Kafka;
using EventReader.Monitoring;
using Event.Streaming.Processing.Monitoring;
using Event.Streaming.Processing.Runtime;
using Event.Streaming.Processing.WorkStore;
using KoreForge.Jex;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace EventReader;

public static class EventReaderServiceCollectionExtensions
{
    public static IServiceCollection AddEventReaderDurablePipeline(
        this IServiceCollection services,
        Action<EventReaderDurablePipelineOptions> configure)
    {
        var options = new EventReaderDurablePipelineOptions();
        configure(options);

        // Event.Data factory — uses same connection string as KFSettings
        services.AddEventDataFactory(sp =>
        {
            var cfg = sp.GetRequiredService<IConfiguration>();
            return cfg.GetConnectionString("KFSettings") ?? string.Empty;
        });

        services.AddSingleton(new EventReaderShardWorkerPoolOptions
        {
            WorkerCount = options.WorkerCount,
            LogicalShardCount = options.LogicalShardCount,
            LeaseDuration = options.LeaseDuration,
            BatchSize = options.ProcessingBatchSize,
            IdleDelay = options.IdleDelay,
        });

        services.AddSingleton(new EventReaderOutputPublisherOptions
        {
            BatchSize = options.OutputBatchSize,
            LeaseDuration = options.LeaseDuration,
            IdleDelay = options.IdleDelay,
        });

        services.AddSingleton(new FasterEventReaderWorkStoreOptions
        {
            LogPath = options.FasterLogPath,
            CheckpointPath = options.FasterCheckpointPath,
            IndexSizeBuckets = options.FasterIndexSizeBuckets,
        });

        services.AddSingleton<IEventReaderWorkStore, FasterEventReaderWorkStore>();
        services.AddSingleton<IOutputMessagePublisher, LoggingOutputMessagePublisher>();
        services.AddSingleton<IWorkItemProcessor, EventReaderWorkItemProcessor>();

        services.AddSingleton<JsonFieldScanner>();
        services.AddSingleton<IUsernameIdentityLookup, NoOpUsernameLookup>();
        services.AddSingleton<ClientIdentityResolver>();
        services.AddSingleton(sp => new ShardAssigner(options.LogicalShardCount));

        services.AddSingleton(sp => new EventReaderRuntimeModelProvider(
            sp.GetRequiredService<Microsoft.EntityFrameworkCore.IDbContextFactory<EventDataContext>>(),
            sp.GetRequiredService<IConfiguration>()));
        services.AddSingleton<IEventReaderRuntimeModelProvider>(sp => sp.GetRequiredService<EventReaderRuntimeModelProvider>());

        services.AddScoped<EventReaderDurableBatchProcessor>();

        // Startup service runs first: eagerly loads the runtime model from DB before
        // EventReaderConsumerWorker opens the Kafka consumer.
        services.AddHostedService<EventReaderModelStartupService>();
        services.AddHostedService<EventReaderShardWorkerPool>();
        services.AddHostedService<EventReaderOutputPublisher>();
        services.AddHostedService<EventReaderRuntimeModelGcService>();

        return services;
    }

    private sealed class NoOpUsernameLookup : IUsernameIdentityLookup
    {
        public Task<long?> ResolveNedbankIdAsync(string username, CancellationToken cancellationToken) =>
            Task.FromResult<long?>(null);
    }
}

public sealed class EventReaderDurablePipelineOptions
{
    public string FasterLogPath { get; set; } = "data/eventreader/faster/work-log";
    public string FasterCheckpointPath { get; set; } = "data/eventreader/faster/checkpoints";
    public long FasterIndexSizeBuckets { get; set; } = 1L << 20;
    public int WorkerCount { get; set; } = 32;
    public int LogicalShardCount { get; set; } = 1024;
    public int ProcessingBatchSize { get; set; } = 10;
    public int OutputBatchSize { get; set; } = 100;
    public TimeSpan LeaseDuration { get; set; } = TimeSpan.FromMinutes(5);
    public TimeSpan IdleDelay { get; set; } = TimeSpan.FromMilliseconds(100);
}
