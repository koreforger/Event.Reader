using EventReader;
using EventReader.Hubs;
using EventReader.Kafka;
using EventReader.Logging;
using EventReader.Monitoring;
using EventReader.Configuration;
using KF.Settings.Extensions;
using KF.Settings.Reload;
using KF.Kafka.Configuration.Extensions;
using KF.Metrics;
using KF.Metrics.AspNet;
using KF.Scripts.AspNet;
using KF.Scripts.Core;
using Event.Streaming.Processing.Monitoring;
using Event.Streaming.Processing.Runtime;
using Event.Streaming.Processing.WorkStore;
using KF.Time;
using KF.Web.HealthChecks;
using Microsoft.Extensions.Hosting;
using KoreForge.Jex;
using KoreForge.AppLifecycle;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

builder.Configuration.AddKFSettings();
builder.Services.AddKFSettingsServices(builder.Configuration);

builder.Services.AddApplicationLifecycleManager(_ => { });
builder.Services.AddGeneratedLogging();
builder.Services.AddSingleton<ISystemClock>(_ => UtcSystemClock.Instance);
builder.Services.AddKafkaConfiguration(builder.Configuration);

// -- App options --
builder.Services
    .AddOptions<EventReaderRuntimeOptions>()
    .BindConfiguration("EventReader");
builder.Services.AddSingleton<IEventReaderRuntimeOptionsProvider, ConfigurationEventReaderRuntimeOptionsProvider>();

builder.Services.AddSingleton<EventReaderMonitoringSnapshot>();
builder.Services.AddSingleton<EventReaderMetricsAccumulator>();
builder.Services.AddSingleton<IIncidentStore, InMemoryIncidentStore>();
builder.Services.AddSingleton<EventReaderIncidentTelemetryProvider>();
builder.Services.AddSingleton<IIncidentTelemetryProvider>(sp => sp.GetRequiredService<EventReaderIncidentTelemetryProvider>());

// -- Durable pipeline --
builder.Services.AddSingleton(new EventReaderShardWorkerPoolOptions
{
    WorkerCount = 4,
    LogicalShardCount = 16,
    LeaseDuration = TimeSpan.FromMinutes(5),
    BatchSize = 10,
    IdleDelay = TimeSpan.FromMilliseconds(100),
});
builder.Services.AddSingleton(new EventReaderOutputPublisherOptions
{
    BatchSize = 100,
    LeaseDuration = TimeSpan.FromMinutes(5),
    IdleDelay = TimeSpan.FromMilliseconds(100),
});
builder.Services.AddSingleton<IOutputMessagePublisher, LoggingOutputMessagePublisher>();
builder.Services.AddSingleton<JsonFieldScanner>();
builder.Services.AddSingleton<IUsernameIdentityLookup, NoOpLookup>();
builder.Services.AddSingleton<ClientIdentityResolver>();
builder.Services.AddSingleton(sp => new ShardAssigner(16));
builder.Services.AddSingleton(sp => new EventReaderRuntimeModelProvider(
    sp.GetRequiredService<Microsoft.EntityFrameworkCore.IDbContextFactory<Event.Data.EventDataContext>>(),
    sp.GetRequiredService<IConfiguration>()));
builder.Services.AddSingleton<IEventReaderRuntimeModelProvider>(sp => sp.GetRequiredService<EventReaderRuntimeModelProvider>());
builder.Services.AddSingleton(sp => sp.GetRequiredService<IEventReaderRuntimeModelProvider>().Current);
builder.Services.AddSingleton<IEventReaderWorkStore>(_ =>
    new FasterEventReaderWorkStore(new FasterEventReaderWorkStoreOptions
    {
        LogPath = "data/eventreader/faster/work-log",
        CheckpointPath = "data/eventreader/faster/checkpoints",
    }));
builder.Services.AddSingleton<IWorkItemProcessor, EventReaderWorkItemProcessor>();
builder.Services.AddSingleton<IJexCompiler>(_ => new Jex());
builder.Services.AddScoped<EventReaderDurableBatchProcessor>();
builder.Services.AddHostedService<EventReaderShardWorkerPool>();
builder.Services.AddHostedService<EventReaderOutputPublisher>();
builder.Services.AddHostedService<EventReaderConsumerWorker>();

// -- Metrics --
builder.Services.AddKoreForgeMetrics();

// -- Health checks --
builder.Services.AddHealthChecks();

// -- Web + SignalR --
builder.Services.AddControllers();
builder.Services.AddCors(options =>
{
    options.AddPolicy("EventAdminUI", policy =>
        policy
            .WithOrigins("http://127.0.0.1:5174", "http://localhost:5174")
            .AllowAnyHeader()
            .AllowAnyMethod());
});
builder.Services.AddSignalR();

// -- Script + Function services --
var scriptsConnStr = builder.Configuration["KoreForge:Settings:ConnectionString"]
    ?? builder.Configuration.GetConnectionString("KFSettings")
    ?? throw new InvalidOperationException("Connection string for KFSettings is required.");
builder.Services.AddPooledDbContextFactory<Event.Data.EventDataContext>(opts =>
    opts.UseSqlServer(scriptsConnStr));
builder.Services.AddKoreForgeScripts(opts =>
{
    opts.ConnectionString = scriptsConnStr;
    opts.ApplicationId = "EventReader";
});

var app = builder.Build();

app.UseCors("EventAdminUI");
app.MapControllers();
app.MapHub<SettingsHub>("/hubs/settings");
app.MapHub<MonitoringHub>("/hubs/monitoring");
app.MapKfHealthEndpoints();
app.MapMonitoringEndpoints();
app.MapScriptEndpoints();

app.Run();
