using EventReader.Monitoring;
using KoreForge.Kafka.Configuration.Exceptions;
using KoreForge.Kafka.Configuration.Factory;
using KoreForge.Kafka.Consumer.Hosting;
using KoreForge.Metrics;
using Microsoft.Extensions.Logging;

namespace EventReader.Kafka;

public sealed class EventReaderConsumerWorker : BackgroundService
{
    private static readonly TimeSpan StartupRetryDelay = TimeSpan.FromSeconds(10);

    private readonly IServiceScopeFactory _scopeFactory;
    private readonly IKafkaClientConfigFactory _configFactory;
    private readonly ILoggerFactory _loggerFactory;
    private readonly IOperationMonitor _monitor;
    private readonly EventReaderIncidentTelemetryProvider _incidentTelemetryProvider;
    private AsyncServiceScope? _scope;
    private ILogger<EventReaderConsumerWorker>? _log;
    private KafkaConsumerHost? _host;

    public EventReaderConsumerWorker(
        IServiceScopeFactory scopeFactory,
        IKafkaClientConfigFactory configFactory,
        ILoggerFactory loggerFactory,
        IOperationMonitor monitor,
        EventReaderIncidentTelemetryProvider incidentTelemetryProvider)
    {
        _scopeFactory = scopeFactory;
        _configFactory = configFactory;
        _loggerFactory = loggerFactory;
        _monitor = monitor;
        _incidentTelemetryProvider = incidentTelemetryProvider;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _scope = _scopeFactory.CreateAsyncScope();
        var processor = _scope.Value.ServiceProvider.GetRequiredService<EventReaderDurableBatchProcessor>();
        _log = _scope.Value.ServiceProvider.GetRequiredService<ILogger<EventReaderConsumerWorker>>();

        while (!stoppingToken.IsCancellationRequested)
        {
            _log.LogInformation("Building Kafka consumer host for profile 'Default'");
            try
            {
                _host = KafkaConsumerHost.Create()
                    .UseKafkaConfigurationProfile("Default", _configFactory)
                    .UseOperationMonitor(_monitor)
                    .UseLoggerFactory(_loggerFactory)
                    .UseProcessor(() => processor)
                    .Build();

                await _host.StartAsync(stoppingToken);
                _log.LogInformation("Kafka consumer host started");
                break;
            }
            catch (KafkaProfileNotFoundException ex)
            {
                _log.LogWarning(ex, "Kafka profile 'Default' not available yet, retrying in {Seconds}s", StartupRetryDelay.TotalSeconds);
            }
            catch (KafkaProfileValidationException ex)
            {
                _log.LogWarning(ex, "Kafka profile 'Default' not valid yet, retrying in {Seconds}s", StartupRetryDelay.TotalSeconds);
            }

            await Task.Delay(StartupRetryDelay, stoppingToken);
        }

        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException)
        {
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _log?.LogInformation("Stopping Kafka consumer host");

        if (_host is not null)
        {
            await _host.StopAsync(cancellationToken);
            await _host.DisposeAsync();
        }

        if (_scope is not null)
        {
            await _scope.Value.DisposeAsync();
        }

        _log?.LogInformation("Kafka consumer host stopped");
        await base.StopAsync(cancellationToken);
    }
}
