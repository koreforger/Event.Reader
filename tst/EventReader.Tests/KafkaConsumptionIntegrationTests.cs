using EventReader.Kafka;
using EventReader.Logging;
using EventReader.Monitoring;
using EventReader.Configuration;
using EventReader.Output;
using EventReader.Profiles;
using EventReader.Tests.Infrastructure;
using KoreForge.Kafka.Configuration.Extensions;
using KoreForge.Kafka.Configuration.Factory;
using KoreForge.Kafka.Consumer.Hosting;
using Event.Streaming.Out.Dlq;
using Event.Streaming.Out.Routing;
using Event.Streaming.Processing.Envelopes;
using Event.Streaming.Processing.Monitoring;
using Event.Streaming.Processing.Runtime;
using KoreForge.Time;
using KoreForge.Jex;
using System.Text.Json;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace EventReader.Tests;

[Collection(KafkaClusterCollection.CollectionName)]
public sealed class KafkaConsumptionIntegrationTests
{
    private readonly KafkaTestClusterFixture _fixture;

    public KafkaConsumptionIntegrationTests(KafkaTestClusterFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task EventReader_consumes_real_kafka_messages()
    {
        var topic = $"event-reader-{Guid.NewGuid():N}";
        await _fixture.CreateTopicAsync(topic);

        var payloads = Enumerable.Range(0, 8)
            .Select(i => JsonSerializer.Serialize(new
            {
                eventType = "payment.created",
                eventId = $"evt-{i}",
                entityId = $"acct-{i % 2}",
                amount = 10 + i,
                customer = new { country = "GB", segment = "retail" },
                items = new[] { new { sku = $"sku-{i}", qty = 1 } },
            }))
            .ToArray();

        await _fixture.ProduceJsonAsync(topic, payloads);

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddGeneratedLogging();
        services.AddSingleton<ISystemClock>(_ => UtcSystemClock.Instance);
        services.AddSingleton<EventReaderMonitoringSnapshot>();
        services.AddSingleton<EventReaderMetricsAccumulator>();
        services.AddSingleton<IIncidentStore, InMemoryIncidentStore>();

        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Kafka:Clusters:Local:BootstrapServers"] = _fixture.BootstrapServers,
                ["Kafka:Profiles:Default:Type"] = "Consumer",
                ["Kafka:Profiles:Default:Cluster"] = "Local",
                ["Kafka:Profiles:Default:ExplicitGroupId"] = $"reader-it-{Guid.NewGuid():N}",
                ["Kafka:Profiles:Default:Topics:0"] = topic,
                ["Kafka:Profiles:Default:ConfluentOptions:auto.offset.reset"] = "earliest",
                ["Kafka:Profiles:Default:ConfluentOptions:enable.auto.commit"] = "false",
                ["Kafka:Profiles:Default:ExtendedConsumer:ConsumerCount"] = "1",
                ["Kafka:Profiles:Default:ExtendedConsumer:MaxBatchSize"] = "8",
                ["Kafka:Profiles:Default:ExtendedConsumer:MaxBatchWaitMs"] = "200",
                ["Kafka:Profiles:Default:ExtendedConsumer:StartMode"] = "Earliest",
                ["EventReader:DiagnosticStage"] = "ClassifyOnly",
                ["EventReader:Profiles:0:Name"] = "payments",
                ["EventReader:Profiles:0:IsActive"] = "true",
                ["EventReader:Profiles:0:ClassificationExpression"] = "equals:payment.created",
            })
            .Build();

        services.AddKafkaConfiguration(config);
        services.AddSingleton<IConfiguration>(config);
        services.AddSingleton<IEventReaderRuntimeOptionsProvider, ConfigurationEventReaderRuntimeOptionsProvider>();
        services.AddSingleton<IProcessingProfileCatalog, EventReaderProfileCatalog>();
        services.AddSingleton<RootJsonFieldScanner>();
        services.AddSingleton<ProfileClassifier>();
        services.AddSingleton<IJexCompiler>(_ => new Jex());
        services.AddSingleton<JexPayloadTransformer>();
        services.AddScoped<IEventReaderOutputRouter, NoopOutputRouter>();
        services.AddScoped<IDlqWriter>(_ => NullDlqWriter.Instance);
        services.AddScoped<EventReaderKafkaBatchProcessor>();

        await using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var processor = scope.ServiceProvider.GetRequiredService<EventReaderKafkaBatchProcessor>();
        var factory = scope.ServiceProvider.GetRequiredService<IKafkaClientConfigFactory>();
        var metrics = scope.ServiceProvider.GetRequiredService<EventReaderMetricsAccumulator>();
        var snapshot = scope.ServiceProvider.GetRequiredService<EventReaderMonitoringSnapshot>();
        var loggerFactory = scope.ServiceProvider.GetRequiredService<ILoggerFactory>();

        await using var host = KafkaConsumerHost.Create()
            .UseKafkaConfigurationProfile("Default", factory)
            .UseLoggerFactory(loggerFactory)
            .UseProcessor(() => processor)
            .Build();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        await host.StartAsync(cts.Token);
        await WaitUntilAsync(() => metrics.TotalProcessed == payloads.Length, TimeSpan.FromSeconds(30), cts.Token);
        await host.StopAsync(cts.Token);

        Assert.Equal(payloads.Length, metrics.TotalProcessed);
        Assert.Equal(0, metrics.TotalErrors);
        Assert.Equal(Event.Streaming.Processing.Monitoring.HealthStatus.Healthy, snapshot.Health);
    }

    private static async Task WaitUntilAsync(Func<bool> condition, TimeSpan timeout, CancellationToken cancellationToken)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!condition())
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException("Condition was not satisfied before timeout.");
            }

            await Task.Delay(200, cancellationToken).ConfigureAwait(false);
        }
    }

    private sealed class NoopOutputRouter : IEventReaderOutputRouter
    {
        public Task<IReadOnlyList<WriteResult>> RouteAsync(
            OperationalEnvelope envelope,
            ProcessingProfile profile,
            CancellationToken ct)
        {
            IReadOnlyList<WriteResult> results = profile.OutputRoutes
                .Select(route => WriteResult.Ok(route.RouteName, route.TargetTopic, null, TimeSpan.Zero))
                .ToArray();
            return Task.FromResult(results);
        }
    }
}
