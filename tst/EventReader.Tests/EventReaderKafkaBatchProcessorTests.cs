using System.Text;
using Confluent.Kafka;
using EventReader.Configuration;
using EventReader.Kafka;
using EventReader.Logging;
using EventReader.Monitoring;
using EventReader.Output;
using EventReader.Profiles;
using KF.Kafka.Consumer.Batch;
using Event.Streaming.Out.Dlq;
using Event.Streaming.Out.Routing;
using Event.Streaming.Processing.Envelopes;
using Event.Streaming.Processing.Monitoring;
using Event.Streaming.Processing.Runtime;
using KF.Time;
using KoreForge.Jex;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace EventReader.Tests;

public sealed class EventReaderKafkaBatchProcessorTests
{
    [Fact]
    public async Task Required_route_failure_without_dlq_fails_batch()
    {
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["EventReader:EnableDlq"] = "false",
                ["EventReader:Profiles:0:Name"] = "payments",
                ["EventReader:Profiles:0:IsActive"] = "true",
                ["EventReader:Profiles:0:ClassificationExpression"] = "equals:payment.created",
                ["EventReader:Profiles:0:OutputRoutes:0:RouteName"] = "required-out",
                ["EventReader:Profiles:0:OutputRoutes:0:TargetTopic"] = "stream.curated",
                ["EventReader:Profiles:0:OutputRoutes:0:IsRequired"] = "true",
            })
            .Build();

        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddConsole());
        services.AddGeneratedLogging();
        services.AddSingleton<IConfiguration>(config);
        services.AddSingleton<ISystemClock>(_ => UtcSystemClock.Instance);
        services.AddSingleton<EventReaderMonitoringSnapshot>();
        services.AddSingleton<EventReaderMetricsAccumulator>();
        services.AddSingleton<IIncidentStore, InMemoryIncidentStore>();
        services.AddSingleton<IEventReaderRuntimeOptionsProvider, ConfigurationEventReaderRuntimeOptionsProvider>();
        services.AddSingleton<IProcessingProfileCatalog, EventReaderProfileCatalog>();
        services.AddSingleton<RootJsonFieldScanner>();
        services.AddSingleton<ProfileClassifier>();
        services.AddSingleton<IJexCompiler>(_ => new Jex());
        services.AddSingleton<JexPayloadTransformer>();
        services.AddScoped<IEventReaderOutputRouter, FailingOutputRouter>();
        services.AddScoped<IDlqWriter>(_ => NullDlqWriter.Instance);
        services.AddScoped<EventReaderKafkaBatchProcessor>();

        await using var provider = services.BuildServiceProvider();
        var processor = provider.GetRequiredService<EventReaderKafkaBatchProcessor>();
        var batch = new KafkaRecordBatch(
            [
                new ConsumeResult<byte[], byte[]>
                {
                    Topic = "stream.input",
                    Partition = new Partition(0),
                    Offset = new Offset(12),
                    Message = new Message<byte[], byte[]>
                    {
                        Value = Encoding.UTF8.GetBytes("""{"eventType":"payment.created","amount":10}"""),
                        Timestamp = new Timestamp(DateTime.UtcNow),
                    },
                },
            ],
            DateTimeOffset.UtcNow);

        await Assert.ThrowsAsync<EventReaderProcessingException>(
            () => processor.ProcessAsync(batch, CancellationToken.None));
    }

    private sealed class FailingOutputRouter : IEventReaderOutputRouter
    {
        public Task<IReadOnlyList<WriteResult>> RouteAsync(
            OperationalEnvelope envelope,
            ProcessingProfile profile,
            CancellationToken ct)
        {
            IReadOnlyList<WriteResult> result =
            [
                WriteResult.Fail("required-out", "simulated route failure", TimeSpan.Zero)
            ];
            return Task.FromResult(result);
        }
    }
}
