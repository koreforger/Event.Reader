using System.Diagnostics;
using System.Text.Json;
using Confluent.Kafka;
using EventReader.Configuration;
using EventReader.Logging;
using Event.Streaming.Out.Dlq;
using Event.Streaming.Out.Routing;
using Event.Streaming.Processing.Envelopes;
using Event.Streaming.Processing.Runtime;
using KF.Time;

namespace EventReader.Output;

public interface IEventReaderOutputRouter
{
    Task<IReadOnlyList<WriteResult>> RouteAsync(
        OperationalEnvelope envelope,
        ProcessingProfile profile,
        CancellationToken ct);
}

public sealed class EventReaderOutputRouter : IEventReaderOutputRouter, IAsyncDisposable
{
    private readonly EventReaderLogger<EventReaderOutputRouter> _log;
    private readonly IConfiguration _configuration;
    private readonly ISystemClock _clock;
    private IProducer<string, string>? _producer;

    public EventReaderOutputRouter(
        EventReaderLogger<EventReaderOutputRouter> log,
        IConfiguration configuration,
        ISystemClock clock)
    {
        _log = log;
        _configuration = configuration;
        _clock = clock;
    }

    public async Task<IReadOnlyList<WriteResult>> RouteAsync(
        OperationalEnvelope envelope,
        ProcessingProfile profile,
        CancellationToken ct)
    {
        var results = new List<WriteResult>(profile.OutputRoutes.Count);

        foreach (var route in profile.OutputRoutes)
        {
            if (string.IsNullOrWhiteSpace(route.TargetTopic))
            {
                results.Add(WriteResult.Fail(route.RouteName, "Route target topic is empty.", TimeSpan.Zero));
                continue;
            }

            var sw = Stopwatch.StartNew();
            try
            {
                _log.Route.Attempt.LogInformation(
                    "Route attempt {RouteName} for profile {Profile} to topic {Topic}",
                    route.RouteName,
                    profile.Name,
                    route.TargetTopic);

                var delivery = await GetProducer().ProduceAsync(
                    route.TargetTopic,
                    new Message<string, string>
                    {
                        Key = envelope.CorrelationId,
                        Value = JsonSerializer.Serialize(ToOutputEnvelope(envelope, route.RouteName, _clock.UtcNow)),
                        Headers = BuildHeaders(envelope, profile.Name, route.RouteName),
                    },
                    ct).ConfigureAwait(false);

                sw.Stop();
                _log.Route.Success.LogInformation(
                    "Route {RouteName} produced to {Topic} partition {Partition} offset {Offset}",
                    route.RouteName,
                    delivery.Topic,
                    delivery.Partition.Value,
                    delivery.Offset.Value);

                results.Add(WriteResult.Ok(route.RouteName, delivery.Topic, delivery.Offset.Value, sw.Elapsed));
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                sw.Stop();
                _log.Route.Failed.LogError(
                    ex,
                    "Route {RouteName} failed for profile {Profile} to topic {Topic}",
                    route.RouteName,
                    profile.Name,
                    route.TargetTopic);
                results.Add(WriteResult.Fail(route.RouteName, ex.Message, sw.Elapsed));
            }
        }

        return results;
    }

    private IProducer<string, string> GetProducer()
    {
        if (_producer is not null)
        {
            return _producer;
        }

        _producer = new ProducerBuilder<string, string>(EventReaderProducerConfig.Build(_configuration)).Build();
        return _producer;
    }

    private static Headers BuildHeaders(OperationalEnvelope envelope, string profileName, string routeName)
    {
        var headers = new Headers
        {
            { "x-envelope-id", System.Text.Encoding.UTF8.GetBytes(envelope.EnvelopeId.ToString("N")) },
            { "x-correlation-id", System.Text.Encoding.UTF8.GetBytes(envelope.CorrelationId) },
            { "x-profile", System.Text.Encoding.UTF8.GetBytes(profileName) },
            { "x-route", System.Text.Encoding.UTF8.GetBytes(routeName) },
        };
        return headers;
    }

    private static object ToOutputEnvelope(OperationalEnvelope envelope, string routeName, DateTimeOffset routedAt) => new
    {
        envelope.EnvelopeId,
        envelope.SchemaVersion,
        envelope.SourceTopic,
        envelope.SourcePartition,
        envelope.SourceOffset,
        envelope.SourceTimestamp,
        envelope.IngestedTimestamp,
        RoutedAt = routedAt,
        RouteName = routeName,
        envelope.ProducerApp,
        envelope.ProducerInstance,
        envelope.ClassificationProfile,
        envelope.PayloadContentType,
        envelope.RoutingTags,
        envelope.ProcessingWarnings,
        envelope.CorrelationId,
        envelope.TraceId,
        envelope.Payload,
    };

    public ValueTask DisposeAsync()
    {
        _producer?.Flush(TimeSpan.FromSeconds(5));
        _producer?.Dispose();
        return ValueTask.CompletedTask;
    }
}

public sealed class EventReaderDlqWriter : IDlqWriter, IAsyncDisposable
{
    private readonly EventReaderLogger<EventReaderDlqWriter> _log;
    private readonly IConfiguration _configuration;
    private readonly IEventReaderRuntimeOptionsProvider _optionsProvider;
    private IProducer<string, string>? _producer;

    public EventReaderDlqWriter(
        EventReaderLogger<EventReaderDlqWriter> log,
        IConfiguration configuration,
        IEventReaderRuntimeOptionsProvider optionsProvider)
    {
        _log = log;
        _configuration = configuration;
        _optionsProvider = optionsProvider;
    }

    public bool IsEnabled => _optionsProvider.Current.EnableDlq;

    public async Task<bool> WriteAsync(IOperationalEnvelope envelope, DlqMetadata metadata, CancellationToken ct)
    {
        var options = _optionsProvider.Current;
        if (!options.EnableDlq)
        {
            _log.Route.Dlq.Disabled.LogWarning(
                "DLQ disabled; cannot write failed message from {Topic} partition {Partition} offset {Offset}",
                metadata.OriginalTopic,
                metadata.OriginalPartition,
                metadata.OriginalOffset);
            return false;
        }

        try
        {
            var delivery = await GetProducer().ProduceAsync(
                options.DlqTopic,
                new Message<string, string>
                {
                    Key = envelope.CorrelationId,
                    Value = JsonSerializer.Serialize(new { Metadata = metadata, Envelope = envelope }),
                },
                ct).ConfigureAwait(false);

            _log.Route.Dlq.Written.LogWarning(
                "DLQ message written to {Topic} partition {Partition} offset {Offset}",
                delivery.Topic,
                delivery.Partition.Value,
                delivery.Offset.Value);
            return true;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _log.Route.Dlq.Failed.LogError(
                ex,
                "DLQ write failed for original topic {Topic} partition {Partition} offset {Offset}",
                metadata.OriginalTopic,
                metadata.OriginalPartition,
                metadata.OriginalOffset);
            return false;
        }
    }

    private IProducer<string, string> GetProducer()
    {
        if (_producer is not null)
        {
            return _producer;
        }

        _producer = new ProducerBuilder<string, string>(EventReaderProducerConfig.Build(_configuration)).Build();
        return _producer;
    }

    public ValueTask DisposeAsync()
    {
        _producer?.Flush(TimeSpan.FromSeconds(5));
        _producer?.Dispose();
        return ValueTask.CompletedTask;
    }
}

internal static class EventReaderProducerConfig
{
    public static ProducerConfig Build(IConfiguration configuration)
    {
        var clusterName = configuration["Kafka:Profiles:Default:Cluster"] ?? "Local";
        var bootstrap = configuration[$"Kafka:Clusters:{clusterName}:BootstrapServers"]
            ?? configuration["Kafka:Clusters:Local:BootstrapServers"];

        if (string.IsNullOrWhiteSpace(bootstrap))
        {
            throw new InvalidOperationException("Kafka bootstrap servers are not configured in runtime settings.");
        }

        return new ProducerConfig
        {
            BootstrapServers = bootstrap,
            Acks = Acks.All,
            EnableIdempotence = true,
        };
    }
}
