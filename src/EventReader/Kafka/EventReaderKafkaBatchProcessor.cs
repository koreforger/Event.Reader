using System.Diagnostics;
using System.Text.Json;
using System.Text;
using Confluent.Kafka;
using EventReader.Logging;
using EventReader.Monitoring;
using EventReader.Configuration;
using EventReader.Output;
using EventReader.Scripts.API.Hubs;
using EventReader.Scripts.API.Models;
using EventReader.Scripts.API.Services;
using KoreForge.Kafka.Consumer.Abstractions;
using KoreForge.Kafka.Consumer.Batch;
using Event.Streaming.Out.Dlq;
using Event.Streaming.Out.Routing;
using Event.Streaming.Processing.Envelopes;
using Event.Streaming.Processing.Monitoring;
using Event.Streaming.Processing.Pipeline;
using Event.Streaming.Processing.Runtime;
using KoreForge.Time;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json.Linq;

namespace EventReader.Kafka;

public sealed class EventReaderKafkaBatchProcessor : IKafkaBatchProcessor
{
    private static readonly TimeSpan RateQuietThreshold = TimeSpan.FromSeconds(15);

    private readonly EventReaderLogger<EventReaderKafkaBatchProcessor> _log;
    private readonly EventReaderMetricsAccumulator _metrics;
    private readonly EventReaderMonitoringSnapshot _snapshot;
    private readonly IIncidentStore _incidents;
    private readonly ISystemClock _clock;
    private readonly IProcessingProfileCatalog _profiles;
    private readonly ProfileClassifier _classifier;
    private readonly JexPayloadTransformer _transformer;
    private readonly IEventReaderOutputRouter _router;
    private readonly IDlqWriter _dlqWriter;
    private readonly IEventReaderRuntimeOptionsProvider _optionsProvider;
    private readonly ShadowTestService? _shadowTests;
    private readonly IHubContext<ShadowTestHub>? _shadowHub;
    private long _lastSnapshotProcessed;
    private DateTimeOffset? _lastSnapshotAt;
    private double _lastMessagesPerSecond;

    public EventReaderKafkaBatchProcessor(
        EventReaderLogger<EventReaderKafkaBatchProcessor> log,
        EventReaderMetricsAccumulator metrics,
        EventReaderMonitoringSnapshot snapshot,
        IIncidentStore incidents,
        ISystemClock clock,
        IProcessingProfileCatalog profiles,
        ProfileClassifier classifier,
        JexPayloadTransformer transformer,
        IEventReaderOutputRouter router,
        IDlqWriter dlqWriter,
        IEventReaderRuntimeOptionsProvider optionsProvider,
        IServiceProvider serviceProvider)
    {
        _log = log;
        _metrics = metrics;
        _snapshot = snapshot;
        _incidents = incidents;
        _clock = clock;
        _profiles = profiles;
        _classifier = classifier;
        _transformer = transformer;
        _router = router;
        _dlqWriter = dlqWriter;
        _optionsProvider = optionsProvider;
        _shadowTests = serviceProvider.GetService<ShadowTestService>();
        _shadowHub = serviceProvider.GetService<IHubContext<ShadowTestHub>>();
    }

    public async Task ProcessAsync(KafkaRecordBatch batch, CancellationToken cancellationToken)
    {
        var options = _optionsProvider.Current;
        var activeProfiles = _profiles.GetActiveProfiles();
        _classifier.Load(activeProfiles);

        _log.Kafka.Batch.Received.LogInformation("EventReader received batch with {Count} records", batch.Count);
        _metrics.RecordBatch(batch.Count, _clock.UtcNow);

        foreach (var record in batch.Records)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var recordStart = Stopwatch.GetTimestamp();

            try
            {
                var payloadBytes = record.Message.Value;
                if (payloadBytes is null || payloadBytes.Length == 0)
                {
                    await HandleRecordFailureAsync(
                        CreateEnvelope(record, EmptyJsonPayload()),
                        "Decode",
                        "Empty Kafka payload",
                        null,
                        cancellationToken).ConfigureAwait(false);
                    continue;
                }

                using var document = System.Text.Json.JsonDocument.Parse(payloadBytes);
                var payload = document.RootElement.Clone();

                if (options.DiagnosticStage == DiagnosticStage.KafkaOnly)
                {
                    RecordProcessed(recordStart);
                    continue;
                }

                var matched = _classifier.Match(payloadBytes);

                if (matched is null)
                {
                    _metrics.RecordUnclassified();
                    await HandleRecordFailureAsync(
                        CreateEnvelope(record, payload),
                        "Classify",
                        "No EventReader profile matched the inbound message.",
                        null,
                        cancellationToken).ConfigureAwait(false);
                    continue;
                }

                var envelope = CreateEnvelope(record, payload);
                envelope.ClassificationProfile = matched.Name;
                await RunShadowTestsAsync(payloadBytes, cancellationToken).ConfigureAwait(false);

                _log.Profile.Matched.LogInformation(
                    "EventReader classified record as {Profile} from topic {Topic} partition {Partition} offset {Offset}",
                    matched.Name, record.Topic, record.Partition.Value, record.Offset.Value);

                _metrics.RecordClassified(matched.Name);

                if (options.DiagnosticStage == DiagnosticStage.DecodeOnly ||
                    options.DiagnosticStage == DiagnosticStage.ClassifyOnly)
                {
                    RecordProcessed(recordStart);
                    continue;
                }

                var transformed = _transformer.Transform(payload, matched.ParseExpression);
                if (!transformed.Success)
                {
                    _metrics.RecordParseError();
                    envelope.Warnings.Add($"Parse failed for profile '{matched.Name}': {transformed.ErrorMessage}");
                    await HandleRecordFailureAsync(
                        envelope,
                        "Parse",
                        $"JEX parse failed for profile '{matched.Name}'",
                        null,
                        cancellationToken).ConfigureAwait(false);
                    continue;
                }

                envelope.Payload = transformed.Payload;

                foreach (var (k, v) in matched.RoutingTagOverrides)
                {
                    envelope.Tags[k] = v;
                }

                if (options.DiagnosticStage == DiagnosticStage.ParseOnly)
                {
                    RecordProcessed(recordStart);
                    continue;
                }

                if (options.DiagnosticStage == DiagnosticStage.NullOutput)
                {
                    foreach (var route in matched.OutputRoutes)
                    {
                        _metrics.RecordRouteSkipped(route.RouteName);
                        _log.Route.Skipped.LogInformation(
                            "Route {RouteName} skipped for profile {Profile} because NullOutput diagnostic mode is active",
                            route.RouteName,
                            matched.Name);
                    }

                    RecordProcessed(recordStart);
                    continue;
                }

                var routeResults = await _router.RouteAsync(envelope, matched, cancellationToken).ConfigureAwait(false);
                RecordRouteResults(matched, routeResults);

                var requiredFailure = routeResults.FirstOrDefault(r =>
                    !r.Success &&
                    matched.OutputRoutes.Any(route => route.IsRequired && string.Equals(route.RouteName, r.RouteName, StringComparison.OrdinalIgnoreCase)));

                if (requiredFailure is not null)
                {
                    await HandleRecordFailureAsync(
                        envelope,
                        "Route",
                        $"Required route '{requiredFailure.RouteName}' failed: {requiredFailure.ErrorMessage}",
                        null,
                        cancellationToken).ConfigureAwait(false);
                    continue;
                }

                RecordProcessed(recordStart);
            }
            catch (System.Text.Json.JsonException ex)
            {
                _log.Pipeline.Decode.Error.LogError(
                    ex,
                    "EventReader failed to parse payload from topic {Topic} partition {Partition} offset {Offset}",
                    record.Topic, record.Partition.Value, record.Offset.Value);
                await HandleRecordFailureAsync(
                    CreateEnvelope(record, EmptyJsonPayload()),
                    "Decode",
                    "Failed to parse EventReader payload",
                    ex,
                    cancellationToken).ConfigureAwait(false);
            }
        }

        UpdateSnapshot(options);
    }

    private OperationalEnvelope CreateEnvelope(ConsumeResult<byte[], byte[]> record, JsonElement payload) => new()
    {
        SourceTopic = record.Topic,
        SourcePartition = record.Partition.Value,
        SourceOffset = record.Offset.Value,
        SourceTimestamp = new DateTimeOffset(record.Message.Timestamp.UtcDateTime),
        IngestedTimestamp = _clock.UtcNow,
        ProducerApp = "EventReader",
        ProducerInstance = _snapshot.Instance,
        PayloadContentType = "application/json",
        Payload = payload.Clone(),
    };

    private static JsonElement EmptyJsonPayload()
    {
        using var document = JsonDocument.Parse("{}");
        return document.RootElement.Clone();
    }

    private async Task HandleRecordFailureAsync(
        OperationalEnvelope envelope,
        string category,
        string message,
        Exception? exception,
        CancellationToken ct)
    {
        _metrics.RecordError(category);
        _incidents.Record(new OperationalIncident
        {
            Application = "EventReader",
            InstanceId = _snapshot.Instance,
            Category = category,
            Severity = "Error",
            Message = message,
            Detail = exception?.Message,
            OccurredAt = _clock.UtcNow,
        });

        var metadata = new DlqMetadata
        {
            OriginalTopic = envelope.SourceTopic,
            OriginalPartition = envelope.SourcePartition,
            OriginalOffset = envelope.SourceOffset,
            FailureReason = message,
            FailureCategory = category,
            ExceptionType = exception?.GetType().Name,
            ExceptionMessage = exception?.Message,
            ProcessorApp = "EventReader",
            ProcessorInstance = _snapshot.Instance,
            FailedAt = _clock.UtcNow,
            StageName = category,
            ProfileName = envelope.ClassificationProfile,
        };

        if (await _dlqWriter.WriteAsync(envelope, metadata, ct).ConfigureAwait(false))
        {
            _metrics.RecordDlqWritten();
            return;
        }

        throw new EventReaderProcessingException(
            $"EventReader cannot commit record {envelope.SourceTopic}[{envelope.SourcePartition}]@{envelope.SourceOffset}: {message}");
    }

    private void RecordRouteResults(ProcessingProfile profile, IReadOnlyList<WriteResult> results)
    {
        foreach (var route in profile.OutputRoutes)
        {
            _metrics.RecordRouteAttempt(route.RouteName);
        }

        foreach (var result in results)
        {
            if (result.Success)
            {
                _metrics.RecordRouteSuccess(result.RouteName);
            }
            else
            {
                _metrics.RecordRouteFailure(result.RouteName);
            }
        }
    }

    private async Task RunShadowTestsAsync(byte[] payloadBytes, CancellationToken cancellationToken)
    {
        if (_shadowTests is null || _shadowHub is null || _shadowTests.ActiveSessions.Count == 0)
        {
            return;
        }

        JObject input;
        try
        {
            input = JObject.Parse(Encoding.UTF8.GetString(payloadBytes));
        }
        catch
        {
            return;
        }

        foreach (var session in _shadowTests.ActiveSessions.ToList())
        {
            var result = _shadowTests.ExecuteComparison(session.FunctionId, input);
            if (result is null)
            {
                continue;
            }

            var payload = ToShadowResultPayload(result);
            await Task.WhenAll(
                _shadowHub.Clients.Group(result.SessionId).SendAsync("shadow-result", payload, cancellationToken),
                _shadowHub.Clients.Group(result.SessionId).SendAsync("ShadowTestResult", payload, cancellationToken))
                .ConfigureAwait(false);

            if (!_shadowTests.IsSessionComplete(result.SessionId))
            {
                continue;
            }

            var summary = _shadowTests.StopSession(result.SessionId, "sample_size_reached");
            if (summary is null)
            {
                continue;
            }

            var summaryPayload = ToShadowSummaryPayload(summary);
            await Task.WhenAll(
                _shadowHub.Clients.Group(result.SessionId).SendAsync("shadow-complete", summaryPayload, cancellationToken),
                _shadowHub.Clients.Group(result.SessionId).SendAsync("ShadowTestComplete", summaryPayload, cancellationToken))
                .ConfigureAwait(false);
        }
    }

    private static object ToShadowResultPayload(ShadowTestResult result) => new
    {
        result.SessionId,
        result.MessageIndex,
        MessageKey = $"msg-{result.MessageIndex}",
        result.InputSnippet,
        result.CurrentOutput,
        result.CandidateOutput,
        result.CurrentTimeMs,
        result.CandidateTimeMs,
        Match = result.Diffs.Count == 0,
        result.Diffs,
        ProcessedAt = result.Timestamp,
        result.Timestamp,
    };

    private static object ToShadowSummaryPayload(ShadowTestSummary summary)
    {
        var mismatchCount = Math.Max(0, summary.TotalProcessed - summary.MatchCount);
        var matchPercentage = summary.TotalProcessed == 0
            ? 0
            : (summary.MatchCount / (double)summary.TotalProcessed) * 100;

        return new
        {
            summary.SessionId,
            summary.FunctionId,
            summary.TotalProcessed,
            summary.MatchCount,
            summary.DiffCount,
            summary.AvgCurrentTimeMs,
            summary.AvgCandidateTimeMs,
            summary.CompletedAt,
            summary.Reason,
            TotalSamples = summary.TotalProcessed,
            MismatchCount = mismatchCount,
            MatchPercentage = matchPercentage,
        };
    }

    private void UpdateSnapshot(EventReaderRuntimeOptions options)
    {
        var capturedAt = _clock.UtcNow;
        _snapshot.Instance = options.InstanceId;
        _snapshot.Version = options.Version;
        _snapshot.Timestamp = capturedAt;
        _snapshot.Health = _metrics.TotalErrors == 0 ? HealthStatus.Healthy : HealthStatus.Degraded;
        _snapshot.PipelineMetrics = new PipelineMetrics
        {
            TotalProcessed = _metrics.TotalProcessed,
            TotalErrors = _metrics.TotalErrors,
            AvgLatencyMs = _metrics.AverageLatencyMs,
            P95LatencyMs = _metrics.P95LatencyMs,
            P99LatencyMs = _metrics.P99LatencyMs,
            ErrorCountByCategory = _metrics.ErrorCountsByCategory,
            MessagesPerSecond = CalculateMessagesPerSecond(capturedAt),
        };
        _snapshot.AppSpecificMetrics = new Dictionary<string, object>
        {
            ["DiagnosticStage"] = options.DiagnosticStage.ToString(),
            ["Processed"] = _metrics.TotalProcessed,
            ["Errors"] = _metrics.TotalErrors,
            ["Batches"] = _metrics.TotalBatches,
            ["MessagesReceived"] = _metrics.TotalMessagesReceived,
            ["Unclassified"] = _metrics.Unclassified,
            ["ParseErrors"] = _metrics.ParseErrors,
            ["DlqWritten"] = _metrics.DlqWritten,
            ["Classifications"] = _metrics.ClassificationCounts,
            ["RouteAttempts"] = _metrics.RouteAttempts,
            ["RouteSuccesses"] = _metrics.RouteSuccesses,
            ["RouteFailures"] = _metrics.RouteFailures,
            ["RouteSkipped"] = _metrics.RouteSkipped,
        };
        _snapshot.DiagnosticStage = options.DiagnosticStage.ToString();
    }

    private void RecordProcessed(long recordStartTimestamp) =>
        _metrics.RecordProcessed(Stopwatch.GetElapsedTime(recordStartTimestamp), _clock.UtcNow);

    private double CalculateMessagesPerSecond(DateTimeOffset capturedAt)
    {
        var processed = _metrics.TotalProcessed;
        if (_lastSnapshotAt is null)
        {
            _lastSnapshotAt = capturedAt;
            _lastSnapshotProcessed = processed;
            return 0;
        }

        var elapsedSeconds = (capturedAt - _lastSnapshotAt.Value).TotalSeconds;
        var delta = processed - _lastSnapshotProcessed;

        if (delta <= 0)
        {
            if (elapsedSeconds < RateQuietThreshold.TotalSeconds)
            {
                return _lastMessagesPerSecond;
            }

            _lastSnapshotAt = capturedAt;
            _lastSnapshotProcessed = processed;
            _lastMessagesPerSecond = 0;
            return 0;
        }

        _lastSnapshotAt = capturedAt;
        _lastSnapshotProcessed = processed;

        _lastMessagesPerSecond = elapsedSeconds <= 0 ? 0 : Math.Max(0, delta / elapsedSeconds);
        return _lastMessagesPerSecond;
    }
}
