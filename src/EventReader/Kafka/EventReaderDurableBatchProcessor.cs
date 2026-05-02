using System.Diagnostics;
using Confluent.Kafka;
using EventReader.Configuration;
using EventReader.Logging;
using EventReader.Monitoring;
using KoreForge.Kafka.Consumer.Abstractions;
using KoreForge.Kafka.Consumer.Batch;
using Event.Streaming.Processing.Runtime;
using Event.Streaming.Processing.WorkStore;

namespace EventReader.Kafka;

public sealed class EventReaderDurableBatchProcessor : IKafkaBatchProcessor
{
    private readonly EventReaderLogger<EventReaderDurableBatchProcessor> _log;
    private readonly EventReaderMetricsAccumulator _metrics;
    private readonly IEventReaderWorkStore _workStore;
    private readonly IEventReaderRuntimeModelProvider _modelProvider;
    private readonly JsonFieldScanner _scanner;
    private readonly ClientIdentityResolver _identityResolver;
    private readonly ShardAssigner _shardAssigner;

    public EventReaderDurableBatchProcessor(
        EventReaderLogger<EventReaderDurableBatchProcessor> log,
        EventReaderMetricsAccumulator metrics,
        IEventReaderWorkStore workStore,
        IEventReaderRuntimeModelProvider modelProvider,
        JsonFieldScanner scanner,
        ClientIdentityResolver identityResolver,
        ShardAssigner shardAssigner)
    {
        _log = log;
        _metrics = metrics;
        _workStore = workStore;
        _modelProvider = modelProvider;
        _scanner = scanner;
        _identityResolver = identityResolver;
        _shardAssigner = shardAssigner;
    }

    public async Task ProcessAsync(KafkaRecordBatch batch, CancellationToken cancellationToken)
    {
        _metrics.RecordBatch(batch.Count, DateTimeOffset.UtcNow);

        foreach (var record in batch.Records)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var recordStart = Stopwatch.GetTimestamp();

            try
            {
                await ProcessRecordAsync(record, cancellationToken).ConfigureAwait(false);

                var elapsed = Stopwatch.GetElapsedTime(recordStart);
                _metrics.RecordProcessed(elapsed, DateTimeOffset.UtcNow);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _metrics.RecordError("ClassifyEnqueue");
                _log.Durable.Enqueue.Failed.LogError(
                    ex,
                    "EventReader durable enqueue failed for topic {Topic} partition {Partition} offset {Offset}",
                    record.Topic, record.Partition.Value, record.Offset.Value);
                throw;
            }
        }
    }

    private async Task ProcessRecordAsync(
        ConsumeResult<byte[], byte[]> record,
        CancellationToken cancellationToken)
    {
        var runtimeModel = _modelProvider.Current;

        var payload = record.Message.Value;
        if (payload is null || payload.Length == 0)
        {
            _metrics.RecordUnclassified();
            _log.Durable.Classify.Emptypayload.LogInformation(
                "EventReader received empty payload from topic {Topic} partition {Partition} offset {Offset}",
                record.Topic, record.Partition.Value, record.Offset.Value);
            return;
        }

        var sourceSystemId = runtimeModel.ResolveSourceSystemId(record.Topic);
        if (sourceSystemId is null)
        {
            _metrics.RecordUnclassified();
            _log.Durable.Topic.Notmapped.LogWarning(
                "EventReader received record from topic {Topic} which is not mapped to any source system",
                record.Topic);
            return;
        }

        var allPaths = new List<JsonPathSelector>();
        allPaths.AddRange(runtimeModel.FunctionDiscriminatorPaths);
        allPaths.AddRange(runtimeModel.ClientIdentityPaths);

        var scanResult = _scanner.Scan(payload, allPaths);

        if (!scanResult.IsValidJson)
        {
            _metrics.RecordError("Decode");
            _log.Durable.Invalidjson.LogWarning(
                "EventReader received invalid JSON from topic {Topic} partition {Partition} offset {Offset}",
                record.Topic, record.Partition.Value, record.Offset.Value);
            return;
        }

        var discriminatorValue = ResolveDiscriminatorValue(scanResult, runtimeModel.FunctionDiscriminatorPaths);
        if (discriminatorValue is null)
        {
            _metrics.RecordUnclassified();
            _log.Durable.Classify.Nodiscriminator.LogWarning(
                "EventReader found no discriminator value in payload from topic {Topic} partition {Partition} offset {Offset}",
                record.Topic, record.Partition.Value, record.Offset.Value);
            return;
        }

        var matchResult = runtimeModel.FunctionMatchers.Match(sourceSystemId, discriminatorValue);
        if (matchResult is null)
        {
            _metrics.RecordUnclassified();
            _log.Durable.Classify.Nomatch.LogWarning(
                "EventReader found no function match for discriminator '{Discriminator}' source system '{SourceSystem}'",
                discriminatorValue, sourceSystemId);
            return;
        }

        var functionId = matchResult.FunctionId;

        var identityResult = await _identityResolver
            .ResolveAsync(scanResult, cancellationToken)
            .ConfigureAwait(false);

        var shardAssignment = _shardAssigner.Assign(identityResult.NedbankId);

        var functionPlan = runtimeModel.Functions.TryGetValue(functionId, out var func)
            ? func
            : null;

        var classified = new ClassifiedWorkItem(
            workItemId: 0,
            source: new KafkaSourceIdentity(
                sourceSystemId,
                record.Topic,
                record.Partition.Value,
                record.Offset.Value,
                record.Message.Timestamp.UtcDateTime != default
                    ? new DateTimeOffset(record.Message.Timestamp.UtcDateTime)
                    : null),
            functionId: functionId,
            nedbankId: identityResult.NedbankId,
            shardId: shardAssignment.ShardId,
            runtimeModelVersion: runtimeModel.Version,
            functionVersion: functionPlan?.FunctionVersion ?? 0,
            extractionScriptVersion: functionPlan?.ExtractionScript.Version ?? 0,
            ruleSetVersion: functionPlan?.RuleSet.Version ?? 0,
            outputRouteVersion: functionPlan?.OutputRoute.Version ?? 0,
            rawPayload: payload,
            createdUtc: DateTimeOffset.UtcNow);

        await _workStore.EnqueueClassifiedAsync(classified, cancellationToken).ConfigureAwait(false);

        _metrics.RecordClassified(functionId.ToString());
        _log.Durable.Enqueue.Complete.LogInformation(
            "EventReader classified and enqueued FunctionID {FunctionId} NID {NedbankId} Shard {ShardId} from source '{SourceSystemId}' topic {Topic} partition {Partition} offset {Offset}",
            functionId, identityResult.NedbankId, shardAssignment.ShardId,
            sourceSystemId, record.Topic, record.Partition.Value, record.Offset.Value);
    }

    private static string? ResolveDiscriminatorValue(
        JsonFieldScanResult scanResult,
        IReadOnlyList<JsonPathSelector> discriminatorPaths)
    {
        foreach (var path in discriminatorPaths)
        {
            if (scanResult.Fields.TryGetValue(path, out var value) && !string.IsNullOrEmpty(value))
            {
                return value;
            }
        }

        return null;
    }
}
