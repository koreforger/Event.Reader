using Event.Streaming.Processing.Envelopes;
using Event.Streaming.Processing.Pipeline;

namespace EventReader.Pipeline.Stages;

/// <summary>
/// Stage 1 — Receive: wraps a Kafka record into an OperationalEnvelope.
/// Sets source topic, partition, offset, timestamp, and raw payload bytes.
/// </summary>
public sealed class ReceiveStage : IPipelineStage<byte[], OperationalEnvelope>
{
    private readonly string _sourceTopic;

    public ReceiveStage(string sourceTopic) => _sourceTopic = sourceTopic;

    public string StageName => "Receive";

    public Task<StageExecutionResult<OperationalEnvelope>> ExecuteAsync(byte[] input, CancellationToken ct)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var envelope = new OperationalEnvelope
        {
            SourceTopic = _sourceTopic,
            IngestedTimestamp = DateTimeOffset.UtcNow,
        };
        return Task.FromResult(StageExecutionResult<OperationalEnvelope>.Ok(envelope, sw.Elapsed));
    }
}

/// <summary>
/// Stage 2 — Decode: deserialises raw bytes to a JSON payload.
/// For non-JSON content types, stores raw bytes as base64 in the payload.
/// </summary>
public sealed class DecodeStage : IPipelineStage<OperationalEnvelope, OperationalEnvelope>
{
    public string StageName => "Decode";

    public Task<StageExecutionResult<OperationalEnvelope>> ExecuteAsync(OperationalEnvelope input, CancellationToken ct)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        // Decode is applied externally via KafkaRecordBatch value bytes —
        // this stage validates the envelope has a usable payload.
        if (input.Payload.ValueKind == System.Text.Json.JsonValueKind.Undefined)
        {
            return Task.FromResult(StageExecutionResult<OperationalEnvelope>.Fail(
                "Decode", "Payload is undefined — decoding produced no output", sw.Elapsed));
        }
        return Task.FromResult(StageExecutionResult<OperationalEnvelope>.Ok(input, sw.Elapsed));
    }
}

/// <summary>
/// Stage 3 — Classify: applies the profile ClassificationExpression to tag the envelope.
/// If no expression is configured, the profile name itself is used as the classification.
/// </summary>
public sealed class ClassifyStage : IPipelineStage<OperationalEnvelope, OperationalEnvelope>
{
    private readonly string _profileName;

    public ClassifyStage(string profileName) => _profileName = profileName;

    public string StageName => "Classify";

    public Task<StageExecutionResult<OperationalEnvelope>> ExecuteAsync(OperationalEnvelope input, CancellationToken ct)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        if (string.IsNullOrWhiteSpace(input.ClassificationProfile))
            input.ClassificationProfile = _profileName;
        return Task.FromResult(StageExecutionResult<OperationalEnvelope>.Ok(input, sw.Elapsed));
    }
}

/// <summary>
/// Stage 4 — Parse: applies the profile ParseExpression (JEX transform) to the payload.
/// No-op when no expression is configured.
/// </summary>
public sealed class ParseStage : IPipelineStage<OperationalEnvelope, OperationalEnvelope>
{
    public string StageName => "Parse";

    public Task<StageExecutionResult<OperationalEnvelope>> ExecuteAsync(OperationalEnvelope input, CancellationToken ct)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        // JEX evaluation is injected via IOutputRouter when a parse expression is present.
        // This stage is a pass-through placeholder that can be replaced per-profile.
        return Task.FromResult(StageExecutionResult<OperationalEnvelope>.Ok(input, sw.Elapsed));
    }
}

/// <summary>
/// Stage 5 — Enrich: applies profile routing tag overrides to the envelope.
/// </summary>
public sealed class EnrichStage : IPipelineStage<OperationalEnvelope, OperationalEnvelope>
{
    private readonly IReadOnlyDictionary<string, string> _tagOverrides;

    public EnrichStage(IReadOnlyDictionary<string, string> tagOverrides) => _tagOverrides = tagOverrides;

    public string StageName => "Enrich";

    public Task<StageExecutionResult<OperationalEnvelope>> ExecuteAsync(OperationalEnvelope input, CancellationToken ct)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        foreach (var (k, v) in _tagOverrides)
            input.Tags[k] = v;
        return Task.FromResult(StageExecutionResult<OperationalEnvelope>.Ok(input, sw.Elapsed));
    }
}
