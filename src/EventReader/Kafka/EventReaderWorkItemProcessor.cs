using System.Text;
using System.Text.Json;
using Event.Streaming.Processing.Runtime;
using Event.Streaming.Processing.WorkStore;
using KoreForge.Jex;
using Newtonsoft.Json.Linq;

namespace EventReader.Kafka;

public sealed class EventReaderWorkItemProcessor : IWorkItemProcessor
{
    private readonly EventReaderRuntimeModel _runtimeModel;
    private readonly IJexCompiler _jexCompiler;

    public EventReaderWorkItemProcessor(
        EventReaderRuntimeModel runtimeModel,
        IJexCompiler jexCompiler)
    {
        _runtimeModel = runtimeModel;
        _jexCompiler = jexCompiler;
    }

    public Task<WorkItemProcessingResult> ProcessAsync(WorkLease lease, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();

        try
        {
            var rawJson = Encoding.UTF8.GetString(lease.Item.RawPayload);

            var functionPlan = _runtimeModel.Functions.GetValueOrDefault(lease.Item.FunctionId);
            if (functionPlan is null)
            {
                return Task.FromResult(new WorkItemProcessingResult
                {
                    Success = false,
                    ErrorMessage = $"Function {lease.Item.FunctionId} not found in runtime model",
                    ShouldRetry = false,
                });
            }

            var mainObject = ParseFullJson(rawJson);

            mainObject = ExecuteExtraction(mainObject, functionPlan.ExtractionScript);

            var ruleOutputs = ExecuteRules(mainObject, functionPlan.RuleSet);

            var outputPayload = BuildOutputObject(lease, functionPlan, ruleOutputs);

            return Task.FromResult(new WorkItemProcessingResult
            {
                Success = true,
                OutputPayload = outputPayload,
            });
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return Task.FromResult(new WorkItemProcessingResult
            {
                Success = false,
                ErrorMessage = ex.Message,
                ShouldRetry = true,
            });
        }
    }

    private static JObject ParseFullJson(string rawJson)
    {
        return JObject.Parse(rawJson);
    }

    private JObject ExecuteExtraction(JObject input, CompiledJexScript script)
    {
        if (string.IsNullOrWhiteSpace(script.Name))
        {
            return input;
        }

        try
        {
            var program = _jexCompiler.Compile(script.Name);
            var result = program.Execute(input.DeepClone());
            return result as JObject ?? input;
        }
        catch
        {
            return input;
        }
    }

    private static List<RuleOutput> ExecuteRules(JObject mainObject, CompiledRuleSet ruleSet)
    {
        var outputs = new List<RuleOutput>();

        foreach (var rule in ruleSet.Rules)
        {
            var output = new JObject
            {
                ["ruleId"] = rule.RuleId,
                ["ruleVersion"] = rule.RuleVersion,
                ["ruleName"] = rule.Name,
                ["timestamp"] = DateTimeOffset.UtcNow.ToString("O"),
            };

            outputs.Add(new RuleOutput(rule.RuleId, rule.RuleVersion, output));
        }

        return outputs;
    }

    private static byte[] BuildOutputObject(
        WorkLease lease,
        CompiledFunctionPlan functionPlan,
        List<RuleOutput> ruleOutputs)
    {
        var output = new JObject
        {
            ["workItemId"] = lease.WorkItemId,
            ["sourceSystemId"] = lease.Item.Source.SourceSystemId,
            ["sourceTopic"] = lease.Item.Source.Topic,
            ["sourcePartition"] = lease.Item.Source.Partition,
            ["sourceOffset"] = lease.Item.Source.Offset,
            ["functionId"] = lease.Item.FunctionId,
            ["functionVersion"] = lease.Item.FunctionVersion,
            ["nedbankId"] = lease.Item.NedbankId,
            ["runtimeModelVersion"] = lease.Item.RuntimeModelVersion,
            ["extractionScriptVersion"] = lease.Item.ExtractionScriptVersion,
            ["outputRouteVersion"] = lease.Item.OutputRouteVersion,
            ["processingAttempt"] = lease.ProcessingAttempt,
            ["createdUtc"] = lease.Item.CreatedUtc.ToString("O"),
            ["processedUtc"] = DateTimeOffset.UtcNow.ToString("O"),
            ["output"] = new JArray(ruleOutputs.Select(r => r.Output)),
        };

        return Encoding.UTF8.GetBytes(output.ToString(Newtonsoft.Json.Formatting.None));
    }

    private sealed record RuleOutput(int RuleId, long RuleVersion, JObject Output);
}
