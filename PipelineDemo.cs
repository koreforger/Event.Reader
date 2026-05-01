using System.Diagnostics;
using System.Text;
using System.Text.RegularExpressions;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Event.Streaming.Processing.Runtime;
using Event.Streaming.Processing.WorkStore;
using KoreForge.Jex;

var bootstrapServers = "localhost:29092";
var topic = $"pipeline-demo-{Guid.NewGuid():N}";
var testDir = Path.Combine(Path.GetTempPath(), "pipeline-demo", Guid.NewGuid().ToString("N"));

Console.WriteLine("=== EventReader Durable Pipeline Demo ===");
Console.WriteLine();

// 1. Create topic
Console.WriteLine("[1/7] Creating Kafka topic...");
using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build();
await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 1 }]);
await Task.Delay(500);

// 2. Produce test messages
Console.WriteLine("[2/7] Producing 3 test messages to Kafka...");
using var producer = new ProducerBuilder<string, string>(new ProducerConfig { BootstrapServers = bootstrapServers }).Build();

var messages = new[]
{
    (key: "cust-1", body: """{"Action":"payment.created","NedbankID":"11111","amount":99,"Channel":"web"}"""),
    (key: "cust-2", body: """{"Action":"payment.created","NedbankID":"22222","amount":150,"Channel":"mobile"}"""),
    (key: "cust-3", body: """{"Action":"payment.created","NedbankID":"33333","amount":200,"Channel":"branch"}"""),
};

foreach (var (key, body) in messages)
{
    await producer.ProduceAsync(topic, new Message<string, string> { Key = key, Value = body });
    Console.WriteLine($"  Produced: key={key}, body={body}");
}
producer.Flush(TimeSpan.FromSeconds(5));

// 3. Consume from Kafka
Console.WriteLine("[3/7] Consuming from Kafka...");
using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
{
    BootstrapServers = bootstrapServers,
    GroupId = $"demo-{Guid.NewGuid():N}",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false,
}).Build();
consumer.Subscribe(topic);

var records = new List<ConsumeResult<string, string>>();
var deadline = DateTime.UtcNow.AddSeconds(10);
while (records.Count < 3 && DateTime.UtcNow < deadline)
{
    var cr = consumer.Consume(TimeSpan.FromSeconds(2));
    if (cr is not null) records.Add(cr);
}
Console.WriteLine($"  Consumed {records.Count} records");

// 4. Build runtime model
Console.WriteLine("[4/7] Building runtime model...");
var runtimeModel = new EventReaderRuntimeModel(
    version: 1, createdUtc: DateTimeOffset.UtcNow,
    sourceSystems: new Dictionary<string, SourceSystemDefinition>(StringComparer.OrdinalIgnoreCase)
    {
        ["demo-source"] = new SourceSystemDefinition("demo-source", topic, "Demo Provider", true, new Dictionary<string, string>()),
    },
    functionDiscriminatorPaths: [JsonPathSelector.Create("$.Action")],
    clientIdentityPaths: [JsonPathSelector.Create("$.NedbankID")],
    functionMatchers: new FunctionMatcherIndex(
        [new FunctionMatcherDefinition(null, 100, 1, "payment.c",
            new Regex("^payment\\.created$", RegexOptions.Compiled | RegexOptions.CultureInvariant))],
        prefixLength: 9),
    functions: new Dictionary<int, CompiledFunctionPlan>
    {
        [100] = new CompiledFunctionPlan(100, 1, "PaymentCreated",
            SourceSystemBehavior.Global,
            new CompiledJexScript("", 1),
            new CompiledRuleSet(1, [new CompiledRule(1, 1, "Audit")]),
            new OutputRoutePlan(1, "default", "output-topic"),
            FunctionFailurePolicy.Default),
    });

var scanner = new JsonFieldScanner();
var identityResolver = new ClientIdentityResolver(new DemoUsernameLookup());
var shardAssigner = new ShardAssigner(16);

// 5. Classify + enqueue to FASTER
Console.WriteLine("[5/7] Classifying and enqueuing to FASTER...");
var storeOpts = new FasterEventReaderWorkStoreOptions
{
    LogPath = Path.Combine(testDir, "work-log"),
    CheckpointPath = Path.Combine(testDir, "checkpoints"),
    CheckpointIntervalMs = 0,
};
using var workStore = new FasterEventReaderWorkStore(storeOpts);

foreach (var record in records)
{
    var payload = Encoding.UTF8.GetBytes(record.Message.Value);
    var sourceId = runtimeModel.ResolveSourceSystemId(record.Topic)!;
    var allPaths = runtimeModel.FunctionDiscriminatorPaths.Concat(runtimeModel.ClientIdentityPaths).ToList();
    var scanResult = scanner.Scan(payload, allPaths);
    var discValue = scanResult.Fields.Values.FirstOrDefault(v => v is not null) ?? "";
    var match = runtimeModel.FunctionMatchers.Match(sourceId, discValue)!;
    var identity = await identityResolver.ResolveAsync(scanResult, CancellationToken.None);
    var shard = shardAssigner.Assign(identity.NedbankId);

    var item = new ClassifiedWorkItem(0,
        new KafkaSourceIdentity(sourceId, record.Topic, record.Partition.Value, record.Offset.Value,
            new DateTimeOffset(record.Message.Timestamp.UtcDateTime)),
        match.FunctionId, identity.NedbankId, shard.ShardId,
        runtimeModel.Version, 1, 1, 1, 1, payload, DateTimeOffset.UtcNow);

    await workStore.EnqueueClassifiedAsync(item, CancellationToken.None);
    Console.WriteLine($"  Enqueued: WorkItemId ~ FunctionId={match.FunctionId}, NID={identity.NedbankId}, Shard={shard.ShardId}, Offset={record.Offset.Value}");
}

// 6. Process via shard worker
Console.WriteLine("[6/7] Processing via shard worker...");
var jex = new Jex();
var processor = new EventReader.Kafka.EventReaderWorkItemProcessor(runtimeModel, jex);

var metrics = workStore.GetDetailedMetrics();
var totalEnqueued = metrics.BacklogByState.Values.Sum();
Console.WriteLine($"  FASTER store: {totalEnqueued} items (states: {string.Join(", ", metrics.BacklogByState.Select(kv => $"{kv.Key}={kv.Value}"))})");

// Lease and process
for (var shardId = 0; shardId < 16; shardId++)
{
    var leases = await workStore.LeaseShardBatchAsync(shardId, 10, TimeSpan.FromMinutes(5), CancellationToken.None);
    foreach (var lease in leases)
    {
        var sw = Stopwatch.StartNew();
        var result = await processor.ProcessAsync(lease, CancellationToken.None);
        sw.Stop();

        if (result.Success && result.OutputPayload is not null)
        {
            await workStore.MarkReadyToOutputAsync(lease.WorkItemId, result.OutputPayload, CancellationToken.None);
            var outputJson = Encoding.UTF8.GetString(result.OutputPayload);
            Console.WriteLine($"  Processed: WorkItemId={lease.WorkItemId} ({sw.ElapsedMilliseconds}ms)");
            Console.WriteLine($"    Output: {outputJson[..Math.Min(200, outputJson.Length)]}...");
        }
    }
}

// 7. Show final state
Console.WriteLine("[7/7] Final pipeline state:");
var finalMetrics = workStore.GetDetailedMetrics();
Console.WriteLine($"  Backlog by state: {string.Join(", ", finalMetrics.BacklogByState.Select(kv => $"{kv.Key}={kv.Value}"))}");
Console.WriteLine($"  Oldest unfinished: {finalMetrics.OldestUnfinishedAge}");
Console.WriteLine($"  Avg enqueue latency: {finalMetrics.AverageEnqueueLatencyMs:F2}ms");
Console.WriteLine($"  Avg lease latency: {finalMetrics.AverageShardLeaseLatencyMs:F2}ms");
Console.WriteLine($"  Store ops: {finalMetrics.AverageOutputLeaseLatencyMs:F2}");
Console.WriteLine();
Console.WriteLine("=== Pipeline demo complete ===");

consumer.Close();
try { Directory.Delete(testDir, recursive: true); } catch { }

sealed class DemoUsernameLookup : IUsernameIdentityLookup
{
    public Task<long?> ResolveNedbankIdAsync(string username, CancellationToken ct) =>
        Task.FromResult<long?>(null);
}
