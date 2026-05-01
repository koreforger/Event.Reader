# EventReader Message Processing Architecture

This document explains how EventReader processes messages from Kafka through durable staging, processing, and output publication.

## 0) What Happens To Your Message (Abstraction-First)

This is the shortest accurate version of the flow:

1. Parse and classify incoming Kafka records.
2. Batch-ingest records from Kafka and enqueue matched records into the durable queue.
3. Lease work items in shard batches and run extraction + rules per leased item.
4. Batch-lease output-ready items for publication.
5. Publish to Kafka and mark durable state as Completed (or retry/fail).

Class mapping for those five steps:

- Step 1 and 2: `EventReaderDurableBatchProcessor` in [apps/EventReader/src/EventReader/Kafka/EventReaderDurableBatchProcessor.cs](apps/EventReader/src/EventReader/Kafka/EventReaderDurableBatchProcessor.cs)
- Step 3: `EventReaderShardWorkerPool` + `EventReaderWorkItemProcessor` in [apps/EventReader/src/EventReader/Kafka/EventReaderShardWorkerPool.cs](apps/EventReader/src/EventReader/Kafka/EventReaderShardWorkerPool.cs) and [apps/EventReader/src/EventReader/Kafka/EventReaderWorkItemProcessor.cs](apps/EventReader/src/EventReader/Kafka/EventReaderWorkItemProcessor.cs)
- Step 4 and 5: `EventReaderOutputPublisher` in [apps/EventReader/src/EventReader/Kafka/EventReaderOutputPublisher.cs](apps/EventReader/src/EventReader/Kafka/EventReaderOutputPublisher.cs)

## 0.1) Queue Entry And Exit Points (Exact)

Where an item is added to durable queue:
- Added in classification stage via `EnqueueClassifiedAsync(...)` in [apps/EventReader/src/EventReader/Kafka/EventReaderDurableBatchProcessor.cs](apps/EventReader/src/EventReader/Kafka/EventReaderDurableBatchProcessor.cs)

Where an item leaves active queue states:
- Processing success: `MarkReadyToOutputAsync(...)` in [apps/EventReader/src/EventReader/Kafka/EventReaderShardWorkerPool.cs](apps/EventReader/src/EventReader/Kafka/EventReaderShardWorkerPool.cs)
- Output publish success: `MarkCompletedAsync(...)` in [apps/EventReader/src/EventReader/Kafka/EventReaderOutputPublisher.cs](apps/EventReader/src/EventReader/Kafka/EventReaderOutputPublisher.cs)
- Retry paths: `MarkRetryPendingAsync(...)` in both processing and output stages
- Terminal failure: `MarkFailedAsync(...)` in processing/output exception paths

Practical interpretation:
- "Added to queue" means state `Classified` was persisted.
- "Removed from active processing" means state reached `Completed` or `Failed`.

## 0.2) Batching Boundaries (What Is Batched, What Is Not)

Ingest batching:
- Kafka host delivers record batches to `EventReaderDurableBatchProcessor.ProcessAsync(...)`.
- Batch size is controlled by Kafka profile extended consumer options (`MaxBatchSize`, `MaxBatchWaitMs`).

Processing batching:
- Workers lease a batch per shard using `LeaseShardBatchAsync(shardId, batchSize, ...)`.
- `batchSize` comes from `EventReaderShardWorkerPoolOptions.BatchSize`.
- Extraction and rules are still executed per individual work item, not vectorized across a batch.

Output batching:
- Publisher leases output items using `LeaseOutputBatchAsync(batchSize, ...)`.
- Batch size comes from `EventReaderOutputPublisherOptions.BatchSize`.
- Publish call is currently per leased output item (`PublishAsync` invoked item-by-item inside the leased batch loop).

## 0.3) Parse / Rule Timing (Important)

Parsing occurs in two places for different purposes:

- First parse (classification parse): during ingest in `EventReaderDurableBatchProcessor`.
    - Uses `JsonFieldScanner` over configured discriminator and identity paths.
    - Decides function match + identity and whether item is enqueued.

- Second parse (full payload parse): during processing in `EventReaderWorkItemProcessor`.
    - Parses full payload into `JObject` before extraction/rules.
    - Runs extraction JEX and rule generation for the matched function.

So the system does not "run rules on the Kafka ingest batch". It runs rules per durable work item after dequeue/lease.

## 1) Logical Flow (Business-Level)

```mermaid
flowchart LR
    A[Kafka Message] --> B[Classify Message]
    B --> C{Function Match Found?}
    C -- No --> D[Mark Unclassified]
    C -- Yes --> E[Enqueue Durable Work Item]
    E --> F[Process Work Item]
    F --> G[Build Output Payload]
    G --> H[Publish Output]
    H --> I[Mark Completed]
```

Notes:
- Classification maps topic + discriminator fields to a function.
- Unclassified messages are counted and observed, but not transformed.
- Durable work store is the system of record for in-flight state.

## 2) Runtime Model Control Plane

```mermaid
flowchart TB
    A[SQL Server: EventReader schema] --> B[EventReaderRuntimeModelProvider]
    C[KF.Settings key: EventReader:ModelVersion] --> D[Configuration Reload Token]
    D --> B
    B --> E[Current Runtime Model Snapshot]
    E --> F[Kafka Batch Classifier]
    E --> G[Shard Workers]
    G --> H[Runtime Model GC Service]
```

Notes:
- Runtime model entities are stored in SQL, not in IConfiguration payload trees.
- KF.Settings only acts as a version-change trigger signal.
- On startup, EventReader now performs an eager model load before consumers run.

## 3) Startup and Hosted Service Sequencing

```mermaid
sequenceDiagram
    participant Host as ASP.NET Host
    participant Startup as EventReaderModelStartupService
    participant Provider as EventReaderRuntimeModelProvider
    participant DB as Event.Data SQL
    participant Consumer as EventReaderConsumerWorker

    Host->>Startup: StartAsync
    Startup->>Provider: ReloadAsync
    Provider->>DB: Read Configs + SourceSystems + Functions + Matchers
    DB-->>Provider: Runtime model rows
    Provider-->>Startup: Current model updated (version N)
    Startup-->>Host: StartAsync complete
    Host->>Consumer: StartAsync
    Consumer->>Consumer: Open Kafka host and begin consume loop
```

Notes:
- Because startup loading is an IHostedService StartAsync step, host startup awaits it.
- This avoids long windows where the runtime model stays empty at version 0.

## 4) Detailed Data Plane (Physical)

```mermaid
flowchart TD
    subgraph Ingest
        A1[KafkaConsumerHost]
        A2[EventReaderDurableBatchProcessor]
        A3[JsonFieldScanner]
        A4[FunctionMatcherIndex]
        A5[ClientIdentityResolver]
        A1 --> A2
        A2 --> A3
        A2 --> A4
        A2 --> A5
    end

    subgraph DurableStore
        B1[FasterEventReaderWorkStore]
        B2[State: Classified]
        B3[State: RetryPending]
        B4[State: ReadyToOutput]
        B5[State: Completed]
        B6[State: Failed]
        B1 --> B2
        B1 --> B3
        B1 --> B4
        B1 --> B5
        B1 --> B6
    end

    subgraph Processing
        C1[EventReaderShardWorkerPool]
        C2[EventReaderWorkItemProcessor]
        C3[IJexCompiler / JEX]
        C4[Rule Execution]
        C5[Output Payload Builder]
        C1 --> C2
        C2 --> C3
        C2 --> C4
        C2 --> C5
    end

    subgraph Output
        D1[EventReaderOutputPublisher]
        D2[IOutputMessagePublisher]
        D3[Output Topic]
        D1 --> D2
        D2 --> D3
    end

    A2 --> B1
    B1 --> C1
    C1 --> B1
    B1 --> D1
    D1 --> B1
```

State transitions:
- Classified -> ReadyToOutput on successful processing.
- Classified -> RetryPending on transient failures.
- ReadyToOutput -> Completed on publish success.
- ReadyToOutput -> RetryPending on publish retry condition.
- RetryPending -> Classified or ReadyToOutput depending on retry phase.

## 5) Runtime Model Load Detail

```mermaid
flowchart LR
    A[Load ErConfig] --> B[Parse PrefixLength and JSON paths]
    C[Load ErSourceSystem + Tags where IsEnabled] --> D[SourceSystem map]
    E[Load ErFunction + Rules + SourceOverrides where IsEnabled] --> F[Function plan map]
    G[Load ErFunctionMatcher for active functions] --> H[FunctionMatcherIndex]
    B --> I[Assemble EventReaderRuntimeModel]
    D --> I
    F --> I
    H --> I
```

Validation checkpoints:
- Empty model is allowed for cold start.
- Functions without any source systems are rejected.
- Matchers pointing to missing functions are rejected.

## 6) Operational Observability

```mermaid
flowchart LR
    A[EventReaderMetricsAccumulator] --> B[Processed rate]
    A --> C[Classified / Unclassified counts]
    A --> D[Error counters by stage]
    E[Faster work-store metrics] --> F[Backlog by state]
    E --> G[Lease and queue depth indicators]
    H[OutputPublisherMetrics] --> I[Publish latency p95/p99]
    H --> J[Publish success/failure/retry]
```

Recommended dashboards:
- Ingest throughput and unclassified ratio.
- Backlog by work state over time.
- Processing and publish retry rates.
- Runtime model version currently active and number of retained versions.

## 7) Error Flow (End-to-End)

```mermaid
flowchart TD
    A[Kafka Record] --> B[Ingest Classification]
    B --> C{Valid JSON + Mapped Topic + Match?}
    C -- No --> U[Count Unclassified / Decode Error]
    C -- Yes --> D[EnqueueClassifiedAsync]

    D --> E[LeaseShardBatchAsync]
    E --> F[EventReaderWorkItemProcessor]
    F --> G{Process Result}

    G -- Success --> H[MarkReadyToOutputAsync]
    G -- Retry --> I[MarkRetryPendingAsync
    stage=Processing]
    G -- Fatal --> J[MarkFailedAsync]

    H --> K[LeaseOutputBatchAsync]
    K --> L[PublishAsync]
    L --> M{Publish Result}

    M -- Success --> N[MarkCompletedAsync]
    M -- Retryable failure --> O[MarkRetryPendingAsync
    stage=Output]
    M -- Exception/fatal --> P[MarkFailedAsync]
```

Error handling behavior summary:
- Ingest/classification non-match scenarios are not retried; they become unclassified observations.
- Processing-stage retriable failures move back to retry pipeline from `Classified` stage.
- Output-stage retriable failures move back to retry pipeline from `ReadyToOutput` stage.
- Fatal/exception paths are marked failed to avoid infinite loops.
