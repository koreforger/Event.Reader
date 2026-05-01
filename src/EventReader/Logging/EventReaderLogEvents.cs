using KF.Logging;

namespace EventReader.Logging;

[LogEventSource(LoggerRootTypeName = "EventReaderLogger", BasePath = "EventReader")]
public enum EventReaderLogEvents
{
    // 1000–1099 APP — startup, shutdown, configuration
    APP_Startup_Begin = 1000,
    APP_Startup_Complete = 1001,
    APP_Shutdown_Begin = 1002,
    APP_Shutdown_Complete = 1003,
    APP_Config_Reloaded = 1010,

    // 2000–2099 KAFKA — consumer lifecycle
    KAFKA_Consumer_Starting = 2000,
    KAFKA_Consumer_Started = 2001,
    KAFKA_Consumer_Stopping = 2002,
    KAFKA_Consumer_Stopped = 2003,
    KAFKA_Consumer_Assigned = 2010,
    KAFKA_Consumer_Revoked = 2011,
    KAFKA_Consumer_Rebalance = 2012,
    KAFKA_Consumer_Error = 2020,
    KAFKA_Batch_Received = 2030,
    KAFKA_Batch_Committed = 2031,
    KAFKA_Batch_CommitFailed = 2032,

    // 3000–3099 PROFILE — profile load, classification, match
    PROFILE_Loaded = 3000,
    PROFILE_LoadFailed = 3001,
    PROFILE_Refreshed = 3002,
    PROFILE_Classified = 3010,
    PROFILE_ClassifyFailed = 3011,
    PROFILE_Matched = 3020,
    PROFILE_NotMatched = 3021,

    // 4000–4099 PIPELINE — stage execution
    PIPELINE_Stage_Started = 4000,
    PIPELINE_Stage_Completed = 4001,
    PIPELINE_Stage_Failed = 4002,
    PIPELINE_Decode_Error = 4010,
    PIPELINE_Parse_Error = 4011,
    PIPELINE_Diagnostic_Active = 4020,

    // 5000–5099 ROUTE — output routing, optional DLQ
    ROUTE_Attempt = 5000,
    ROUTE_Success = 5001,
    ROUTE_Failed = 5002,
    ROUTE_Skipped = 5003,
    ROUTE_Dlq_Disabled = 5010,
    ROUTE_Dlq_Written = 5011,
    ROUTE_Dlq_Failed = 5012,

    // 6000–6099 SEEK — offset/datetime seek feature
    SEEK_Mode_None = 6000,
    SEEK_From_Offset = 6001,
    SEEK_From_Timestamp = 6002,
    SEEK_Range_Configured = 6003,
    SEEK_Applied = 6010,
    SEEK_Failed = 6011,
    SEEK_Stop_Reached = 6020,
    SEEK_Timestamp_Resolved = 6030,

    // 7000–7099 DURABLE — FASTER-backed durable pipeline
    DURABLE_Enqueue_Complete = 7000,
    DURABLE_Enqueue_Failed = 7001,
    DURABLE_Classify_Miss = 7010,
    DURABLE_Classify_NoDiscriminator = 7011,
    DURABLE_Classify_NoMatch = 7012,
    DURABLE_Classify_EmptyPayload = 7013,
    DURABLE_Topic_NotMapped = 7020,
    DURABLE_InvalidJson = 7021,

    // 7100–7199 WORKER — shard worker pool
    WORKER_Pool_Starting = 7100,
    WORKER_Pool_Started = 7101,
    WORKER_Pool_Stopped = 7102,
    WORKER_Started = 7110,
    WORKER_Stopped = 7111,
    WORKER_Item_Failed = 7120,
    WORKER_Loop_Error = 7130,

    // 9000–9099 INFRA — health, SQL, HTTP
    INFRA_Health_Started = 9000,
    INFRA_Health_Failed = 9001,
    INFRA_Sql_Error = 9010,
    INFRA_Http_Error = 9020,
}
