# Sample Walkthrough

This document preserves the full self-contained sample that was previously shipped as a separate `SampleWebApp` project. The tests in `tst/KF.AppLifecycle.Tests` now serve as the primary executable demonstration of the library's features; this document shows the equivalent end-to-end wiring in a real ASP.NET Core host.

## Complete Program

```csharp
using KoreForge.AppLifecycle;
using KoreForge.AppLifecycle.Flows;
using KoreForge.AppLifecycle.Hosting;
using KoreForge.AppLifecycle.Scheduling;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddApplicationLifecycleManager(options =>
{
    options.Startup.Flow("Bootstrap")
        .BeginWith<WarmupCacheStep>()
        .EndFlow();

    options.Shutdown.Flow("Cleanup")
        .BeginWith<FlushMetricsStep>()
        .EndFlow();

    options.Scheduled.Flow("Heartbeat")
        .OnSchedule<HalfMinuteTrigger>()
        .NoOverlap()
        .BeginWith<HeartbeatStep>()
        .EndFlow();

    options.Events.Configure(events =>
    {
        events.StartupStepExecuted += args =>
        {
            Console.WriteLine($"[{args.Section}] {args.FlowName}:{args.StepType.Name} => {args.Outcome}");
            return Task.CompletedTask;
        };
    });
});

builder.Services.AddTransient<WarmupCacheStep>();
builder.Services.AddTransient<FlushMetricsStep>();
builder.Services.AddTransient<HeartbeatStep>();
builder.Services.AddTransient<HalfMinuteTrigger>();

var app = builder.Build();

app.UseApplicationLifecycleManager();

app.MapGet("/", () => "KoreForge.AppLifecycle sample is running.");

app.Run();
```

## Step and Trigger Implementations

```csharp
// Startup step — runs once when the host starts.
sealed class WarmupCacheStep : IFlowStep<StartupContext>
{
    public Task<FlowOutcome> ExecuteAsync(StartupContext context, CancellationToken cancellationToken)
    {
        // Warm caches, call remote services, etc.
        return Task.FromResult(FlowOutcome.Success);
    }
}

// Shutdown step — runs once when the host is stopping.
sealed class FlushMetricsStep : IFlowStep<ShutdownContext>
{
    public Task<FlowOutcome> ExecuteAsync(ShutdownContext context, CancellationToken cancellationToken)
    {
        // Flush telemetry/metrics before process exits.
        return Task.FromResult(FlowOutcome.Success);
    }
}

// Scheduled step — runs on the cadence defined by HalfMinuteTrigger.
sealed class HeartbeatStep : IFlowStep<ScheduledContext>
{
    public Task<FlowOutcome> ExecuteAsync(ScheduledContext context, CancellationToken cancellationToken)
    {
        // Record application heartbeat.
        return Task.FromResult(FlowOutcome.Success);
    }
}

// Trigger — returns the delay until the next run.
sealed class HalfMinuteTrigger : IScheduleTrigger
{
    public Task<TimeSpan> GetNextDelayAsync(ScheduledContext context, CancellationToken cancellationToken)
        => Task.FromResult(TimeSpan.FromSeconds(30));
}
```

## What Each Part Does

| Part | Role |
| --- | --- |
| `AddApplicationLifecycleManager` | Registers the hosted services and wires the flow engine into the DI container. |
| `UseApplicationLifecycleManager` | Adds the middleware that drives startup before the application pipeline begins accepting requests. |
| `options.Startup.Flow(...)` | Declares an ordered sequence of steps to run once at host startup. |
| `options.Shutdown.Flow(...)` | Declares steps to run once when the host receives a shutdown signal. |
| `options.Scheduled.Flow(...)` | Declares a recurring flow driven by an `IScheduleTrigger`. |
| `NoOverlap()` | Prevents a scheduled flow from running again if the previous run has not finished. |
| `options.Events.Configure(...)` | Subscribes to lifecycle events for logging, tracing, or metrics. |

See [UsageGuide.md](UsageGuide.md) for the full API reference and configuration options.
