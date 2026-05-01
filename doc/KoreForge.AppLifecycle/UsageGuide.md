# KoreForge.AppLifecycle Usage Guide

This guide targets developers consuming the NuGet package in their applications.

## Installation

```powershell
Install-Package KoreForge.AppLifecycle
```

Or add the package reference directly in your project file:

```xml
<ItemGroup>
    <PackageReference Include="KoreForge.AppLifecycle" Version="x.y.z" />
</ItemGroup>
```

## Minimal Setup

```csharp
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddApplicationLifecycleManager(options =>
{
    options.Startup.Flow("Warmup")
        .BeginWith<WarmupStep>()
        .EndFlow();

    options.Shutdown.Flow("Cleanup")
        .BeginWith<CleanupStep>()
        .EndFlow();

    options.Scheduled.Flow("Heartbeat")
        .OnSchedule<EveryMinuteTrigger>()
        .NoOverlap()
        .BeginWith<HeartbeatStep>()
        .EndFlow();
});

builder.Services.AddTransient<WarmupStep>();
builder.Services.AddTransient<CleanupStep>();
builder.Services.AddTransient<HeartbeatStep>();
builder.Services.AddTransient<EveryMinuteTrigger>();

var app = builder.Build();
app.UseApplicationLifecycleManager();
app.Run();
```

## Configuring Flows

- **Startup** and **Shutdown** flows use `StartupContext` / `ShutdownContext`.
- **Scheduled** flows use `ScheduledContext`, which includes the `FlowName`, scheduled time, and `IHostEnvironment`.
- Use `If(FlowOutcome.Custom("name"))` for custom branching; omit `If(...)` to treat `Then<>()` as the success path.

## Behavior Flags

Set these on `ApplicationLifecycleOptions`:

- `FailFastOnStartupFailure` (default `true`)
- `FailFastOnShutdownFailure` (default `false`)
- `UnmappedOutcomePolicy` (`StopFlow`, `TreatAsFailure`, `Throw`)
- `LogStepExceptions` and `LogUnmappedOutcomes`

## Events

```csharp
options.Events.Configure(events =>
{
    events.StartupStepExecuted += args =>
    {
        logger.LogInformation("{Flow}:{Step} -> {Outcome}", args.FlowName, args.StepType.Name, args.Outcome);
        return Task.CompletedTask;
    };
});
```

Available events cover the beginning/end of startup/shutdown flows and per-step execution notifications.

## Scheduler Triggers

Implement `IScheduleTrigger` to control cadence:

```csharp
public sealed class EveryMinuteTrigger : IScheduleTrigger
{
    public Task<TimeSpan> GetNextDelayAsync(ScheduledContext context, CancellationToken cancellationToken)
        => Task.FromResult(TimeSpan.FromMinutes(1));
}
```

Use `NoOverlap()` when the flow must not run concurrently; otherwise each tick runs regardless of the previous run's status.

## Diagnostics

- Subscribe to lifecycle events for telemetry.
- Enable `LogUnmappedOutcomes` to surface transitions that stop unexpectedly under `StopFlow` or `TreatAsFailure` policies.
- Scheduled flow exceptions are logged by `ScheduledTasksHostedService`; unhandled exceptions in steps are converted to `FlowOutcome.Failure` and bubble up through events/logs.
