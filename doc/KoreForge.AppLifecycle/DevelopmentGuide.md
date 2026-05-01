# KoreForge.AppLifecycle Development Guide

This guide is for contributors extending the library. It captures the architectural hotspots and the workflow for implementing new functionality.

## Project Structure Recap

- `src/KF.AppLifecycle`
  - `Flows/` unified flow engine and DSL internals.
  - `Hosting/` contexts and hosted services that wire the engine into the .NET host lifecycle.
  - `Scheduling/` trigger abstractions and scheduler definitions.
  - `Events/` async instrumentation surface.
  - `Options/` fluent configuration for startup, shutdown, scheduled flows, and behavior flags.
  - `Internal/` shared builder cores and helpers (not part of the public API).
- `tst/KF.AppLifecycle.Tests`
  - Covers flow execution, DSL validation, scheduler behavior, events, and host integration.

## External Dependencies

This library has no external time or clock dependencies. Scheduling uses `TimeProvider` (built-in .NET 8+), which is injected as `TimeProvider.System` by default and can be replaced in tests.

## Coding Conventions

- Target `net10.0`; keep code analyzers happy with `TreatWarningsAsErrors=true` (CS1591 suppressed intentionally).
- Prefer constructor injection; avoid service locators beyond the required `IServiceProvider` scope usage inside the flow executor.
- Keep public API XML docs concise and action-oriented.
- Internal helper classes should remain sealed/internal and live under the `Internal` folder unless they are part of the API surface.

## Adding or Modifying Flows

1. Define new `IFlowStep<TContext>` implementations in a consumer-facing assembly (tests can place them inline).
2. Register steps/triggers via DI in the consuming application (`services.AddTransient<MyStep>()`).
3. Use the relevant configurator (`options.Startup`, `options.Shutdown`, or `options.Scheduled`) to declare flows.
4. Ensure every flow has:
   - A unique, non-empty name.
   - A `BeginWith<TStep>()` call establishing the start step.
   - Valid transitions for each outcome you expect; specify `If(...)` before `Then<>()` when branching on custom outcomes.
5. For scheduled flows, call `OnSchedule<TTrigger>()` and optionally `NoOverlap()` before `BeginWith`.

## Scheduler Notes

- `ScheduledTasksHostedService` creates an independent loop per scheduled flow.
- `IScheduleTrigger.GetNextDelayAsync` is resolved per iteration; implementers can read state from DI or the supplied `ScheduledContext`.
- Trigger failures are logged and backed off (configurable fallback delay via constructor, default 1 minute).
- `NoOverlap()` uses a per-flow `SemaphoreSlim` guard to skip overlapping runs.

## Events

- Subscribe via `options.Events.Configure(events => { ... })` inside `AddApplicationLifecycleManager`.
- Handlers are awaited sequentially; exceptions are captured and logged without breaking flow execution.
- Use events for telemetry, metrics, or custom tracing.

## Testing Expectations

- Add/modify tests in `tst/KF.AppLifecycle.Tests`.
- Each new engine capability should include:
  - Unit tests at the flow level (`FlowExecutorTests`).
  - DSL validation tests if new builder rules are introduced.
  - Scheduler/host tests for timing or lifecycle changes.
- Use the provided `TestHostEnvironment` and `TestLogger<T>` doubles when host APIs are needed.
- Run `dotnet test` from the repo root; see `doc/BuildAndRelease.md` for coverage commands.

## Submitting Changes

1. Update docs as necessary (`README.md` plus any relevant files under `doc/`).
2. Keep commits scoped and include rationale in messages.
3. Ensure `dotnet test` and any coverage/reporting commands described in the build guide succeed locally before opening a PR.
