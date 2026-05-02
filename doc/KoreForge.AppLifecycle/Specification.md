# KoreForge.AppLifecycle – Full Specification

## 1. Overview

**Goal:** A **NuGet package** that provides:

- Startup control flows that run **before** other hosted services.
    
- Shutdown control flows that run **after** other hosted services.
    
- A unified **conditional flow engine** (if / then / else) reused for:
    
    - Startup flows
        
    - Shutdown flows
        
    - Scheduled flows
        
- A small in-process **scheduler** (time-based triggers + flows).
    
- Lifecycle **events** for logging/metrics.
    
- **Folder scaffolding** via content files: `AppLifecycle/Startup`, `Shutdown`, `Scheduled/Triggers`, `Scheduled/Actions`.
    

---

## 2. Project Shape

### 2.1 Solution Layout

```text
KoreForge.AppLifecycle
├─ src
│  └─ KoreForge.AppLifecycle
│      ├─ KoreForge.AppLifecycle.csproj
│      ├─ Flows
│      ├─ Scheduling
│      ├─ Hosting
│      ├─ Events
│      ├─ Options
│      └─ Internal
└─ tst
   └─ KoreForge.AppLifecycle.Tests
       ├─ FlowExecutorTests.cs
       ├─ DslValidationTests.cs
       ├─ SchedulerTests.cs
       ├─ StartupShutdownIntegrationTests.cs
       └─ ...
```

### 2.2 Assemblies & Namespaces

- **Assembly:** `KoreForge.AppLifecycle.dll`
    
- Primary namespaces:
    

```text
KoreForge.AppLifecycle
KoreForge.AppLifecycle.Flows
KoreForge.AppLifecycle.Scheduling
KoreForge.AppLifecycle.Hosting
KoreForge.AppLifecycle.Events
KoreForge.AppLifecycle.Options
```

Folders in `src/KoreForge.AppLifecycle` should roughly match these namespaces.

---

## 3. Public API Contract

### 3.1 Extension Methods

```csharp
namespace KoreForge.AppLifecycle;

public static class ApplicationLifecycleServiceCollectionExtensions
{
    public static IServiceCollection AddApplicationLifecycleManager(
        this IServiceCollection services,
        Action<ApplicationLifecycleOptions> configure);
}

public static class ApplicationLifecycleApplicationBuilderExtensions
{
    public static IApplicationBuilder UseApplicationLifecycleManager(
        this IApplicationBuilder app);
}
```

- `AddApplicationLifecycleManager`:
    
    - MUST register:
        
        - `ApplicationLifecycleOptions` as a singleton.
            
        - `IApplicationLifecycleEvents` as singleton.
            
        - `ApplicationLifecycleHostedService` as `IHostedService`.
            
        - `ScheduledTasksHostedService` as `IHostedService`.
            
    - MUST invoke supplied `configure` callback once at startup.
        
- `UseApplicationLifecycleManager`:
    
    - MUST NOT do heavy work; mainly there for:
        
        - Visual clarity.
            
        - Optional runtime validations (e.g. options sanity checks).
            
    - Returns `app` unchanged.
        

### 3.2 Options

```csharp
namespace KoreForge.AppLifecycle.Options;

public sealed class ApplicationLifecycleOptions
{
    public StartupFlowConfigurator Startup { get; }
    public ShutdownFlowConfigurator Shutdown { get; }
    public ScheduledFlowConfigurator Scheduled { get; }

    public LifecycleEventsConfigurator Events { get; }

    public bool FailFastOnStartupFailure { get; set; } = true;
    public bool FailFastOnShutdownFailure { get; set; } = false;

    public UnmappedOutcomePolicy UnmappedOutcomePolicy { get; set; } = UnmappedOutcomePolicy.StopFlow;

    public bool LogStepExceptions { get; set; } = true;
    public bool LogUnmappedOutcomes { get; set; } = false;
}

public enum UnmappedOutcomePolicy
{
    /// <summary>Terminate the flow without error when an unmapped outcome is returned.</summary>
    StopFlow = 0,

    /// <summary>Treat unmapped outcome as Failure and continue using Failure transitions.</summary>
    TreatAsFailure = 1,

    /// <summary>Throw an ApplicationLifecycleException when unmapped outcome is encountered.</summary>
    Throw = 2
}
```

### 3.3 Contexts

```csharp
namespace KoreForge.AppLifecycle.Hosting;

public interface IFlowContext
{
    IServiceProvider Services { get; }
}

public sealed class StartupContext : IFlowContext
{
    public IServiceProvider Services { get; }
    public IHostEnvironment HostEnvironment { get; }

    public StartupContext(IServiceProvider services, IHostEnvironment hostEnvironment);
}

public sealed class ShutdownContext : IFlowContext
{
    public IServiceProvider Services { get; }
    public IHostEnvironment HostEnvironment { get; }

    public ShutdownContext(IServiceProvider services, IHostEnvironment hostEnvironment);
}

public sealed class ScheduledContext : IFlowContext
{
    public IServiceProvider Services { get; }
    public IHostEnvironment HostEnvironment { get; }

    public DateTimeOffset ScheduledTime { get; }
    public string FlowName { get; }

    public ScheduledContext(
        IServiceProvider services,
        IHostEnvironment hostEnvironment,
        string flowName,
        DateTimeOffset scheduledTime);
}
```

- Contexts are **immutable** and created by the hosting layer, not by user code.
    
- Context factories MAY be introduced later; v1 uses internal factories.
    

### 3.4 Hosted Services

```csharp
namespace KoreForge.AppLifecycle.Hosting;

internal sealed class ApplicationLifecycleHostedService : IHostedService
{
    // internal constructor via DI
    public Task StartAsync(CancellationToken cancellationToken);
    public Task StopAsync(CancellationToken cancellationToken);
}

internal sealed class ScheduledTasksHostedService : BackgroundService
{
    // internal constructor via DI
    protected override Task ExecuteAsync(CancellationToken stoppingToken);
}
```

- `ApplicationLifecycleHostedService` MUST be registered as the **first** `IHostedService`.
    
- `ScheduledTasksHostedService` MUST be registered after it.
    

### 3.5 Events

```csharp
namespace KoreForge.AppLifecycle.Events;

public interface IApplicationLifecycleEvents
{
    event Func<LifecycleFlowsEventArgs, Task>? BeforeStartupFlows;
    event Func<LifecycleFlowsEventArgs, Task>? AfterStartupFlows;

    event Func<LifecycleStepExecutingEventArgs, Task>? StartupStepExecuting;
    event Func<LifecycleStepExecutedEventArgs, Task>? StartupStepExecuted;

    event Func<LifecycleFlowsEventArgs, Task>? BeforeShutdownFlows;
    event Func<LifecycleFlowsEventArgs, Task>? AfterShutdownFlows;

    event Func<LifecycleStepExecutingEventArgs, Task>? ShutdownStepExecuting;
    event Func<LifecycleStepExecutedEventArgs, Task>? ShutdownStepExecuted;
}

public sealed class LifecycleFlowsEventArgs
{
    public IServiceProvider Services { get; }
    public LifecycleSection Section { get; }

    public LifecycleFlowsEventArgs(IServiceProvider services, LifecycleSection section);
}

public sealed class LifecycleStepExecutingEventArgs
{
    public IServiceProvider Services { get; }
    public LifecycleSection Section { get; }
    public string FlowName { get; }
    public Type StepType { get; }

    public LifecycleStepExecutingEventArgs(
        IServiceProvider services,
        LifecycleSection section,
        string flowName,
        Type stepType);
}

public sealed class LifecycleStepExecutedEventArgs
{
    public IServiceProvider Services { get; }
    public LifecycleSection Section { get; }
    public string FlowName { get; }
    public Type StepType { get; }
    public FlowOutcome Outcome { get; }
    public TimeSpan Duration { get; }
    public Exception? Exception { get; }

    public LifecycleStepExecutedEventArgs(
        IServiceProvider services,
        LifecycleSection section,
        string flowName,
        Type stepType,
        FlowOutcome outcome,
        TimeSpan duration,
        Exception? exception);
}

public enum LifecycleSection
{
    Startup = 0,
    Shutdown = 1
}
```

- All events are **async**: `Func<..., Task>`.
    
- The executor MUST `await` all registered event handlers sequentially.
    

---

## 4. Flow Engine & Outcome Handling

### 4.1 Core Interfaces

```csharp
namespace KoreForge.AppLifecycle.Flows;

public readonly struct FlowOutcome : IEquatable<FlowOutcome>
{
    public string Name { get; }

    public static FlowOutcome Success { get; }
    public static FlowOutcome Failure { get; }

    public FlowOutcome(string name);

    public static FlowOutcome Custom(string name);

    // value semantics: Equals/GetHashCode/==/!=
}

public interface IFlowStep<TContext>
    where TContext : IFlowContext
{
    Task<FlowOutcome> ExecuteAsync(TContext context, CancellationToken cancellationToken);
}
```

- **Failures**:
    
    - Prefer `FlowOutcome.Failure` as return value.
        
    - If a step throws an exception:
        
        - Executor MUST catch it.
            
        - If `LogStepExceptions == true` → log at `LogLevel.Error`.
            
        - Executor MUST treat that as outcome = `FlowOutcome.Failure` and pass `Exception` into `LifecycleStepExecutedEventArgs`.
            

### 4.2 Flow Definition & Executor

```csharp
public sealed class FlowDefinition<TContext>
    where TContext : IFlowContext
{
    public string Name { get; }

    internal string StartStepKey { get; }
    internal IReadOnlyDictionary<string, FlowStepDefinition> Steps { get; }

    internal FlowDefinition(
        string name,
        string startStepKey,
        IReadOnlyDictionary<string, FlowStepDefinition> steps);
}

public sealed class FlowStepDefinition
{
    public Type StepType { get; }
    public IReadOnlyDictionary<FlowOutcome, string> Transitions { get; }

    internal FlowStepDefinition(
        Type stepType,
        IReadOnlyDictionary<FlowOutcome, string> transitions);
}
```

Executor:

```csharp
internal sealed class FlowExecutor<TContext>
    where TContext : IFlowContext
{
    public Task ExecuteAsync(
        FlowDefinition<TContext> flow,
        TContext context,
        UnmappedOutcomePolicy unmappedOutcomePolicy,
        IApplicationLifecycleEvents events,
        ILogger logger,
        CancellationToken cancellationToken);
}
```

**Unmapped outcome behavior:**

- When a step returns `outcome` with **no transition** defined:
    
    - If `UnmappedOutcomePolicy == StopFlow`:
        
        - **Stop** the flow gracefully; no error is thrown.
            
    - If `UnmappedOutcomePolicy == TreatAsFailure`:
        
        - Treat as `FlowOutcome.Failure` and look for Failure transition.
            
        - If there is no Failure transition either, stop flow.
            
    - If `UnmappedOutcomePolicy == Throw`:
        
        - Executor MUST throw `ApplicationLifecycleException` with details.
            
        - Startup/shutdown behavior then depends on `FailFast*` flags.
            
- Failures are primarily **outcome values** (`FlowOutcome.Failure`).  
    Exceptions are caught and converted into failure outcomes plus event/log metadata.
    

---

## 5. Flow DSL Details

All DSL types are **public** entry points but constructed by the options (`ApplicationLifecycleOptions`); constructors may be `internal`.

### 5.1 Startup DSL

```csharp
public sealed class StartupFlowConfigurator
{
    public StartupFlowBuilder Flow(string name);
}

public sealed class StartupFlowBuilder
{
    public StartupStepBuilder BeginWith<TStep>()
        where TStep : class, IFlowStep<StartupContext>;

    public StartupFlowBuilder EndFlow();
}

public sealed class StartupStepBuilder
{
    public StartupStepBuilder If(FlowOutcome outcome);
    public StartupStepBuilder IfSuccess();
    public StartupStepBuilder IfFailure();

    public StartupStepBuilder Then<TNextStep>()
        where TNextStep : class, IFlowStep<StartupContext>;

    public StartupFlowBuilder EndFlow();
}
```

Semantics:

- `Flow(name)`:
    
    - Name MUST be unique per section.
        
    - Name MUST be non-empty, non-whitespace.
        
- `BeginWith<TStep>`:
    
    - Sets the start step.
        
    - Creates initial step definition.
        
- `If(...)` + `Then<TNextStep>`:
    
    - Adds a transition from the **current step** for the given `FlowOutcome` to `TNextStep`.
        
- `IfSuccess/IfFailure`:
    
    - Sugar for `If(FlowOutcome.Success)` / `If(FlowOutcome.Failure)`.
        
- `Then<TNextStep>` called _without_ prior `If*`:
    
    - Sugar for `IfSuccess().Then<TNextStep>()`.
        

### 5.2 Shutdown DSL

Mirror of Startup:

```csharp
public sealed class ShutdownFlowConfigurator
{
    public ShutdownFlowBuilder Flow(string name);
}

public sealed class ShutdownFlowBuilder
{
    public ShutdownStepBuilder BeginWith<TStep>()
        where TStep : class, IFlowStep<ShutdownContext>;

    public ShutdownFlowBuilder EndFlow();
}

public sealed class ShutdownStepBuilder
{
    public ShutdownStepBuilder If(FlowOutcome outcome);
    public ShutdownStepBuilder IfSuccess();
    public ShutdownStepBuilder IfFailure();

    public ShutdownStepBuilder Then<TNextStep>()
        where TNextStep : class, IFlowStep<ShutdownContext>;

    public ShutdownFlowBuilder EndFlow();
}
```

### 5.3 Scheduled DSL

```csharp
public sealed class ScheduledFlowConfigurator
{
    public ScheduledFlowBuilder Flow(string name);
}

public sealed class ScheduledFlowBuilder
{
    public ScheduledFlowBuilder OnSchedule<TTrigger>()
        where TTrigger : class, IScheduleTrigger;

    public ScheduledFlowBuilder NoOverlap(); // optional

    public ScheduledStepBuilder BeginWith<TStep>()
        where TStep : class, IFlowStep<ScheduledContext>;

    public ScheduledFlowBuilder EndFlow();
}

public sealed class ScheduledStepBuilder
{
    public ScheduledStepBuilder If(FlowOutcome outcome);
    public ScheduledStepBuilder IfSuccess();
    public ScheduledStepBuilder IfFailure();

    public ScheduledStepBuilder Then<TNextStep>()
        where TNextStep : class, IFlowStep<ScheduledContext>;

    public ScheduledFlowBuilder EndFlow();
}
```

### 5.4 Validation Expectations

- During `Flow(...).EndFlow()`:
    
    - MUST validate that:
        
        - Flow has a `BeginWith` / start step.
            
        - All referenced step keys exist (no dangling transitions).
            
        - There are no simple cycles (`A → B`, `B → A`, etc.).
            
    - On failure, MUST throw `ApplicationLifecycleException` with diagnostic info.
        
- Custom outcomes:
    
    - Supported via `FlowOutcome.Custom("Name")` or user-defined static fields.
        
    - Names MUST be case-sensitive, non-empty.
        

---

## 6. Dependency Injection

### 6.1 Step & Trigger Registration

- The library **does not** auto-register steps/triggers.
    
- The **consumer** is responsible for DI registration:
    

```csharp
services.AddTransient<HistoryFileReader>();
services.AddTransient<HistoryLoader>();
services.AddTransient<HistoryNotFoundMessageLogger>();

services.AddTransient<EveryFiveMinutesTrigger>();
services.AddTransient<OncePerDayAt10AmTrigger>();
```

- The executor resolves steps via:
    

```csharp
var step = (IFlowStep<TContext>)scope.ServiceProvider.GetRequiredService(stepType);
```

### 6.2 DI Container Compatibility

- Designed for `Microsoft.Extensions.DependencyInjection`.
    
- Compatible with any container that backs `IServiceProvider` for ASP.NET Core/generic host.
    
- No use of keyed services, open generics, or non-standard DI features in v1.
    

---

## 7. Scheduler Specifics

### 7.1 Trigger Contract

```csharp
namespace KoreForge.AppLifecycle.Scheduling;

public interface IScheduleTrigger
{
    Task<TimeSpan> GetNextDelayAsync(
        ScheduledContext context,
        CancellationToken cancellationToken);
}
```

- Implementations decide how to compute delay:
    
    - Simple intervals (e.g. `TimeSpan.FromMinutes(5)`).
        
    - Cron-based logic (user-implemented; no built-in cron parser in v1).
        
- If `GetNextDelayAsync` throws:
    
    - The scheduler MUST:
        
        - Log error at `LogLevel.Error`.
            
        - Sleep for a fallback delay (`TimeSpan.FromMinutes(1)` configurable via internal constant or future option) to avoid tight failure loops.
            

### 7.2 Scheduled Flow Definition

Internally:

```csharp
internal sealed class ScheduledFlowDefinition
{
    public string FlowName { get; }
    public Type TriggerType { get; }
    public FlowDefinition<ScheduledContext> Flow { get; }
    public bool NoOverlap { get; }

    public ScheduledFlowDefinition(
        string flowName,
        Type triggerType,
        FlowDefinition<ScheduledContext> flow,
        bool noOverlap);
}
```

### 7.3 Scheduler Behavior

- For each `ScheduledFlowDefinition`, `ScheduledTasksHostedService`:
    
    - Resolves trigger instance once per loop iteration from a scoped provider.
        
    - Computes delay via trigger.
        
    - Awaits delay respecting `stoppingToken`.
        
    - Builds `ScheduledContext` with:
        
        - Root `IServiceProvider`.
            
        - `IHostEnvironment`.
            
        - Flow name.
            
        - Current scheduled time (`DateTimeOffset.Now` at tick).
            
    - Executes flow via `FlowExecutor<ScheduledContext>`.
        
- **NoOverlap** semantics:
    
    - Internal `SemaphoreSlim` per `ScheduledFlowDefinition`.
        
    - If `NoOverlap` is `true` and semaphore not available:
        
        - Must skip that tick (no queueing).
            
    - Errors in scheduled steps:
        
        - Logged.
            
        - Do NOT stop the scheduler.
            
- No built-in retries/backoff for steps:
    
    - Retries must be implemented inside steps or by specialized triggers.
        

---

## 8. Events Payloads & Behavior

Already defined in §3.5.

Additional behavior:

- Events are **optional**:
    
    - If no subscribers, no overhead except a null check.
        
- Invocation:
    
    - For each event, executor MUST:
        
        - Snapshot the delegate.
            
        - If not null, await `Invoke(args)`; if multiple handlers are attached, .NET event invocation list semantics apply.
            
- Exceptions in event handlers:
    
    - MUST be caught and logged as `LogLevel.Error`.
        
    - MUST NOT break the main flow execution.
        

---

## 9. Options / Behavior Flags (Precise List)

In `ApplicationLifecycleOptions`:

- `bool FailFastOnStartupFailure` (default `true`)
    
    - If any startup flow ends in `FlowOutcome.Failure` or throws:
        
        - If true → host startup MUST fail (throw from `ApplicationLifecycleHostedService.StartAsync`).
            
        - If false → error logged; startup continues.
            
- `bool FailFastOnShutdownFailure` (default `false`)
    
    - If any shutdown flow ends in failure or throws:
        
        - If true → `StopAsync` MUST aggregate exceptions and surface them.
            
        - If false → errors are logged; shutdown flows continue.
            
- `UnmappedOutcomePolicy UnmappedOutcomePolicy` (default `StopFlow`)
    
    - See §4.2 for exact semantics.
        
- `bool LogStepExceptions` (default `true`)
    
    - If a step throws, exception MUST be logged.
        
- `bool LogUnmappedOutcomes` (default `false`)
    
    - If `UnmappedOutcomePolicy == StopFlow` or `TreatAsFailure` and an unmapped outcome is encountered:
        
        - If true → log at `LogLevel.Warning`.
            
        - If false → no log.
            

Ordering of flows:

- Startup flows MUST execute in the order configured.
    
- Shutdown flows MUST execute in the order configured.
    
- Scheduled flows have no global ordering; each is an independent loop.
    

---

## 10. Error Handling & Logging

### 10.1 Logging Abstraction

- Use `ILogger<ApplicationLifecycleHostedService>` and `ILogger<ScheduledTasksHostedService>` from `Microsoft.Extensions.Logging`.
    

Suggested levels:

- `Information`:
    
    - Start/complete of all startup flows.
        
    - Start/complete of all shutdown flows.
        
- `Debug`:
    
    - Start/complete of individual steps (if desired).
        
- `Warning`:
    
    - Step returns `FlowOutcome.Failure` without throwing.
        
    - Unmapped outcomes when `LogUnmappedOutcomes == true`.
        
- `Error`:
    
    - Exceptions thrown by steps.
        
    - Exceptions thrown in triggers.
        
    - Exceptions thrown from event handlers (not fatal to flows).
        

### 10.2 Exceptions

- **Within steps:**
    
    - Catch and convert to `FlowOutcome.Failure`.
        
    - Pass `Exception` to `LifecycleStepExecutedEventArgs`.
        
- **Within scheduler triggers:**
    
    - Catch and log.
        
    - Use fallback delay; loop continues.
        
- **Within event handlers:**
    
    - Catch and log.
        
- **Aggregated errors:**
    
    - For startup:
        
        - If `FailFastOnStartupFailure` → throw `ApplicationLifecycleException` (custom type) with aggregated error info.
            
    - For shutdown:
        
        - If `FailFastOnShutdownFailure` → same.
            
    - `ApplicationLifecycleException`:
        

```csharp
public sealed class ApplicationLifecycleException : Exception
{
    public ApplicationLifecycleException(string message);
    public ApplicationLifecycleException(string message, Exception innerException);
}
```

---

## 11. NuGet Packaging

### 11.1 Target Frameworks

- Multi-target:
    

```xml
<TargetFrameworks>net8.0;net9.0</TargetFrameworks>
```

- No strong-name requirement in v1:
    
    - `<SignAssembly>false</SignAssembly>`.
        

### 11.2 Metadata

Suggested:

- `PackageId`: `KoreForge.AppLifecycle`
    
- `Authors`: `Derick Olivier` (or `KoreForge` – your call)
    
- `Description`:
    
    - “Application lifecycle manager for .NET: startup/shutdown flows with a unified flow engine and scheduled jobs.”
        
- `RepositoryUrl`: your Git repo.
    
- `PackageLicenseExpression`: `MIT`.
    

### 11.3 Folder Scaffolding

In `.csproj`:

```xml
<ItemGroup>
  <Content Include="content/AppLifecycle/Startup/_README_Startup.txt"
           Pack="true"
           PackagePath="contentFiles/any/any/AppLifecycle/Startup/" />
  <Content Include="content/AppLifecycle/Shutdown/_README_Shutdown.txt"
           Pack="true"
           PackagePath="contentFiles/any/any/AppLifecycle/Shutdown/" />
  <Content Include="content/AppLifecycle/Scheduled/Triggers/_README_Triggers.txt"
           Pack="true"
           PackagePath="contentFiles/any/any/AppLifecycle/Scheduled/Triggers/" />
  <Content Include="content/AppLifecycle/Scheduled/Actions/_README_Actions.txt"
           Pack="true"
           PackagePath="contentFiles/any/any/AppLifecycle/Scheduled/Actions/" />
</ItemGroup>
```

NuGet will drop these into the consuming project, effectively creating the folder structure.

---

## 12. Testing Expectations

- **Unit tests** (in `KoreForge.AppLifecycle.Tests`):
    
    - `FlowExecutorTests`:
        
        - Happy path success.
            
        - Failure outcome.
            
        - Unmapped outcomes for each `UnmappedOutcomePolicy`.
            
        - Exception in step → converted to failure & logged.
            
    - `DslValidationTests`:
        
        - Duplicate flow names → exception.
            
        - Missing start step → exception.
            
        - Dangling transition → exception.
            
        - Simple cycle detection.
            
    - `SchedulerTests`:
        
        - Trigger producing delays correctly.
            
        - `NoOverlap` behavior (skipped overlapping runs).
            
        - Trigger exceptions handled with fallback delay.
            
    - `EventsTests`:
        
        - Events invoked in correct order.
            
        - Event handler exceptions logged but not fatal.
            
- **Integration sample** (in `samples`):
    
    - Minimal ASP.NET Core app using `AddApplicationLifecycleManager` and `UseApplicationLifecycleManager`.
        
    - Demonstrates:
        
        - One startup flow.
            
        - One shutdown flow.
            
        - One scheduled flow.
            

Coverage: no hard numeric requirement, but core engine and DSL should be heavily covered.

---

## 13. License & Documentation Artifacts

- **License**: `LICENSE` file with MIT license.
    
- **Readme**: `README.md` at repo root including:
    
    - Quick start snippet.
        
    - Example flows.
        
    - Explanation of startup/shutdown wrapping.
        
- **XML docs**:
    
    - `GenerateDocumentationFile` set to `true`.
        
    - Public types and methods SHOULD have `<summary>` plus `<param>` and `<returns>` where relevant.
        
- Doc comment style:
    
    - Concise, action-oriented summaries.
        
    - No redundant wording (“Gets or sets ...” if obvious).
        

---

## 14. Performance & Threading

- Startup flows:
    
    - MUST run **sequentially** within `ApplicationLifecycleHostedService.StartAsync`.
        
    - Steps within a flow execute one by one; no parallel execution in v1.
        
- Shutdown flows:
    
    - Same: sequential within `StopAsync`.
        
- Scheduled flows:
    
    - Each scheduled flow has its own loop.
        
    - Flows run **concurrently** relative to each other on the thread pool.
        
    - Each flow instance is still sequential; `NoOverlap` ensures single-step-at-a-time per flow.
        
- No blocking `.Result`/`.Wait()` inside the library; everything is async/await.
    
- Host integration:
    
    - `ApplicationLifecycleHostedService.StartAsync` runs first among hosted services.
        
    - `ApplicationLifecycleHostedService.StopAsync` runs last.
        

---

## 15. Extensibility Hooks

- **Custom outcomes**:
    
    - Fully supported via `FlowOutcome.Custom("Name")`.
        
    - DSL allows `If(new FlowOutcome("NotFound")).Then<...>()`.
        
- **Custom contexts**:
    
    - v1 uses fixed `StartupContext`, `ShutdownContext`, `ScheduledContext`.
        
    - Future extension: pluggable context factories:
        
        - e.g. `Func<IServiceProvider, StartupContext>` passed into options.
            
    - Not implemented in v1, but flow engine is generic enough.
        
- **Pipeline behaviors / filters**:
    
    - v1 does not provide a full middleware pipeline.
        
    - Cross-cutting behaviors should use:
        
        - Events (`StartupStepExecuting/Executed`, etc.).
            
        - Or implement wrapper steps (e.g. `LoggingFlowStep<TInnerStep>`).
            
    - Engine is designed so that a future “step filter” can be injected if needed.
        
