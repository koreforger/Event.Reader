
# KoreForge.Logging – Enum-Driven Hierarchical Logging Spec (Final)

## 1. Purpose & Goals

This library provides an opinionated logging layer built on top of **Microsoft.Extensions.Logging** that:

* Uses one or more **enums as the single source of truth for log events**.

* Generates a **hierarchical, discoverable logging API** based on enum names, e.g.:

  ```csharp
  _log.DB.Connection.Open.LogInformation("Opening connection...");
  ```

* Preserves **per-calling-type categories** via generics: `MyLogger<T>`, `AppLogger<T>`, `DbLogger<T>`, etc.

* Hides `EventId` handling from callers – it is always generated from the enum.

* Integrates with DI via a generated `AddGeneratedLogging` extension.

The output is a NuGet package suitable for general consumption.

Target frameworks:

* Runtime: `net8.0` (optionally `netstandard2.1` if you want wider reach).
* Generator / analyzer: typical Roslyn targets (`netstandard2.0`, `net8.0`).

---

## 2. Package Layout, Assemblies, Namespaces

### 2.1 Assemblies

* **Runtime**: `KoreForge.Logging.Runtime`
* **Source generator**: `KoreForge.Logging.Generator`
* **Analyzer**: `KoreForge.Logging.Analyzers`

### 2.2 Namespaces

* Public runtime surface:

  * `KoreForge.Logging`
* Runtime internals:

  * `KoreForge.Logging.Internal`
* Generator implementation:

  * `KoreForge.Logging.Generator`
* Analyzer implementation:

  * `KoreForge.Logging.Analyzers`

### 2.3 Generated Types’ Namespace

For each enum marked with `[LogEventSource]`:

* If `LogEventSource.Namespace` is **not null**, emit all generated types for that enum into that namespace.
* If `LogEventSource.Namespace` is **null**, emit all generated types into the **same namespace as the enum**.

---

## 3. Public Runtime API

### 3.1 `LogEventSourceAttribute`

File: `KoreForge.Logging.Runtime`

```csharp
namespace KoreForge.Logging;

[AttributeUsage(AttributeTargets.Enum, AllowMultiple = false, Inherited = false)]
public sealed class LogEventSourceAttribute : Attribute
{
    /// <summary>
    /// Root logger type name to generate, e.g. "MyLogger".
    /// If null or not set, the generator will use the fallback:
    /// &lt;EnumName&gt;Logger&lt;T&gt; (e.g. LogEventIdsLogger&lt;T&gt;).
    /// </summary>
    public string? LoggerRootTypeName { get; set; }

    /// <summary>
    /// Namespace for generated loggers. If null, uses the enum's namespace.
    /// </summary>
    public string? Namespace { get; set; }

    /// <summary>
    /// Optional prefix for the event path, e.g. "MyApp".
    /// When set, event path becomes "MyApp.Area.Group.Action".
    /// </summary>
    public string? BasePath { get; set; }

    /// <summary>
    /// Which log levels to generate methods for.
    /// Default = Debug, Information, Warning, Error.
    /// </summary>
    public LogLevels Levels { get; set; } = LogLevels.Default;
}
```

### 3.2 `LogLevels` Enum

```csharp
namespace KoreForge.Logging;

[Flags]
public enum LogLevels
{
    None        = 0,
    Trace       = 1 << 0,
    Debug       = 1 << 1,
    Information = 1 << 2,
    Warning     = 1 << 3,
    Error       = 1 << 4,
    Critical    = 1 << 5,

    Default     = Debug | Information | Warning | Error
}
```

> For v1, you may generate **all** level methods and treat `Levels` as future expansion; but the property must exist and be honored eventually.

### 3.3 `IEventLogger`

`IEventLogger` represents a **single log event** (one enum value). It provides standard `ILogger`-style methods, but each one is bound to a fixed `EventId`.

```csharp
namespace KoreForge.Logging;

public interface IEventLogger
{
    bool IsEnabled(LogLevel level);

    void LogTrace(string message, params object?[] args);
    void LogTrace(Exception exception, string message, params object?[] args);

    void LogDebug(string message, params object?[] args);
    void LogDebug(Exception exception, string message, params object?[] args);

    void LogInformation(string message, params object?[] args);
    void LogInformation(Exception exception, string message, params object?[] args);

    void LogWarning(string message, params object?[] args);
    void LogWarning(Exception exception, string message, params object?[] args);

    void LogError(string message, params object?[] args);
    void LogError(Exception exception, string message, params object?[] args);

    void LogCritical(string message, params object?[] args);
    void LogCritical(Exception exception, string message, params object?[] args);
}
```

**Design rule:**
No method on `IEventLogger` may accept `EventId` or the enum – the event is fully determined by the logger instance.

### 3.4 `EventLogger` Implementation

Runtime implementation used by generated types.

```csharp
namespace KoreForge.Logging.Internal;

internal sealed class EventLogger : IEventLogger
{
    private readonly ILogger _logger;
    private readonly EventId _eventId;
    private readonly string  _eventPath;

    public EventLogger(ILogger logger, int eventId, string eventPath)
    {
        _logger    = logger ?? throw new ArgumentNullException(nameof(logger));
        _eventId   = new EventId(eventId, eventPath);
        _eventPath = eventPath ?? throw new ArgumentNullException(nameof(eventPath));
    }

    public bool IsEnabled(LogLevel level) => _logger.IsEnabled(level);

    private void LogInternal(LogLevel level, Exception? exception, string message, object?[]? args)
    {
        if (!_logger.IsEnabled(level))
        {
            return;
        }

        using (_logger.BeginScope(new Dictionary<string, object?>
               {
                   ["EventPath"] = _eventPath
               }))
        {
            _logger.Log(level,
                        _eventId,
                        exception,
                        message,
                        args ?? Array.Empty<object?>());
        }
    }

    // Trace
    public void LogTrace(string message, params object?[] args)
        => LogInternal(LogLevel.Trace, null, message, args);

    public void LogTrace(Exception exception, string message, params object?[] args)
        => LogInternal(LogLevel.Trace, exception, message, args);

    // Debug
    public void LogDebug(string message, params object?[] args)
        => LogInternal(LogLevel.Debug, null, message, args);

    public void LogDebug(Exception exception, string message, params object?[] args)
        => LogInternal(LogLevel.Debug, exception, message, args);

    // Information
    public void LogInformation(string message, params object?[] args)
        => LogInternal(LogLevel.Information, null, message, args);

    public void LogInformation(Exception exception, string message, params object?[] args)
        => LogInternal(LogLevel.Information, exception, message, args);

    // Warning
    public void LogWarning(string message, params object?[] args)
        => LogInternal(LogLevel.Warning, null, message, args);

    public void LogWarning(Exception exception, string message, params object?[] args)
        => LogInternal(LogLevel.Warning, exception, message, args);

    // Error
    public void LogError(string message, params object?[] args)
        => LogInternal(LogLevel.Error, null, message, args);

    public void LogError(Exception exception, string message, params object?[] args)
        => LogInternal(LogLevel.Error, exception, message, args);

    // Critical
    public void LogCritical(string message, params object?[] args)
        => LogInternal(LogLevel.Critical, null, message, args);

    public void LogCritical(Exception exception, string message, params object?[] args)
        => LogInternal(LogLevel.Critical, exception, message, args);
}
```

**Fixed decisions:**

* Scope property key is exactly `"EventPath"`.
* `EventId.Id` = `(int)enumValue`.
* `EventId.Name` = `eventPath`.

---

## 4. Enum Conventions & Event Path Rules

### 4.1 Marking an Enum

Users declare an enum and mark it with `[LogEventSource]`:

```csharp
using KoreForge.Logging;

namespace MyApp.Logging;

[LogEventSource(LoggerRootTypeName = "MyLogger", BasePath = "MyApp")]
public enum LogEventIds
{
    APP_Startup                    = 1000,
    APP_ReadConfiguration          = 1001,

    DB_Connection_Open             = 2000,
    DB_Connection_Close            = 2001,
    DB_Pool_Timing_Measurement     = 2002,
    DB_Pool_Timing_Count           = 2003
}
```

### 4.2 Tokenization

Each enum member name is split on `_` into tokens.

Example:

* `APP_Startup` → `["APP", "Startup"]`
* `DB_Connection_Open` → `["DB", "Connection", "Open"]`
* `DB_Pool_Timing_Measurement` → `["DB", "Pool", "Timing", "Measurement"]`

Interpretation:

* **First token** = **Area** (e.g. `APP`, `DB`).
* **Middle tokens** = nested **groups**.
* **Last token** = **Action**.

If a member name has fewer than 2 tokens (e.g. `STARTUP`):

* The analyzer will issue a **Warning** (KLG0003).
* The generator should still produce something best-effort (treat as Area+Action same, or a degenerate leaf) but this is “don’t care” for v1; the expectation is that users follow the convention.

### 4.3 Name Conversion to Types/Properties

Token → type/property name mapping:

* Convert to **PascalCase**.
* Acronyms:

  * `APP` → `App`
  * `DB` → `Db`
  * `HTTP` → `Http`
* If result conflicts with C# keyword, append `_`.

Usage:

* Area token (first):

  * Logger type: `<AreaPascal>Logger<T>` (e.g. `AppLogger<T>`, `DbLogger<T>`).
  * Property on root logger: exactly the original token, PascalCased or slightly normalized:

    * For `APP` → `App` or `APP`?
      **Decision**: use PascalName for property: `public AppLogger<T> App { get; }`
      (Not shouting `APP` in code.)
* Group tokens:

  * Each group path generates a logger type: `<AreaPascal><Group1Pascal><Group2Pascal>Logger<T>`.
  * Property name is the group PascalName.
* Action tokens:

  * Final segment; represented as an `IEventLogger` property with same PascalName.

Example:

* `DB_Connection_Open`:

  * Area: `DB` → type: `DbLogger<T>`, root property: `AppLog.DB` or `log.DB`.
    (Property name `DB` is acceptable; you can also choose `Db`—pick one and be consistent. For this spec we’ll use `Db` as property name.)
  * Group: `Connection` → `DbConnectionLogger<T>`, property: `Connection`.
  * Action: `Open` → `IEventLogger` property: `Open`.

### 4.4 Event Path

Event path is used as:

* `EventId.Name`.
* The `EventPath` scope value.

Rule:

* `EventPath` = `[BasePath?].[AreaPascal].[GroupPascal...] .[ActionPascal]`
* If `BasePath` is null:

  * `"DB.Connection.Open"`.
* If `BasePath = "MyApp"`:

  * `"MyApp.DB.Connection.Open"`.

Example:

* `DB_Connection_Open` + `BasePath = "MyApp"` → `"MyApp.DB.Connection.Open"`.

---

## 5. Generated Types (Logger Shapes)

All types below are **generated** by the source generator.

Assume:

```csharp
[LogEventSource(LoggerRootTypeName = "MyLogger", BasePath = "MyApp")]
public enum LogEventIds { ... }
```

in namespace `MyApp.Logging`.

### 5.1 Root Logger Type Name (Critical Decision)

If `LoggerRootTypeName` is:

* **Provided** (e.g. `"MyLogger"`):

  * Root logger type MUST be: `MyLogger<T>`.
* **Not provided** (`null` or omitted):

  * Root logger type MUST be: **`<EnumName>Logger<T>`**.

Examples:

* Enum `LogEventIds` without specifying `LoggerRootTypeName`:

  * Root type name = `LogEventIdsLogger<T>`.
* Enum `MyAppEvents` without specifying:

  * Root = `MyAppEventsLogger<T>`.

**No non-generic root wrapper** is generated in v1.

### 5.2 Root Logger: Example

With `LoggerRootTypeName = "MyLogger"`:

```csharp
namespace MyApp.Logging
{
    public sealed class MyLogger<T>
    {
        public MyLogger(AppLogger<T> app, DbLogger<T> db)
        {
            App = app ?? throw new ArgumentNullException(nameof(app));
            DB  = db  ?? throw new ArgumentNullException(nameof(db));
        }

        public AppLogger<T> App { get; }
        public DbLogger<T>  DB  { get; }
    }
}
```

With no `LoggerRootTypeName`:

```csharp
// Enum: public enum LogEventIds { ... }

namespace MyApp.Logging
{
    public sealed class LogEventIdsLogger<T>
    {
        public LogEventIdsLogger(AppLogger<T> app, DbLogger<T> db)
        {
            App = app ?? throw new ArgumentNullException(nameof(app));
            DB  = db  ?? throw new ArgumentNullException(nameof(db));
        }

        public AppLogger<T> App { get; }
        public DbLogger<T>  DB  { get; }
    }
}
```

### 5.3 Area Loggers

For each **Area token** (first segment), emit a generic logger type `<AreaPascal>Logger<T>`.

#### `AppLogger<T>` (for `APP`)

```csharp
namespace MyApp.Logging
{
    public sealed class AppLogger<T>
    {
        private readonly ILogger<T> _inner;

        public AppLogger(ILogger<T> inner)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));

            Startup = new KoreForge.Logging.Internal.EventLogger(
                _inner,
                (int)LogEventIds.APP_Startup,
                "MyApp.APP.Startup");

            ReadConfiguration = new KoreForge.Logging.Internal.EventLogger(
                _inner,
                (int)LogEventIds.APP_ReadConfiguration,
                "MyApp.APP.ReadConfiguration");
        }

        public IEventLogger Startup           { get; }
        public IEventLogger ReadConfiguration { get; }
    }
}
```

#### `DbLogger<T>` (for `DB`)

```csharp
namespace MyApp.Logging
{
    public sealed class DbLogger<T>
    {
        private readonly ILogger<T> _inner;

        public DbLogger(ILogger<T> inner)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
            Connection = new DbConnectionLogger<T>(_inner);
            Pool       = new DbPoolLogger<T>(_inner);
        }

        public DbConnectionLogger<T> Connection { get; }
        public DbPoolLogger<T>       Pool       { get; }
    }
}
```

### 5.4 Nested Group Loggers

For each unique group path under an Area, generate a nested logger type.

#### `DbConnectionLogger<T>`

```csharp
namespace MyApp.Logging
{
    public sealed class DbConnectionLogger<T>
    {
        private readonly ILogger<T> _inner;

        internal DbConnectionLogger(ILogger<T> inner)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));

            Open = new KoreForge.Logging.Internal.EventLogger(
                _inner,
                (int)LogEventIds.DB_Connection_Open,
                "MyApp.DB.Connection.Open");

            Close = new KoreForge.Logging.Internal.EventLogger(
                _inner,
                (int)LogEventIds.DB_Connection_Close,
                "MyApp.DB.Connection.Close");
        }

        public IEventLogger Open  { get; }
        public IEventLogger Close { get; }
    }
}
```

#### `DbPoolLogger<T>` and `DbPoolTimingLogger<T>`

```csharp
namespace MyApp.Logging
{
    public sealed class DbPoolLogger<T>
    {
        private readonly ILogger<T> _inner;

        internal DbPoolLogger(ILogger<T> inner)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
            Timing = new DbPoolTimingLogger<T>(_inner);
        }

        public DbPoolTimingLogger<T> Timing { get; }
    }

    public sealed class DbPoolTimingLogger<T>
    {
        private readonly ILogger<T> _inner;

        internal DbPoolTimingLogger(ILogger<T> inner)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));

            Measurement = new KoreForge.Logging.Internal.EventLogger(
                _inner,
                (int)LogEventIds.DB_Pool_Timing_Measurement,
                "MyApp.DB.Pool.Timing.Measurement");

            Count = new KoreForge.Logging.Internal.EventLogger(
                _inner,
                (int)LogEventIds.DB_Pool_Timing_Count,
                "MyApp.DB.Pool.Timing.Count");
        }

        public IEventLogger Measurement { get; }
        public IEventLogger Count       { get; }
    }
}
```

**Construction visibility:**
Group logger constructors MUST be `internal` – not part of public API.

---

## 6. DI Registration

### 6.1 Generated `AddGeneratedLogging`

For each enum with `[LogEventSource]`, the generator MUST emit an extension method on `IServiceCollection`:

* Name: `AddGeneratedLogging`
* Namespace: same as generated loggers.
* Behavior: register all generated loggers as **Scoped**.

Example for `LogEventIds` + `LoggerRootTypeName = "MyLogger"`:

```csharp
using KoreForge.Logging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace MyApp.Logging
{
    public static class GeneratedLoggingServiceCollectionExtensions
    {
        /// <summary>
        /// Registers generated loggers for the LogEventIds enum.
        /// </summary>
        public static IServiceCollection AddGeneratedLogging(this IServiceCollection services)
        {
            // Area loggers
            services.AddScoped(typeof(AppLogger<>));
            services.AddScoped(typeof(DbLogger<>));

            // Root logger
            services.AddScoped(typeof(MyLogger<>));

            return services;
        }
    }
}
```

If `LoggerRootTypeName` is omitted:

* Root type is `LogEventIdsLogger<T>` and registration is:

  ```csharp
  services.AddScoped(typeof(LogEventIdsLogger<>));
  ```

**Lifetime decision:**
All generated loggers MUST be **Scoped**.

### 6.2 Usage

In program startup:

```csharp
using MyApp.Logging;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddLogging();
builder.Services.AddGeneratedLogging();

var app = builder.Build();
```

In a consumer class:

```csharp
using MyApp.Logging;

public sealed class OrderService
{
    private readonly MyLogger<OrderService> _log;

    public OrderService(MyLogger<OrderService> log)
    {
        _log = log;
    }

    public void Start()
    {
        _log.App.Startup.LogInformation("OrderService starting");
        _log.DB.Connection.Open.LogDebug(
            "Opening DB for order {OrderId}",
            1234);
    }
}
```

Or injecting an area logger directly:

```csharp
public sealed class DbPoolService
{
    private readonly DbLogger<DbPoolService> _db;

    public DbPoolService(DbLogger<DbPoolService> db)
    {
        _db = db;
    }

    public void TestPool()
    {
        _db.Pool.Timing.Measurement.LogDebug("Pool timing {DurationMs}ms", 42);
    }
}
```

---

## 7. Source Generator Behavior

### 7.1 Implementation Type

* Implement an **Incremental Source Generator** (`IIncrementalGenerator`).
* Use Roslyn APIs to:

  1. Find enums with `KoreForge.Logging.LogEventSourceAttribute`.
  2. Build a model per enum:

     * Enum name, namespace.
     * Members and underlying integer values.
     * Attribute arguments (`LoggerRootTypeName`, `Namespace`, `BasePath`, `Levels`).
  3. Tokenize member names.
  4. Build a tree: Area → Groups → Action.
  5. Generate source files:

     * Root logger `<RootName><T>` (explicit or fallback).
     * Area loggers `<AreaPascal>Logger<T>`.
     * Group loggers.
     * DI extension.

### 7.2 Fallback Logic for Root Type Name

* If attribute has `LoggerRootTypeName` set to a non-empty string `R`:

  * Root logger type = `R<T>`.
* If attribute has `LoggerRootTypeName` null / not provided:

  * Root logger type = **`<EnumName>Logger<T>`** exactly.
  * For `LogEventIds`:

    * Root = `LogEventIdsLogger<T>`.
  * For `CustomerEvents`:

    * Root = `CustomerEventsLogger<T>`.

This is the **final fixed rule** for implementation.

---

## 8. Analyzer Spec

Assembly: `KoreForge.Logging.Analyzers`.

### 8.1 Diagnostics

Use IDs `KLG000x`.

1. **KLG0001 – Duplicate event numeric value**

   * Condition:

     * In a `[LogEventSource]` enum, two or more members share the same integer value.
   * Severity: **Error**
   * Default: Enabled.
   * Message:

     * `"Duplicate log event value '{0}' in enum '{1}'. Event numeric values must be unique."`

2. **KLG0002 – `[LogEventSource]` applied to non-enum**

   * Condition:

     * `LogEventSourceAttribute` found on a type that is not `enum`.
   * Severity: **Error**
   * Default: Enabled.
   * Message:

     * `"[LogEventSource] can only be applied to enums."`

3. **KLG0003 – Invalid naming convention (no underscore)**

   * Condition:

     * Enum member name contains no `_` and the enum has `[LogEventSource]`.
   * Severity: **Warning**
   * Default: Enabled.
   * Message:

     * `"Enum member '{0}' does not follow 'AREA_Action' naming convention. At least one '_' is recommended."`

4. **KLG0004 – Suspicious EventId value (<= 0)**

   * Condition:

     * `(int)enumMember <= 0`.
   * Severity: **Warning**
   * Default: Enabled.
   * Message:

     * `"Log event '{0}' has non-positive value '{1}'. Event IDs should be positive integers."`

5. **KLG0005 – Single-member area (style)**

   * Condition:

     * A given Area token (first segment) appears only on a single enum member.
   * Severity: **Info** or `Hidden`
   * Default: **Disabled**.
   * Message:

     * `"Area '{0}' is used only by event '{1}'. Consider reviewing this grouping."`

### 8.2 Defaults Summary

* **Errors (enabled):**

  * KLG0001, KLG0002
* **Warnings (enabled):**

  * KLG0003, KLG0004
* **Info/Hidden (disabled by default):**

  * KLG0005

Users can modify severity via `.editorconfig`.

---

## 9. Behavioral Guarantees & Non-Goals

### 9.1 Guarantees

* All logs go through `Microsoft.Extensions.Logging.ILogger` / `ILogger<T>`.
* `EventId` is derived exclusively from the enum:

  * `Id` = `(int)enumMember`.
  * `Name` = computed event path.
* Every call on an `IEventLogger` produces a scope with:

  * Key: `"EventPath"`.
  * Value: event path string.

### 9.2 Non-Goals (v1)

* No runtime configuration to map names/paths; everything is convention + attributes.
* No strongly-typed parameter signatures per event (message templates + typed parameters can be added in a future version using an additional attribute).
* No custom logging sinks; the library sits strictly on top of `Microsoft.Extensions.Logging.Abstractions`.
* No automatic caller member/file/line capture (could be added later using caller info attributes).

---

## 10. Example: End-to-End

### 10.1 Enum

```csharp
using KoreForge.Logging;

namespace MyApp.Logging;

[LogEventSource(LoggerRootTypeName = "MyLogger", BasePath = "MyApp")]
public enum LogEventIds
{
    APP_Startup                    = 1000,
    APP_ReadConfiguration          = 1001,

    DB_Connection_Open             = 2000,
    DB_Connection_Close            = 2001,
    DB_Pool_Timing_Measurement     = 2002,
    DB_Pool_Timing_Count           = 2003
}
```

### 10.2 DI Setup

```csharp
using MyApp.Logging;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddLogging();          // standard logging
builder.Services.AddGeneratedLogging(); // generated extension

var app = builder.Build();
```

### 10.3 Use in Application Code

```csharp
using MyApp.Logging;

public sealed class StartupService
{
    private readonly MyLogger<StartupService> _log;

    public StartupService(MyLogger<StartupService> log)
    {
        _log = log;
    }

    public void Start()
    {
        _log.App.Startup.LogInformation("StartupService starting...");

        _log.DB.Connection.Open.LogDebug(
            "Opening DB connection for {UserId}",
            42);
    }
}
```

Resulting log entry (conceptually):

* Category: `MyApp.Logging.StartupService`
* EventId: `Id = 2000`, `Name = "MyApp.DB.Connection.Open"`
* Scope property: `EventPath = "MyApp.DB.Connection.Open"`
* Message template and properties as passed to `LogDebug`.

---

## 11. Key Fixed Decisions (Checklist)

To make implementation trivial, here are the key choices explicitly:

1. **Root type fallback name**

   * If `LoggerRootTypeName` is **not provided**:

     * Root logger type is **`<EnumName>Logger<T>`**.
   * No non-generic root wrapper is generated.

2. **DI lifetime**

   * All generated loggers (root and area/nested) are registered as **Scoped** in `AddGeneratedLogging`.

3. **Scope property**

   * Every `IEventLogger` call creates a scope with:

     * Key `"EventPath"`.
     * Value = full event path string.

4. **EventId**

   * `Id` = `(int)enumMember`.
   * `Name` = event path string.

5. **Analyzer severities**

   * Duplicate value / non-enum attribute misuse: **Error**, enabled.
   * Naming / non-positive value: **Warning**, enabled.
   * Single-member area: Info/Hidden, disabled by default.

With these in place, there should be no open design questions left for the implementer.

