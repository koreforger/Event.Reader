# KoreForge.Logging Developer Guide

This document explains how the solution is structured, how it is built and tested, and what you need to know when extending or maintaining the package.

## Solution Layout

```
KoreForge.Logging.slnx
├─ src/
│  ├─ KoreForge.Logging.Runtime/           # Public runtime assembly + packaged analyzers/generator
│  ├─ KoreForge.Logging.Generator/         # Incremental source generator (netstandard2.0)
│  └─ KoreForge.Logging.Analyzers/         # Diagnostic analyzers + release tracking (netstandard2.0)
├─ tests/
│  └─ KoreForge.Logging.Tests/             # xUnit tests covering runtime, generator, analyzer
├─ docs/                               # Specification + guides, packed into NuGet
├─ artifacts/                          # NuGet/snupkg output (git ignored)
└─ TestResults/                        # Test + coverage output (git ignored)
```

The `KoreForge.Logging.Runtime` package references the generator and analyzer projects as analyzers, so consumers get all three components from one NuGet. The `docs` folder plus `buildTransitive/KoreForge.Logging.targets` ensure documentation is shipped and copied automatically into a consuming solution (`docs/KoreForge.Logging`).

## Build & Test Overview

- **Build**: `dotnet build KoreForge.Logging.slnx`
- **Test**: `dotnet test` (coverage + HTML report emitted to `TestResults/KoreForge.Logging.Tests/coverage-html/index.html`)
- **Pack**: `dotnet pack -c Release` (packages land in `artifacts/`)
- **Clean**: `scr/build-clean.ps1` removes `scr/`, `obj/`, `artifacts/`, `TestResults/`

Automation scripts live under `scr/` and simply wrap the commands above so CI or local contributors can stay consistent:

| Script | Purpose |
| --- | --- |
| `scr/build-rebuild.ps1 [-Configuration Debug|Release]` | Runs `dotnet build` with the chosen configuration. |
| `scr/build-test.ps1` | Executes tests without coverage (`CollectCoverage=false`) for quick inner-loop runs. |
| `scr/build-test-codecoverage.ps1` | Executes tests with Coverlet + ReportGenerator enabled. |
| `scr/build-pack.ps1` | Runs `dotnet pack` and drops `.nupkg/.snupkg` into `artifacts/`. |
| `scr/build-clean.ps1` | Deletes `bin`, `obj`, `artifacts`, and `TestResults`. |
| `scr/release-nuget-from-local.ps1 -ApiKey ... [-Source ...]` | Packs (unless `-SkipPack`) and pushes both `.nupkg` and `.snupkg` to the configured feed. |

MSBuild-wide settings live in `Directory.Build.props/targets`. Key points:

- MinVer drives semantic versioning using git tags (`KoreForge.Logging/v*`).
- Test projects (suffix `.Tests`) automatically enable Coverlet + ReportGenerator.
- ReportGenerator runs via `dotnet exec ...tools/net9.0/ReportGenerator.dll` after VSTest to produce HTML.
- Docs copy target (`buildTransitive/KoreForge.Logging.docs.targets`) mirrors `/docs` into consumer solutions at build time.

## Target Frameworks & Dependencies

- Runtime: `net9.0` (per customer requirement). Update this if multi-targeting is needed.
- Generator/Analyzer: `netstandard2.0` to keep Roslyn happy (RS1041). Switching to higher TFMs requires disabling that diagnostic or multi-targeting.
- Tests: `net9.0`, referencing Coverlet collector, Microsoft.NET.Test.Sdk, xUnit.

## Analyzer Release Tracking

Keep `src/KoreForge.Logging.Analyzers/AnalyzerReleases/AnalyzerReleases.(Shipped|Unshipped).md` up to date whenever diagnostics change. The analyzer project treats RS2008 as errors.

## Adding Features / Fixes

1. Update or add tests under `tests/KoreForge.Logging.Tests`.
2. Run `scr/build-test.ps1` (or `scr/build-test-codecoverage.ps1` if you want HTML output).
3. Update docs (spec, user, developer guides) when behavior or requirements change.
4. Bump tags per MinVer conventions before releasing.

## Release Checklist

1. `scr/build-clean.ps1`
2. `scr/build-test-codecoverage.ps1`
3. Inspect `TestResults/KoreForge.Logging.Tests/coverage-html/index.html`
4. `scr/build-pack.ps1`
5. Push artifacts from `artifacts/` to NuGet feed (or internal store)
6. Tag commit `KoreForge.Logging/vX.Y.Z` so MinVer picks it up.

## Troubleshooting Notes

- If coverage HTML fails to generate, verify `ReportGenerator` package restored and `dotnet exec` path points to `tools/net9.0/ReportGenerator.dll`.
- If analyzer build errors about release tracking, edit the `.md` files instead of deleting them.
- Auto-doc copy can be disabled or redirected via the `KoreForgeLoggingDocsFolderName` MSBuild property in consumer projects.

