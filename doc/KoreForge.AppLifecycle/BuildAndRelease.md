# Build, Test, Coverage, and Release Guide

Everything below assumes execution from the repository root. Test artifacts are written to `out/TestResults/` (excluded from git). NuGet packages are written to `artifacts/`.

## Prerequisites

- .NET SDK 10.0 for builds.
- PowerShell 7+ for helper scripts under `bin/`.
- Local dotnet tools restored (`dotnet tool restore`).

## Helper Scripts (`bin/`)

All scripts are in the `/bin` folder and auto-locate the repo root.

| Script | Purpose |
| --- | --- |
| `build-clean.ps1` | `dotnet clean` + delete `/out` |
| `build-rebuild.ps1` | Full rebuild (`dotnet build --force`) |
| `build-test.ps1` | Rebuild then run all tests, HTML results in `out/TestResults/TestResults.html` |
| `build-test-codecoverage.ps1` | Rebuild, run tests with coverage, generate HTML coverage report in `out/TestResults/coverage/index.html` |
| `git-push.ps1 -Message "..."` | Stage all, commit, and push |
| `git-push-nuget.ps1 -Version 1.2.0 -Note "..."` | Push working tree (if dirty), create release tag, push tag to trigger NuGet CI |

Run scripts from any location:

```powershell
.\bin\build-test-codecoverage.ps1 -Open
```

## Cleaning the Workspace

| Task | Command |
| --- | --- |
| Standard clean | `dotnet clean` |
| Release clean | `dotnet clean -c Release` |
| Clean + delete /out | `.\bin\build-clean.ps1` |
| Git hard clean (removes ALL untracked files) | `git clean -xdf` |

## Building

| Scenario | Command |
| --- | --- |
| Restore dependencies | `dotnet restore` |
| Build (Debug) | `dotnet build` |
| Build (Release) | `dotnet build -c Release` |
| Build the solution explicitly | `dotnet build KoreForge.AppLifecycle.slnx -c Release` |
| Build only the library | `dotnet build src/KoreForge.AppLifecycle/KoreForge.AppLifecycle.csproj` |

## Testing

| Goal | Command |
| --- | --- |
| Default unit tests | `dotnet test` |
| Release config tests | `dotnet test -c Release` |
| With HTML test log | `dotnet test --logger:"html;LogFileName=TestResults.html" --results-directory out/TestResults` |
| Full rebuild + test | `.\bin\build-test.ps1` |

## Code Coverage

### Script (recommended)

```powershell
.\bin\build-test-codecoverage.ps1 -Open
```

This rebuilds the solution, runs tests (coverlet.msbuild collects coverage), then generates HTML + Cobertura reports.

### Manual

```powershell
dotnet test --results-directory out/TestResults
dotnet tool restore
dotnet tool run reportgenerator `
    -reports:"out/TestResults/raw/coverage.cobertura.xml" `
    -targetdir:"out/TestResults/coverage" `
    -reporttypes:"Html;Cobertura"
Start-Process (Resolve-Path .\out\TestResults\coverage\index.html)
```

### Output locations

| Artifact | Location |
| --- | --- |
| Cobertura XML | `out/TestResults/raw/coverage.cobertura.xml` |
| HTML coverage report | `out/TestResults/coverage/index.html` |
| HTML test results | `out/TestResults/TestResults.html` |

> `/out` is excluded from git.

## Packaging & Publishing

| Task | Command |
| --- | --- |
| Pack NuGet (drops into `artifacts/`) | `dotnet pack src/KoreForge.AppLifecycle/KoreForge.AppLifecycle.csproj -c Release` |
| Inspect `.nupkg` | `dotnet nuget locals all --list` + `tar -tf artifacts/KoreForge.AppLifecycle.<version>.nupkg` |
| Push to NuGet.org | `dotnet nuget push artifacts/KoreForge.AppLifecycle.<version>.nupkg --api-key <KEY> --source https://api.nuget.org/v3/index.json --skip-duplicate` |
| Tag and push for CI | `.\bin\git-push-nuget.ps1 -Version 1.2.0 -Note "Add feature X"` |

## Useful Extras

- Format code: `dotnet format`.
- List local tools: `dotnet tool list`.
- Rebuild + test + pack (Release) in one pipeline:

  ```powershell
  dotnet restore
  dotnet build -c Release
  dotnet test -c Release --no-build
  dotnet pack src/KoreForge.AppLifecycle/KoreForge.AppLifecycle.csproj -c Release
  ```


