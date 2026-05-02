# Scripts Reference

Automation lives in the `scr/` directory and wraps the most common repository tasks so contributors and CI jobs use identical commands. Every script enforces strict PowerShell behavior (`Set-StrictMode -Version Latest`, `$ErrorActionPreference = 'Stop'`) and executes from the repository root.

| Script | Arguments | Description |
| --- | --- | --- |
| `build.ps1` | `-Configuration Debug|Release` (default `Debug`) | Runs `dotnet build KoreForge.Logging.slnx` for the chosen configuration. |
| `test.ps1` | `-Configuration Debug|Release` (default `Debug`) | Executes `dotnet test` with coverage disabled for faster inner-loop runs. |
| `test-with-coverage.ps1` | `-Configuration Debug|Release` (default `Debug`) | Executes `dotnet test` with Coverlet + ReportGenerator enabled. HTML output: `TestResults/<project>/coverage-html/index.html`. |
| `pack.ps1` | `-Configuration Debug|Release` (default `Release`) | Produces `.nupkg` and `.snupkg` in `artifacts/` via `dotnet pack`. |
| `clean.ps1` | *(none)* | Deletes `scr/`, `obj/`, `artifacts/`, and `TestResults/` to reset the repo. |
| `publish.ps1` | `-ApiKey <token>` (required), `-Source <url>` (default NuGet), `-Configuration Debug|Release`, `-SkipPack` | Packs (unless `-SkipPack`), then pushes both `.nupkg` and `.snupkg` to the specified NuGet feed using `dotnet nuget push --skip-duplicate`. |

## Usage Tips

- Run scripts from any folder: each script resolves the repo root before executing.
- Pass `-SkipPack` to `publish.ps1` when you already have packages in `artifacts/`.
- Combine with PowerShell's `-Verbose` flag to trace the underlying `dotnet` invocations.

