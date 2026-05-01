# Versioning Guide

## Overview

This solution uses **Semantic Versioning 2.0.0** with Git tags as the single source of truth. We rely on [MinVer](https://github.com/adamralph/minver) (configured in `Directory.Build.props`) to compute the version during every build, pack, and publish. All packable projects within this solution share the exact same version for a given commit. Test and sample projects inherit the configuration but remain non-packable.

### Configuration Summary

| Setting | Value |
| --- | --- |
| Tag Prefix | `KoreForge.AppLifecycle/v` |
| Auto Increment | `minor` |
| Default Pre-release | `alpha.0` |

Example release tag: `KoreForge.AppLifecycle/v1.4.0`

## Versioning Scripts

Version tags are managed with `bin/git-push-nuget.ps1`.

### Check the current version

```powershell
# Recent release tags
git tag --list "KoreForge.AppLifecycle/v*" --sort=-version:refname | Select-Object -First 10

# Version MinVer will compute for the current commit
dotnet build src/KF.AppLifecycle/KF.AppLifecycle.csproj -p:MinVerVerbosity=normal 2>&1 | Select-String MinVer
```

### Create and push a release tag

```powershell
# Push any uncommitted changes, create the annotated tag, and push it
.\bin\git-push-nuget.ps1 -Version 1.2.0 -Note "Brief release note"
```

## Semantic Versioning Rules

- **MAJOR** (`X.y.z`): Breaking changes in public API or behavior.
  - Examples: removing or renaming a public type, changing method signatures, altering behavior in a way that breaks existing consumers.
- **MINOR** (`x.Y.z`): Backwards-compatible feature additions.
  - Examples: adding new options, methods, events, or features that do not break existing code.
- **PATCH** (`x.y.Z`): Backwards-compatible fixes and improvements.
  - Examples: bug fixes, performance tuning, documentation updates, internal refactors without API changes.

## Release Workflow

1. Ensure the working tree is clean and tests pass:
   ```powershell
   .\bin\build-test.ps1
   ```

2. Decide the new SemVer (MAJOR.MINOR.PATCH) according to the rules above.

3. Create and push the release tag (commits any pending changes first):
   ```powershell
   .\bin\git-push-nuget.ps1 -Version 1.2.0 -Note "Add feature X"
   ```

4. Build and pack using the tagged version:
   ```powershell
   dotnet pack src/KF.AppLifecycle/KF.AppLifecycle.csproj -c Release
   ```

5. Verify the package version in the `artifacts/` folder matches your tag.

6. Publish the packages to your NuGet feed:
   ```powershell
   dotnet nuget push artifacts/KoreForge.AppLifecycle.<version>.nupkg --api-key <KEY> --source https://api.nuget.org/v3/index.json --skip-duplicate
   ```

## Pre-release and Development Builds

- Commits after the latest tag automatically produce pre-release versions such as `1.3.0-alpha.0.1`, `1.3.0-alpha.0.2`, etc.
- These builds are suitable for internal consumption, previews, or testing feeds but should not be published as official releases.
- To publish a preview release, use a pre-release tag like `1.4.0-beta.1`:
  ```powershell
  .\bin\git-push-nuget.ps1 -Version 1.4.0-beta.1 -Note "Preview release"
  ```

## Do's and Don'ts

**Do:**
  - ✅ Use `git tag --list "KoreForge.AppLifecycle/v*"` to check current version before releasing
- ✅ Use `bin/git-push-nuget.ps1` to create version tags
- ✅ Follow the SemVer rules when choosing MAJOR vs MINOR vs PATCH
- ✅ Ensure tags are pushed to origin so CI sees the same version

**Don't:**
- ❌ Manually edit `<Version>`, `<PackageVersion>`, etc. in project files
- ❌ Create tags that don't follow the `{ProductName}/vX.Y.Z` pattern
- ❌ Forget to push tags to origin

## Cheat Sheet

| Scenario | Command |
| --- | --- |
| Check current version | `git tag --list "KoreForge.AppLifecycle/v*" --sort=-version:refname` |
| Breaking change release | `.\bin\git-push-nuget.ps1 -Version 2.0.0 -Note "Breaking: ..."` |
| New feature release | `.\bin\git-push-nuget.ps1 -Version 1.3.0 -Note "Add feature X"` |
| Bug fix / patch release | `.\bin\git-push-nuget.ps1 -Version 1.2.1 -Note "Fix bug Y"` |
| Preview/beta release | `.\bin\git-push-nuget.ps1 -Version 1.4.0-beta.1 -Note "Preview"` |

## Relation to Other KoreForge Libraries

All KoreForge.* repositories follow this same versioning pattern:
- Each solution has its own tag prefix (e.g., `KoreForge.Logging/v`, `KoreForge.Kafka/v`)
- Each solution maintains its own version and release cadence
- Cross-solution dependencies use standard NuGet package references

## Technical Details

MinVer configuration in `Directory.Build.props`:

```xml
<PropertyGroup>
  <MinVerTagPrefix>KoreForge.AppLifecycle/v</MinVerTagPrefix>
  <MinVerAutoIncrement>minor</MinVerAutoIncrement>
  <MinVerDefaultPreReleaseIdentifiers>alpha.0</MinVerDefaultPreReleaseIdentifiers>
</PropertyGroup>
```

This ensures:
- `Version`, `PackageVersion`, `AssemblyVersion`, and `FileVersion` are all derived from Git tags
- Consistent versioning across all packable projects in the solution
