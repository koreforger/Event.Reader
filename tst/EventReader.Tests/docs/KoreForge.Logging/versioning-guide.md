# Versioning Guide

## Overview

This solution uses **Semantic Versioning 2.0.0** with Git tags as the single source of truth. We rely on [MinVer](https://github.com/adamralph/minver) (configured in `Directory.Build.props`) to compute the version during every build, pack, and publish. All packable projects within this solution share the exact same version for a given commit. Test and sample projects inherit the configuration but remain non-packable.

### Configuration Summary

| Setting | Value |
| --- | --- |
| Tag Prefix | `KoreForge.Logging/v` |
| Auto Increment | `minor` |
| Default Pre-release | `alpha.0` |

Example release tag: `KoreForge.Logging/v1.4.0`

## Versioning Scripts

This solution provides two scripts in the `scr/` folder to manage versions:

### Get-Version.ps1

Displays current version information:

```powershell
.\scr\Get-Version.ps1
```

Output includes:
- Tag prefix configuration
- Latest release tag
- Recent release history
- Current commit and working tree status

### Tag-Release.ps1

Creates release tags:

```powershell
# Create a tag locally
.\scr\Tag-Release.ps1 -Version 1.2.0

# Create and push to origin
.\scr\Tag-Release.ps1 -Version 1.2.0 -Push

# Overwrite an existing tag
.\scr\Tag-Release.ps1 -Version 1.2.0 -Push -Force
```

## Semantic Versioning Rules

- **MAJOR** (`X.y.z`): Breaking changes in public API or behavior.
  - Examples: removing or renaming a public type, changing method signatures, altering behavior in a way that breaks existing consumers.
- **MINOR** (`x.Y.z`): Backwards-compatible feature additions.
  - Examples: adding new options, methods, events, or features that do not break existing code.
- **PATCH** (`x.y.Z`): Backwards-compatible fixes and improvements.
  - Examples: bug fixes, performance tuning, documentation updates, internal refactors without API changes.

## Release Workflow

1. Ensure the working tree is clean:
   ```powershell
   .\scr\Get-Version.ps1
   ```

2. Run all tests:
   ```powershell
   .\scr\build-test.ps1
   # or with coverage
   .\scr\Test-Coverage.ps1
   ```

3. Decide the new SemVer (MAJOR.MINOR.PATCH) according to the rules above.

4. Create and push the release tag:
   ```powershell
   .\scr\Tag-Release.ps1 -Version 1.2.0 -Push
   ```

5. Build and pack:
   ```powershell
   .\scr\build-pack.ps1 -Configuration Release
   ```

6. Verify the package version in the `artifacts/` folder matches your tag.

7. Publish the packages to your NuGet feed.

## Pre-release and Development Builds

- Commits after the latest tag automatically produce pre-release versions such as `1.3.0-alpha.0.1`, `1.3.0-alpha.0.2`, etc.
- These builds are suitable for internal consumption, previews, or testing feeds but should not be published as official releases.
- To publish a preview release, use a pre-release tag like `1.4.0-beta.1`:
  ```powershell
  .\scr\Tag-Release.ps1 -Version 1.4.0-beta.1 -Push
  ```

## Do's and Don'ts

**Do:**
- ✅ Use `Get-Version.ps1` to check current version before releasing
- ✅ Use `Tag-Release.ps1` to create version tags
- ✅ Follow the SemVer rules when choosing MAJOR vs MINOR vs PATCH
- ✅ Ensure tags are pushed to origin so CI sees the same version

**Don't:**
- ❌ Manually edit `<Version>`, `<PackageVersion>`, etc. in project files
- ❌ Create tags that don't follow the `{ProductName}/vX.Y.Z` pattern
- ❌ Forget to push tags to origin

## Cheat Sheet

| Scenario | Command |
| --- | --- |
| Check current version | `.\scr\Get-Version.ps1` |
| Breaking change release | `.\scr\Tag-Release.ps1 -Version 2.0.0 -Push` |
| New feature release | `.\scr\Tag-Release.ps1 -Version 1.3.0 -Push` |
| Bug fix / patch release | `.\scr\Tag-Release.ps1 -Version 1.2.1 -Push` |
| Preview/beta release | `.\scr\Tag-Release.ps1 -Version 1.4.0-beta.1 -Push` |

## Relation to Other KoreForge Libraries

All KoreForge.* repositories follow this same versioning pattern:
- Each solution has its own tag prefix (e.g., `KoreForge.Logging/v`, `KoreForge.Kafka/v`)
- Each solution maintains its own version and release cadence
- Cross-solution dependencies use standard NuGet package references

## Technical Details

MinVer configuration in `Directory.Build.props`:

```xml
<PropertyGroup>
  <MinVerTagPrefix>KoreForge.Logging/v</MinVerTagPrefix>
  <MinVerAutoIncrement>minor</MinVerAutoIncrement>
  <MinVerDefaultPreReleaseIdentifiers>alpha.0</MinVerDefaultPreReleaseIdentifiers>
</PropertyGroup>
```

This ensures:
- `Version`, `PackageVersion`, `AssemblyVersion`, and `FileVersion` are all derived from Git tags
- Consistent versioning across all packable projects in the solution


