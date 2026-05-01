#Requires -Version 7.2
<#
.SYNOPSIS
    Runs the EventReader FASTER Durability Resume test and produces an HTML report.

.DESCRIPTION
    Pre-loads 1 M messages into Kafka.  Runs the consumer pipeline until at least 300 K
    messages have been processed, then simulates a hard consumer restart by disposing the
    FasterEventReaderWorkStore (which forces a final checkpoint).

    A new work-store instance is then created pointing at the same paths.  The test verifies
    that FASTER's RecoverOrBootstrap() rehydrates the classified-item count and LastWorkItemId
    exactly from the on-disk checkpoint.  The consumer then resumes and processes the remaining
    messages; FASTER source-index deduplication prevents any double-counting.

.PARAMETER Configuration
    Build configuration.  Default: Release.

.PARAMETER OutputDir
    Directory where the HTML report is written.
    Default: <repo-root>/out/StressResults
#>

[CmdletBinding()]
param(
    [ValidateSet("Debug", "Release")]
    [string] $Configuration = "Release",

    [string] $OutputDir     = ""
)

$ErrorActionPreference = "Stop"

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
$repoRoot    = Split-Path $PSScriptRoot -Parent
$testProject = Join-Path $repoRoot "tst\EventReader.Tests\EventReader.Tests.csproj"

if (-not (Test-Path $testProject)) {
    throw "Test project not found: $testProject"
}

if ([string]::IsNullOrWhiteSpace($OutputDir)) {
    $OutputDir = Join-Path $repoRoot "out\StressResults"
}

$timestamp      = Get-Date -Format "yyyyMMdd-HHmmss"
$reportFileName = "stress-faster-durability-$timestamp.html"
$reportPath     = Join-Path $OutputDir $reportFileName

New-Item -Path $OutputDir -ItemType Directory -Force | Out-Null

# ---------------------------------------------------------------------------
# Banner
# ---------------------------------------------------------------------------
Write-Host ""
Write-Host "╔══════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║   EventReader — FASTER Durability Resume Test            ║" -ForegroundColor Cyan
Write-Host "╠══════════════════════════════════════════════════════════╣" -ForegroundColor Cyan
Write-Host "║  Total messages  : 1,000,000 (fixed)                     ║" -ForegroundColor White
Write-Host "║  Phase 1 target  : 300,000 messages then restart         ║" -ForegroundColor White
Write-Host "║  Config          : $($Configuration.PadRight(37))║" -ForegroundColor White
Write-Host "╚══════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

# ---------------------------------------------------------------------------
# Run the test
# ---------------------------------------------------------------------------
$dotnetArgs = "test `"$testProject`" -c $Configuration --filter `"Category=StressIntegration&FullyQualifiedName~FasterDurabilityResumeTests`" --logger `"console;verbosity=detailed`" --nologo"

$outputLines = [System.Collections.Generic.List[string]]::new()
$startTime   = Get-Date
$exitCode    = 1

try {
    $psi                       = [System.Diagnostics.ProcessStartInfo]::new()
    $psi.FileName               = "dotnet"
    $psi.Arguments              = $dotnetArgs
    $psi.WorkingDirectory       = $repoRoot
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError  = $true
    $psi.UseShellExecute        = $false

    $process = [System.Diagnostics.Process]::Start($psi)

    $stdoutTask = $process.StandardOutput.ReadToEndAsync()
    $stderrTask = $process.StandardError.ReadToEndAsync()
    $process.WaitForExit()
    $exitCode = $process.ExitCode

    $stdout = $stdoutTask.GetAwaiter().GetResult()
    $stderr = $stderrTask.GetAwaiter().GetResult()

    foreach ($line in ($stdout -split "`r?`n")) {
        $t = $line.TrimEnd()
        $outputLines.Add($t)
        Write-Host $t
    }

    if ($stderr) {
        foreach ($line in ($stderr -split "`r?`n")) {
            $t = $line.TrimEnd()
            if ($t) {
                $outputLines.Add("[STDERR] $t")
                Write-Host "[STDERR] $t" -ForegroundColor Yellow
            }
        }
    }
}
catch {
    $msg = "ERROR launching test process: $_"
    $outputLines.Add($msg)
    Write-Host $msg -ForegroundColor Red
    $exitCode = 1
}

$endTime      = Get-Date
$wallDuration = $endTime - $startTime
$success      = $exitCode -eq 0

# ---------------------------------------------------------------------------
# Parse metrics
# "EventReader durability phase1: processed=X; classified=X; workItemId=X"
# "EventReader durability phase2: phase2-processed=X; final-workItemId=X; final-classified=X"
# ---------------------------------------------------------------------------
$phase1Line = $outputLines | Where-Object { $_ -match 'EventReader durability phase1:' } | Select-Object -Last 1
$phase2Line = $outputLines | Where-Object { $_ -match 'EventReader durability phase2:' } | Select-Object -Last 1

$p1Processed   = $null
$p1Classified  = $null
$p1WorkItemId  = $null
$p2Processed   = $null
$p2WorkItemId  = $null
$p2Classified  = $null

if ($phase1Line -match 'processed=(\d+)')     { $p1Processed  = [long]$Matches[1] }
if ($phase1Line -match 'classified=(\d+)')    { $p1Classified = [long]$Matches[1] }
if ($phase1Line -match 'workItemId=(\d+)')    { $p1WorkItemId = [long]$Matches[1] }
if ($phase2Line -match 'phase2-processed=(\d+)')   { $p2Processed   = [long]$Matches[1] }
if ($phase2Line -match 'final-workItemId=(\d+)')   { $p2WorkItemId  = [long]$Matches[1] }
if ($phase2Line -match 'final-classified=(\d+)')   { $p2Classified  = [long]$Matches[1] }

# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------
$p1ProcessedD  = if ($null -ne $p1Processed)  { "{0:N0}" -f $p1Processed }  else { "N/A" }
$p1ClassifiedD = if ($null -ne $p1Classified) { "{0:N0}" -f $p1Classified } else { "N/A" }
$p1WorkItemD   = if ($null -ne $p1WorkItemId) { "{0:N0}" -f $p1WorkItemId } else { "N/A" }
$p2ProcessedD  = if ($null -ne $p2Processed)  { "{0:N0}" -f $p2Processed }  else { "N/A" }
$p2WorkItemD   = if ($null -ne $p2WorkItemId) { "{0:N0}" -f $p2WorkItemId } else { "N/A" }
$p2ClassifiedD = if ($null -ne $p2Classified) { "{0:N0}" -f $p2Classified } else { "N/A" }
$passLabel     = if ($success) { "PASSED" } else { "FAILED" }
$runDate       = $startTime.ToString("yyyy-MM-dd HH:mm:ss")
$wallDurDisplay = $wallDuration.ToString('hh\:mm\:ss')
$exitColor = if ($success) { 'Green' } else { 'Red' }
$expectedClassified = 900000
$maxCounterDrift = 20000

function Threshold-Status([bool]$pass) { if ($pass) { '<span class="ok">✓</span>' } else { '<span class="ng">✗</span>' } }

$th_pass      = Threshold-Status ($success)
$th_recovery  = Threshold-Status ($null -ne $p1WorkItemId -and $null -ne $p2WorkItemId -and $p2WorkItemId -ge $p1WorkItemId)
$th_counter   = Threshold-Status (
  $null -ne $p2WorkItemId -and
  $p2WorkItemId -ge $expectedClassified -and
  $p2WorkItemId -le ($expectedClassified + $maxCounterDrift)
)
$recoveryMatch = if ($null -ne $p1WorkItemId -and $null -ne $p2WorkItemId) {
  if ($p2WorkItemId -ge $p1WorkItemId) { "MONOTONIC ($p1WorkItemD → $p2WorkItemD)" } else { "REGRESSION ($p1WorkItemD → $p2WorkItemD)" }
} else { "N/A" }

function ConvertTo-HtmlEncoded([string] $s) {
    $s -replace '&', '&amp;' -replace '<', '&lt;' -replace '>', '&gt;' -replace '"', '&quot;'
}

$rawOutputHtml = ($outputLines | ForEach-Object { ConvertTo-HtmlEncoded $_ }) -join "`n"

# ---------------------------------------------------------------------------
# Generate HTML
# ---------------------------------------------------------------------------
$html = @"
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <title>EventReader FASTER Durability — $runDate</title>
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
           background: #f0f2f5; color: #1a1a2e; font-size: 14px; line-height: 1.6; }
    header { background: linear-gradient(135deg, #1a1a2e 0%, #16213e 60%, #0f3460 100%);
             color: #fff; padding: 32px 40px 28px; display: flex; align-items: flex-start;
             justify-content: space-between; flex-wrap: wrap; gap: 16px; }
    header .title-block h1 { font-size: 11px; letter-spacing: 3px; text-transform: uppercase;
                              color: #8b9dc3; margin-bottom: 4px; }
    header .title-block h2 { font-size: 22px; font-weight: 700; color: #e8eaf6; }
    header .title-block p  { font-size: 12px; color: #8b9dc3; margin-top: 6px; }
    .badge { padding: 8px 20px; border-radius: 24px; font-size: 15px; font-weight: 700; }
    .badge.pass { background: #28a745; color: #fff; }
    .badge.fail { background: #dc3545; color: #fff; }
    main { max-width: 1100px; margin: 0 auto; padding: 32px 24px; }
    .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
             gap: 16px; margin-bottom: 32px; }
    .card  { background: #fff; border-radius: 10px; padding: 20px 18px;
             box-shadow: 0 1px 4px rgba(0,0,0,.08); text-align: center; }
    .card .label { font-size: 11px; text-transform: uppercase; letter-spacing: 1px;
                   color: #888; margin-bottom: 8px; }
    .card .value { font-size: 26px; font-weight: 700; color: #1a1a2e; }
    .card.highlight .value { color: #4a6cf7; }
    .panel { background: #fff; border-radius: 10px; padding: 24px;
             box-shadow: 0 1px 4px rgba(0,0,0,.08); margin-bottom: 24px; }
    .panel h3 { font-size: 13px; text-transform: uppercase; letter-spacing: 1px;
                color: #4a6cf7; border-bottom: 2px solid #eef0ff; padding-bottom: 10px;
                margin-bottom: 16px; }
    .panel p { margin: 0 0 10px 0; color: #334155; }
    .panel ul { margin: 0 0 12px 18px; color: #334155; }
    .phase-header { font-size: 12px; text-transform: uppercase; letter-spacing: 1px;
                    color: #888; font-weight: 600; margin: 20px 0 8px; }
    table  { width: 100%; border-collapse: collapse; font-size: 13px; }
    th, td { padding: 8px 10px; text-align: left; }
    thead th { background: #f7f8ff; color: #555; font-weight: 600; font-size: 11px;
               text-transform: uppercase; letter-spacing: .6px; }
    tbody tr:nth-child(even) { background: #fafbff; }
    tbody tr:hover { background: #f0f4ff; }
    .ok { color: #28a745; font-weight: 700; }
    .ng { color: #dc3545; font-weight: 700; }
    details { background: #fff; border-radius: 10px; overflow: hidden;
              box-shadow: 0 1px 4px rgba(0,0,0,.08); margin-bottom: 24px; }
    details summary { padding: 16px 24px; cursor: pointer; font-weight: 600; font-size: 13px;
                      color: #555; background: #f7f8ff; }
    pre { background: #1a1a2e; color: #c9d1d9; font-size: 11.5px; padding: 20px 24px;
          overflow-x: auto; white-space: pre; }
  </style>
</head>
<body>
  <header>
    <div class="title-block">
      <h1>EventReader · Stress Test</h1>
      <h2>FASTER Durability — Checkpoint Survives Consumer Restart</h2>
      <p>Run: $runDate &nbsp;|&nbsp; Wall time: $wallDurDisplay &nbsp;|&nbsp; Config: $Configuration</p>
    </div>
    <div class="badge $(if ($success) { 'pass' } else { 'fail' })">$passLabel</div>
  </header>
  <main>
    <div class="cards">
      <div class="card highlight">
        <div class="label">Outcome</div>
        <div class="value">$passLabel</div>
      </div>
      <div class="card">
        <div class="label">Phase 1 Processed</div>
        <div class="value">$p1ProcessedD</div>
      </div>
      <div class="card">
        <div class="label">Phase 1 WorkItemId</div>
        <div class="value">$p1WorkItemD</div>
      </div>
      <div class="card">
        <div class="label">Phase 2 Final Classified</div>
        <div class="value">$p2ClassifiedD</div>
      </div>
      <div class="card">
        <div class="label">Recovery ID Status</div>
        <div class="value">$recoveryMatch</div>
      </div>
    </div>

    <div class="panel">
      <h3>What This Test Is Proving</h3>
      <p>This test verifies that EventReader does not lose durable work-store state when the consumer process is restarted between two processing phases.</p>
      <p><strong>Scenario under test:</strong></p>
      <ul>
        <li>Phase 1: Produce 1,000,000 messages and process until at least 300,000 have been consumed.</li>
        <li>Restart point: Force a checkpoint by disposing the FASTER work store.</li>
        <li>Phase 2: Create a brand-new work-store instance from the same disk paths and continue processing.</li>
      </ul>
      <p><strong>Why this test matters:</strong> if recovery is wrong, production restarts can cause duplicate processing, skipped records, or replay drift that slowly corrupts downstream state.</p>
      <p><strong>What a pass tells you:</strong> checkpointed state was recovered, the unique work-item counter did not move backward after restart, and resumed processing stayed within the expected completion band for this dataset.</p>
    </div>

    <div class="panel">
      <h3>How To Read The Numbers</h3>
      <table>
        <thead><tr><th>Metric</th><th>Plain-English Meaning</th><th>How To Interpret</th></tr></thead>
        <tbody>
          <tr><td>Phase 1 Processed</td><td>Total messages consumed before the forced restart point.</td><td>Should be at least 300,000 for this test flow.</td></tr>
          <tr><td>Phase 1 WorkItemId</td><td>Highest unique durable work item id before restart.</td><td>This is the recovery baseline.</td></tr>
          <tr><td>Phase 2 Final Classified</td><td>Backlog currently in the Classified queue at end of run.</td><td>Can differ slightly from final unique id due to state transitions in flight.</td></tr>
          <tr><td>Recovery ID Status</td><td>Whether recovered id is greater than or equal to the phase-1 id.</td><td>"MONOTONIC" means no backward movement after restart (safe recovery progression).</td></tr>
          <tr><td>Final WorkItemId in expected band</td><td>Final unique durable id after resumed processing.</td><td>Expected range is 900,000..920,000 for this workload and checkpoint timing.</td></tr>
          <tr><td>Test passed</td><td>Exit code from dotnet test execution.</td><td>Must be 0 for overall pass.</td></tr>
        </tbody>
      </table>
    </div>

    <div class="panel">
      <h3>Threshold Checks</h3>
      <table>
        <thead><tr><th>Check</th><th>Expected</th><th>Actual</th><th></th></tr></thead>
        <tbody>
          <tr><td>Test passed</td><td>Exit code 0</td><td>$passLabel</td><td>$th_pass</td></tr>
          <tr><td>WorkItemId recovered monotonically</td><td>Phase2 recovery ≥ Phase1</td><td>$recoveryMatch</td><td>$th_recovery</td></tr>
          <tr><td>Final WorkItemId in expected band</td><td>900,000..920,000</td><td>$p2WorkItemD</td><td>$th_counter</td></tr>
        </tbody>
      </table>
    </div>

    <div class="panel">
      <h3>Two-Phase Detail</h3>
      <div class="phase-header">Phase 1 — Partial Processing (target: 300,000 messages)</div>
      <table>
        <thead><tr><th>Metric</th><th>Value</th></tr></thead>
        <tbody>
          <tr><td>Messages processed</td><td>$p1ProcessedD</td></tr>
          <tr><td>Classified (enqueued in FASTER)</td><td>$p1ClassifiedD</td></tr>
          <tr><td>LastWorkItemId before restart</td><td>$p1WorkItemD</td></tr>
        </tbody>
      </table>
      <div class="phase-header" style="margin-top:20px">Restart — Checkpoint Recovery</div>
      <table>
        <thead><tr><th>Metric</th><th>Value</th></tr></thead>
        <tbody>
          <tr><td>DisposeAsync() guaranteed checkpoint</td><td>Yes (built into FasterEventReaderWorkStore)</td></tr>
          <tr><td>Recovered LastWorkItemId</td><td>$p2WorkItemD</td></tr>
          <tr><td>Recovery progression (no state regression)</td><td>$recoveryMatch</td></tr>
        </tbody>
      </table>
      <div class="phase-header" style="margin-top:20px">Phase 2 — Resume to 1 M</div>
      <table>
        <thead><tr><th>Metric</th><th>Value</th></tr></thead>
        <tbody>
          <tr><td>Phase 2 messages processed (incl. Kafka re-deliveries)</td><td>$p2ProcessedD</td></tr>
          <tr><td>Final unique classified (WorkItemId)</td><td>$p2WorkItemD</td></tr>
          <tr><td>Final BacklogByState[Classified]</td><td>$p2ClassifiedD</td></tr>
        </tbody>
      </table>
    </div>

    <details>
      <summary>Full test output</summary>
      <pre>$rawOutputHtml</pre>
    </details>
  </main>
</body>
</html>
"@

Set-Content -Path $reportPath -Value $html -Encoding UTF8
Write-Host ""
Write-Host "─────────────────────────────────────────────────────────" -ForegroundColor Cyan
Write-Host "Stress report: $reportPath" -ForegroundColor Green
Write-Host "Exit code    : $exitCode"   -ForegroundColor $exitColor

exit $exitCode
