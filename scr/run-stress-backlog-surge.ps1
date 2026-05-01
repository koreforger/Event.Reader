#Requires -Version 7.2
<#
.SYNOPSIS
    Runs the EventReader Backlog Surge stress test (10 M records) and produces an HTML report.

.DESCRIPTION
    Pre-loads Kafka with MessageCount messages, processes the full pipeline, and records
    CPU, RAM, FASTER disk-footprint and throughput samples every 5 seconds.

    The FINDING this test surfaces: FASTER uses an append-only log.  Completed items are
    never deleted — disk grows proportionally to record volume unless explicit log compaction
    (FasterKV.Log.Compact) is added to the application.  The report includes a resource
    timeline so you can see the growth rate and plan disk capacity accordingly.

.PARAMETER MessageCount
    Number of messages.  Default: 10,000,000.  Override via EVENTREADER_SURGE_COUNT env var.

.PARAMETER Configuration
    Build configuration.  Default: Release.

.PARAMETER OutputDir
    Directory where the HTML report is written.
    Default: <repo-root>/out/StressResults
#>

[CmdletBinding()]
param(
    [int]    $MessageCount  = 10000000,

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
$reportFileName = "stress-backlog-surge-$timestamp.html"
$reportPath     = Join-Path $OutputDir $reportFileName

New-Item -Path $OutputDir -ItemType Directory -Force | Out-Null

# ---------------------------------------------------------------------------
# Banner
# ---------------------------------------------------------------------------
Write-Host ""
Write-Host "╔══════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║   EventReader — Backlog Surge Stress (10 M records)      ║" -ForegroundColor Cyan
Write-Host "╠══════════════════════════════════════════════════════════╣" -ForegroundColor Cyan
Write-Host "║  Messages    : $($MessageCount.ToString('N0').PadRight(41))║" -ForegroundColor White
Write-Host "║  Config      : $($Configuration.PadRight(41))║" -ForegroundColor White
Write-Host "║  Samples     : every 5 s (CPU, RAM, FASTER log size)     ║" -ForegroundColor White
Write-Host "╚══════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

# ---------------------------------------------------------------------------
# Run the test
# ---------------------------------------------------------------------------
$env:EVENTREADER_SURGE_COUNT = $MessageCount.ToString()

$dotnetArgs = "test `"$testProject`" -c $Configuration --filter `"Category=StressIntegration&FullyQualifiedName~KafkaBacklogSurgeStressTests`" --logger `"console;verbosity=detailed`" --nologo"

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
# ---------------------------------------------------------------------------
# Console.WriteLine line from the test: "EventReader surge rate: X msg/s; peak-ram=XMB; faster-log-growth=XMB; classified=X; accuracy=X%"
$summaryLine = $outputLines | Where-Object { $_ -match 'EventReader surge rate:' } | Select-Object -Last 1

$parsedRate        = $null
$parsedPeakRamMb   = $null
$parsedLogGrowthMb = $null
$parsedClassified  = $null
$parsedAccuracy    = $null

if ($summaryLine -match 'rate: ([\d.]+) msg/s')              { $parsedRate        = [double]$Matches[1] }
if ($summaryLine -match 'peak-ram=(\d+)MB')                  { $parsedPeakRamMb   = [long]  $Matches[1] }
if ($summaryLine -match 'faster-log-growth=([\d.]+)MB')      { $parsedLogGrowthMb = [double]$Matches[1] }
if ($summaryLine -match 'classified=(\d+)')                  { $parsedClassified  = [long]  $Matches[1] }
if ($summaryLine -match 'accuracy=([\d.]+) ?%')              { $parsedAccuracy    = [double]$Matches[1] }

# Collect RESOURCE SAMPLE lines for the timeline table
$sampleLines = @($outputLines | Where-Object { $_ -match '^RESOURCE SAMPLE:' })

# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------
$rateDisplay       = if ($null -ne $parsedRate)        { "{0:N0} msg/s"  -f $parsedRate }        else { "N/A" }
$ramDisplay        = if ($null -ne $parsedPeakRamMb)   { "{0:N0} MB"     -f $parsedPeakRamMb }   else { "N/A" }
$logGrowthDisplay  = if ($null -ne $parsedLogGrowthMb) { "{0:F1} MB"     -f $parsedLogGrowthMb } else { "N/A" }
$classifiedDisplay = if ($null -ne $parsedClassified)  { "{0:N0}"        -f $parsedClassified }  else { "N/A" }
$accuracyDisplay   = if ($null -ne $parsedAccuracy)    { "{0:F2}%"       -f $parsedAccuracy }    else { "N/A" }
$passLabel         = if ($success) { "PASSED" } else { "FAILED" }
$runDate           = $startTime.ToString("yyyy-MM-dd HH:mm:ss")
$wallDurDisplay    = $wallDuration.ToString('hh\:mm\:ss')
$exitColor         = if ($success) { 'Green' } else { 'Red' }

$expectedClassified = [Math]::Ceiling($MessageCount * 0.9)

function Threshold-Status([bool]$pass) { if ($pass) { '<span class="ok">✓</span>' } else { '<span class="ng">✗</span>' } }

$th_pass       = Threshold-Status ($success)
$th_accuracy   = Threshold-Status ($null -ne $parsedAccuracy   -and $parsedAccuracy   -ge 90.0)
$th_ram        = Threshold-Status ($null -ne $parsedPeakRamMb  -and $parsedPeakRamMb  -lt 4096)
$th_classified = Threshold-Status ($null -ne $parsedClassified -and $parsedClassified -ge $expectedClassified)

function ConvertTo-HtmlEncoded([string] $s) {
    $s -replace '&', '&amp;' -replace '<', '&lt;' -replace '>', '&gt;' -replace '"', '&quot;'
}

# Build resource timeline HTML rows
$timelineRows = ""
foreach ($sl in $sampleLines) {
    # RESOURCE SAMPLE: t=HH:mm:ss processed=N ram=NMB faster-log=N.NMB disk-free=N.NNGB cpu=N.N%
    $tm       = if ($sl -match 't=(\S+)')               { $Matches[1] }  else { "-" }
    $proc     = if ($sl -match 'processed=([\d,]+)')    { $Matches[1] }  else { "-" }
    $ram      = if ($sl -match 'ram=(\d+)MB')           { $Matches[1] }  else { "-" }
    $log      = if ($sl -match 'faster-log=([\d.]+)MB') { $Matches[1] }  else { "-" }
    $disk     = if ($sl -match 'disk-free=([\d.]+)GB')  { $Matches[1] }  else { "-" }
    $cpu      = if ($sl -match 'cpu=([\d.]+)%')         { $Matches[1] }  else { "-" }
    $timelineRows += "<tr><td>$tm</td><td>$proc</td><td>${ram} MB</td><td>${log} MB</td><td>${disk} GB</td><td>${cpu}%</td></tr>`n"
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
  <title>EventReader Surge Stress — $runDate</title>
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
    table  { width: 100%; border-collapse: collapse; font-size: 13px; }
    th, td { padding: 8px 10px; text-align: left; }
    thead th { background: #f7f8ff; color: #555; font-weight: 600; font-size: 11px;
               text-transform: uppercase; letter-spacing: .6px; }
    tbody tr:nth-child(even) { background: #fafbff; }
    tbody tr:hover { background: #f0f4ff; }
    .ok { color: #28a745; font-weight: 700; }
    .ng { color: #dc3545; font-weight: 700; }
    .finding { background: #fffbe6; border-left: 4px solid #f59e0b; border-radius: 0 8px 8px 0;
               padding: 18px 22px; margin-bottom: 24px; }
    .finding h4 { color: #92400e; font-size: 13px; text-transform: uppercase; letter-spacing: 1px;
                  margin-bottom: 8px; }
    .finding p  { color: #78350f; font-size: 13px; margin-bottom: 6px; }
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
      <h2>Backlog Surge — 10 M Record Resource Profile</h2>
      <p>Run: $runDate &nbsp;|&nbsp; Wall time: $wallDurDisplay &nbsp;|&nbsp; Config: $Configuration</p>
    </div>
    <div class="badge $(if ($success) { 'pass' } else { 'fail' })">$passLabel</div>
  </header>
  <main>
    <div class="cards">
      <div class="card highlight">
        <div class="label">Throughput</div>
        <div class="value">$rateDisplay</div>
      </div>
      <div class="card">
        <div class="label">Messages</div>
        <div class="value">$($MessageCount.ToString('N0'))</div>
      </div>
      <div class="card">
        <div class="label">Classified</div>
        <div class="value">$classifiedDisplay</div>
      </div>
      <div class="card">
        <div class="label">Accuracy</div>
        <div class="value">$accuracyDisplay</div>
      </div>
      <div class="card">
        <div class="label">Peak RAM</div>
        <div class="value">$ramDisplay</div>
      </div>
      <div class="card">
        <div class="label">Log Growth</div>
        <div class="value">$logGrowthDisplay</div>
      </div>
    </div>

    <div class="panel">
      <h3>What This Test Is Proving</h3>
      <p>This test measures how EventReader behaves under a very large one-time backlog surge and verifies throughput, classification quality, memory ceiling, and FASTER storage growth.</p>
      <p><strong>Scenario under test:</strong></p>
      <ul>
        <li>Publish a large batch of records (default: 10,000,000).</li>
        <li>Run the pipeline continuously until completion criteria are met.</li>
        <li>Sample process and storage resources every 5 seconds during execution.</li>
      </ul>
      <p><strong>Why this test matters:</strong> backlog spikes happen in production after outages, replay events, or upstream burst traffic. This report shows whether the system stays efficient and stable during that stress window.</p>
      <p><strong>What a pass tells you:</strong> the pipeline maintained acceptable classification accuracy, kept memory inside the target cap, and processed enough records for the expected dataset size.</p>
    </div>

    <div class="panel">
      <h3>How To Read The Numbers</h3>
      <table>
        <thead><tr><th>Metric</th><th>Plain-English Meaning</th><th>How To Interpret</th></tr></thead>
        <tbody>
          <tr><td>Throughput</td><td>Average records processed per second during the run.</td><td>Higher is better, but compare against CPU/memory and accuracy together.</td></tr>
          <tr><td>Messages</td><td>Total test input records sent to Kafka for this run.</td><td>This is the workload size, not the success count.</td></tr>
          <tr><td>Classified</td><td>Records that reached the Classified outcome in the pipeline.</td><td>Should be at least 90% of input for this stress profile.</td></tr>
          <tr><td>Accuracy</td><td>Percent of records correctly classified by test expectations.</td><td>Target is 90% or higher.</td></tr>
          <tr><td>Peak RAM</td><td>Highest observed process working set in MB.</td><td>Must stay below 4,096 MB in this report's threshold.</td></tr>
          <tr><td>Log Growth</td><td>How much the FASTER log file grew during the run.</td><td>Growth is expected with append-only storage; use this for capacity planning.</td></tr>
          <tr><td>Records processed (timeline)</td><td>Cumulative processed count at each 5-second sample point.</td><td>Should trend upward over time; flat regions indicate slowdowns or stalls.</td></tr>
        </tbody>
      </table>
    </div>

    <div class="finding">
      <h4>Finding — FASTER Disk Growth Is Expected</h4>
      <p>FASTER uses an append-only hybrid log. Completed work items are <strong>never deleted</strong>
         from the log device. Disk usage grows proportionally to the number of records written.
         The log-growth figure above reflects this normal behaviour for this record volume.</p>
      <p>To reclaim disk space, add explicit compaction via <code>FasterKV.Log.Compact()</code> after
         state-machine transitions move items beyond <em>Classified</em>.  Until then, capacity planning
         should assume <strong>~400 – 800 bytes per record</strong> on disk (serialised WorkItemRecord +
         source-index entry + queue entries).</p>
    </div>

    <div class="panel">
      <h3>Threshold Checks</h3>
      <table>
        <thead><tr><th>Check</th><th>Target</th><th>Result</th><th></th></tr></thead>
        <tbody>
          <tr><td>Test passed</td><td>Exit code 0</td><td>$passLabel</td><td>$th_pass</td></tr>
          <tr><td>Classification accuracy</td><td>≥ 90 %</td><td>$accuracyDisplay</td><td>$th_accuracy</td></tr>
          <tr><td>Classified count</td><td>≥ $($expectedClassified.ToString('N0'))</td><td>$classifiedDisplay</td><td>$th_classified</td></tr>
          <tr><td>Peak working set</td><td>&lt; 4 096 MB</td><td>$ramDisplay</td><td>$th_ram</td></tr>
        </tbody>
      </table>
    </div>

    <div class="panel">
      <h3>Resource Timeline (sampled every 5 s)</h3>
      <table>
        <thead>
          <tr>
            <th>Time</th>
            <th>Processed</th>
            <th>RAM (working set)</th>
            <th>FASTER Log Size</th>
            <th>Disk Free</th>
            <th>CPU %</th>
          </tr>
        </thead>
        <tbody>
          $timelineRows
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
