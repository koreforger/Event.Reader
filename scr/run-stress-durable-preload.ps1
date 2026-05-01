#Requires -Version 7.2
<#
.SYNOPSIS
    Runs the EventReader Durable Preload stress test and produces a self-contained HTML report.

.DESCRIPTION
    Pre-loads Kafka with MessageCount messages, then runs the full EventReader durable
    processing pipeline (JSON scan → identity resolution → shard assignment → classification →
    FASTER work-store write) and measures throughput, accuracy and durability.

.PARAMETER MessageCount
    Number of messages to pre-load into Kafka before the consumer starts.
    Default: 1,000,000.  Override via EVENTREADER_PRELOAD_STRESS_COUNT env var.

.PARAMETER Configuration
    Build configuration passed to dotnet test.  Default: Release.

.PARAMETER OutputDir
    Directory where the HTML report is written.
    Default: <repo-root>/out/StressResults
#>

[CmdletBinding()]
param(
    [int]    $MessageCount  = 1000000,

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
$reportFileName = "stress-durable-preload-$timestamp.html"
$reportPath     = Join-Path $OutputDir $reportFileName

New-Item -Path $OutputDir -ItemType Directory -Force | Out-Null

# ---------------------------------------------------------------------------
# Banner
# ---------------------------------------------------------------------------
Write-Host ""
Write-Host "╔══════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║   EventReader — Durable Preload Stress Test              ║" -ForegroundColor Cyan
Write-Host "╠══════════════════════════════════════════════════════════╣" -ForegroundColor Cyan
Write-Host "║  Messages    : $($MessageCount.ToString('N0').PadRight(41))║" -ForegroundColor White
Write-Host "║  Config      : $($Configuration.PadRight(41))║" -ForegroundColor White
Write-Host "║  Report      : (written after run)                       ║" -ForegroundColor DarkGray
Write-Host "╚══════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""
Write-Host "Phase 1 — Pre-loading $($MessageCount.ToString('N0')) messages into Kafka..." -ForegroundColor Yellow
Write-Host "         (Producer runs in the test fixture before consumption starts;" -ForegroundColor DarkGray
Write-Host "          this eliminates producer speed as a variable in the measurement.)" -ForegroundColor DarkGray
Write-Host ""

# ---------------------------------------------------------------------------
# Run the test
# ---------------------------------------------------------------------------
$env:EVENTREADER_PRELOAD_STRESS_COUNT = $MessageCount.ToString()

$dotnetArgs = "test `"$testProject`" -c $Configuration --filter `"Category=Stress`" --logger `"console;verbosity=detailed`" --nologo"

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

    # Read stdout and stderr concurrently to avoid deadlocks on large output
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
$rateLine       = $outputLines | Where-Object { $_ -match 'EventReader durable preload rate:' }   | Select-Object -Last 1
$processedLine  = $outputLines | Where-Object { $_ -match 'Processed \d+ messages in' }            | Select-Object -Last 1
$classifiedLine = $outputLines | Where-Object { $_ -match 'Classified \d+ messages =>' }           | Select-Object -Last 1

$parsedRate         = $null
$parsedProcessed    = $null
$parsedDurationSec  = $null
$parsedClassified   = $null
$parsedAccuracy     = $null
$parsedUnclassified = $null

if ($processedLine -match 'Processed (\d+) messages in ([\d.]+)s => ([\d.]+) msg/s') {
    $parsedProcessed   = [long]  $Matches[1]
    $parsedDurationSec = [double]$Matches[2]
    $parsedRate        = [double]$Matches[3]
}

# C# P2 format: "90.00 %" (with space) or "90.00%" depending on culture
if ($classifiedLine -match 'Classified (\d+) messages => accuracy ([\d.]+) ?%; unclassified=(\d+)') {
    $parsedClassified   = [long]  $Matches[1]
    $parsedAccuracy     = [double]$Matches[2]
    $parsedUnclassified = [long]  $Matches[3]
}
elseif ($classifiedLine -match 'Classified (\d+) messages => accuracy [^;]+; unclassified=(\d+)') {
    $parsedClassified   = [long]$Matches[1]
    $parsedUnclassified = [long]$Matches[2]
    if ($parsedProcessed -gt 0) {
        $parsedAccuracy = [Math]::Round($parsedClassified / $parsedProcessed * 100, 2)
    }
}

# Fallback: parse the Console.WriteLine line emitted by the test
if ($null -eq $parsedRate        -and $rateLine -match 'rate: ([\d.]+) msg/s')     { $parsedRate        = [double]$Matches[1] }
if ($null -eq $parsedClassified  -and $rateLine -match 'classified=(\d+)')         { $parsedClassified  = [long]  $Matches[1] }
if ($null -eq $parsedUnclassified -and $rateLine -match 'unclassified=(\d+)')      { $parsedUnclassified= [long]  $Matches[1] }
if ($null -eq $parsedAccuracy    -and $rateLine -match 'accuracy=([\d.]+) ?%')     { $parsedAccuracy    = [double]$Matches[1] }

# Derive accuracy if we have the components
if ($null -eq $parsedAccuracy -and $null -ne $parsedClassified -and $null -ne $parsedProcessed -and $parsedProcessed -gt 0) {
    $parsedAccuracy = [Math]::Round($parsedClassified / $parsedProcessed * 100, 2)
}

# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------
$rateDisplay         = if ($null -ne $parsedRate)          { "{0:N0} msg/s" -f $parsedRate }          else { "N/A" }
$processedDisplay    = if ($null -ne $parsedProcessed)     { "{0:N0}"       -f $parsedProcessed }     else { "$($MessageCount.ToString('N0')) (target)" }
$durationDisplay     = if ($null -ne $parsedDurationSec)   { "{0:F2}s"      -f $parsedDurationSec }   else { "$($wallDuration.TotalSeconds.ToString('F2'))s (wall)" }
$classifiedDisplay   = if ($null -ne $parsedClassified)    { "{0:N0}"       -f $parsedClassified }    else { "N/A" }
$accuracyDisplay     = if ($null -ne $parsedAccuracy)      { "{0:F2}%"      -f $parsedAccuracy }      else { "N/A" }
$unclassifiedDisplay = if ($null -ne $parsedUnclassified)  { "{0:N0}"       -f $parsedUnclassified }  else { "N/A" }

$expectedClassified  = [Math]::Ceiling($MessageCount * 0.9)
$expectedUnclassified = $MessageCount - $expectedClassified
$passLabel           = if ($success) { "PASSED" } else { "FAILED" }
$runDate             = $startTime.ToString("yyyy-MM-dd HH:mm:ss")
$wallDurationDisplay = $wallDuration.ToString('hh\:mm\:ss')

# Threshold pass/fail helpers (null-safe)
function Threshold-Status([bool]$pass) { if ($pass) { '<span class="ok">✓</span>' } else { '<span class="ng">✗</span>' } }

$th_processed   = Threshold-Status ($null -ne $parsedProcessed   -and $parsedProcessed   -eq $MessageCount)
$th_errors      = Threshold-Status ($success)
$th_classified  = Threshold-Status ($null -ne $parsedClassified  -and $parsedClassified  -ge $expectedClassified)
$th_accuracy    = Threshold-Status ($null -ne $parsedAccuracy    -and $parsedAccuracy    -ge 90.0)

# ---------------------------------------------------------------------------
# HTML-encode raw output
# ---------------------------------------------------------------------------
function ConvertTo-HtmlEncoded([string] $s) {
    $s -replace '&', '&amp;' -replace '<', '&lt;' -replace '>', '&gt;' -replace '"', '&quot;'
}

$rawOutputHtml = ($outputLines | ForEach-Object { ConvertTo-HtmlEncoded $_ }) -join "`n"

# ---------------------------------------------------------------------------
# Generate HTML report
# ---------------------------------------------------------------------------
$html = @"
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>EventReader Stress — Durable Preload — $runDate</title>
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', sans-serif;
           background: #f0f2f5; color: #1a1a2e; font-size: 14px; line-height: 1.6; }
    a    { color: #4a6cf7; }

    /* ---- header ---- */
    header { background: linear-gradient(135deg, #1a1a2e 0%, #16213e 60%, #0f3460 100%);
             color: #fff; padding: 32px 40px 28px; display: flex; align-items: flex-start;
             justify-content: space-between; flex-wrap: wrap; gap: 16px; }
    header .title-block h1  { font-size: 11px; letter-spacing: 3px; text-transform: uppercase;
                               color: #8b9dc3; margin-bottom: 4px; }
    header .title-block h2  { font-size: 22px; font-weight: 700; color: #e8eaf6; }
    header .title-block p   { font-size: 12px; color: #8b9dc3; margin-top: 6px; }
    .badge { padding: 8px 20px; border-radius: 24px; font-size: 15px; font-weight: 700;
             letter-spacing: 1px; align-self: flex-start; margin-top: 4px; }
    .badge.pass { background: #28a745; color: #fff; }
    .badge.fail { background: #dc3545; color: #fff; }

    /* ---- main layout ---- */
    main { max-width: 1100px; margin: 0 auto; padding: 32px 24px; }

    /* ---- metric cards ---- */
    .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
             gap: 16px; margin-bottom: 32px; }
    .card  { background: #fff; border-radius: 10px; padding: 20px 18px;
             box-shadow: 0 1px 4px rgba(0,0,0,.08); text-align: center; }
    .card .label  { font-size: 11px; text-transform: uppercase; letter-spacing: 1px;
                    color: #888; margin-bottom: 8px; }
    .card .value  { font-size: 26px; font-weight: 700; color: #1a1a2e; }
    .card .sub    { font-size: 11px; color: #aaa; margin-top: 4px; }
    .card.highlight .value { color: #4a6cf7; }

    /* ---- two-column section ---- */
    .two-col { display: grid; grid-template-columns: 1fr 1fr; gap: 24px; margin-bottom: 32px; }
    @media (max-width: 700px) { .two-col { grid-template-columns: 1fr; } }

    /* ---- panels ---- */
    .panel { background: #fff; border-radius: 10px; padding: 24px;
             box-shadow: 0 1px 4px rgba(0,0,0,.08); }
    .panel h3 { font-size: 13px; text-transform: uppercase; letter-spacing: 1px;
                color: #4a6cf7; border-bottom: 2px solid #eef0ff; padding-bottom: 10px;
                margin-bottom: 16px; }

    /* ---- tables ---- */
    table  { width: 100%; border-collapse: collapse; font-size: 13px; }
    th, td { padding: 8px 10px; text-align: left; }
    thead th { background: #f7f8ff; color: #555; font-weight: 600; font-size: 11px;
               text-transform: uppercase; letter-spacing: .6px; }
    tbody tr:nth-child(even) { background: #fafbff; }
    tbody tr:hover { background: #f0f4ff; }
    td.key  { color: #666; white-space: nowrap; }
    td.val  { font-weight: 600; font-family: 'Consolas', monospace; }
    .ok { color: #28a745; font-weight: 700; }
    .ng { color: #dc3545; font-weight: 700; }

    /* ---- explanation ---- */
    .explanation { background: #fff; border-radius: 10px; padding: 28px 32px;
                   box-shadow: 0 1px 4px rgba(0,0,0,.08); margin-bottom: 32px; }
    .explanation h3 { font-size: 13px; text-transform: uppercase; letter-spacing: 1px;
                      color: #4a6cf7; border-bottom: 2px solid #eef0ff; padding-bottom: 10px;
                      margin-bottom: 18px; }
    .explanation h4 { font-size: 13px; font-weight: 700; color: #1a1a2e; margin: 18px 0 6px; }
    .explanation p  { color: #444; margin-bottom: 10px; }
    .explanation ul { color: #444; padding-left: 22px; margin-bottom: 10px; }
    .explanation li { margin-bottom: 4px; }
    .explanation .pipeline-step { display: flex; gap: 12px; align-items: flex-start;
                                  margin: 8px 0; }
    .explanation .step-num { background: #4a6cf7; color: #fff; border-radius: 50%;
                              width: 22px; height: 22px; font-size: 11px; font-weight: 700;
                              flex-shrink: 0; display: flex; align-items: center;
                              justify-content: center; margin-top: 2px; }
    .explanation .step-body strong { display: block; color: #1a1a2e; }

    /* ---- raw output ---- */
    details { background: #fff; border-radius: 10px; padding: 0;
              box-shadow: 0 1px 4px rgba(0,0,0,.08); overflow: hidden; margin-bottom: 24px; }
    details summary { padding: 16px 24px; cursor: pointer; font-weight: 600; font-size: 13px;
                      color: #555; background: #f7f8ff; user-select: none; }
    details summary:hover { background: #eef0ff; }
    details[open] summary { border-bottom: 1px solid #eee; }
    pre { background: #1a1a2e; color: #c9d1d9; font-size: 11.5px; padding: 20px 24px;
          overflow-x: auto; white-space: pre-wrap; word-break: break-all;
          max-height: 500px; overflow-y: auto; margin: 0; }

    footer { text-align: center; color: #aaa; font-size: 11px; padding: 20px 0 32px; }
  </style>
</head>
<body>

<header>
  <div class="title-block">
    <h1>EventReader · Stress Test</h1>
    <h2>Durable Preload — Full Classification Pipeline</h2>
    <p>Run: $runDate &nbsp;|&nbsp; Configuration: $Configuration &nbsp;|&nbsp; Wall time: $wallDurationDisplay</p>
  </div>
  <div class="badge $passLabel.ToLower()">$passLabel</div>
</header>

<main>

  <!-- ===== Metric Cards ===== -->
  <section class="cards">
    <div class="card highlight">
      <div class="label">Processing Rate</div>
      <div class="value">$rateDisplay</div>
      <div class="sub">sustained throughput</div>
    </div>
    <div class="card">
      <div class="label">Records Processed</div>
      <div class="value">$processedDisplay</div>
    </div>
    <div class="card">
      <div class="label">Pipeline Duration</div>
      <div class="value">$durationDisplay</div>
      <div class="sub">Kafka produce excluded</div>
    </div>
    <div class="card">
      <div class="label">Classified</div>
      <div class="value">$classifiedDisplay</div>
      <div class="sub">$accuracyDisplay accuracy</div>
    </div>
    <div class="card">
      <div class="label">Unclassified</div>
      <div class="value">$unclassifiedDisplay</div>
      <div class="sub">expected: $($expectedUnclassified.ToString('N0'))</div>
    </div>
    <div class="card">
      <div class="label">Errors</div>
      <div class="value">$(if ($success) { "0" } else { "See log" })</div>
    </div>
  </section>

  <!-- ===== Settings + Thresholds ===== -->
  <div class="two-col">
    <div class="panel">
      <h3>Test Settings</h3>
      <table>
        <thead><tr><th>Setting</th><th>Value</th></tr></thead>
        <tbody>
          <tr><td class="key">Message Count</td>          <td class="val">$($MessageCount.ToString('N0'))</td></tr>
          <tr><td class="key">Expected Classified (90%)</td><td class="val">$($expectedClassified.ToString('N0'))</td></tr>
          <tr><td class="key">Expected Unclassified (10%)</td><td class="val">$($expectedUnclassified.ToString('N0'))</td></tr>
          <tr><td class="key">Kafka Batch Size</td>         <td class="val">5,000</td></tr>
          <tr><td class="key">Kafka Batch Wait</td>         <td class="val">200 ms</td></tr>
          <tr><td class="key">Consumer Count</td>           <td class="val">1</td></tr>
          <tr><td class="key">Shard Count</td>              <td class="val">1,024</td></tr>
          <tr><td class="key">FASTER Checkpoint Interval</td><td class="val">disabled (0 ms)</td></tr>
          <tr><td class="key">Build Configuration</td>      <td class="val">$Configuration</td></tr>
          <tr><td class="key">Test Timeout</td>             <td class="val">10 minutes</td></tr>
        </tbody>
      </table>
    </div>

    <div class="panel">
      <h3>Pass Thresholds</h3>
      <table>
        <thead><tr><th>Condition</th><th>Threshold</th><th>Actual</th><th>OK</th></tr></thead>
        <tbody>
          <tr>
            <td class="key">All records processed</td>
            <td>= $($MessageCount.ToString('N0'))</td>
            <td class="val">$processedDisplay</td>
            <td>$th_processed</td>
          </tr>
          <tr>
            <td class="key">Zero processing errors</td>
            <td>= 0</td>
            <td class="val">$(if ($success) { "0" } else { "errors" })</td>
            <td>$th_errors</td>
          </tr>
          <tr>
            <td class="key">Classification accuracy</td>
            <td>≥ 90.00%</td>
            <td class="val">$accuracyDisplay</td>
            <td>$th_accuracy</td>
          </tr>
          <tr>
            <td class="key">Classified messages</td>
            <td>≥ $($expectedClassified.ToString('N0'))</td>
            <td class="val">$classifiedDisplay</td>
            <td>$th_classified</td>
          </tr>
          <tr>
            <td class="key">FASTER store integrity</td>
            <td>work-item count = classified</td>
            <td class="val">(asserted in test)</td>
            <td>$th_errors</td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>

  <!-- ===== Detailed Explanation ===== -->
  <section class="explanation">
    <h3>What This Test Validates — And Why</h3>

    <h4>The Core Question</h4>
    <p>
      Can the EventReader pipeline sustain high-throughput Kafka consumption while simultaneously
      performing full JSON parsing, client identity resolution, shard assignment, event classification,
      and durable state writes — all without dropping a single record or misclassifying a record
      that should match?
    </p>

    <h4>Why Pre-Load Kafka?</h4>
    <p>
      Rather than producing and consuming messages concurrently (which would measure the slower of
      the two), we first pre-load <strong>all $($MessageCount.ToString('N0')) messages</strong> into the
      Kafka topic before the consumer starts. This eliminates producer throughput as a variable and
      gives us a clean measurement of how fast the <em>consumer pipeline</em> alone can process work.
      Think of it as filling a bucket to the brim, then measuring how quickly it drains.
    </p>

    <h4>What the Pipeline Does for Each Record</h4>
    <p>Every message passes through all six stages of the EventReader durable pipeline:</p>

    <div class="pipeline-step">
      <div class="step-num">1</div>
      <div class="step-body">
        <strong>Kafka Batch Consumption</strong>
        Records are pulled in batches of up to 5,000 from a local embedded Kafka cluster.
        The consumer uses manual offset commit, so no record is acknowledged until it has
        been fully processed and durably written.
      </div>
    </div>
    <div class="pipeline-step">
      <div class="step-num">2</div>
      <div class="step-body">
        <strong>JSON Field Scanning (JsonFieldScanner)</strong>
        Two fields are extracted from each raw JSON payload: the discriminator path
        (<code>$.Action</code>) used for classification, and the client identity path
        (<code>$.NedbankID</code>) used for identity resolution. The scanner handles
        any field ordering — 50% of classifiable test messages have the fields in
        reverse order to exercise this.
      </div>
    </div>
    <div class="pipeline-step">
      <div class="step-num">3</div>
      <div class="step-body">
        <strong>Client Identity Resolution (ClientIdentityResolver)</strong>
        The extracted <code>NedbankID</code> value is resolved to a canonical long
        identity. In this stress test a no-op resolver is used (no database round-trips),
        which isolates pipeline throughput from external I/O latency.
      </div>
    </div>
    <div class="pipeline-step">
      <div class="step-num">4</div>
      <div class="step-body">
        <strong>Shard Assignment (ShardAssigner)</strong>
        Each record is assigned to one of 1,024 virtual shards based on its identity.
        Sharding is used by downstream components for parallelism and ordering guarantees.
      </div>
    </div>
    <div class="pipeline-step">
      <div class="step-num">5</div>
      <div class="step-body">
        <strong>Profile Classification (EventReaderRuntimeModel)</strong>
        The <code>$.Action</code> value is matched against the compiled function matcher
        index. Messages with <code>Action=payment.created</code> (90% of the load) are
        classified to the <em>PaymentCreated</em> function plan. Messages with
        <code>Action=payment.unknown</code> (10% of the load) intentionally do not match
        any rule and are recorded as unclassified. This lets the test verify both the
        happy path and the unclassified branch without injecting synthetic errors.
      </div>
    </div>
    <div class="pipeline-step">
      <div class="step-num">6</div>
      <div class="step-body">
        <strong>Durable Work Store Write (FasterEventReaderWorkStore)</strong>
        Each classified record is written to a FASTER (Microsoft.FASTER) embedded
        key–value store with state <code>WorkState.Classified</code>. At the end of the
        run the test asserts that the number of entries in the store exactly equals the
        number of classified messages, verifying no silent data loss.
      </div>
    </div>

    <h4>Data Mix Design</h4>
    <p>
      90% of messages carry <code>Action=&quot;payment.created&quot;</code> and should be classified.
      The remaining 10% carry <code>Action=&quot;payment.unknown&quot;</code> and should be left
      unclassified. This split is intentional: it tests that the classifier correctly counts
      <em>both</em> branches, and the final assertion checks that
      <code>classified + unclassified == total</code> with no records silently discarded.
    </p>

    <h4>What &quot;Durable&quot; Means Here</h4>
    <p>
      The FASTER store is an embedded append-log + hash-index. Each classified message gets
      a monotonically increasing work-item ID written to the store. The test then reads back
      <code>LastWorkItemId</code> and <code>BacklogByState[Classified]</code> from the store
      metrics and asserts both equal the classified message count. This confirms that every
      classification result survived the write path — nothing was counted in memory and dropped
      before hitting the store.
    </p>

    <h4>What the Throughput Number Tells You</h4>
    <p>
      The rate reported ($rateDisplay) covers the full pipeline from Kafka batch poll through
      FASTER store write — it is end-to-end application throughput, not a micro-benchmark of
      any single component. A drop in this number across releases indicates a regression in
      one of the six stages above. The wall-clock producer time is excluded because producing
      into a local Kafka is a one-time setup cost, not steady-state work.
    </p>

    <h4>Infrastructure Used</h4>
    <ul>
      <li><strong>Kafka</strong>: in-process test cluster fixture (started once per test collection run)</li>
      <li><strong>FASTER store</strong>: ephemeral directory under <code>%TEMP%</code>, deleted after each run</li>
      <li><strong>No database</strong>: identity lookups are no-ops; this is a pure pipeline throughput test</li>
    </ul>
  </section>

  <!-- ===== Raw Output ===== -->
  <details>
    <summary>Raw Test Output (click to expand)</summary>
    <pre>$rawOutputHtml</pre>
  </details>

</main>

<footer>EventReader Stress Report · Generated $runDate · KoreForge</footer>
</body>
</html>
"@

Set-Content -Path $reportPath -Value $html -Encoding UTF8

# ---------------------------------------------------------------------------
# Final console summary
# ---------------------------------------------------------------------------
Write-Host ""
Write-Host "Stress report: $reportPath" -ForegroundColor Cyan
Write-Host ""

if ($success) {
    Write-Host "✓  Stress test PASSED" -ForegroundColor Green
    Write-Host "   Rate         : $rateDisplay"        -ForegroundColor Green
    Write-Host "   Processed    : $processedDisplay"    -ForegroundColor Green
    Write-Host "   Duration     : $durationDisplay"     -ForegroundColor Green
    Write-Host "   Classified   : $classifiedDisplay ($accuracyDisplay)" -ForegroundColor Green
    Write-Host "   Unclassified : $unclassifiedDisplay" -ForegroundColor Green
}
else {
    Write-Host "✗  Stress test FAILED (exit code $exitCode)" -ForegroundColor Red
    Write-Host "   See report for details: $reportPath" -ForegroundColor Yellow
}

exit $exitCode
