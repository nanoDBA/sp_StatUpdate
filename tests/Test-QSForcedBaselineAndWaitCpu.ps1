<#
.SYNOPSIS
    QS forced-plan baseline/delta + WAIT_CPU metric acceptance tests
    (zks1 / 6aa2 -- gh-533, gh-534).

.DESCRIPTION
    Deterministic checks against a SQL 2025 container (default target
    127.0.0.1,1431).  Verifies:
      1. @QueryStore = WAIT_CPU is accepted in a dry run.
      2. An invalid metric value is still rejected (regression: adding
         WAIT_CPU to the allowed list did not weaken the guard).
      3. The WAIT_CPU run's SP_STATUPDATE_START ExtendedInfo XML logs
         the metric so downstream diag/analysis can pivot on it.
      4. A dry run with no forced plans still completes cleanly -- the
         new #qs_forced_baseline capture does not error on empty DMVs.
      5. With @Debug = 1 and QS enabled the debug output contains the
         baseline-capture note ("QS forced-plan baseline: capturing ...").
      6. @QueryStore = WAITS is still accepted (regression: the metric
         list swap did not accidentally drop the WAITS case).

    The script is READ-ONLY on the target: all sp_StatUpdate calls use
    @Execute = N'N' (except test 3 which uses @Execute = N'Y' only so the
    START row lands in CommandLog and we can query the XML), and it
    tolerates missing test databases by falling back to master.

    Uses ADO.NET SqlConnection + InfoMessage capture (same pattern as
    Test-QSEnrichmentSafety.ps1 / Test-ForcedPlanCheckPerf.ps1).

.PARAMETER Server
    Remote Docker host IP. Default: 100.88.59.72.

.PARAMETER Port
    SQL Server port. Default: 1431 (SQL 2025 on remote host).

.PARAMETER Password
    SA password. Default: $env:SPSTATUPDATE_TEST_SQL_PASSWORD.

.PARAMETER Database
    Test database name. Default: StatUpdateTest. Falls back to master if
    the test database does not exist.

.EXAMPLE
    .\tests\Test-QSForcedBaselineAndWaitCpu.ps1
    .\tests\Test-QSForcedBaselineAndWaitCpu.ps1 -Server 127.0.0.1 -Port 1433
#>
[CmdletBinding()]
param(
    [string]$Server   = "127.0.0.1",
    [int]   $Port     = 1431,
    [string]$Password = $(if ($env:SPSTATUPDATE_TEST_SQL_PASSWORD) { $env:SPSTATUPDATE_TEST_SQL_PASSWORD } else { '' }),
    [string]$Database = "StatUpdateTest",
    [string]$ResultsPath = ""
)

$ErrorActionPreference = "Continue"
$script:RunStartedAt = Get-Date
$script:LastCheckStartedAt = $script:RunStartedAt
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $ScriptDir

# =====================================================================
# ADO.NET HELPER (matches Test-QSEnrichmentSafety.ps1 conventions)
# =====================================================================
function Invoke-Sql {
    param(
        [string]$ServerAddr,
        [int]   $Port,
        [string]$Pwd,
        [string]$Query,
        [string]$Db      = "master",
        [int]   $Timeout = 300
    )
    $connStr = "Server=$ServerAddr,$Port;Database=$Db;User Id=sa;Password=$Pwd;Encrypt=false;TrustServerCertificate=true;Pooling=false;Connection Timeout=30"
    $messages = [System.Collections.ArrayList]::new()

    try {
        $conn = [System.Data.SqlClient.SqlConnection]::new($connStr)
        $conn.add_InfoMessage({
            param($sender, $e)
            $null = $messages.Add($e.Message)
        })
        $conn.FireInfoMessageEventOnUserErrors = $true
        $conn.Open()

        $cmd = $conn.CreateCommand()
        $cmd.CommandText = $Query
        $cmd.CommandTimeout = $Timeout

        $adapter = [System.Data.SqlClient.SqlDataAdapter]::new($cmd)
        $ds = [System.Data.DataSet]::new()
        $null = $adapter.Fill($ds)

        $conn.Close()

        return @{
            Output   = $messages
            DataSet  = $ds
            ExitCode = 0
        }
    }
    catch {
        $errBits = @()
        if ($_.Exception.InnerException) { $errBits += $_.Exception.InnerException.Message }
        $errBits += $_.Exception.Message
        $errBits += $messages
        return @{
            Output   = $errBits
            DataSet  = $null
            ExitCode = 1
        }
    }
}

$results = [System.Collections.ArrayList]::new()

function Add-Result {
    param([string]$Test, [string]$Result, [string]$Detail = "")
    $recordedAt = Get-Date
    $durationMs = [long][Math]::Max(0, ($recordedAt - $script:LastCheckStartedAt).TotalMilliseconds)
    $script:LastCheckStartedAt = $recordedAt
    $null = $results.Add([PSCustomObject]@{ Test = $Test; Result = $Result; Detail = $Detail; DurationMs = $durationMs })
    $color = switch ($Result) { "PASS" { "Green" } "FAIL" { "Red" } "SKIP" { "DarkGray" } default { "Gray" } }
    $d = if ($Detail) { " ($Detail)" } else { "" }
    Write-Host "  [$Result] $Test$d" -ForegroundColor $color
}

function Write-StructuredResults {
    if (-not $ResultsPath) { return }
    if ($results.Count -ne 6) { throw "QSForcedBaselineAndWaitCpu expected six checks; got $($results.Count)." }
    $version = switch ($serverMajor) { 15 { "2019" } 16 { "2022" } 17 { "2025" } default { throw "Unsupported SQL major version $serverMajor." } }
    $alive = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "SELECT CONVERT(nvarchar(64), SERVERPROPERTY(N'ProductVersion')) AS ProductVersion;" -Timeout 10
    $serverAliveAtEnd = $alive.ExitCode -eq 0 -and $alive.DataSet.Tables.Count -gt 0
    $sqlBuild = if ($serverAliveAtEnd) { [string]$alive.DataSet.Tables[0].Rows[0]["ProductVersion"] } else { "unavailable" }
    $checks = @($results | ForEach-Object {
        $status = switch ($_.Result) {
            "PASS" { "PASS" }
            "FAIL" { "FAIL" }
            "NOT_APPLICABLE" { "NOT_APPLICABLE" }
            default { throw "Unexpected legacy result '$($_.Result)' for $($_.Test)." }
        }
        $check = [ordered]@{ check = [string]$_.Test; result = $status; duration_ms = [long]$_.DurationMs }
        if ($status -ne "PASS") { $check.reason = [string]$_.Detail }
        $check
    })
    $commit = (& git -C $RepoRoot rev-parse HEAD).Trim()
    if ($LASTEXITCODE -ne 0 -or $commit -notmatch '^[0-9a-fA-F]{40}$') { throw "Could not resolve the tested source commit." }
    Import-Module (Join-Path $ScriptDir "ReleaseResults.psm1") -Force
    $endedAt = Get-Date
    [void](Write-ReleaseRunList -Path $ResultsPath -Commit $commit -SqlVersion $version -SqlBuild $sqlBuild `
        -ServerAliveAtEnd $serverAliveAtEnd -Suite "QSForcedBaselineAndWaitCpu" -ExpectedCount 6 -Checks $checks `
        -StartedAt $script:RunStartedAt -EndedAt $endedAt -DurationMs ([long](($endedAt - $script:RunStartedAt).TotalMilliseconds)))
}

# =====================================================================
# HEADER
# =====================================================================
Write-Host ""
Write-Host ("=" * 72) -ForegroundColor Cyan
Write-Host " QS Forced-Plan Baseline/Delta + WAIT_CPU (zks1 / 6aa2)" -ForegroundColor Cyan
Write-Host ("=" * 72) -ForegroundColor Cyan
Write-Host ""
Write-Host "  Server:   ${Server}:${Port}"
Write-Host "  Database: $Database (falls back to master if missing)"
Write-Host ""

# =====================================================================
# CONNECTIVITY + VERSION CHECK
# =====================================================================
Write-Host "[Setup] Verifying connectivity..." -ForegroundColor Yellow
$vr = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "SELECT CONVERT(int, SERVERPROPERTY(N'ProductMajorVersion')) AS MajorVer, CONVERT(nvarchar(64), SERVERPROPERTY(N'ProductVersion')) AS ProductVersion;"
if ($vr.ExitCode -ne 0) {
    Write-Host "  ERROR: Cannot connect to ${Server}:${Port}" -ForegroundColor Red
    Write-Host "  " + ($vr.Output -join "; ") -ForegroundColor Red
    exit 1
}
$serverMajor = [int]$vr.DataSet.Tables[0].Rows[0]["MajorVer"]
Write-Host "  Connected (SQL major version: $serverMajor)" -ForegroundColor Green
Write-Host ""

if ($serverMajor -in @(15, 16)) {
    $supportedVersion = if ($serverMajor -eq 15) { "2019" } else { "2022" }
    foreach ($check in @(
        "T1: WAIT_CPU accepted (dry run)",
        "T2: Invalid metric rejected",
        "T3: WAIT_CPU logged in START XML",
        "T4: Dry run completes with no baseline errors",
        "T5: Debug baseline-capture note emitted",
        "T6: WAITS still accepted (regression)"
    )) {
        Add-Result $check "NOT_APPLICABLE" "Requires SQL Server 2025; tested engine version is $supportedVersion."
    }
    Write-StructuredResults
    exit 0
}
if ($serverMajor -ne 17) {
    throw "Unsupported SQL major version $serverMajor; expected 15, 16, or 17."
}

# Verify the test database exists; fall back to master otherwise.
$dbExists = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "SELECT COUNT(*) AS n FROM sys.databases WHERE name = N'$Database';"
if ($dbExists.ExitCode -ne 0 -or [int]$dbExists.DataSet.Tables[0].Rows[0]["n"] -eq 0) {
    Write-Host "  Note: $Database not found -- falling back to master." -ForegroundColor Yellow
    $Database = "master"
}

# Best-effort: enable QS on the target so debug baseline note fires.  Silent on failure.
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "BEGIN TRY ALTER DATABASE [$Database] SET QUERY_STORE = ON; END TRY BEGIN CATCH END CATCH;" | Out-Null

# Clear CommandLog rows we will inspect (only START/END from prior runs).
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
    IF OBJECT_ID(N'master.dbo.CommandLog') IS NOT NULL
        DELETE FROM master.dbo.CommandLog
        WHERE CommandType IN (N'SP_STATUPDATE_START', N'SP_STATUPDATE_END');
" | Out-Null

Write-Host ""
Write-Host "[Tests] Running WAIT_CPU + forced-plan baseline checks..." -ForegroundColor Yellow
Write-Host ""

# =====================================================================
# TEST 1: @QueryStore = WAIT_CPU accepted in a dry run.
#         SQL 2017+ only (same gate as WAITS).
# =====================================================================
if ($serverMajor -ge 14) {
    $r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Timeout 120 -Query "
        EXEC dbo.sp_StatUpdate
            @Databases = N'$Database',
            @QueryStore = N'WAIT_CPU',
            @QueryStoreTopPlans = 5,
            @ModificationThreshold = 100,
            @Execute = N'N';
    "
    $out = ($r.Output -join "`n")
    if ($r.ExitCode -eq 0) {
        Add-Result "T1: WAIT_CPU accepted (dry run)" "PASS" ""
    }
    elseif ($out -match "Invalid @QueryStore metric") {
        Add-Result "T1: WAIT_CPU accepted (dry run)" "FAIL" "WAIT_CPU rejected by validation"
    }
    else {
        # Non-validation error -- most likely proc-body issue or WAIT_CPU flow bug.
        $errMsg = ($r.Output | Select-Object -First 1)
        Add-Result "T1: WAIT_CPU accepted (dry run)" "FAIL" "Execution error: $errMsg"
    }
}
else {
    Add-Result "T1: WAIT_CPU accepted (dry run)" "SKIP" "SQL $serverMajor < 2017"
}

# =====================================================================
# TEST 2: Invalid metric value still rejected.
#         Regression guard: adding WAIT_CPU must not weaken the check.
# =====================================================================
$r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Timeout 60 -Query "
    EXEC dbo.sp_StatUpdate
        @Databases = N'$Database',
        @QueryStore = N'BOGUS_METRIC',
        @Execute = N'N';
"
$out = ($r.Output -join "`n")
# The harness uses FireInfoMessageEventOnUserErrors, so the proc's severity-16
# validation RAISERROR arrives as a captured MESSAGE, not an exception -- assert
# on the message text (verified 2026-07-02: a non-lenient direct call IS rejected).
if ($out -match "Invalid @QueryStore metric") {
    Add-Result "T2: Invalid metric rejected" "PASS" "Validation message observed"
}
else {
    Add-Result "T2: Invalid metric rejected" "FAIL" "No 'Invalid @QueryStore metric' validation message in output"
}

# =====================================================================
# TEST 3: WAIT_CPU run's START row logs the metric in ExtendedInfo XML.
#         Requires @Execute = 'Y' so the START row lands in CommandLog.
# =====================================================================
if ($serverMajor -ge 14) {
    # Fire a real (very short) run; @ModificationThreshold high enough that
    # we don't force any actual UPDATE STATISTICS -- the START row still lands.
    $r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Timeout 180 -Query "
        EXEC dbo.sp_StatUpdate
            @Databases = N'$Database',
            @QueryStore = N'WAIT_CPU',
            @QueryStoreTopPlans = 5,
            @ModificationThreshold = 9999999999,
            @Execute = N'Y',
            @LogToTable = N'Y';
    "
    if ($r.ExitCode -ne 0) {
        $errMsg = ($r.Output | Select-Object -First 1)
        Add-Result "T3: WAIT_CPU logged in START XML" "FAIL" "Execution error: $errMsg"
    }
    else {
        $r2 = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
            SELECT TOP 1
                Metric = ExtendedInfo.value('(/Parameters/QueryStoreMetric)[1]', 'nvarchar(20)'),
                Priority = ExtendedInfo.value('(/Parameters/QueryStorePriority)[1]', 'nvarchar(1)')
            FROM master.dbo.CommandLog
            WHERE CommandType = N'SP_STATUPDATE_START'
            ORDER BY ID DESC;
        "
        if ($r2.ExitCode -eq 0 -and $r2.DataSet -and $r2.DataSet.Tables.Count -gt 0 -and $r2.DataSet.Tables[0].Rows.Count -gt 0) {
            $metric = "$($r2.DataSet.Tables[0].Rows[0]['Metric'])"
            $prio   = "$($r2.DataSet.Tables[0].Rows[0]['Priority'])"
            if ($metric -eq "WAIT_CPU" -and $prio -eq "Y") {
                Add-Result "T3: WAIT_CPU logged in START XML" "PASS" "QueryStoreMetric=WAIT_CPU"
            }
            else {
                Add-Result "T3: WAIT_CPU logged in START XML" "FAIL" "Metric='$metric' Priority='$prio'"
            }
        }
        else {
            Add-Result "T3: WAIT_CPU logged in START XML" "FAIL" "No SP_STATUPDATE_START row in master.dbo.CommandLog"
        }
    }
}
else {
    Add-Result "T3: WAIT_CPU logged in START XML" "SKIP" "SQL $serverMajor < 2017"
}

# =====================================================================
# TEST 4: Dry run with no forced plans completes without baseline errors.
#         The #qs_forced_baseline capture must survive zero-forced-plan
#         DBs and disabled QS DBs without raising an error to the caller.
# =====================================================================
$r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Timeout 180 -Query "
    EXEC dbo.sp_StatUpdate
        @Databases = N'$Database',
        @QueryStore = N'CPU',
        @QueryStoreTopPlans = 5,
        @ModificationThreshold = 100,
        @Execute = N'N';
"
$out = ($r.Output -join "`n")
if ($r.ExitCode -ne 0) {
    $errMsg = ($r.Output | Select-Object -First 1)
    Add-Result "T4: Dry run completes with no baseline errors" "FAIL" "Execution error: $errMsg"
}
elseif ($out -match "Msg\s+\d+.*Level\s+1[6-9]|Level\s+2[0-5]") {
    Add-Result "T4: Dry run completes with no baseline errors" "FAIL" "High-severity message in output"
}
else {
    Add-Result "T4: Dry run completes with no baseline errors" "PASS" ""
}

# =====================================================================
# TEST 5: With @Debug = 1 and QS enabled, debug output contains the
#         baseline-capture note ("QS forced-plan baseline: capturing").
#         Requires @Execute = 'Y' (baseline is gated on @Execute = 'Y').
# =====================================================================
$r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Timeout 180 -Query "
    EXEC dbo.sp_StatUpdate
        @Databases = N'$Database',
        @QueryStore = N'CPU',
        @QueryStoreTopPlans = 5,
        @ModificationThreshold = 9999999999,
        @Execute = N'Y',
        @LogToTable = N'Y',
        @Debug = 1;
"
$out = ($r.Output -join "`n")
if ($r.ExitCode -ne 0) {
    $errMsg = ($r.Output | Select-Object -First 1)
    Add-Result "T5: Debug baseline-capture note emitted" "FAIL" "Execution error: $errMsg"
}
elseif ($out -match "QS forced-plan baseline: capturing") {
    Add-Result "T5: Debug baseline-capture note emitted" "PASS" ""
}
else {
    Add-Result "T5: Debug baseline-capture note emitted" "FAIL" "Note not found in debug output"
}

# =====================================================================
# TEST 6: @QueryStore = WAITS still accepted (regression).
#         Confirms the metric list swap did not drop the WAITS branch.
# =====================================================================
if ($serverMajor -ge 14) {
    $r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Timeout 120 -Query "
        EXEC dbo.sp_StatUpdate
            @Databases = N'$Database',
            @QueryStore = N'WAITS',
            @QueryStoreTopPlans = 5,
            @ModificationThreshold = 100,
            @Execute = N'N';
    "
    $out = ($r.Output -join "`n")
    if ($r.ExitCode -eq 0) {
        Add-Result "T6: WAITS still accepted (regression)" "PASS" ""
    }
    elseif ($out -match "Invalid @QueryStore metric") {
        Add-Result "T6: WAITS still accepted (regression)" "FAIL" "WAITS rejected by validation"
    }
    else {
        $errMsg = ($r.Output | Select-Object -First 1)
        Add-Result "T6: WAITS still accepted (regression)" "FAIL" "Execution error: $errMsg"
    }
}
else {
    Add-Result "T6: WAITS still accepted (regression)" "SKIP" "SQL $serverMajor < 2017"
}

# =====================================================================
# SUMMARY
# =====================================================================
Write-Host ""
Write-Host ("=" * 72) -ForegroundColor Cyan
Write-Host " SUMMARY" -ForegroundColor Cyan
Write-Host ("=" * 72) -ForegroundColor Cyan
Write-Host ""

$passed  = @($results | Where-Object { $_.Result -eq "PASS" }).Count
$failed  = @($results | Where-Object { $_.Result -eq "FAIL" }).Count
$skipped = @($results | Where-Object { $_.Result -eq "SKIP" }).Count
$total   = $passed + $failed + $skipped

$color = if ($failed -eq 0) { "Green" } else { "Red" }
Write-Host "  $passed/$total passed, $failed failed, $skipped skipped" -ForegroundColor $color
Write-Host ""

Write-StructuredResults

if ($failed -gt 0) {
    Write-Host "  FAILED:" -ForegroundColor Red
    foreach ($t in ($results | Where-Object { $_.Result -eq "FAIL" })) {
        Write-Host "    - $($t.Test): $($t.Detail)" -ForegroundColor Red
    }
    Write-Host ""
    exit 1
}

exit 0
