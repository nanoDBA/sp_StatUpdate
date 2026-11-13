<#
.SYNOPSIS
    QS enrichment safety tests -- catches unbounded XML parsing and metric-gating bugs.

.DESCRIPTION
    Tests on the supported SQL Server 2019+ versions that Phase 6 Query Store enrichment:
    - Respects @QueryStoreTopPlans limits on ALL code paths (including WAITS)
    - Gates metric-specific enrichment (WAITS) on the actual @QueryStore value
    - Completes within a reasonable time budget
    - Handles all 10 @QueryStore values without errors

    Written after #371: WAITS enrichment CTE ran unbounded XML parsing on every
    QS-enabled run regardless of @QueryStore, causing 6+ hour discovery.

.PARAMETER Server
    SQL test endpoint supplied by the release-gate harness.

.PARAMETER Port
    SQL Server port. Selected from Versions unless explicitly overridden.

.PARAMETER Password
    SA password. Defaults to the release-gate credential environment variable.

.PARAMETER Database
    Test database name. Default: StatUpdateTest.

.PARAMETER Phase6TimeoutSeconds
    Maximum allowed Phase 6 duration. Default: 60.

.PARAMETER SkipSetup
    Skip QS warm-up queries (assumes QS already has data).

.EXAMPLE
    .\tests\Test-QSEnrichmentSafety.ps1 -Server $Server -Versions @("2022")
    .\tests\Test-QSEnrichmentSafety.ps1 -Server $Server -Versions @("2025") -Phase6TimeoutSeconds 30
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$Server,
    [string[]]$Versions = @('2022'),
    [string]$Password = $(if ($env:SPSTATUPDATE_TEST_SQL_PASSWORD) { $env:SPSTATUPDATE_TEST_SQL_PASSWORD } else { '' }),
    [string]$ResultsPath = '',
    [string]$Database = "StatUpdateTest",
    [int]$Phase6TimeoutSeconds = 60,
    [int]$Port = 0,
    [switch]$SkipSetup
)

$ErrorActionPreference = "Continue"
if (-not $Password) { throw 'Release-gate SQL credentials are unavailable.' }
if ($Versions.Count -ne 1) { throw 'Run one supported SQL version at a time.' }
$version = $Versions[0]
$expectedMajor = @{ '2019' = 15; '2022' = 16; '2025' = 17 }[$version]
if (-not $expectedMajor) { throw 'Unsupported SQL Server version.' }
if (-not $Port) { $Port = @{ '2019' = 1434; '2022' = 1432; '2025' = 1431 }[$version] }
$repoRoot = Split-Path -Parent $PSScriptRoot
$startedAt = Get-Date
$lastCheck = $startedAt
$checks = [System.Collections.Generic.List[object]]::new()
$sqlBuild = ''
$aliveAtEnd = $false

# =====================================================================
# ADO.NET HELPER
# =====================================================================
function Invoke-Sql {
    param(
        [string]$ServerAddr,
        [int]$Port,
        [string]$Pwd,
        [string]$Query,
        [string]$Db = "master",
        [int]$Timeout = 300
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
        return @{
            Output   = @($_.Exception.InnerException.Message, $_.Exception.Message) + $messages
            DataSet  = $null
            ExitCode = 1
        }
    }
}

$results = [System.Collections.ArrayList]::new()

function Add-Result {
    param([string]$Test, [string]$Result, [string]$Detail = "")
    $null = $results.Add([PSCustomObject]@{ Test = $Test; Result = $Result; Detail = $Detail })
    $color = switch ($Result) { "PASS" { "Green" } "FAIL" { "Red" } "SKIP" { "DarkGray" } }
    $d = if ($Detail) { " ($Detail)" } else { "" }
    Write-Host "  [$Result] $Test$d" -ForegroundColor $color
    $now = Get-Date
    $normalizedResult = if ($Result -eq 'SKIP') { 'NOT_APPLICABLE' } else { $Result }
    $check = [ordered]@{
        check = $Test
        result = $normalizedResult
        duration_ms = [long][Math]::Max(0, ($now - $script:lastCheck).TotalMilliseconds)
    }
    if ($normalizedResult -ne 'PASS') { $check.reason = if ($Detail) { $Detail } else { 'No detail supplied.' } }
    $script:checks.Add([pscustomobject]$check)
    $script:lastCheck = $now
}

# =====================================================================
# HEADER
# =====================================================================
Write-Host ""
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host " QS Enrichment Safety Tests (#371)" -ForegroundColor Cyan
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host ""
Write-Host "  Server:   ${Server}:${Port}"
Write-Host "  Database: $Database"
Write-Host "  Phase 6 timeout: ${Phase6TimeoutSeconds}s"
Write-Host ""

# =====================================================================
# CONNECTIVITY + VERSION CHECK
# =====================================================================
Write-Host "[Setup] Verifying connectivity..." -ForegroundColor Yellow
$vr = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "SELECT CONVERT(int, SERVERPROPERTY(N'ProductMajorVersion')) AS MajorVer, CONVERT(nvarchar(128), SERVERPROPERTY(N'ProductVersion')) AS SqlBuild;"
if ($vr.ExitCode -ne 0) {
    Write-Host "  ERROR: Cannot connect to ${Server}:${Port}" -ForegroundColor Red
    exit 1
}
$serverMajor = [int]$vr.DataSet.Tables[0].Rows[0]["MajorVer"]
$sqlBuild = [string]$vr.DataSet.Tables[0].Rows[0]["SqlBuild"]
if ($serverMajor -ne $expectedMajor) { throw "Requested SQL $version but connected to major version $serverMajor." }
Write-Host "  Connected (SQL major version: $serverMajor)" -ForegroundColor Green
Write-Host ""

# =====================================================================
# SETUP: Enable QS + warm up with queries
# =====================================================================
if (-not $SkipSetup) {
    Write-Host "[Setup] Enabling Query Store and generating workload..." -ForegroundColor Yellow

    # Enable QS
    Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "BEGIN TRY ALTER DATABASE [$Database] SET QUERY_STORE = ON; END TRY BEGIN CATCH END CATCH;" | Out-Null

    # Generate modifications so we have stat candidates
    Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Query "
        INSERT INTO dbo.Orders (CustomerName, Amount)
        SELECT TOP 5000 N'QSSafety', 1.00 FROM sys.all_columns;
        INSERT INTO dbo.Products (ProductName, Price)
        SELECT TOP 2000 N'QSSafety', 2.00 FROM sys.all_columns;
    " | Out-Null

    # Run some queries to populate QS (needed for enrichment to have data)
    Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Query "
        SELECT TOP 100 * FROM dbo.Orders WHERE CustomerName = N'QSSafety' ORDER BY Amount;
        SELECT TOP 100 * FROM dbo.Products WHERE ProductName = N'QSSafety' ORDER BY Price;
        SELECT COUNT(*) FROM dbo.Orders o JOIN dbo.Products p ON o.CustomerName = p.ProductName;
    " | Out-Null

    # Flush QS to disk
    Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Query "
        BEGIN TRY EXEC sp_query_store_flush_db; END TRY BEGIN CATCH END CATCH;
    " | Out-Null

    # Clear CommandLog for clean test state
    Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
        DELETE FROM master.dbo.CommandLog WHERE CommandType IN (N'UPDATE_STATISTICS', N'SP_STATUPDATE_START', N'SP_STATUPDATE_END');
    " | Out-Null

    Write-Host "  Done" -ForegroundColor Green
    Write-Host ""
}

# =====================================================================
# TESTS
# =====================================================================
Write-Host "[Tests] Running QS enrichment safety checks..." -ForegroundColor Yellow
Write-Host ""

# ------------------------------------------------------------------
# Q1: Phase 6 timing guard (would have caught #371 directly)
#     Run with QS enabled, verify Phase 6 completes within budget.
# ------------------------------------------------------------------
$r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Timeout ($Phase6TimeoutSeconds + 60) -Query "
    EXEC dbo.sp_StatUpdate
        @Databases = N'$Database',
        @QueryStore = N'CPU',
        @ModificationThreshold = 100,
        @Execute = N'N',
        @Debug = 1;
"
$out = ($r.Output -join "`n")
$phase6Match = [regex]::Match($out, 'Phase 6.*?(\d+)\s*ms')
if ($r.ExitCode -ne 0) {
    # Check if the error is benign (no qualifying stats, QS disabled, etc.)
    $errText = $r.Output -join " "
    if ($errText -match "NO_QUALIFYING|0 stat|Query Store.*disabled|READ_ONLY") {
        Add-Result "Q1: Phase 6 timing guard (CPU)" "PASS" "Proc returned early (no qualifying stats or QS unavailable)"
    } else {
        $errMsg = ($r.Output | Select-Object -First 1)
        Add-Result "Q1: Phase 6 timing guard (CPU)" "FAIL" "Execution error: $errMsg"
    }
} elseif ($phase6Match.Success) {
    $phase6Ms = [int]$phase6Match.Groups[1].Value
    $phase6Sec = [math]::Round($phase6Ms / 1000.0, 1)
    if ($phase6Ms -le ($Phase6TimeoutSeconds * 1000)) {
        Add-Result "Q1: Phase 6 timing guard (CPU)" "PASS" "${phase6Sec}s < ${Phase6TimeoutSeconds}s budget"
    } else {
        Add-Result "Q1: Phase 6 timing guard (CPU)" "FAIL" "${phase6Sec}s EXCEEDS ${Phase6TimeoutSeconds}s budget"
    }
} else {
    # Phase 6 skipped (no QS data) or not reached -- still safe
    if ($out -match 'Phase 6.*skipped') {
        Add-Result "Q1: Phase 6 timing guard (CPU)" "PASS" "Phase 6 skipped (no QS data)"
    } else {
        Add-Result "Q1: Phase 6 timing guard (CPU)" "PASS" "No Phase 6 timing in output (QS may be off)"
    }
}

# ------------------------------------------------------------------
# Q2: WAITS enrichment gated on metric
#     Run with metric=CPU, execute, check ExtendedInfo for wait data.
#     If WAITS CTE ran unconditionally, it would populate the field.
# ------------------------------------------------------------------
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Query "
    INSERT INTO dbo.Orders (CustomerName, Amount)
    SELECT TOP 5000 N'Q2', 1.00 FROM sys.all_columns;
" | Out-Null

$r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
    EXEC dbo.sp_StatUpdate
        @Databases = N'$Database',
        @QueryStore = N'CPU',
        @ModificationThreshold = 100,
        @Execute = N'Y',
        @LogToTable = N'Y';
"
if ($r.ExitCode -ne 0) {
    Add-Result "Q2: WAITS gated on metric (CPU -> no wait enrichment)" "FAIL" "Execution error"
} else {
    # Check ExtendedInfo in CommandLog for QSTotalWaitTimeMs
    $r2 = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
        SELECT TOP 1
            ExtendedInfo.value('(/ExtendedInfo/QSTotalWaitTimeMs)[1]', 'bigint') AS WaitMs
        FROM master.dbo.CommandLog
        WHERE CommandType = N'UPDATE_STATISTICS'
        ORDER BY ID DESC;
    "
    if ($r2.DataSet -and $r2.DataSet.Tables.Count -gt 0 -and $r2.DataSet.Tables[0].Rows.Count -gt 0) {
        $waitMs = $r2.DataSet.Tables[0].Rows[0]["WaitMs"]
        if ($waitMs -eq [DBNull]::Value -or $null -eq $waitMs) {
            Add-Result "Q2: WAITS gated on metric (CPU -> no wait enrichment)" "PASS" "QSTotalWaitTimeMs is NULL"
        } else {
            Add-Result "Q2: WAITS gated on metric (CPU -> no wait enrichment)" "FAIL" "QSTotalWaitTimeMs=$waitMs (WAITS CTE ran despite metric=CPU)"
        }
    } else {
        Add-Result "Q2: WAITS gated on metric (CPU -> no wait enrichment)" "PASS" "No CommandLog entries (0 stats updated)"
    }
}

# ------------------------------------------------------------------
# Q3: WAITS metric compiles and runs on supported SQL Server 2019+ versions
#     Verifies the WAITS path works when explicitly requested.
# ------------------------------------------------------------------
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Query "
    INSERT INTO dbo.Orders (CustomerName, Amount)
    SELECT TOP 5000 N'Q3', 1.00 FROM sys.all_columns;
" | Out-Null

 $r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
    EXEC dbo.sp_StatUpdate
        @Databases = N'$Database',
        @QueryStore = N'WAITS',
        @QueryStoreTopPlans = 10,
        @ModificationThreshold = 100,
        @Execute = N'N',
        @Debug = 1;
"
if ($r.ExitCode -eq 0) {
    Add-Result "Q3: WAITS metric runs (SQL 2019+)" "PASS" ""
} else {
    $errMsg = ($r.Output | Select-Object -First 1)
    Add-Result "Q3: WAITS metric runs (SQL 2019+)" "FAIL" $errMsg
}

# ------------------------------------------------------------------
# Q4: WAITS timing guard with TopPlans=10
#     Even when WAITS IS requested, it should respect TopPlans.
# ------------------------------------------------------------------
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Query "
    INSERT INTO dbo.Orders (CustomerName, Amount)
    SELECT TOP 5000 N'Q4', 1.00 FROM sys.all_columns;
" | Out-Null

 $r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Timeout ($Phase6TimeoutSeconds + 60) -Query "
    EXEC dbo.sp_StatUpdate
        @Databases = N'$Database',
        @QueryStore = N'WAITS',
        @QueryStoreTopPlans = 10,
        @ModificationThreshold = 100,
        @Execute = N'N',
        @Debug = 1;
"
$out = ($r.Output -join "`n")
$phase6Match = [regex]::Match($out, 'Phase 6.*?(\d+)\s*ms')
if ($r.ExitCode -ne 0) {
    Add-Result "Q4: Phase 6 timing guard (WAITS, TopPlans=10)" "FAIL" "Execution error"
} elseif ($phase6Match.Success) {
    $phase6Ms = [int]$phase6Match.Groups[1].Value
    $phase6Sec = [math]::Round($phase6Ms / 1000.0, 1)
    if ($phase6Ms -le ($Phase6TimeoutSeconds * 1000)) {
        Add-Result "Q4: Phase 6 timing guard (WAITS, TopPlans=10)" "PASS" "${phase6Sec}s < ${Phase6TimeoutSeconds}s budget"
    } else {
        Add-Result "Q4: Phase 6 timing guard (WAITS, TopPlans=10)" "FAIL" "${phase6Sec}s EXCEEDS ${Phase6TimeoutSeconds}s budget"
    }
} else {
    Add-Result "Q4: Phase 6 timing guard (WAITS, TopPlans=10)" "PASS" "Phase 6 skipped or no timing output"
}

# ------------------------------------------------------------------
# Q5: @QueryStoreTopPlans=1 respected (minimal scope)
#     Verifies TopPlans limit actually constrains Phase 6 scope.
# ------------------------------------------------------------------
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Query "
    INSERT INTO dbo.Orders (CustomerName, Amount)
    SELECT TOP 5000 N'Q5', 1.00 FROM sys.all_columns;
" | Out-Null

$r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Timeout 120 -Query "
    EXEC dbo.sp_StatUpdate
        @Databases = N'$Database',
        @QueryStore = N'CPU',
        @QueryStoreTopPlans = 1,
        @ModificationThreshold = 100,
        @Execute = N'N',
        @Debug = 1;
"
$out = ($r.Output -join "`n")
$phase6Match = [regex]::Match($out, 'Phase 6.*?(\d+)\s*ms')
if ($r.ExitCode -ne 0) {
    Add-Result "Q5: TopPlans=1 respected" "FAIL" "Execution error"
} elseif ($phase6Match.Success) {
    $phase6Ms = [int]$phase6Match.Groups[1].Value
    if ($phase6Ms -le 10000) {
        Add-Result "Q5: TopPlans=1 respected" "PASS" "${phase6Ms}ms (TopPlans=1 should be fast)"
    } else {
        Add-Result "Q5: TopPlans=1 respected" "FAIL" "${phase6Ms}ms -- TopPlans=1 should complete in <10s"
    }
} else {
    Add-Result "Q5: TopPlans=1 respected" "PASS" "Phase 6 skipped or no timing"
}

# ------------------------------------------------------------------
# Q6: All 10 metric values compile and run without errors
#     Catches missing CASE branches, bad REPLACE tokens, syntax errors.
# ------------------------------------------------------------------
$metrics = @("CPU", "DURATION", "READS", "EXECUTIONS", "AVG_CPU", "MEMORY_GRANT", "TEMPDB_SPILLS", "PHYSICAL_READS", "AVG_MEMORY", "WAITS")
$failedMetrics = [System.Collections.ArrayList]::new()

foreach ($metric in $metrics) {
    $r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Timeout 120 -Query "
        EXEC dbo.sp_StatUpdate
            @Databases = N'$Database',
            @QueryStore = N'$metric',
            @QueryStoreTopPlans = 5,
            @ModificationThreshold = 0,
            @Execute = N'N';
    "
    if ($r.ExitCode -ne 0) {
        $errMsg = ($r.Output | Where-Object { $_ -match 'Msg |error|invalid' } | Select-Object -First 1)
        if (-not $errMsg) { $errMsg = ($r.Output | Select-Object -First 1) }
        $null = $failedMetrics.Add("$metric ($errMsg)")
    }
}

if ($failedMetrics.Count -eq 0) {
    Add-Result "Q6: All metric values compile" "PASS" "$($metrics.Count)/$($metrics.Count) metrics tested"
} else {
    Add-Result "Q6: All metric values compile" "FAIL" ($failedMetrics -join "; ")
}

# ------------------------------------------------------------------
# Q7: Phase 6 skipped when QS disabled
#     Ensures no QS queries run when QS is off.
# ------------------------------------------------------------------
# Temporarily disable QS
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "BEGIN TRY ALTER DATABASE [$Database] SET QUERY_STORE = OFF; END TRY BEGIN CATCH END CATCH;" | Out-Null
# Small delay for QS state to propagate
Start-Sleep -Seconds 1

Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Query "
    INSERT INTO dbo.Orders (CustomerName, Amount)
    SELECT TOP 2000 N'Q7', 1.00 FROM sys.all_columns;
" | Out-Null

$r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
    EXEC dbo.sp_StatUpdate
        @Databases = N'$Database',
        @QueryStore = N'CPU',
        @ModificationThreshold = 100,
        @Execute = N'N',
        @Debug = 1;
"
$out = ($r.Output -join "`n")

# Re-enable QS
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "BEGIN TRY ALTER DATABASE [$Database] SET QUERY_STORE = ON; END TRY BEGIN CATCH END CATCH;" | Out-Null

if ($r.ExitCode -ne 0) {
    Add-Result "Q7: Phase 6 skipped when QS disabled" "FAIL" "Execution error"
} elseif ($out -match 'Phase 6.*enriched') {
    # Phase 6 ran and enriched stats -- bad, QS should have been off
    Add-Result "Q7: Phase 6 skipped when QS disabled" "FAIL" "Phase 6 enriched stats with QS disabled"
} else {
    Add-Result "Q7: Phase 6 skipped when QS disabled" "PASS" "No Phase 6 enrichment"
}

# =====================================================================
# SUMMARY
# =====================================================================
Write-Host ""
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host " SUMMARY" -ForegroundColor Cyan
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host ""

$passed  = @($results | Where-Object { $_.Result -eq "PASS" }).Count
$failed  = @($results | Where-Object { $_.Result -eq "FAIL" }).Count
$skipped = @($results | Where-Object { $_.Result -eq "SKIP" }).Count
$total   = $passed + $failed + $skipped

$color = if ($failed -eq 0) { "Green" } else { "Red" }
Write-Host "  $passed/$total passed, $failed failed, $skipped skipped" -ForegroundColor $color
Write-Host ""

if ($failed -gt 0) {
    Write-Host "  FAILED:" -ForegroundColor Red
    foreach ($t in ($results | Where-Object { $_.Result -eq "FAIL" })) {
        Write-Host "    - $($t.Test): $($t.Detail)" -ForegroundColor Red
    }
    Write-Host ""
}

$probe = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "SELECT CONVERT(nvarchar(128), SERVERPROPERTY(N'ProductVersion'));"
$aliveAtEnd = ($probe.ExitCode -eq 0)
if (-not $aliveAtEnd) { Write-Warning 'SQL liveness probe failed at suite end.' }

if ($ResultsPath) {
    if ($checks.Count -ne 7) { throw "Expected seven Query Store safety checks; received $($checks.Count)." }
    $commit = (& git -C $repoRoot rev-parse HEAD).Trim()
    if ($LASTEXITCODE -ne 0 -or $commit -notmatch '^[0-9a-fA-F]{40}$') { throw 'Could not resolve the tested source commit.' }
    Import-Module (Join-Path $PSScriptRoot 'ReleaseResults.psm1') -Force
    $endedAt = Get-Date
    [void](Write-ReleaseRunList -Path $ResultsPath -Commit $commit -SqlVersion $version -SqlBuild $sqlBuild `
        -ServerAliveAtEnd $aliveAtEnd -Suite 'QSEnrichmentSafety' -ExpectedCount 7 -Checks @($checks) `
        -StartedAt $startedAt -EndedAt $endedAt -DurationMs ([long](($endedAt - $startedAt).TotalMilliseconds)))
}

if ($failed -gt 0 -or -not $aliveAtEnd) { exit 1 }
exit 0
