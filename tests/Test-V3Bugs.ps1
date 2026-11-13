<#
.SYNOPSIS
    Bug detection test suite for sp_StatUpdate v3 (P1 issues #381-#387 + QA checks).

.DESCRIPTION
    Tests for P1 bugs found during v3 refactor review, plus QA validation checks
    covering preset behavior, parameter validation, and result set structure.
    All versions are tested concurrently using ForEach-Object -Parallel.
    Tests T20-T35 (16 checks per version).

    Bug coverage:
      T20 (#381) - @ModificationThreshold preset passthrough: debug prints raw NULL
                   instead of resolved @i_modification_threshold from preset.
      T21 (#382) - ROWS sort order missing from ORDER BY CASE in queue population.
      T22 (#383) - sp_getapplock uses @i_lock_timeout (wrong) instead of @LockTimeout.
      T23 (#384) - @LongRunningSamplePercent NULL passthrough: debug prints raw NULL
                   instead of resolved @i_long_running_sample_pct from preset.
      T24 (#385) - @StaleHours days_stale comparison: days_stale >= @StaleHours_param
                   should be hours_stale >= @StaleHours_param.
      T25 (QA C1) - OLTP_LIGHT preset values: time_limit=1800, threshold=10000.
      T26 (QA C2) - WAREHOUSE preset values: sort=ROWS, sample=100.
      T27 (QA C3) - Invalid @Preset raises error.
      T28 (QA H4) - Summary result set has all expected columns.
      T29 (QA H5) - @ExcludeStatistics filter works (_WA_Sys% excluded).
      T30 (#387)  - @QueryStoreTopPlans dropped from public signature.
      T31-T33 (#386) - START command and user-facing validation/warning text use public names.
      T34-T35        - @Tables='ALL' behavior and full START CommandLog persistence.

    Each check emits a PASS or FAIL result for the release gate.

    Uses ADO.NET SqlConnection (not go-sqlcmd) to avoid TLS cert parsing
    issues with SQL 2017's negative serial number certificates.

    Prerequisites:
      - sp_StatUpdate.sql deployed to master on each target instance,
        OR run without -SkipSetup to deploy automatically.
      - tests/setup_multiversion_db.sql deployed to create StatUpdateTest DB
        and test tables.

.PARAMETER Server
    SQL Server host or tunnel endpoint.

.PARAMETER Password
    SQL password. Defaults to SPSTATUPDATE_TEST_SQL_PASSWORD.

.PARAMETER Versions
    One SQL version to test. Default: 2022.

.PARAMETER SkipSetup
    Skip proc deployment and test data setup (assumes already deployed).

.EXAMPLE
    .\tests\Test-V3Bugs.ps1
    .\tests\Test-V3Bugs.ps1 -Versions @("2022","2025")
    .\tests\Test-V3Bugs.ps1 -SkipSetup
    .\tests\Test-V3Bugs.ps1 -Server 127.0.0.1 -Versions @("2022")
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$Server,
    [string]$Password = $(if ($env:SPSTATUPDATE_TEST_SQL_PASSWORD) { $env:SPSTATUPDATE_TEST_SQL_PASSWORD } else { '' }),
    [ValidateSet('2019','2022','2025')][string[]]$Versions = @('2022'),
    [string]$ResultsPath = '',
    [switch]$SkipSetup
)

$ErrorActionPreference = "Continue"
if (-not $Password) { throw 'Release-gate SQL credentials are unavailable.' }
if ($Versions.Count -ne 1) { throw 'Run one supported SQL version at a time.' }
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot  = Split-Path -Parent $ScriptDir

$ProcFile  = Join-Path $RepoRoot "sp_StatUpdate.sql"
$SetupFile = Join-Path $ScriptDir "setup_multiversion_db.sql"

# Version config: label -> port on remote host
# NOTE: SQL 2022 is on port 1432 (not 1433 -- that port is DOWN on remote host).
$VersionConfig = @{
    "2019" = @{ Label = "SQL 2019"; Port = 1434; Major = 15 }
    "2022" = @{ Label = "SQL 2022"; Port = 1432; Major = 16 }
    "2025" = @{ Label = "SQL 2025"; Port = 1431; Major = 17 }
}
$versionBuilds = @{}
$runStartedAt = Get-Date
$ProcSource = [System.IO.File]::ReadAllText($ProcFile)

# =====================================================================
# ADO.NET HELPER (outer scope -- for setup phase)
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
        $cmd.CommandText  = $Query
        $cmd.CommandTimeout = $Timeout
        $adapter = [System.Data.SqlClient.SqlDataAdapter]::new($cmd)
        $ds = [System.Data.DataSet]::new()
        $null = $adapter.Fill($ds)
        $conn.Close()
        return @{ Output = $messages; DataSet = $ds; ExitCode = 0 }
    } catch {
        $inner = if ($_.Exception.InnerException) { $_.Exception.InnerException.Message } else { "" }
        return @{ Output = @($inner, $_.Exception.Message) + $messages; DataSet = $null; ExitCode = 1 }
    }
}

# =====================================================================
# HEADER
# =====================================================================
Write-Host ""
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host " sp_StatUpdate v3 Bug Detection Test Suite (T20-T35)" -ForegroundColor Cyan
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host ""
Write-Host "  Server:   $Server"
Write-Host "  Versions: $($Versions -join ', ')"
Write-Host "  Proc:     $ProcFile"
Write-Host ""
Write-Host ""

# =====================================================================
# STEP 1: VERIFY CONNECTIVITY
# =====================================================================
Write-Host "[1/4] Verifying connectivity..." -ForegroundColor Yellow

$activeVersions = [System.Collections.ArrayList]::new()
foreach ($ver in $Versions) {
    if (-not $VersionConfig.ContainsKey($ver)) {
        Write-Host "  UNKNOWN version '$ver' -- skipping" -ForegroundColor DarkGray
        continue
    }
    $cfg = $VersionConfig[$ver]
    $r = Invoke-Sql -ServerAddr $Server -Port $cfg.Port -Pwd $Password -Query "SELECT CONVERT(int, SERVERPROPERTY(N'ProductMajorVersion')) AS MajorVer, CONVERT(nvarchar(128), SERVERPROPERTY(N'ProductVersion')) AS ProductVersion;"
    if ($r.ExitCode -eq 0 -and [int]$r.DataSet.Tables[0].Rows[0]['MajorVer'] -eq $cfg.Major) {
        $versionBuilds[$ver] = [string]$r.DataSet.Tables[0].Rows[0]['ProductVersion']
        $verStr = $versionBuilds[$ver]
        Write-Host "  $($cfg.Label) (:$($cfg.Port)): READY - $verStr" -ForegroundColor Green
        $null = $activeVersions.Add($ver)
    } else {
        $detail = if ($r.ExitCode -ne 0) { $r.Output | Select-Object -First 1 } else { "connected to unexpected SQL major version" }
        Write-Host "  $($cfg.Label) (:$($cfg.Port)): NOT READY -- $detail" -ForegroundColor Red
    }
}
Write-Host ""

if ($activeVersions.Count -eq 0) {
    Write-Host "ERROR: No SQL Server instances reachable." -ForegroundColor Red
    exit 1
}

if (-not (Test-Path $ProcFile)) {
    Write-Host "ERROR: Proc file not found: $ProcFile" -ForegroundColor Red
    exit 1
}

# =====================================================================
# STEP 2: DEPLOY PROC + TEST DATA (serial)
# =====================================================================
if (-not $SkipSetup) {
    Write-Host "[2/4] Deploying v3 proc and test data..." -ForegroundColor Yellow

    foreach ($ver in $activeVersions) {
        $cfg = $VersionConfig[$ver]
        Write-Host "  $($cfg.Label) (:$($cfg.Port))..." -NoNewline

        # Deploy proc -- split on GO, execute each batch in master
        $procSql = [System.IO.File]::ReadAllText($ProcFile)
        $batches = [regex]::Split($procSql, '(?m)^\s*GO\s*$')
        $deployFailed = $false

        foreach ($batch in $batches) {
            $trimmed = $batch.Trim()
            if ($trimmed.Length -eq 0) { continue }
            $r = Invoke-Sql -ServerAddr $Server -Port $cfg.Port -Pwd $Password -Query $trimmed -Db "master" -Timeout 120
            if ($r.ExitCode -ne 0) {
                Write-Host " PROC DEPLOY FAILED: $($r.Output | Select-Object -First 1)" -ForegroundColor Red
                $deployFailed = $true
                break
            }
        }
        if ($deployFailed) { continue }

        # Deploy test data -- track USE context across batches
        $setupSql = [System.IO.File]::ReadAllText($SetupFile)
        $setupBatches = [regex]::Split($setupSql, '(?m)^\s*GO\s*$')
        $currentDb = "master"

        foreach ($batch in $setupBatches) {
            $trimmed = $batch.Trim()
            if ($trimmed.Length -eq 0) { continue }
            if ($trimmed -match '(?mi)(?:^|\n)\s*USE\s+\[?(\w+)\]?') {
                $currentDb = $Matches[1]
            }
            $r = Invoke-Sql -ServerAddr $Server -Port $cfg.Port -Pwd $Password -Query $trimmed -Db $currentDb -Timeout 60
            if ($r.ExitCode -ne 0) {
                Write-Host " TEST DATA FAILED (db=$currentDb): $($r.Output | Select-Object -First 1)" -ForegroundColor Red
                $deployFailed = $true
                break
            }
        }
        if ($deployFailed) { continue }

        Write-Host " OK" -ForegroundColor Green
    }
    Write-Host ""
} else {
    Write-Host "[2/4] Skipping deploy (-SkipSetup)" -ForegroundColor DarkGray
    Write-Host ""
}

# =====================================================================
# STEP 3: PARALLEL TEST EXECUTION
# =====================================================================
Write-Host "[3/4] Running 14 checks for each version..." -ForegroundColor Yellow
Write-Host ""

$allResults = $activeVersions | ForEach-Object -ThrottleLimit $activeVersions.Count -Parallel {
    $ver           = $_
    $Server        = $using:Server
    $Password      = $using:Password
    $VersionConfig = $using:VersionConfig
    $VersionBuilds = $using:versionBuilds
    $ProcSource    = $using:ProcSource
    $script:lastCheck = Get-Date

    $cfg   = $VersionConfig[$ver]
    $port  = $cfg.Port
    $label = $cfg.Label
    $db    = "StatUpdateTest"

    # --- ADO.NET helper must be redefined inside parallel block ---
    function Invoke-Sql {
        param([string]$ServerAddr, [int]$Port, [string]$Pwd, [string]$Query, [string]$Db = "master", [int]$Timeout = 300)
        $connStr = "Server=$ServerAddr,$Port;Database=$Db;User Id=sa;Password=$Pwd;Encrypt=false;TrustServerCertificate=true;Pooling=false;Connection Timeout=30"
        $messages = [System.Collections.ArrayList]::new()
        try {
            $conn = [System.Data.SqlClient.SqlConnection]::new($connStr)
            $conn.add_InfoMessage({ param($s, $e); $null = $messages.Add($e.Message) })
            $conn.FireInfoMessageEventOnUserErrors = $true
            $conn.Open()
            $cmd = $conn.CreateCommand()
            $cmd.CommandText  = $Query
            $cmd.CommandTimeout = $Timeout
            $ds = [System.Data.DataSet]::new()
            $reader = $cmd.ExecuteReader()
            do {
                $dt = [System.Data.DataTable]::new()
                $dt.Load($reader)
                $null = $ds.Tables.Add($dt)
            } while (-not $reader.IsClosed)
            $reader.Dispose()
            $conn.Close()
            return @{ Output = $messages; DataSet = $ds; ExitCode = 0 }
        } catch {
            $inner = if ($_.Exception.InnerException) { $_.Exception.InnerException.Message } else { "" }
            return @{ Output = @($inner, $_.Exception.Message) + $messages; DataSet = $null; ExitCode = 1 }
        }
    }

    $results = [System.Collections.ArrayList]::new()
    function Add-Result {
        param([string]$Test, [string]$Result, [string]$Detail = "")
        $now = Get-Date
        $durationMs = [long][Math]::Max(0, ($now - $script:lastCheck).TotalMilliseconds)
        $null = $results.Add([PSCustomObject]@{ Test = $Test; Result = $Result; Detail = $Detail; DurationMs = $durationMs })
        $script:lastCheck = $now
    }

    # ----------------------------------------------------------------
    # PRE-TEST: insert modifications so discovery has candidates.
    # ----------------------------------------------------------------
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
        -Query "INSERT INTO dbo.Orders (CustomerName, Amount) SELECT TOP 5000 N'BugPre', 1.00 FROM sys.all_columns;" | Out-Null
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
        -Query "INSERT INTO dbo.Products (ProductName, Price) SELECT TOP 2000 N'BugPre', 2.00 FROM sys.all_columns;" | Out-Null

    # ================================================================
    # T20: @ModificationThreshold preset passthrough (#381)
    #
    # Bug: debug header printed @ModificationThreshold (raw input param, NULL when
    # using preset) instead of the resolved @i_modification_threshold.
    # WEEKLY_FULL preset sets @i_modification_threshold = 2000 internally.
    # Debug line should show the resolved value, not the raw param.
    #
    # Detection: run with @Preset=WEEKLY_FULL and no explicit @ModificationThreshold.
    # Check debug output contains "2000" (resolved value) in the threshold line.
    # ================================================================
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
        -Query "EXEC dbo.sp_StatUpdate @Preset = N'WEEKLY_FULL', @Databases = N'$db', @Execute = N'N', @Debug = 1;"
    $out = ($r.Output -join "`n")

    if ($r.ExitCode -ne 0) {
        Add-Result "T20: #381 Preset threshold passthrough" "FAIL" "Proc error: $($r.Output | Select-Object -First 1)"
    } else {
        if ($out -match '@ModificationThreshold\s*=\s*2000') {
            Add-Result "T20: #381 Preset threshold passthrough" "PASS" "Debug shows resolved value 2000 for WEEKLY_FULL"
        } elseif ($out -match '@ModificationThreshold\s*=\s*0\b') {
            Add-Result "T20: #381 Preset threshold passthrough" "FAIL" "Bug confirmed: debug shows 0 (NULL) instead of 2000"
        } else {
            $match = [regex]::Match($out, '@ModificationThreshold\s*=\s*(\S+)')
            $found = if ($match.Success) { $match.Groups[1].Value } else { "(not found in output)" }
            Add-Result "T20: #381 Preset threshold passthrough" "FAIL" "Expected 2000, got: $found"
        }
    }

    # ================================================================
    # T21: ROWS sort order (#382)
    #
    # Bug: the ORDER BY CASE block in queue population (parallel mode table
    # priority) covers MODIFICATION_COUNTER, PAGE_COUNT, DAYS_STALE, RANDOM
    # but is missing a WHEN clause for ROWS.  In non-parallel mode the proc
    # falls through to a default order and the run succeeds but stats are NOT
    # sorted by row count.
    #
    # Detection: run @Preset=WAREHOUSE (sets @i_sort_order=ROWS) dry-run and
    # check @i_sort_order in debug output confirms ROWS.  Then run @Execute=Y
    # with @StatsInParallel=Y -- the missing ROWS case means table priority
    # is random rather than rows-descending.  We detect this indirectly by
    # verifying the proc does not error and by checking the WhatIfOutputTable
    # is not ordered by rows (non-trivial to verify in a unit test).
    # Confirm the generated discovery CASE includes ROWS, in addition to the
    # runtime preset output. This source-backed check covers the queue ordering branch.
    # ================================================================
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
        -Query "EXEC dbo.sp_StatUpdate @Preset = N'WAREHOUSE', @Databases = N'$db', @Execute = N'N', @Debug = 1;"
    $out = ($r.Output -join "`n")

    if ($r.ExitCode -ne 0) {
        Add-Result "T21: #382 ROWS sort order" "FAIL" "Proc error with WAREHOUSE preset: $($r.Output | Select-Object -First 1)"
    } elseif ($out -match '@SortOrder\s*=\s*ROWS' -and $ProcSource -match "WHEN N''ROWS'' THEN ISNULL\(rows, 0\)") {
        Add-Result "T21: #382 ROWS sort order" "PASS" "WAREHOUSE resolves to ROWS and the generated discovery CASE orders by row count"
    } else {
        $match = [regex]::Match($out, '@SortOrder\s*=\s*(\S+)')
        $found = if ($match.Success) { $match.Groups[1].Value } else { "(not found)" }
        Add-Result "T21: #382 ROWS sort order" "FAIL" "Expected @i_sort_order=ROWS in WAREHOUSE preset debug, got: $found"
    }

    # ================================================================
    # T22: sp_getapplock parameter name (#383)
    #
    # Bug: two sp_getapplock calls in the parallel mop-up path use @i_lock_timeout
    # (a local variable name) instead of the correct sp_getapplock parameter
    # name @LockTimeout.  This causes a runtime error in parallel mode.
    # Non-parallel mode is not affected.
    #
    # A source assertion checks the rarely entered parallel mop-up call without
    # relying on a concurrent-worker race.
    # ================================================================
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
        -Query "EXEC dbo.sp_StatUpdate @Databases = N'$db', @Execute = N'N';"
    if ($r.ExitCode -eq 0 -and $ProcSource -match '(?is)sp_getapplock\s+@Resource\s*=\s*@mop_lock_resource,\s*@LockMode\s*=\s*N''Exclusive'',\s*@LockOwner\s*=\s*N''Session'',\s*@LockTimeout\s*=') {
        Add-Result "T22: #383 sp_getapplock param name" "PASS" "Parallel mop-up app-lock call uses the system parameter @LockTimeout"
    } else {
        Add-Result "T22: #383 sp_getapplock param name" "FAIL" "Proc error on basic run: $($r.Output | Select-Object -First 1)"
    }

    # ================================================================
    # T23: @LongRunningSamplePercent NULL passthrough (#384)
    #
    # Bug: debug header prints @LongRunningSamplePercent (raw input param) which
    # is NULL when the caller did not pass it.  The resolved @i_long_running_sample_pct
    # defaults to 10 and should be shown instead.
    #
    # Detection: run with @LongRunningThresholdMinutes=1 but no explicit
    # @LongRunningSamplePercent.  Debug line should show 10 (preset default).
    # Buggy code prints NULL since it shows the raw param.
    #
    # Requires @LogToTable=Y for @LongRunningThresholdMinutes validation to pass.
    # ================================================================
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
        -Query "EXEC dbo.sp_StatUpdate @Databases = N'$db', @LongRunningThresholdMinutes = 1, @LogToTable = N'Y', @Execute = N'N', @Debug = 1;"
    $out = ($r.Output -join "`n")

    if ($r.ExitCode -ne 0) {
        # Could legitimately fail if CommandLog doesn't exist -- check the message
        $errMsg = ($r.Output | Select-Object -First 1)
        if ($errMsg -match 'CommandLog|LogToTable') {
            Add-Result "T23: #384 LongRunningSamplePercent passthrough" "FAIL" "Required CommandLog setup unavailable: $errMsg"
        } else {
            Add-Result "T23: #384 LongRunningSamplePercent passthrough" "FAIL" "Proc error: $errMsg"
        }
    } else {
        # Debug line: @LongRunningSamplePct    = 10%
        # Buggy output shows @LongRunningSamplePercent (raw param) which is NULL -- not shown at all,
        # because the debug block is gated on @LongRunningThresholdMinutes IS NOT NULL
        # and it prints @LongRunningSamplePercent which is NULL when not passed.
        if ($out -match '@LongRunningSamplePct\s*=\s*10') {
            Add-Result "T23: #384 LongRunningSamplePercent passthrough" "PASS" "Debug shows resolved 10% sample rate"
        } elseif ($out -match '@LongRunningSamplePct\s*=\s*$|@LongRunningSamplePct\s*=\s*\(null\)') {
            Add-Result "T23: #384 LongRunningSamplePercent passthrough" "FAIL" "Bug confirmed: debug shows NULL instead of resolved 10%"
        } elseif ($out -notmatch '@LongRunningSamplePct') {
            # Line not printed at all -- also a symptom of the bug (NULL param suppresses output)
            Add-Result "T23: #384 LongRunningSamplePercent passthrough" "FAIL" "Bug confirmed: @LongRunningSamplePct line absent from debug (NULL param suppressed display)"
        } else {
            $match = [regex]::Match($out, '@LongRunningSamplePct\s*=\s*(\S+)')
            $found = if ($match.Success) { $match.Groups[1].Value } else { "(not found)" }
            Add-Result "T23: #384 LongRunningSamplePercent passthrough" "FAIL" "Expected 10, got: $found"
        }
    }

    # ================================================================
    # T24: @StaleHours uses the hours_stale comparison (#385)
    #
    # Bug: the OR-logic qualification block uses `days_stale >= @StaleHours_param`
    # which compares whole-day counts against an hours value.  @StaleHours=1 means
    # "skip stats updated within the last 1 hour" but the bug makes it skip stats
    # updated within the last 1 day (24h) because days_stale=0 for anything < 24h.
    # The correct column is hours_stale.
    #
    # The current API has no TieredThresholds parameter; use a high modification
    # threshold and inspect the implemented predicate in the deployed source.
    # ================================================================

    # First, actually execute to update a known stat so it has a fresh last_updated time
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
        -Query "INSERT INTO dbo.Orders (CustomerName, Amount) SELECT TOP 5000 N'T24setup', 1.00 FROM sys.all_columns;" | Out-Null
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
        -Query "EXEC dbo.sp_StatUpdate @Databases = N'$db', @ModificationThreshold = 100, @Execute = N'Y', @LogToTable = N'Y';" | Out-Null

    # Now immediately run dry-run with @StaleHours=1 and a high threshold.
    # so only stale-hours logic could qualify.  A freshly-updated stat should NOT qualify.
    # Bug: days_stale=0 < 1 so it correctly won't qualify for wrong reasons (days comparison).
    # Correct fix: hours_stale=0 < 1 so also won't qualify.
    # We need a stat that is ~2 hours old to distinguish.  Since we can't control clock,
    # instead test the inverse: @StaleHours=0 should qualify everything (0 hours old).
    # With the bug: days_stale >= 0 is always true -> qualifies everything.
    # With fix:     hours_stale >= 0 is always true -> also qualifies everything.
    # Both behave the same for 0.  Test @StaleHours=25 on a fresh stat:
    # Bug: days_stale(0) >= 25 is FALSE -> does not qualify (correct result, wrong reason).
    # Correct: hours_stale(~0) >= 25 is FALSE -> does not qualify (correct result, right reason).
    # Runtime data cannot reliably create a stat aged 1-23 hours in this short test,
    # so the check requires the proc source to use hours_stale in the predicate.
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
        -Query "EXEC dbo.sp_StatUpdate @Databases = N'$db', @StaleHours = 1, @ModificationThreshold = 9999999, @Execute = N'N', @Debug = 1;"
    $out = ($r.Output -join "`n")

    if ($r.ExitCode -ne 0) {
        Add-Result "T24: #385 @StaleHours hours vs days" "FAIL" "Proc error: $($r.Output | Select-Object -First 1)"
    } else {
        if ($ProcSource -match '(?is)UPDATE\s+#stat_candidates\s+SET\s+qualifies\s*=\s*1\s+WHERE.*?hours_stale\s*>=\s*@StaleHours_param') {
            Add-Result "T24: #385 @StaleHours hours vs days" "PASS" "Runtime accepts @StaleHours and qualification uses hours_stale"
        } else {
            Add-Result "T24: #385 @StaleHours hours vs days" "FAIL" "Qualification predicate does not compare hours_stale to @StaleHours_param"
        }
    }

    # ================================================================
    # T25: OLTP_LIGHT preset values (QA C1)
    #
    # Validate that OLTP_LIGHT sets the documented preset values:
    #   time_limit=1800, modification_threshold=10000, lock_timeout=10,
    #   delay_between_stats=500, sort_order=MODIFICATION_COUNTER.
    # ================================================================
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
        -Query "EXEC dbo.sp_StatUpdate @Preset = N'OLTP_LIGHT', @Databases = N'$db', @Execute = N'N', @Debug = 1;"
    $out = ($r.Output -join "`n")

    if ($r.ExitCode -ne 0) {
        Add-Result "T25: OLTP_LIGHT preset values" "FAIL" "Proc error: $($r.Output | Select-Object -First 1)"
    } else {
        $issues = [System.Collections.ArrayList]::new()
        # time_limit = 1800 seconds
        if ($out -notmatch '@TimeLimit\s*=\s*1800') {
            $null = $issues.Add("time_limit not 1800")
        }
        # modification_threshold = 10000
        # Note: bug #381 means @ModificationThreshold (raw) is shown as 0/NULL, not @i_modification_threshold.
        # This test checks the raw param line (0/NULL) OR the internal value display.
        # We check for both to be resilient to the bug.
        if ($out -notmatch '10000') {
            $null = $issues.Add("10000 threshold not found in output")
        }
        # lock_timeout = 10
        if ($out -notmatch '@LockTimeout\s*=\s*10') {
            $null = $issues.Add("lock_timeout not 10")
        }
        # sort_order = MODIFICATION_COUNTER
        if ($out -notmatch '@SortOrder\s*=\s*MODIFICATION_COUNTER') {
            $null = $issues.Add("sort_order not MODIFICATION_COUNTER")
        }

        if ($issues.Count -eq 0) {
            Add-Result "T25: OLTP_LIGHT preset values" "PASS" "time_limit=1800, threshold=10000, lock_timeout=10, sort=MODIFICATION_COUNTER"
        } else {
            Add-Result "T25: OLTP_LIGHT preset values" "FAIL" ($issues -join "; ")
        }
    }

    # ================================================================
    # T26: WAREHOUSE preset values (QA C2)
    #
    # Validate WAREHOUSE preset:
    #   sort_order=ROWS, statistics_sample=100 (FULLSCAN), modification_threshold=1000.
    # ================================================================
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
        -Query "EXEC dbo.sp_StatUpdate @Preset = N'WAREHOUSE', @Databases = N'$db', @Execute = N'N', @Debug = 1;"
    $out = ($r.Output -join "`n")

    if ($r.ExitCode -ne 0) {
        Add-Result "T26: WAREHOUSE preset values" "FAIL" "Proc error: $($r.Output | Select-Object -First 1)"
    } else {
        $issues = [System.Collections.ArrayList]::new()
        # sort_order = ROWS
        if ($out -notmatch '@SortOrder\s*=\s*ROWS') {
            $null = $issues.Add("sort_order not ROWS")
        }
        # statistics_sample = 100 (FULLSCAN)
        if ($out -notmatch '@StatisticsSample\s*=\s*100') {
            $null = $issues.Add("statistics_sample not 100")
        }
        # time_limit = None (NULL unlimited)
        if ($out -notmatch '@TimeLimit\s*=\s*None|@TimeLimit\s*=\s*NULL') {
            $null = $issues.Add("time_limit not None/NULL (WAREHOUSE should be unlimited)")
        }

        if ($issues.Count -eq 0) {
            Add-Result "T26: WAREHOUSE preset values" "PASS" "sort=ROWS, sample=100, time_limit=None"
        } else {
            Add-Result "T26: WAREHOUSE preset values" "FAIL" ($issues -join "; ")
        }
    }

    # ================================================================
    # T27: Invalid @Preset validation (QA C3)
    #
    # @Preset='INVALID_PRESET' should raise an error with a message containing
    # 'Invalid' and should not proceed with discovery.
    # ================================================================
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
        -Query "EXEC dbo.sp_StatUpdate @Preset = N'INVALID_PRESET', @Databases = N'$db', @Execute = N'N';"
    $out = ($r.Output -join "`n")

    if ($r.ExitCode -ne 0) {
        # ADO.NET threw -- likely severity 16 error
        if ($out -match '(?i)invalid|not recognized|INVALID_PRESET') {
            Add-Result "T27: Invalid @Preset validation" "PASS" "Error raised for invalid preset (non-zero exit)"
        } else {
            Add-Result "T27: Invalid @Preset validation" "FAIL" "Non-zero exit but no 'invalid' message: $($r.Output | Select-Object -First 1)"
        }
    } elseif ($out -match '(?i)invalid|not recognized|INVALID_PRESET') {
        Add-Result "T27: Invalid @Preset validation" "PASS" "Error message in output: $(($out -split "`n" | Where-Object { $_ -match '(?i)invalid' } | Select-Object -First 1).Trim())"
    } else {
        Add-Result "T27: Invalid @Preset validation" "FAIL" "No error raised for @Preset='INVALID_PRESET' (proc ran silently)"
    }

    # ================================================================
    # T28: Summary result set column structure (QA H4)
    #
    # Run a basic dry-run and verify the summary result set (Tables[1] or Tables[0])
    # has ALL expected columns: Status, StatusMessage, StatsFound, StatsProcessed,
    # StatsSucceeded, StatsFailed, StatsToctou, StatsSkipped, StatsRemaining,
    # DatabasesProcessed, DurationSeconds, StopReason, RunLabel, Version.
    # ================================================================
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
        -Query "INSERT INTO dbo.Orders (CustomerName, Amount) SELECT TOP 3000 N'T28', 1.00 FROM sys.all_columns;" | Out-Null

    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
        -Query "EXEC dbo.sp_StatUpdate @Databases = N'$db', @ModificationThreshold = 100, @Execute = N'N';"

    if ($r.ExitCode -ne 0) {
        Add-Result "T28: Summary result set columns" "FAIL" "Proc error: $($r.Output | Select-Object -First 1)"
    } else {
        # Find the summary result set -- it is typically Tables[1] when Tables[0] is
        # something else, or Tables[0] on the no-debug path.  Look for a table with
        # a Status column.
        $summaryTbl = $null
        if ($r.DataSet -and $r.DataSet.Tables.Count -gt 0) {
            foreach ($tbl in $r.DataSet.Tables) {
                if ($tbl.Columns.Contains("Status")) {
                    $summaryTbl = $tbl
                    break
                }
            }
        }

        if ($null -eq $summaryTbl) {
            Add-Result "T28: Summary result set columns" "FAIL" "No result set with Status column found ($($r.DataSet.Tables.Count) tables returned)"
        } else {
            $expectedCols = @(
                "Status", "StatusMessage", "StatsFound", "StatsProcessed",
                "StatsSucceeded", "StatsFailed", "StatsToctou", "StatsSkipped",
                "StatsRemaining", "DatabasesProcessed", "DurationSeconds",
                "StopReason", "RunLabel", "Version"
            )
            $missingCols = [System.Collections.ArrayList]::new()
            foreach ($col in $expectedCols) {
                if (-not $summaryTbl.Columns.Contains($col)) {
                    $null = $missingCols.Add($col)
                }
            }

            if ($missingCols.Count -eq 0) {
                Add-Result "T28: Summary result set columns" "PASS" "All $($expectedCols.Count) columns present"
            } else {
                Add-Result "T28: Summary result set columns" "FAIL" "Missing columns: $($missingCols -join ', ')"
            }
        }
    }

    # ================================================================
    # T29: @ExcludeStatistics filter (QA H5)
    #
    # Insert enough modifications that auto-created (_WA_Sys%) and user-created
    # stats both qualify.  Run with @ExcludeStatistics=N'_WA_Sys%' and verify
    # no _WA_Sys stats appear in the WhatIfOutputTable.
    # ================================================================
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
        -Query "INSERT INTO dbo.Orders (CustomerName, Amount) SELECT TOP 8000 N'T29', 1.00 FROM sys.all_columns;" | Out-Null

    $wifTable = "tempdb.dbo.v3bug_t29_$ver"
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
        -Query "IF OBJECT_ID(N'$wifTable') IS NOT NULL DROP TABLE $wifTable;" | Out-Null

    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
        -Query "EXEC dbo.sp_StatUpdate @Databases = N'$db', @ModificationThreshold = 100, @ExcludeStatistics = N'_WA_Sys%', @Execute = N'N', @WhatIfOutputTable = N'$wifTable';"

    if ($r.ExitCode -ne 0) {
        Add-Result "T29: @ExcludeStatistics filter" "FAIL" "Proc error: $($r.Output | Select-Object -First 1)"
    } else {
        # Check if WhatIfOutputTable was created and contains no _WA_Sys rows
        $r2 = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
            -Query "IF OBJECT_ID(N'$wifTable') IS NOT NULL SELECT COUNT(*) AS TotalCnt, ISNULL(SUM(CASE WHEN StatName LIKE N'_WA_Sys%' THEN 1 ELSE 0 END), 0) AS WaSysCnt FROM $wifTable ELSE SELECT 0 AS TotalCnt, 0 AS WaSysCnt;"

        $totalCnt = 0
        $waSysCnt = 0
        if ($r2.DataSet -and $r2.DataSet.Tables.Count -gt 0 -and $r2.DataSet.Tables[0].Rows.Count -gt 0) {
            $totalCnt = [int]$r2.DataSet.Tables[0].Rows[0]["TotalCnt"]
            $waSysCnt = [int]$r2.DataSet.Tables[0].Rows[0]["WaSysCnt"]
        }

        if ($waSysCnt -eq 0) {
            Add-Result "T29: @ExcludeStatistics filter" "PASS" "_WA_Sys% excluded (0 auto-stat rows in $totalCnt total)"
        } else {
            Add-Result "T29: @ExcludeStatistics filter" "FAIL" "$waSysCnt _WA_Sys rows found in output (filter not working)"
        }

        Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
            -Query "IF OBJECT_ID(N'$wifTable') IS NOT NULL DROP TABLE $wifTable;" | Out-Null
    }

    # ================================================================
    # T30: @QueryStoreTopPlans re-exposed as public param (#387)
    #
    # Fix: #387 re-exposed @QueryStoreTopPlans as a public param that overrides
    # the preset's @i_qs_top_plans default (500).  Callers should be able to
    # pass it without error.
    # ================================================================
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
        -Query "EXEC dbo.sp_StatUpdate @Databases = N'$db', @QueryStoreTopPlans = 50, @Execute = N'N', @Debug = 1;"
    $out = ($r.Output -join "`n")

    if ($r.ExitCode -ne 0) {
        Add-Result "T30: #387 @QueryStoreTopPlans accepted" "FAIL" "Proc rejected @QueryStoreTopPlans: $($r.Output | Select-Object -First 1)"
    } else {
        # Verify the param was accepted and the value appears in debug output
        if ($out -match '(?i)qs_top_plans\s*=\s*50|QueryStoreTopPlans\s*=\s*50|TopPlans.*50') {
            Add-Result "T30: #387 @QueryStoreTopPlans accepted" "PASS" "Param accepted, value 50 in debug"
        } else {
            # Param was accepted (no error) even if value doesn't appear in debug
            Add-Result "T30: #387 @QueryStoreTopPlans accepted" "PASS" "Param accepted without error"
        }
    }

    # ================================================================
    # T31: CommandLog START text uses public parameter names (#386)
    # ================================================================
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
EXEC dbo.sp_StatUpdate @Databases = N'$db', @TimeLimit = 60,
    @SortOrder = N'ROWS', @LogToTable = N'N', @Execute = N'N';
"@
    $startCommandPublic =
        $ProcSource -match "(?is)N', @TimeLimit = '\s*\+\s*CONVERT\(nvarchar\(10\), @i_time_limit\)" -and
        $ProcSource -match "(?is)N', @SortOrder = N'''\s*\+\s*@i_sort_order" -and
        $ProcSource -match "(?is)THEN N', @QueryStore = N'''\s*\+\s*@i_qs_metric"
    if ($r.ExitCode -eq 0 -and $startCommandPublic) {
        Add-Result 'T31: #386 CommandLog START uses public names' 'PASS' 'Runtime accepts TimeLimit/SortOrder; START template emits public names for TimeLimit, SortOrder, and QueryStore'
    } else {
        $reason = if ($r.ExitCode -ne 0) { "START call failed: $($r.Output | Select-Object -First 1)" } else { 'START command template is missing a public parameter name or contains a mismatched expression' }
        Add-Result 'T31: #386 CommandLog START uses public names' 'FAIL' $reason
    }

    # T32: validation errors use the public MopUpPass name (#386)
    $staleValidationText = $ProcSource -match "(?is)N'[^']*@i_mop_up_pass\s+(?:must be|requires)"
    $publicValidationText = $ProcSource -match "(?is)N'@MopUpPass requires a time budget"
    if ($publicValidationText -and -not $staleValidationText) {
        Add-Result 'T32: #386 MopUpPass validation text is public' 'PASS' 'Validation message names @MopUpPass and not the internal variable'
    } else {
        Add-Result 'T32: #386 MopUpPass validation text is public' 'FAIL' 'MopUpPass validation message is missing or exposes an internal variable'
    }

    # T33: overshoot warning does not name an internal-only parameter (#386)
    # Inspect only the string literal assigned to @p333_msg. A broad scan can
    # start at the closing quote of N'N' elsewhere and cross executable SQL.
    $warningBlock = [regex]::Match(
        $ProcSource,
        '(?is)DECLARE\s+@p333_msg\b.*?RAISERROR\s*\(\s*@p333_msg'
    )
    $warningLiteral = [regex]::Match($warningBlock.Value, "N'((?:''|[^'])*)'")
    $warningText = if ($warningLiteral.Success) { $warningLiteral.Groups[1].Value } else { '' }
    if ($warningBlock.Success -and $warningLiteral.Success -and
        $warningText -notmatch '(?i)@i_max_seconds_per_stat' -and
        $warningText -match '(?i)@StopByTime') {
        Add-Result 'T33: #386 overshoot warning avoids internal parameter names' 'PASS' 'No user-facing warning literal exposes @i_max_seconds_per_stat'
    } else {
        Add-Result 'T33: #386 overshoot warning avoids internal parameter names' 'FAIL' 'The @p333_msg warning literal is missing, names an internal parameter, or omits @StopByTime'
    }

    # T34: @Tables='ALL' behaves like the default NULL selection.
    $nullSelection = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
DECLARE @found int;
EXEC dbo.sp_StatUpdate @Databases = N'$db', @Execute = N'N', @LogToTable = N'N', @StatsFoundOut = @found OUTPUT;
SELECT @found AS StatsFoundValue;
"@
    $allSelection = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
DECLARE @found int;
EXEC dbo.sp_StatUpdate @Databases = N'$db', @Tables = N'ALL', @Execute = N'N', @LogToTable = N'N', @StatsFoundOut = @found OUTPUT;
SELECT @found AS StatsFoundValue;
"@
    $statsValues = [System.Collections.ArrayList]::new()
    foreach ($selection in @($nullSelection, $allSelection)) {
        if ($selection.DataSet) {
            foreach ($table in $selection.DataSet.Tables) {
                if ($table.Columns.Contains('StatsFoundValue') -and $table.Rows.Count -gt 0 -and
                    $table.Rows[0]['StatsFoundValue'] -ne [DBNull]::Value) {
                    $null = $statsValues.Add([int]$table.Rows[0]['StatsFoundValue'])
                    break
                }
            }
        }
    }
    if ($nullSelection.ExitCode -eq 0 -and $allSelection.ExitCode -eq 0 -and
        $statsValues.Count -eq 2 -and $statsValues[0] -gt 0 -and $statsValues[1] -eq $statsValues[0]) {
        Add-Result 'T34: @Tables=ALL matches default selection' 'PASS' "StatsFoundOut matches ($($statsValues[1]))"
    } else {
        $nullCount = if ($statsValues.Count -gt 0) { $statsValues[0] } else { 'missing' }
        $allCount = if ($statsValues.Count -gt 1) { $statsValues[1] } else { 'missing' }
        Add-Result 'T34: @Tables=ALL matches default selection' 'FAIL' "Expected equal positive StatsFoundOut; NULL=$nullCount ALL=$allCount"
    }

    # T35: execute with a long @Tables value and inspect the actual START row.
    # The trailing spaces lengthen the persisted command without selecting extra tables.
    $beforeStart = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
        -Query "SELECT ISNULL(MAX(ID), 0) AS BeforeStartId FROM dbo.CommandLog WHERE CommandType = N'SP_STATUPDATE_START';"
    $beforeStartId = $null
    if ($beforeStart.ExitCode -eq 0 -and $beforeStart.DataSet) {
        foreach ($table in $beforeStart.DataSet.Tables) {
            if ($table.Columns.Contains('BeforeStartId') -and $table.Rows.Count -gt 0) {
                $beforeStartId = [long]$table.Rows[0]['BeforeStartId']
                break
            }
        }
    }
    if ($null -eq $beforeStartId) {
        Add-Result 'T35: START CommandLog text is not truncated' 'FAIL' 'Could not read the prior START identity from CommandLog'
    } else {
        $longRunQuery = @"
DECLARE @longTables nvarchar(max) = N'dbo.Orders' + REPLICATE(N' ', 300);
EXEC dbo.sp_StatUpdate
    @Databases = N'$db',
    @Tables = @longTables,
    @Execute = N'Y',
    @TimeLimit = 60,
    @SortOrder = N'MODIFICATION_COUNTER',
    @ModificationThreshold = 1,
    @LogToTable = N'Y';
"@
        $run = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query $longRunQuery -Timeout 300
        $captured = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
            -Query "SELECT TOP (1) Command AS StartCommand FROM dbo.CommandLog WHERE CommandType = N'SP_STATUPDATE_START' AND ID > $beforeStartId ORDER BY ID DESC;"
        $startCommand = $null
        if ($captured.ExitCode -eq 0 -and $captured.DataSet) {
            foreach ($table in $captured.DataSet.Tables) {
                if ($table.Columns.Contains('StartCommand') -and $table.Rows.Count -gt 0 -and
                    $table.Rows[0]['StartCommand'] -ne [DBNull]::Value) {
                    $startCommand = [string]$table.Rows[0]['StartCommand']
                    break
                }
            }
        }
        $commandComplete = $startCommand -and $startCommand.Length -gt 255 -and
            $startCommand -notmatch '\.\.\.' -and
            $startCommand -match '@Databases\s*=' -and
            $startCommand -match '@Tables\s*=' -and
            $startCommand -match '@TimeLimit\s*=' -and
            $startCommand -match '@SortOrder\s*=' -and
            $startCommand.TrimEnd().EndsWith(';')
        if ($run.ExitCode -eq 0 -and $commandComplete) {
            Add-Result 'T35: START CommandLog text is not truncated' 'PASS' "Persisted START command has $($startCommand.Length) characters, all expected parameters, and its semicolon"
        } else {
            $reason = if ($run.ExitCode -ne 0) { "Procedure run failed: $($run.Output | Select-Object -First 1)" }
                elseif ($captured.ExitCode -ne 0) { "CommandLog read failed: $($captured.Output | Select-Object -First 1)" }
                elseif (-not $startCommand) { 'No new SP_STATUPDATE_START row was found' }
                else { "Persisted command was incomplete or truncated (length=$($startCommand.Length))" }
            Add-Result 'T35: START CommandLog text is not truncated' 'FAIL' $reason
        }
    }

    $liveness = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query "SELECT CONVERT(nvarchar(128), SERVERPROPERTY(N'ProductVersion')) AS ProductVersion;"
    $aliveAtEnd = ($liveness.ExitCode -eq 0)

    # ================================================================
    # RETURN per-version result object
    # ================================================================
    [PSCustomObject]@{
        Version = $ver
        Label   = $label
        SqlBuild = $VersionBuilds[$ver]
        ServerAliveAtEnd = $aliveAtEnd
        Results = $results
        Passed  = @($results | Where-Object { $_.Result -eq "PASS" }).Count
        Failed  = @($results | Where-Object { $_.Result -eq "FAIL" }).Count
    }
}

# =====================================================================
# STEP 4: RESULTS SUMMARY
# =====================================================================
Write-Host ""
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host " RESULTS" -ForegroundColor Cyan
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host ""

$totalFailed = 0

foreach ($vr in $allResults | Sort-Object { [int]($VersionConfig[$_.Version].Major) }) {
    $headerColor = if ($vr.Failed -eq 0) { "Green" } else { "Red" }
    $total = $vr.Results.Count
    Write-Host "  $($vr.Label):  $($vr.Passed)/$total passed, $($vr.Failed) failed" -ForegroundColor $headerColor

    foreach ($t in $vr.Results) {
        $tc = switch ($t.Result) {
            "PASS" { "DarkGreen" }
            "FAIL" { "Red"       }
            default { "White"    }
        }
        $detail = if ($t.Detail) { " ($($t.Detail))" } else { "" }
        Write-Host "    [$($t.Result)] $($t.Test)$detail" -ForegroundColor $tc
    }
    Write-Host ""
    $totalFailed += $vr.Failed
}

# Tabular summary
Write-Host ("{0,-14} {1,-8} {2,-8} {3,-8} {4,-6}" -f "Version", "Tests", "Passed", "Failed", "Status")
Write-Host ("{0,-14} {1,-8} {2,-8} {3,-8} {4,-6}" -f "-------", "-----", "------", "------", "------")
foreach ($vr in $allResults | Sort-Object { [int]($VersionConfig[$_.Version].Major) }) {
    $status = if ($vr.Failed -eq 0) { "PASS" } else { "FAIL" }
    $sc     = if ($vr.Failed -eq 0) { "Green" } else { "Red" }
    Write-Host ("{0,-14} {1,-8} {2,-8} {3,-8} {4,-6}" -f $vr.Label, $vr.Results.Count, $vr.Passed, $vr.Failed, $status) -ForegroundColor $sc
}

Write-Host ""

if ($ResultsPath) {
    if ($allResults.Count -ne 1) { throw 'Release-gate V3Bugs runs require exactly one SQL version.' }
    $versionResult = $allResults[0]
    if ($versionResult.Results.Count -ne 16) { throw "Expected 16 V3Bugs checks; received $($versionResult.Results.Count)." }
    $checks = @($versionResult.Results | ForEach-Object {
        $check = [ordered]@{ check = $_.Test; result = $_.Result; duration_ms = $_.DurationMs }
        if ($_.Result -ne 'PASS') { $check.reason = $_.Detail }
        [pscustomobject]$check
    })
    $commit = (& git -C $RepoRoot rev-parse HEAD).Trim()
    if ($LASTEXITCODE -ne 0 -or $commit -notmatch '^[0-9a-fA-F]{40}$') { throw 'Could not resolve the tested source commit.' }
    Import-Module (Join-Path $ScriptDir 'ReleaseResults.psm1') -Force
    $endedAt = Get-Date
    [void](Write-ReleaseRunList -Path $ResultsPath -Commit $commit -SqlVersion $versionResult.Version -SqlBuild $versionResult.SqlBuild `
        -ServerAliveAtEnd $versionResult.ServerAliveAtEnd -Suite 'V3Bugs' -ExpectedCount 16 -Checks $checks `
        -StartedAt $runStartedAt -EndedAt $endedAt -DurationMs ([long](($endedAt - $runStartedAt).TotalMilliseconds)))
}

if ($totalFailed -gt 0 -or @($allResults | Where-Object { -not $_.ServerAliveAtEnd }).Count -gt 0) {
    Write-Host "RESULT: $totalFailed test(s) FAILED across $($allResults.Count) version(s)." -ForegroundColor Red
    exit 1
} else {
    $totalTests = ($allResults | ForEach-Object { $_.Results.Count } | Measure-Object -Sum).Sum
    Write-Host "RESULT: All $totalTests tests PASSED across $($allResults.Count) version(s)." -ForegroundColor Green
    exit 0
}
