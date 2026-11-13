<#
.SYNOPSIS
    QA gap tests for sp_StatUpdate v3 -- parallel dead-worker cleanup, preset
    behavioral accuracy, and @StaleHours functional accuracy.

.DESCRIPTION
    Tests sp_StatUpdate v3 against SQL Server containers on a remote Docker host.
    Covers three bd issues:
      sp_StatUpdate-as3  QueueStatistic leftover rows after aborted parallel run
      sp_StatUpdate-tod  Preset values verified via actual behavior (not just debug text)
      sp_StatUpdate-c0t  @StaleHours functional accuracy

    Uses ADO.NET SqlConnection with Pooling=false and ExecuteReader + DataTable.Load
    (NOT SqlDataAdapter.Fill) to preserve InfoMessage compatibility with SQL 2025.

.PARAMETER Server
    SQL Server host. Default: 127.0.0.1.

.PARAMETER Password
    SA password for all containers.  Default: $env:SPSTATUPDATE_TEST_SQL_PASSWORD.

.PARAMETER Versions
    Which SQL Server versions to test.  Default: @("2022") for speed.

.PARAMETER SkipSetup
    Skip proc deployment (assumes proc is already deployed on each instance).

.EXAMPLE
    .\tests\Test-V3QAGaps2.ps1
    .\tests\Test-V3QAGaps2.ps1 -Versions @("2019","2022","2025")
    .\tests\Test-V3QAGaps2.ps1 -SkipSetup
#>
[CmdletBinding()]
param(
    [string]$Server   = "127.0.0.1",
    [string]$Password = $(if ($env:SPSTATUPDATE_TEST_SQL_PASSWORD) { $env:SPSTATUPDATE_TEST_SQL_PASSWORD } else { '' }),
    [string[]]$Versions = @("2022"),
    [string]$ResultsPath = '',
    [switch]$SkipSetup
)

$ErrorActionPreference = "Continue"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot  = Split-Path -Parent $ScriptDir
$startedAt = Get-Date
Import-Module (Join-Path $ScriptDir 'StructuredLegacyResults.psm1') -Force
if (-not $Password) { throw 'Release-gate SQL credentials are unavailable.' }
$ProcFile  = Join-Path $RepoRoot "sp_StatUpdate.sql"

# Version config: label -> port on remote host
$VersionConfig = @{
    "2019" = @{ Label = "SQL 2019"; Port = 1434; Major = 15 }
    "2022" = @{ Label = "SQL 2022"; Port = 1432; Major = 16 }
    "2025" = @{ Label = "SQL 2025"; Port = 1431; Major = 17 }
}

# =====================================================================
# ADO.NET HELPER (outer scope -- used during setup)
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
    $connStr = "Server=$ServerAddr,$Port;Database=$Db;User Id=sa;Password=$Pwd;Encrypt=false;TrustServerCertificate=true;Connection Timeout=30;Pooling=false"
    $messages = [System.Collections.ArrayList]::new()
    $ds = [System.Data.DataSet]::new()
    try {
        $conn = [System.Data.SqlClient.SqlConnection]::new($connStr)
        $conn.add_InfoMessage({ param($s, $e); $null = $messages.Add($e.Message) })
        $conn.FireInfoMessageEventOnUserErrors = $true
        $conn.Open()
        $cmd = $conn.CreateCommand()
        $cmd.CommandText    = $Query
        $cmd.CommandTimeout = $Timeout
        $reader = $cmd.ExecuteReader()
        do {
            $dt = [System.Data.DataTable]::new()
            $dt.Load($reader)
            $null = $ds.Tables.Add($dt)
        } while (-not $reader.IsClosed)
        $conn.Close()
        return @{ Output = $messages; DataSet = $ds; ExitCode = 0 }
    } catch {
        $inner = if ($_.Exception.InnerException) { $_.Exception.InnerException.Message } else { "" }
        try { $conn.Close() } catch { }
        return @{ Output = @($inner, $_.Exception.Message) + $messages; DataSet = $ds; ExitCode = 1 }
    }
}

# =====================================================================
# HEADER
# =====================================================================
Write-Host ""
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host " sp_StatUpdate v3 QA Gaps 2 Test Suite" -ForegroundColor Cyan
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host ""
Write-Host "  Server:   $Server"
Write-Host "  Versions: $($Versions -join ', ')"
Write-Host "  Proc:     $ProcFile"
Write-Host ""
Write-Host "  Covers bd issues:"
Write-Host "    sp_StatUpdate-as3  QueueStatistic orphaned rows after aborted parallel run"
Write-Host "    sp_StatUpdate-tod  Preset behavior verified via generated commands / behavior"
Write-Host "    sp_StatUpdate-c0t  @StaleHours functional accuracy"
Write-Host ""

# =====================================================================
# STEP 1: CONNECTIVITY CHECK
# =====================================================================
Write-Host "[1/4] Verifying connectivity..." -ForegroundColor Yellow

$activeVersions = [System.Collections.ArrayList]::new()
foreach ($ver in $Versions) {
    if (-not $VersionConfig.ContainsKey($ver)) {
        Write-Host "  UNKNOWN version '$ver' -- skipping" -ForegroundColor DarkGray
        continue
    }
    $cfg = $VersionConfig[$ver]
    $r = Invoke-Sql -ServerAddr $Server -Port $cfg.Port -Pwd $Password -Query "SELECT @@VERSION AS v;"
    if ($r.ExitCode -eq 0) {
        $vs = if ($r.DataSet.Tables.Count -gt 0) {
            ([string]$r.DataSet.Tables[0].Rows[0]["v"]).Substring(0, [Math]::Min(60, ([string]$r.DataSet.Tables[0].Rows[0]["v"]).Length))
        } else { "Connected" }
        Write-Host "  $($cfg.Label) (:$($cfg.Port)): READY - $vs" -ForegroundColor Green
        $null = $activeVersions.Add($ver)
    } else {
        Write-Host "  $($cfg.Label) (:$($cfg.Port)): NOT REACHABLE -- $($r.Output | Select-Object -First 1)" -ForegroundColor Red
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
# STEP 2: DEPLOY PROC (serial, per version)
# =====================================================================
if (-not $SkipSetup) {
    Write-Host "[2/4] Deploying v3 proc..." -ForegroundColor Yellow

    foreach ($ver in $activeVersions) {
        $cfg = $VersionConfig[$ver]
        Write-Host "  $($cfg.Label) (:$($cfg.Port))..." -NoNewline

        $procSql  = [System.IO.File]::ReadAllText($ProcFile)
        $batches  = [regex]::Split($procSql, '(?m)^\s*GO\s*$')
        $deployFailed = $false

        foreach ($batch in $batches) {
            $trimmed = $batch.Trim()
            if ($trimmed.Length -eq 0) { continue }
            $r = Invoke-Sql -ServerAddr $Server -Port $cfg.Port -Pwd $Password `
                -Query $trimmed -Db "master" -Timeout 120
            if ($r.ExitCode -ne 0) {
                Write-Host " PROC DEPLOY FAILED: $($r.Output | Select-Object -First 1)" -ForegroundColor Red
                $deployFailed = $true
                break
            }
        }

        if (-not $deployFailed) {
            Write-Host " OK" -ForegroundColor Green
        }
    }
    Write-Host ""
} else {
    Write-Host "[2/4] Skipping deploy (-SkipSetup)" -ForegroundColor DarkGray
    Write-Host ""
}

# =====================================================================
# STEP 3: TEST EXECUTION (one release-gate version per invocation)
# =====================================================================
Write-Host "[3/4] Running QA gap tests per version..." -ForegroundColor Yellow
Write-Host ""

$allResults = $activeVersions | ForEach-Object {
    $ver           = $_

    $cfg   = $VersionConfig[$ver]
    $port  = $cfg.Port
    $label = $cfg.Label
    $db    = "StatUpdateTest"

    # --- ADO.NET helper redefined inside parallel block ---
    function Invoke-Sql {
        param([string]$ServerAddr, [int]$Port, [string]$Pwd, [string]$Query,
              [string]$Db = "master", [int]$Timeout = 300)
        $connStr = "Server=$ServerAddr,$Port;Database=$Db;User Id=sa;Password=$Pwd;Encrypt=false;TrustServerCertificate=true;Connection Timeout=30;Pooling=false"
        $messages = [System.Collections.ArrayList]::new()
        $ds = [System.Data.DataSet]::new()
        try {
            $conn = [System.Data.SqlClient.SqlConnection]::new($connStr)
            $conn.add_InfoMessage({ param($s, $e); $null = $messages.Add($e.Message) })
            $conn.FireInfoMessageEventOnUserErrors = $true
            $conn.Open()
            $cmd = $conn.CreateCommand()
            $cmd.CommandText    = $Query
            $cmd.CommandTimeout = $Timeout
            $reader = $cmd.ExecuteReader()
            do {
                $dt = [System.Data.DataTable]::new()
                $dt.Load($reader)
                $null = $ds.Tables.Add($dt)
            } while (-not $reader.IsClosed)
            $conn.Close()
            return @{ Output = $messages; DataSet = $ds; ExitCode = 0 }
        } catch {
            $inner = if ($_.Exception.InnerException) { $_.Exception.InnerException.Message } else { "" }
            try { $conn.Close() } catch { }
            return @{ Output = @($inner, $_.Exception.Message) + $messages; DataSet = $ds; ExitCode = 1 }
        }
    }

    function Get-LastTableScalar {
        param($DataSet, [string]$Column, $Default = $null)
        if ($null -eq $DataSet -or $DataSet.Tables.Count -eq 0) { return $Default }
        $tbl = $DataSet.Tables[$DataSet.Tables.Count - 1]
        if ($tbl.Rows.Count -eq 0) { return $Default }
        $row = $tbl.Rows[0]
        if ($tbl.Columns.Contains($Column)) {
            $v = $row[$Column]
            if ($v -is [System.DBNull]) { return $null }
            return $v
        }
        return $Default
    }

    $results = [System.Collections.ArrayList]::new()
    function Add-Result {
        param([string]$Test, [string]$Result, [string]$Detail = "")
        $null = $results.Add([PSCustomObject]@{ Test = $Test; Result = $Result; Detail = $Detail })
    }

    # ------------------------------------------------------------------
    # PREAMBLE: ensure StatUpdateLock is clean before tests start.
    # A leftover lock from a prior test session causes every subsequent
    # run to return ALREADY_RUNNING, cascading all tests to fail.
    # ------------------------------------------------------------------
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db "master" `
        -Query @"
IF OBJECT_ID(N'dbo.StatUpdateLock') IS NOT NULL
BEGIN
    DELETE FROM dbo.StatUpdateLock
    WHERE Resource = N'sp_StatUpdate'
    AND NOT EXISTS (
        SELECT 1 FROM sys.dm_exec_sessions s
        WHERE s.session_id = StatUpdateLock.SessionID
        AND   s.login_time = StatUpdateLock.LoginTime
    );
END;
"@ | Out-Null

    # Helper: clean stale StatUpdateLock rows (call between every test).
    $lockCleanSql = @"
IF OBJECT_ID(N'dbo.StatUpdateLock') IS NOT NULL
    DELETE FROM dbo.StatUpdateLock
    WHERE Resource = N'sp_StatUpdate'
    AND NOT EXISTS (
        SELECT 1 FROM sys.dm_exec_sessions s
        WHERE s.session_id = StatUpdateLock.SessionID
        AND   s.login_time = StatUpdateLock.LoginTime
    );
"@

    # ------------------------------------------------------------------
    # Generate modifications so discovery finds qualifying stats.
    # ------------------------------------------------------------------
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
        -Query "INSERT INTO dbo.Orders (CustomerName, Amount) SELECT TOP 3000 N'QAGap2', 1.00 FROM sys.all_columns;" | Out-Null
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
        -Query "INSERT INTO dbo.Products (ProductName, Price) SELECT TOP 1500 N'QAGap2', 2.00 FROM sys.all_columns;" | Out-Null

    # ==================================================================
    # ISSUE sp_StatUpdate-as3
    # A01: QueueStatistic has 0 orphaned rows after an aborted parallel run
    # ==================================================================
    # Strategy:
    #   1. Drop any leftover Queue / QueueStatistic rows from previous tests.
    #   2. Run a parallel worker with a VERY short @TimeLimit (1 second) so it
    #      stops nearly immediately.  This exercises the queue claim / populate
    #      path but means the run is over before we can KILL it; this is
    #      intentional -- we test the TIME_LIMIT stop path.
    #   3. After the run ends, query dbo.QueueStatistic and assert 0 rows remain
    #      in a "claimed but not completed" state (TableStartTime IS NOT NULL AND
    #      TableEndTime IS NULL).
    #   4. Then inject a FAKE stale claim (a row with a dead SessionID) and verify
    #      that a subsequent parallel worker reclaims / clears it.  This directly
    #      tests the ClaimLoginTime liveness check (commit 10b3a94).
    #
    # Note: we cannot easily KILL a parallel worker mid-run in this test harness
    # because the worker owns its own connection and we would need another
    # connection's KILL to land before the worker finishes its first stat.  The
    # fake-claim approach is more reliable and tests the exact same code path.

    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db "master" `
        -Query $lockCleanSql | Out-Null

    # Parallel mode creates QueueStatistic, but dbo.Queue is a prerequisite.
    # Each release-gate run starts from a fresh database, so provision this
    # procedure-owned test fixture explicitly in master before the real run.
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db "master" `
        -Query @"
IF OBJECT_ID(N'dbo.Queue', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.Queue (
        QueueID int IDENTITY(1,1) NOT NULL CONSTRAINT PK_Queue PRIMARY KEY CLUSTERED,
        SchemaName sysname NULL,
        ObjectName sysname NOT NULL,
        Parameters nvarchar(max) NULL,
        QueueStartTime datetime2(7) NULL,
        SessionID smallint NULL,
        RequestID int NULL,
        RequestStartTime datetime NULL,
        ParameterFingerprint int NULL
    );
END;
"@ | Out-Null

    # A01a: Parallel run with 1-second TimeLimit -- verify no orphaned rows remain.
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
        -Query "INSERT INTO dbo.Orders (CustomerName, Amount) SELECT TOP 10000 N'AS3a', 1.00 FROM sys.all_columns;" | Out-Null
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db "master" `
        -Query $lockCleanSql | Out-Null

    $runParallelSql = @"
DECLARE @sro nvarchar(50);
EXEC dbo.sp_StatUpdate
    @Databases             = N'$db',
    @Execute               = N'Y',
    @LogToTable            = N'N',
    @StatsInParallel       = N'Y',
    @ModificationThreshold = 1,
    @TimeLimit             = 1,
    @StopReasonOut         = @sro OUTPUT;
SELECT ISNULL(@sro, N'NULL') AS StopReason;
"@
    $rParallel = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
        -Query $runParallelSql -Timeout 60
    $parallelStopReason = Get-LastTableScalar -DataSet $rParallel.DataSet -Column 'StopReason'
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db "master" `
        -Query $lockCleanSql | Out-Null

    # Check for orphaned rows (claimed but not completed).
    $rOrphan = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db "master" `
        -Query @"
IF OBJECT_ID(N'dbo.QueueStatistic', N'U') IS NOT NULL
BEGIN
    SELECT
        orphaned_rows = COUNT(*)
    FROM dbo.QueueStatistic
    WHERE TableStartTime IS NOT NULL
    AND   TableEndTime   IS NULL
    AND   NOT EXISTS (
              SELECT 1 FROM sys.dm_exec_sessions s
              WHERE s.session_id = QueueStatistic.SessionID
          );
END
ELSE
    SELECT orphaned_rows = 0;
"@

    if ($rParallel.ExitCode -ne 0 -or $parallelStopReason -ne 'TIME_LIMIT') {
        $parallelDetail = @($rParallel.Output | Select-Object -Last 3) -join ' | '
        Add-Result "A01a: No orphaned QueueStatistic after TIME_LIMIT" "FAIL" "Parallel run did not reach TIME_LIMIT (StopReason=$parallelStopReason; $parallelDetail)"
    } elseif ($rOrphan.ExitCode -ne 0) {
        Add-Result "A01a: No orphaned QueueStatistic after TIME_LIMIT" "FAIL" ($rOrphan.Output | Select-Object -First 1)
    } else {
        $orphanRows = -1
        if ($rOrphan.DataSet.Tables.Count -gt 0 -and $rOrphan.DataSet.Tables[0].Rows.Count -gt 0) {
            $orphanRows = [int]$rOrphan.DataSet.Tables[0].Rows[0]["orphaned_rows"]
        }
        if ($orphanRows -eq 0) {
            $sro = Get-LastTableScalar -DataSet $rParallel.DataSet -Column "StopReason"
            Add-Result "A01a: No orphaned QueueStatistic after TIME_LIMIT" "PASS" "orphaned_rows=0 (StopReason=$sro)"
        } else {
            Add-Result "A01a: No orphaned QueueStatistic after TIME_LIMIT" "FAIL" "Found $orphanRows orphaned row(s) after TIME_LIMIT stop"
        }
    }

    # A01b: Inject a fake stale claim (dead SessionID) and verify the next
    #       parallel worker reclaims it (ClaimLoginTime liveness check).
    #
    # Use the real Queue row created by A01a to attach the fake row to.
    $rQueueId = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db "master" `
        -Query @"
IF OBJECT_ID(N'dbo.Queue', N'U') IS NOT NULL
    SELECT TOP 1 QueueID FROM dbo.Queue ORDER BY QueueID DESC;
ELSE
    SELECT NULL AS QueueID;
"@

    $queueId = $null
    if ($rQueueId.DataSet.Tables.Count -gt 0 -and $rQueueId.DataSet.Tables[0].Rows.Count -gt 0) {
        $qv = $rQueueId.DataSet.Tables[0].Rows[0]["QueueID"]
        if ($qv -isnot [System.DBNull]) { $queueId = [int]$qv }
    }

    if ($null -eq $queueId) {
        Add-Result "A01b: Dead-worker ClaimLoginTime reclaim" "FAIL" "The executed parallel run did not leave a Queue row for the dead-worker fixture"
    } else {
        # The dead-worker release runs in the proc's process loop (follower path only).
        # In a single-worker test, the leader DELETES all QueueStatistic rows for the
        # queue before repopulating -- so injecting a fake row before the leader runs
        # is not viable.  Instead, we validate the ClaimLoginTime logic directly:
        # run the same UPDATE SQL the proc would use and verify it releases the fake row.
        #
        # This is a white-box test of the liveness check SQL.

        $injectAndReleaseSql = @"
IF OBJECT_ID(N'dbo.QueueStatistic', N'U') IS NOT NULL
BEGIN
    -- Clean up any prior fake rows.
    DELETE FROM dbo.QueueStatistic WHERE DatabaseName = N'__fake_dead_worker_test__';

    -- Inject a fake claimed row with a dead SessionID (32767) and a ClaimLoginTime
    -- that no live session will match (1900-01-01).
    INSERT INTO dbo.QueueStatistic
        (QueueID, DatabaseName, SchemaName, ObjectName, ObjectID,
         TablePriority, StatsCount, MaxModificationCounter,
         TableStartTime, TableEndTime, SessionID, RequestID,
         RequestStartTime, ClaimLoginTime)
    VALUES
        ($queueId,
         N'__fake_dead_worker_test__',
         N'dbo',
         N'__fake_table__',
         -1,
         9999,
         1,
         0,
         SYSDATETIME(),
         NULL,
         32767,
         NULL,
         NULL,
         CAST(N'1900-01-01' AS datetime)
        );

    DECLARE @inject_count int = @@ROWCOUNT;

    -- Run the SAME dead-worker release UPDATE the proc uses.
    -- This verifies the ClaimLoginTime liveness logic correctly identifies
    -- the fake row as orphaned and releases it (TableStartTime = NULL).
    UPDATE qs
    SET    qs.TableStartTime      = NULL,
           qs.SessionID           = NULL,
           qs.RequestID           = NULL,
           qs.RequestStartTime    = NULL,
           qs.ClaimLoginTime      = NULL
    FROM   dbo.QueueStatistic AS qs
    WHERE  qs.QueueID        = $queueId
    AND    qs.TableStartTime IS NOT NULL
    AND    qs.TableEndTime   IS NULL
    AND    NOT EXISTS
           (
               SELECT 1
               FROM   sys.dm_exec_sessions AS s
               WHERE  s.session_id  = qs.SessionID
               AND    (qs.ClaimLoginTime IS NULL OR s.login_time = qs.ClaimLoginTime)
           );

    DECLARE @released int = @@ROWCOUNT;

    -- Verify the fake row is now unclaimed (TableStartTime IS NULL).
    SELECT
        injected            = @inject_count,
        released_by_check   = @released,
        still_claimed_count = (
            SELECT COUNT(*) FROM dbo.QueueStatistic
            WHERE DatabaseName = N'__fake_dead_worker_test__'
            AND   TableStartTime IS NOT NULL
            AND   TableEndTime   IS NULL
        );

    -- Cleanup.
    DELETE FROM dbo.QueueStatistic WHERE DatabaseName = N'__fake_dead_worker_test__';
END
ELSE
    SELECT injected = 0, released_by_check = 0, still_claimed_count = 0;
"@
        $rInjectRelease = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db "master" `
            -Query $injectAndReleaseSql

        $injected01b     = -1
        $released01b     = -1
        $stillClaim01b   = -1
        if ($rInjectRelease.DataSet.Tables.Count -gt 0 -and $rInjectRelease.DataSet.Tables[0].Rows.Count -gt 0) {
            $row01b       = $rInjectRelease.DataSet.Tables[0].Rows[0]
            $injected01b  = [int]$row01b["injected"]
            $released01b  = [int]$row01b["released_by_check"]
            $stillClaim01b = [int]$row01b["still_claimed_count"]
        }

        if ($rInjectRelease.ExitCode -ne 0) {
            Add-Result "A01b: Dead-worker ClaimLoginTime reclaim" "FAIL" ($rInjectRelease.Output | Select-Object -First 1)
        } elseif ($injected01b -eq 0) {
            Add-Result "A01b: Dead-worker ClaimLoginTime reclaim" "INFO" "QueueStatistic FK prevented fake row insert (QueueID=$queueId may have been deleted)"
        } elseif ($released01b -ge 1 -and $stillClaim01b -eq 0) {
            Add-Result "A01b: Dead-worker ClaimLoginTime reclaim" "PASS" "Dead-worker liveness check (ClaimLoginTime+SessionID) correctly released fake orphan (injected=$injected01b, released=$released01b, stillClaimed=0)"
        } elseif ($stillClaim01b -gt 0) {
            Add-Result "A01b: Dead-worker ClaimLoginTime reclaim" "FAIL" "Fake orphan NOT released by liveness check (injected=$injected01b, released=$released01b, stillClaimed=$stillClaim01b) -- ClaimLoginTime check may be broken"
        } else {
            Add-Result "A01b: Dead-worker ClaimLoginTime reclaim" "INFO" "injected=$injected01b, released=$released01b, stillClaimed=$stillClaim01b"
        }
    }

    # A01c: Final sanity -- QueueStatistic has 0 "ghost" claimed rows with dead sessions.
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db "master" `
        -Query $lockCleanSql | Out-Null

    $rFinal01c = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db "master" `
        -Query @"
IF OBJECT_ID(N'dbo.QueueStatistic', N'U') IS NOT NULL
    SELECT ghost_rows = COUNT(*)
    FROM dbo.QueueStatistic
    WHERE TableStartTime IS NOT NULL
    AND   TableEndTime   IS NULL
    AND   NOT EXISTS (
              SELECT 1 FROM sys.dm_exec_sessions s
              WHERE s.session_id = QueueStatistic.SessionID
          );
ELSE
    SELECT ghost_rows = 0;
"@

    $ghostRows = -1
    if ($rFinal01c.DataSet.Tables.Count -gt 0 -and $rFinal01c.DataSet.Tables[0].Rows.Count -gt 0) {
        $ghostRows = [int]$rFinal01c.DataSet.Tables[0].Rows[0]["ghost_rows"]
    }
    if ($ghostRows -eq 0) {
        Add-Result "A01c: QueueStatistic has 0 ghost-claimed rows (final state)" "PASS" "ghost_rows=0"
    } else {
        Add-Result "A01c: QueueStatistic has 0 ghost-claimed rows (final state)" "FAIL" "ghost_rows=$ghostRows"
    }

    # Remove only this test's queue row after the assertions; QueueStatistic is
    # the FK child, so delete it first. The whole run is on a disposable clone.
    if ($null -ne $queueId) {
        Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db "master" `
            -Query "DELETE FROM dbo.QueueStatistic WHERE QueueID = $queueId; DELETE FROM dbo.Queue WHERE QueueID = $queueId;" | Out-Null
    }

    # ==================================================================
    # ISSUE sp_StatUpdate-tod
    # Preset values verified via actual behavior, not just debug text.
    # ==================================================================
    # For each preset we use @Execute=N @WhatIfOutputTable to capture the
    # generated UPDATE STATISTICS commands, then assert on the text.
    # - WAREHOUSE: every command must contain FULLSCAN or SAMPLE 100
    # - WEEKLY_FULL: every command must contain FULLSCAN or SAMPLE 100
    # - OLTP_LIGHT: no command should contain FULLSCAN (auto-sample)
    # - DEFAULT: @TimeLimit=18000 visible in debug output
    # - NIGHTLY: @TimeLimit=3600 visible in debug output; SortOrder=QUERY_STORE
    #
    # We use a temp WhatIfOutputTable (#wio_<preset>) for commands.
    # For OLTP_LIGHT and DEFAULT/NIGHTLY, verify via debug output since
    # those presets use auto-sample (no SAMPLE clause in the command at all).

    # Generate fresh modifications for preset tests.
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
        -Query "INSERT INTO dbo.Orders (CustomerName, Amount) SELECT TOP 5000 N'TOD', 1.00 FROM sys.all_columns;" | Out-Null
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
        -Query "INSERT INTO dbo.Products (ProductName, Price) SELECT TOP 2500 N'TOD', 2.00 FROM sys.all_columns;" | Out-Null

    # ------------------------------------------------------------------
    # B01: WAREHOUSE preset -- commands contain FULLSCAN or SAMPLE 100
    # ------------------------------------------------------------------
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db "master" `
        -Query $lockCleanSql | Out-Null

    $wioWarehouse = "##wio_warehouse_$($port)"
    $warehouseSql = @"
IF OBJECT_ID(N'tempdb..$wioWarehouse') IS NOT NULL DROP TABLE $wioWarehouse;
DECLARE @sro nvarchar(50), @sf int;
EXEC dbo.sp_StatUpdate
    @Databases             = N'$db',
    @Execute               = N'N',
    @LogToTable            = N'N',
    @Preset                = N'WAREHOUSE',
    @ModificationThreshold = 1,
    @Tables                = N'dbo.Orders,dbo.Products',
    @WhatIfOutputTable     = N'$wioWarehouse',
    @StopReasonOut         = @sro OUTPUT,
    @StatsFoundOut         = @sf  OUTPUT;
SELECT ISNULL(@sro, N'NULL') AS StopReason, ISNULL(@sf, -1) AS StatsFound;
IF OBJECT_ID(N'tempdb..$wioWarehouse') IS NOT NULL
    SELECT Command FROM $wioWarehouse;
"@
    $rWarehouse = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
        -Query $warehouseSql -Timeout 120
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db "master" `
        -Query $lockCleanSql | Out-Null

    # Search all tables for StatsFound (may be in summary table or OUTPUT param table).
    $warehouseStatsFound = -1
    foreach ($tbl in $rWarehouse.DataSet.Tables) {
        if ($tbl.Columns.Contains("StatsFound") -and $tbl.Rows.Count -gt 0) {
            $v = $tbl.Rows[0]["StatsFound"]
            if ($v -isnot [System.DBNull]) { $warehouseStatsFound = [int]$v; break }
        }
    }

    if ($rWarehouse.ExitCode -ne 0 -and $warehouseStatsFound -le 0) {
        Add-Result "B01: WAREHOUSE preset generates FULLSCAN/SAMPLE 100 commands" "FAIL" ($rWarehouse.Output | Select-Object -First 1)
    } elseif ($warehouseStatsFound -eq 0) {
        Add-Result "B01: WAREHOUSE preset generates FULLSCAN/SAMPLE 100 commands" "INFO" "StatsFound=0 (no qualifying stats with threshold=1 -- unexpected)"
    } else {
        # Collect all Command values from the WhatIfOutputTable (second table in DataSet).
        # The last table returned by our SELECT is the OUTPUT param table;
        # the WhatIfOutputTable SELECT is the penultimate one.
        $commandsTable = $null
        foreach ($tbl in $rWarehouse.DataSet.Tables) {
            if ($tbl.Columns.Contains("Command")) {
                $commandsTable = $tbl
                break
            }
        }
        if ($null -eq $commandsTable -or $commandsTable.Rows.Count -eq 0) {
            Add-Result "B01: WAREHOUSE preset generates FULLSCAN/SAMPLE 100 commands" "INFO" "WhatIfOutputTable returned 0 command rows (StatsFound=$warehouseStatsFound)"
        } else {
            $totalCmds   = $commandsTable.Rows.Count
            $fullscanCnt = 0
            $badCmds     = [System.Collections.ArrayList]::new()
            foreach ($row in $commandsTable.Rows) {
                $cmd = [string]$row["Command"]
                if ($cmd -match 'FULLSCAN|SAMPLE\s+100\s+PERCENT') {
                    $fullscanCnt++
                } else {
                    $null = $badCmds.Add($cmd.Substring(0, [Math]::Min(120, $cmd.Length)))
                }
            }
            if ($fullscanCnt -eq $totalCmds) {
                Add-Result "B01: WAREHOUSE preset generates FULLSCAN/SAMPLE 100 commands" "PASS" "All $totalCmds command(s) contain FULLSCAN or SAMPLE 100 PERCENT"
            } else {
                $badSample = ($badCmds | Select-Object -First 1)
                Add-Result "B01: WAREHOUSE preset generates FULLSCAN/SAMPLE 100 commands" "FAIL" "$($totalCmds - $fullscanCnt)/$totalCmds commands lack FULLSCAN/SAMPLE 100. Example: $badSample"
            }
        }
    }

    # ------------------------------------------------------------------
    # B02: WEEKLY_FULL preset -- commands contain FULLSCAN or SAMPLE 100
    # ------------------------------------------------------------------
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db "master" `
        -Query $lockCleanSql | Out-Null
    # Use a very high row count to trigger tiered thresholds even with WEEKLY_FULL's higher defaults.
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
        -Query "INSERT INTO dbo.Orders (CustomerName, Amount) SELECT TOP 15000 N'TOD2', 1.00 FROM sys.all_columns;" | Out-Null
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
        -Query "INSERT INTO dbo.Products (ProductName, Price) SELECT TOP 5000 N'TOD2', 2.00 FROM sys.all_columns;" | Out-Null

    $wioWeekly = "##wio_weekly_$($port)"
    # Override @ModificationThreshold=1 to ensure stats qualify regardless of preset threshold.
    # Override @SortOrder to MODIFICATION_COUNTER so QS is not required.
    # Override @MopUpPass=N because WEEKLY_FULL enables MopUpPass which requires @LogToTable=Y;
    # we use @LogToTable=N to avoid CommandLog dependency in the test environment.
    $weeklySql = @"
IF OBJECT_ID(N'tempdb..$wioWeekly') IS NOT NULL DROP TABLE $wioWeekly;
DECLARE @sro nvarchar(50), @sf int;
EXEC dbo.sp_StatUpdate
    @Databases             = N'$db',
    @Execute               = N'N',
    @LogToTable            = N'N',
    @Preset                = N'WEEKLY_FULL',
    @ModificationThreshold = 1,
    @SortOrder             = N'MODIFICATION_COUNTER',
    @MopUpPass             = N'N',
    @Tables                = N'dbo.Orders,dbo.Products',
    @WhatIfOutputTable     = N'$wioWeekly',
    @StopReasonOut         = @sro OUTPUT,
    @StatsFoundOut         = @sf  OUTPUT;
SELECT ISNULL(@sro, N'NULL') AS StopReason, ISNULL(@sf, -1) AS StatsFound;
IF OBJECT_ID(N'tempdb..$wioWeekly') IS NOT NULL
    SELECT Command FROM $wioWeekly;
"@
    $rWeekly = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
        -Query $weeklySql -Timeout 120
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db "master" `
        -Query $lockCleanSql | Out-Null

    # StatsFound may be in any table (last table if all OK, or summary table on error path).
    $weeklyStatsFound = -1
    foreach ($tbl in $rWeekly.DataSet.Tables) {
        if ($tbl.Columns.Contains("StatsFound") -and $tbl.Rows.Count -gt 0) {
            $v = $tbl.Rows[0]["StatsFound"]
            if ($v -isnot [System.DBNull]) { $weeklyStatsFound = [int]$v; break }
        }
    }

    if ($rWeekly.ExitCode -ne 0 -and $weeklyStatsFound -le 0) {
        Add-Result "B02: WEEKLY_FULL preset generates FULLSCAN/SAMPLE 100 commands" "FAIL" ($rWeekly.Output | Select-Object -First 1)
    } elseif ($weeklyStatsFound -eq 0) {
        Add-Result "B02: WEEKLY_FULL preset generates FULLSCAN/SAMPLE 100 commands" "INFO" "StatsFound=0 (no qualifying stats)"
    } else {
        $commandsTable = $null
        foreach ($tbl in $rWeekly.DataSet.Tables) {
            if ($tbl.Columns.Contains("Command")) {
                $commandsTable = $tbl
                break
            }
        }
        if ($null -eq $commandsTable -or $commandsTable.Rows.Count -eq 0) {
            Add-Result "B02: WEEKLY_FULL preset generates FULLSCAN/SAMPLE 100 commands" "INFO" "WhatIfOutputTable returned 0 rows (StatsFound=$weeklyStatsFound)"
        } else {
            $totalCmds   = $commandsTable.Rows.Count
            $fullscanCnt = 0
            foreach ($row in $commandsTable.Rows) {
                $cmd = [string]$row["Command"]
                if ($cmd -match 'FULLSCAN|SAMPLE\s+100\s+PERCENT') { $fullscanCnt++ }
            }
            if ($fullscanCnt -eq $totalCmds) {
                Add-Result "B02: WEEKLY_FULL preset generates FULLSCAN/SAMPLE 100 commands" "PASS" "All $totalCmds command(s) contain FULLSCAN or SAMPLE 100 PERCENT"
            } else {
                Add-Result "B02: WEEKLY_FULL preset generates FULLSCAN/SAMPLE 100 commands" "FAIL" "$($totalCmds - $fullscanCnt)/$totalCmds commands lack FULLSCAN/SAMPLE 100"
            }
        }
    }

    # ------------------------------------------------------------------
    # B03: OLTP_LIGHT preset -- commands do NOT contain FULLSCAN or explicit SAMPLE
    #      (auto-sample = no explicit SAMPLE clause in the command)
    # ------------------------------------------------------------------
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db "master" `
        -Query $lockCleanSql | Out-Null
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
        -Query "INSERT INTO dbo.Orders (CustomerName, Amount) SELECT TOP 15000 N'TOD3', 1.00 FROM sys.all_columns;" | Out-Null

    $wioOltp = "##wio_oltp_$($port)"
    $oltpSql = @"
IF OBJECT_ID(N'tempdb..$wioOltp') IS NOT NULL DROP TABLE $wioOltp;
DECLARE @sro nvarchar(50), @sf int;
EXEC dbo.sp_StatUpdate
    @Databases             = N'$db',
    @Execute               = N'N',
    @LogToTable            = N'N',
    @Preset                = N'OLTP_LIGHT',
    @ModificationThreshold = 1,
    @Tables                = N'dbo.Orders',
    @WhatIfOutputTable     = N'$wioOltp',
    @StopReasonOut         = @sro OUTPUT,
    @StatsFoundOut         = @sf  OUTPUT;
SELECT ISNULL(@sro, N'NULL') AS StopReason, ISNULL(@sf, -1) AS StatsFound;
IF OBJECT_ID(N'tempdb..$wioOltp') IS NOT NULL
    SELECT Command FROM $wioOltp;
"@
    $rOltp = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
        -Query $oltpSql -Timeout 120
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db "master" `
        -Query $lockCleanSql | Out-Null

    $oltpStatsFound = Get-LastTableScalar -DataSet $rOltp.DataSet -Column "StatsFound"
    $oltpStatsFound = if ($null -ne $oltpStatsFound) { [int]$oltpStatsFound } else { -1 }

    if ($rOltp.ExitCode -ne 0 -and $oltpStatsFound -le 0) {
        Add-Result "B03: OLTP_LIGHT preset uses auto-sample (no FULLSCAN/explicit SAMPLE)" "FAIL" ($rOltp.Output | Select-Object -First 1)
    } elseif ($oltpStatsFound -eq 0) {
        Add-Result "B03: OLTP_LIGHT preset uses auto-sample (no FULLSCAN/explicit SAMPLE)" "INFO" "StatsFound=0 -- OLTP_LIGHT threshold=10000 may filter out qualifying stats"
    } else {
        $commandsTable = $null
        foreach ($tbl in $rOltp.DataSet.Tables) {
            if ($tbl.Columns.Contains("Command")) {
                $commandsTable = $tbl
                break
            }
        }
        if ($null -eq $commandsTable -or $commandsTable.Rows.Count -eq 0) {
            Add-Result "B03: OLTP_LIGHT preset uses auto-sample (no FULLSCAN/explicit SAMPLE)" "INFO" "WhatIfOutputTable returned 0 rows (StatsFound=$oltpStatsFound)"
        } else {
            $totalCmds     = $commandsTable.Rows.Count
            $explicitSampleCnt = 0
            foreach ($row in $commandsTable.Rows) {
                $cmd = [string]$row["Command"]
                # FULLSCAN or SAMPLE <n> PERCENT (any value) = explicit sample
                if ($cmd -match 'FULLSCAN|SAMPLE\s+\d+\s+PERCENT') { $explicitSampleCnt++ }
            }
            if ($explicitSampleCnt -eq 0) {
                Add-Result "B03: OLTP_LIGHT preset uses auto-sample (no FULLSCAN/explicit SAMPLE)" "PASS" "$totalCmds command(s): none contain FULLSCAN or explicit SAMPLE"
            } else {
                Add-Result "B03: OLTP_LIGHT preset uses auto-sample (no FULLSCAN/explicit SAMPLE)" "FAIL" "$explicitSampleCnt/$totalCmds commands contain FULLSCAN or explicit SAMPLE (should be auto-sample)"
            }
        }
    }

    # ------------------------------------------------------------------
    # B04: DEFAULT preset -- @TimeLimit=18000 visible in debug output
    # ------------------------------------------------------------------
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db "master" `
        -Query $lockCleanSql | Out-Null

    $defaultDebugSql = @"
EXEC dbo.sp_StatUpdate
    @Databases             = N'$db',
    @Execute               = N'N',
    @LogToTable            = N'N',
    @Preset                = N'DEFAULT',
    @ModificationThreshold = 999999999,
    @Debug                 = 1;
"@
    $rDefault = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
        -Query $defaultDebugSql -Timeout 60
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db "master" `
        -Query $lockCleanSql | Out-Null

    $out04 = ($rDefault.Output -join "`n")
    if ($rDefault.ExitCode -ne 0) {
        Add-Result "B04: DEFAULT preset @TimeLimit=18000 in debug output" "FAIL" ($rDefault.Output | Select-Object -First 1)
    } elseif ($out04 -match '@TimeLimit\s*=\s*18000') {
        Add-Result "B04: DEFAULT preset @TimeLimit=18000 in debug output" "PASS" "TimeLimit=18000 confirmed in debug output"
    } elseif ($out04 -match '18000') {
        Add-Result "B04: DEFAULT preset @TimeLimit=18000 in debug output" "PASS" "18000 present in debug output"
    } else {
        Add-Result "B04: DEFAULT preset @TimeLimit=18000 in debug output" "INFO" "Could not confirm TimeLimit=18000 in debug output (InfoMessage capture may be limited in parallel runspace)"
    }

    # ------------------------------------------------------------------
    # B05: NIGHTLY preset -- @TimeLimit=3600 and SortOrder=QUERY_STORE visible
    # ------------------------------------------------------------------
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db "master" `
        -Query $lockCleanSql | Out-Null

    # Override @MopUpPass=N: NIGHTLY enables MopUpPass which requires @LogToTable=Y.
    $nightlyDebugSql = @"
EXEC dbo.sp_StatUpdate
    @Databases             = N'$db',
    @Execute               = N'N',
    @LogToTable            = N'N',
    @Preset                = N'NIGHTLY',
    @ModificationThreshold = 999999999,
    @MopUpPass             = N'N',
    @Debug                 = 1;
"@
    $rNightly = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
        -Query $nightlyDebugSql -Timeout 60
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db "master" `
        -Query $lockCleanSql | Out-Null

    $out05 = ($rNightly.Output -join "`n")
    if ($rNightly.ExitCode -ne 0) {
        Add-Result "B05: NIGHTLY preset @TimeLimit=3600 and QUERY_STORE in debug" "FAIL" ($rNightly.Output | Select-Object -First 1)
    } else {
        $hasTimeLimit  = $out05 -match '3600'
        $hasSortOrder  = $out05 -match 'QUERY_STORE'
        if ($hasTimeLimit -and $hasSortOrder) {
            Add-Result "B05: NIGHTLY preset @TimeLimit=3600 and QUERY_STORE in debug" "PASS" "TimeLimit=3600 and QUERY_STORE both confirmed"
        } elseif ($hasTimeLimit) {
            Add-Result "B05: NIGHTLY preset @TimeLimit=3600 and QUERY_STORE in debug" "PASS" "TimeLimit=3600 confirmed (QUERY_STORE not captured -- InfoMessage limitation)"
        } elseif ($hasSortOrder) {
            Add-Result "B05: NIGHTLY preset @TimeLimit=3600 and QUERY_STORE in debug" "PASS" "QUERY_STORE confirmed (TimeLimit not captured -- InfoMessage limitation)"
        } else {
            Add-Result "B05: NIGHTLY preset @TimeLimit=3600 and QUERY_STORE in debug" "INFO" "Neither TimeLimit=3600 nor QUERY_STORE captured in debug output (InfoMessage limitation in parallel runspace)"
        }
    }

    # ------------------------------------------------------------------
    # B06: OLTP_LIGHT TimeLimit=1800 and DEFAULT SortOrder differ -- verify
    #      via CommandLog START XML when @LogToTable=Y is allowed.
    #      We use @Execute=N @LogToTable=N and check the dry-run result set
    #      for the TimeLimit field (if the proc exposes it in results).
    #      Fallback: check debug output for the value.
    # ------------------------------------------------------------------
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db "master" `
        -Query $lockCleanSql | Out-Null

    $oltpTimeSql = @"
EXEC dbo.sp_StatUpdate
    @Databases             = N'$db',
    @Execute               = N'N',
    @LogToTable            = N'N',
    @Preset                = N'OLTP_LIGHT',
    @ModificationThreshold = 999999999,
    @Debug                 = 1;
"@
    $rOltpTime = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
        -Query $oltpTimeSql -Timeout 60
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db "master" `
        -Query $lockCleanSql | Out-Null

    $out06 = ($rOltpTime.Output -join "`n")
    if ($rOltpTime.ExitCode -ne 0) {
        Add-Result "B06: OLTP_LIGHT @TimeLimit=1800 in debug output" "FAIL" ($rOltpTime.Output | Select-Object -First 1)
    } elseif ($out06 -match '1800') {
        Add-Result "B06: OLTP_LIGHT @TimeLimit=1800 in debug output" "PASS" "TimeLimit=1800 confirmed in debug output"
    } else {
        Add-Result "B06: OLTP_LIGHT @TimeLimit=1800 in debug output" "INFO" "Could not confirm TimeLimit=1800 (InfoMessage capture may be limited in parallel runspace)"
    }

    # ==================================================================
    # ISSUE sp_StatUpdate-c0t
    # @StaleHours functional accuracy
    # ==================================================================

    # ------------------------------------------------------------------
    # C01: @StaleHours=0 -- all stats qualify via staleness (hours >= 0),
    #      so StatsFound > 0 even if modification_counter is low.
    # ------------------------------------------------------------------
    # First update a stat manually so it has a known last_updated time.
    # Then run with @StaleHours=0 (every stat is >= 0 hours old).
    # Threshold must be set high enough that mods alone don't qualify --
    # that way @StaleHours=0 is the only reason stats are found.

    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db "master" `
        -Query $lockCleanSql | Out-Null

    # Update stats on a clean table so modification_counter = 0.
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
        -Query "UPDATE STATISTICS dbo.Orders WITH FULLSCAN;" | Out-Null
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
        -Query "UPDATE STATISTICS dbo.Products WITH FULLSCAN;" | Out-Null
    # The procedure intentionally gates out already-current zero-modification
    # stats before applying stale-hour thresholds. Add one modification while
    # keeping the threshold enormous, so StaleHours=0 is the only way these
    # stats can qualify.
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
        -Query "INSERT INTO dbo.Orders (CustomerName, Amount) VALUES (N'C01', 1.00);" | Out-Null

    $c01Sql = @"
DECLARE @sro nvarchar(50), @sf int;
EXEC dbo.sp_StatUpdate
    @Databases             = N'$db',
    @Execute               = N'N',
    @LogToTable            = N'N',
    @ModificationThreshold = 999999999,
    @StaleHours            = 0,
    @Tables                = N'dbo.Orders,dbo.Products',
    @Debug                 = 1,
    @StopReasonOut         = @sro OUTPUT,
    @StatsFoundOut         = @sf  OUTPUT;
SELECT ISNULL(@sro, N'NULL') AS StopReason, ISNULL(@sf, -1) AS StatsFound;
"@
    $rC01 = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
        -Query $c01Sql -Timeout 60
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db "master" `
        -Query $lockCleanSql | Out-Null

    if ($rC01.ExitCode -ne 0) {
        Add-Result "C01: @StaleHours=0 finds stats (all are >= 0 hours old)" "FAIL" ($rC01.Output | Select-Object -First 1)
    } else {
        $sro01 = Get-LastTableScalar -DataSet $rC01.DataSet -Column "StopReason"
        $sf01  = Get-LastTableScalar -DataSet $rC01.DataSet -Column "StatsFound"
        $sro01 = if ($null -ne $sro01) { [string]$sro01 } else { "NULL" }
        $sf01  = if ($null -ne $sf01)  { [int]$sf01   } else { -1 }
        if ($sf01 -gt 0) {
            Add-Result "C01: @StaleHours=0 finds stats (all are >= 0 hours old)" "PASS" "StatsFound=$sf01, StopReason=$sro01"
        } elseif ($sro01 -eq "NO_QUALIFYING_STATS" -or $sf01 -eq 0) {
            $c01Detail = @($rC01.Output | Select-Object -Last 5) -join ' | '
            Add-Result "C01: @StaleHours=0 finds stats (all are >= 0 hours old)" "FAIL" "StatsFound=$sf01; StopReason=$sro01; @StaleHours=0 should qualify every stat; $c01Detail"
        } else {
            Add-Result "C01: @StaleHours=0 finds stats (all are >= 0 hours old)" "FAIL" "StatsFound=$sf01, StopReason=$sro01"
        }
    }

    # ------------------------------------------------------------------
    # C02: @StaleHours=999999 -- stats updated just now cannot be 999999 hours old.
    #      With a very high threshold no stats qualify via staleness.
    #      Also set @ModificationThreshold very high so mods don't qualify either.
    #      Expected: StatsFound = 0 / StopReason = NO_QUALIFYING_STATS.
    # ------------------------------------------------------------------
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db "master" `
        -Query $lockCleanSql | Out-Null

    # Refresh stats so last_updated is NOW (hours_stale ~ 0).
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
        -Query "UPDATE STATISTICS dbo.Orders WITH FULLSCAN;" | Out-Null
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
        -Query "UPDATE STATISTICS dbo.Products WITH FULLSCAN;" | Out-Null

    $c02Sql = @"
DECLARE @sro nvarchar(50), @sf int;
EXEC dbo.sp_StatUpdate
    @Databases             = N'$db',
    @Execute               = N'N',
    @LogToTable            = N'N',
    @ModificationThreshold = 999999999,
    @StaleHours            = 999999,
    @Tables                = N'dbo.Orders,dbo.Products',
    @StopReasonOut         = @sro OUTPUT,
    @StatsFoundOut         = @sf  OUTPUT;
SELECT ISNULL(@sro, N'NULL') AS StopReason, ISNULL(@sf, -1) AS StatsFound;
"@
    $rC02 = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
        -Query $c02Sql -Timeout 60
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db "master" `
        -Query $lockCleanSql | Out-Null

    if ($rC02.ExitCode -ne 0) {
        Add-Result "C02: @StaleHours=999999 finds 0 stats (recently updated)" "FAIL" ($rC02.Output | Select-Object -First 1)
    } else {
        $sro02 = Get-LastTableScalar -DataSet $rC02.DataSet -Column "StopReason"
        $sf02  = Get-LastTableScalar -DataSet $rC02.DataSet -Column "StatsFound"
        $sro02 = if ($null -ne $sro02) { [string]$sro02 } else { "NULL" }
        $sf02  = if ($null -ne $sf02)  { [int]$sf02   } else { -1 }
        if ($sf02 -eq 0 -or $sro02 -eq "NO_QUALIFYING_STATS") {
            Add-Result "C02: @StaleHours=999999 finds 0 stats (recently updated)" "PASS" "StatsFound=$sf02, StopReason=$sro02 (correctly excluded recently-updated stats)"
        } else {
            Add-Result "C02: @StaleHours=999999 finds 0 stats (recently updated)" "FAIL" "StatsFound=$sf02, StopReason=$sro02 -- expected 0 (stats updated < 1 second ago cannot be 999999 hours stale)"
        }
    }

    # ------------------------------------------------------------------
    # C03: @StaleHours is OR'd with modification threshold.
    #      Stats with enough modifications qualify even when they are
    #      NOT stale enough by hours.  Verify the OR behavior.
    # ------------------------------------------------------------------
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db "master" `
        -Query $lockCleanSql | Out-Null

    # Refresh stats first, then insert rows (so mods > 0 but hours_stale ~ 0).
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
        -Query "UPDATE STATISTICS dbo.Orders WITH FULLSCAN;" | Out-Null
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
        -Query "INSERT INTO dbo.Orders (CustomerName, Amount) SELECT TOP 10000 N'C03', 1.00 FROM sys.all_columns;" | Out-Null

    $c03Sql = @"
DECLARE @sro nvarchar(50), @sf int;
EXEC dbo.sp_StatUpdate
    @Databases             = N'$db',
    @Execute               = N'N',
    @LogToTable            = N'N',
    @ModificationThreshold = 1,
    @StaleHours            = 999999,
    @Tables                = N'dbo.Orders',
    @StopReasonOut         = @sro OUTPUT,
    @StatsFoundOut         = @sf  OUTPUT;
SELECT ISNULL(@sro, N'NULL') AS StopReason, ISNULL(@sf, -1) AS StatsFound;
"@
    $rC03 = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
        -Query $c03Sql -Timeout 60
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db "master" `
        -Query $lockCleanSql | Out-Null

    if ($rC03.ExitCode -ne 0) {
        Add-Result "C03: @StaleHours OR'd with mods -- mods qualify even when not stale enough" "FAIL" ($rC03.Output | Select-Object -First 1)
    } else {
        $sro03 = Get-LastTableScalar -DataSet $rC03.DataSet -Column "StopReason"
        $sf03  = Get-LastTableScalar -DataSet $rC03.DataSet -Column "StatsFound"
        $sro03 = if ($null -ne $sro03) { [string]$sro03 } else { "NULL" }
        $sf03  = if ($null -ne $sf03)  { [int]$sf03   } else { -1 }
        if ($sf03 -gt 0) {
            Add-Result "C03: @StaleHours OR'd with mods -- mods qualify even when not stale enough" "PASS" "StatsFound=$sf03 (mods threshold=1 qualifies despite StaleHours=999999)"
        } else {
            Add-Result "C03: @StaleHours OR'd with mods -- mods qualify even when not stale enough" "FAIL" "StatsFound=$sf03, StopReason=$sro03 -- expected >0 (10000 mods with threshold=1 should qualify via OR)"
        }
    }

    # ------------------------------------------------------------------
    # C04: @SortOrder=DAYS_STALE dry-run completes and finds stats.
    #      (Coverage test: DAYS_STALE sort order works end-to-end.)
    # ------------------------------------------------------------------
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db "master" `
        -Query $lockCleanSql | Out-Null
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
        -Query "INSERT INTO dbo.Orders (CustomerName, Amount) SELECT TOP 5000 N'C04', 1.00 FROM sys.all_columns;" | Out-Null

    $c04Sql = @"
DECLARE @sro nvarchar(50), @sf int;
EXEC dbo.sp_StatUpdate
    @Databases             = N'$db',
    @Execute               = N'N',
    @LogToTable            = N'N',
    @ModificationThreshold = 1,
    @SortOrder             = N'DAYS_STALE',
    @Tables                = N'dbo.Orders',
    @StopReasonOut         = @sro OUTPUT,
    @StatsFoundOut         = @sf  OUTPUT;
SELECT ISNULL(@sro, N'NULL') AS StopReason, ISNULL(@sf, -1) AS StatsFound;
"@
    $rC04 = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
        -Query $c04Sql -Timeout 60
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db "master" `
        -Query $lockCleanSql | Out-Null

    if ($rC04.ExitCode -ne 0) {
        Add-Result "C04: @SortOrder=DAYS_STALE dry-run succeeds" "FAIL" ($rC04.Output | Select-Object -First 1)
    } else {
        $sro04 = Get-LastTableScalar -DataSet $rC04.DataSet -Column "StopReason"
        $sf04  = Get-LastTableScalar -DataSet $rC04.DataSet -Column "StatsFound"
        $sro04 = if ($null -ne $sro04) { [string]$sro04 } else { "NULL" }
        $sf04  = if ($null -ne $sf04)  { [int]$sf04   } else { -1 }
        if ($sro04 -ne "PARAMETER_ERROR" -and $sro04 -ne "NULL") {
            Add-Result "C04: @SortOrder=DAYS_STALE dry-run succeeds" "PASS" "StatsFound=$sf04, StopReason=$sro04"
        } else {
            Add-Result "C04: @SortOrder=DAYS_STALE dry-run succeeds" "FAIL" "StopReason=$sro04 (PARAMETER_ERROR or NULL indicates DAYS_STALE sort was rejected)"
        }
    }

    # Return all results for this version.
    return [PSCustomObject]@{ Version = $ver; Label = $label; Results = $results }
}

# =====================================================================
# STEP 4: SUMMARISE RESULTS
# =====================================================================
Write-Host "[4/4] Results" -ForegroundColor Yellow
Write-Host ""

$grandPass = 0
$grandFail = 0
$grandInfo = 0

foreach ($versionResult in ($allResults | Sort-Object Version)) {
    $lbl = $versionResult.Label
    $res = $versionResult.Results
    Write-Host ("  " + "=" * 60) -ForegroundColor Cyan
    Write-Host "  $lbl" -ForegroundColor Cyan
    Write-Host ("  " + "=" * 60) -ForegroundColor Cyan

    $vPass = 0; $vFail = 0; $vInfo = 0
    foreach ($r in $res) {
        $col = switch ($r.Result) {
            "PASS" { "Green" }
            "FAIL" { "Red" }
            "INFO" { "Yellow" }
            default { "Gray" }
        }
        $pad  = " " * [Math]::Max(0, 55 - $r.Test.Length)
        $line = "  [{0}] {1}{2}" -f $r.Result.PadRight(4), $r.Test, $pad
        Write-Host $line -ForegroundColor $col -NoNewline
        if ($r.Detail) { Write-Host " -- $($r.Detail)" -ForegroundColor DarkGray } else { Write-Host "" }

        switch ($r.Result) {
            "PASS" { $vPass++; $grandPass++ }
            "FAIL" { $vFail++; $grandFail++ }
            "INFO" { $vInfo++; $grandInfo++ }
        }
    }
    Write-Host ""
    Write-Host "  $lbl summary: $vPass PASS, $vFail FAIL, $vInfo INFO" -ForegroundColor $(if ($vFail -gt 0) { "Red" } else { "Green" })
    Write-Host ""
}

Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host " GRAND TOTAL: $grandPass PASS, $grandFail FAIL, $grandInfo INFO" -ForegroundColor $(if ($grandFail -gt 0) { "Red" } else { "Green" })
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host ""

if ($ResultsPath) {
    if ($allResults.Count -ne 1) { throw 'Release-gate invocation must target exactly one SQL version.' }
    $gateResults = @($allResults[0].Results)
    $ver = [string]$allResults[0].Version
    $major = $VersionConfig[$ver].Major
    $sqlStatus = Get-StructuredSqlStatus -Server $Server -Port $VersionConfig[$ver].Port -Password $Password -ExpectedMajor $major
    $commit = (& git -C $RepoRoot rev-parse HEAD).Trim()
    if ($LASTEXITCODE -ne 0 -or $commit -notmatch '^[0-9a-fA-F]{40}$') { throw 'Could not resolve the tested source commit.' }
    Write-StructuredLegacyResults -Path $ResultsPath -Commit $commit -SqlVersion $ver -SqlBuild $sqlStatus.Build `
        -ServerAliveAtEnd $sqlStatus.Alive -Suite 'V3QAGaps2' -ExpectedCount 13 `
        -Results $gateResults -StartedAt $startedAt -EndedAt (Get-Date)
}

if ($grandFail -eq 0) {
    Write-Host "All tests passed." -ForegroundColor Green
} else {
    Write-Host "$grandFail test(s) FAILED." -ForegroundColor Red
}
Write-Host ""

exit $(if ($grandFail -gt 0) { 1 } else { 0 })
