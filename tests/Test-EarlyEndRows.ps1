<#
.SYNOPSIS
    Regression tests for tracking issue (early-return END rows),
    tracking issue (diag silent no-op detection), and tracking issue
    (early-rejection paths publish OUTPUT params, summary row, and the denial
    artifact BEFORE the severity-16 signal).  Tests E01-E04, K01-K03, G01-G04.

.DESCRIPTION
    rse2: four mid-run early returns (NO_QUALIFYING_STATS, FINGERPRINT_CONFLICT,
    MAX_WORKERS, QUEUE_INIT_ERROR) previously exited after SP_STATUPDATE_START
    without writing SP_STATUPDATE_END -- healthy runs were orphaned and later
    fabricated as KILLED.  E01-E03 drive three of the four paths end-to-end and
    assert a real END row with the true StopReason.  (MAX_WORKERS cannot be
    reached through the v3 public API -- @i_max_workers is preset-internal and
    no preset sets it -- so that path has compile-level coverage only.)
    E02 also covers the dead-code fix: the FINGERPRINT_CONFLICT severity-16
    RAISERROR used to fire inside a TRY with an empty CATCH, so the worker
    silently JOINED the queue instead of refusing with RETURN 50001.

    kzx0: synthetic CommandLog fixtures reproduce the tracking issue
    zombie-queue signature (PARALLEL_COMPLETE, StatsFound > 0, StatsProcessed
    = 0).  K01/K02 assert the new C6 SILENT_NOOP_RUN critical, W3
    un-suppression, honest CompletionPct, and the closed COMPLETION grade-A
    hole.  K03 asserts no false positive for a healthy fleet with a late
    joiner (3j5l no-regression).

.PARAMETER Server
    Remote Docker host IP.  Default: 100.88.59.72.

.PARAMETER Password
    SA password.  Default: $env:SPSTATUPDATE_TEST_SQL_PASSWORD.

.PARAMETER Versions
    Which SQL versions to test.  Default: 2019, 2022, 2025.

.PARAMETER SkipSetup
    Skip proc/diag deployment and test DB setup (assumes already deployed).

.EXAMPLE
    .\tests\Test-EarlyEndRows.ps1 -Versions @("2022")
#>
[CmdletBinding()]
param(
    [string]$Server   = "127.0.0.1",
    [string]$Password = $(if ($env:SPSTATUPDATE_TEST_SQL_PASSWORD) { $env:SPSTATUPDATE_TEST_SQL_PASSWORD } else { "$env:SPSTATUPDATE_TEST_SQL_PASSWORD" }),
    [string[]]$Versions = @("2019", "2022", "2025"),
    [switch]$SkipSetup,
    [string]$ResultsPath = ""
)

$ErrorActionPreference = "Continue"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot  = Split-Path -Parent $ScriptDir

$ProcFile  = Join-Path $RepoRoot "sp_StatUpdate.sql"
$DiagFile  = Join-Path $RepoRoot "sp_StatUpdate_Diag.sql"
$SetupFile = Join-Path $ScriptDir "setup_multiversion_db.sql"
$script:RunStartedAt = Get-Date
$script:LastRecordAt = $script:RunStartedAt

$VersionConfig = @{
    "2019" = @{ Label = "SQL 2019"; Port = 1434 }
    "2022" = @{ Label = "SQL 2022"; Port = 1432 }
    "2025" = @{ Label = "SQL 2025"; Port = 1431 }
}

function Invoke-Sql {
    param(
        [string]$ServerAddr, [int]$Port, [string]$Pwd,
        [string]$Query, [string]$Db = "master", [int]$Timeout = 300
    )
    $connStr = "Server=$ServerAddr,$Port;Database=$Db;User Id=sa;Password=$Pwd;Encrypt=false;TrustServerCertificate=true;Connection Timeout=30;Pooling=false"
    $messages = [System.Collections.ArrayList]::new()
    try {
        $conn = [System.Data.SqlClient.SqlConnection]::new($connStr)
        $conn.add_InfoMessage({ param($s, $e) $null = $messages.Add($e.Message) })
        $conn.FireInfoMessageEventOnUserErrors = $true
        $conn.Open()
        $cmd = $conn.CreateCommand()
        $cmd.CommandText = $Query
        $cmd.CommandTimeout = $Timeout
        $ds = [System.Data.DataSet]::new()
        $reader = $cmd.ExecuteReader()
        do {
            $dt = [System.Data.DataTable]::new()
            $dt.Load($reader)
            $null = $ds.Tables.Add($dt)
        } while (-not $reader.IsClosed)
        $conn.Close()
        return @{ Output = $messages; DataSet = $ds; ExitCode = 0 }
    } catch {
        try { $conn.Close() } catch { }
        $inner = if ($_.Exception.InnerException) { $_.Exception.InnerException.Message } else { "" }
        return @{ Output = @($inner, $_.Exception.Message) + $messages; DataSet = $null; ExitCode = 1 }
    }
}

function Deploy-SqlFile {
    param([string]$ServerAddr, [int]$Port, [string]$Pwd, [string]$Path)
    $sql = [System.IO.File]::ReadAllText($Path)
    $batches = [regex]::Split($sql, '(?mi)^\s*GO\s*$') | Where-Object { $_.Trim().Length -gt 0 }
    $connStr = "Server=$ServerAddr,$Port;Database=master;User Id=sa;Password=$Pwd;Encrypt=false;TrustServerCertificate=true;Connection Timeout=30;Pooling=false"
    $conn = [System.Data.SqlClient.SqlConnection]::new($connStr)
    $conn.Open()
    try {
        $n = 0
        foreach ($b in $batches) {
            $n++
            $cmd = $conn.CreateCommand()
            $cmd.CommandText = $b
            $cmd.CommandTimeout = 300
            $reader = $cmd.ExecuteReader()
            do {
                $dt = [System.Data.DataTable]::new()
                $dt.Load($reader)
            } while (-not $reader.IsClosed)
        }
        return $true
    } catch {
        $msg = if ($_.Exception.InnerException) { $_.Exception.InnerException.Message } else { $_.Exception.Message }
        Write-Host "    DEPLOY FAIL ($(Split-Path -Leaf $Path), batch $n): $msg" -ForegroundColor Red
        return $false
    } finally {
        $conn.Close()
    }
}

# Find DataSet tables containing all named columns.
# Returns an ArrayList so PowerShell never unrolls a lone DataTable into DataRows.
function Find-Tables {
    param([System.Data.DataSet]$Ds, [string[]]$Columns)
    $hits = [System.Collections.ArrayList]::new()
    if ($null -eq $Ds) { return , $hits }
    foreach ($t in $Ds.Tables) {
        $all = $true
        foreach ($c in $Columns) {
            if (-not $t.Columns.Contains($c)) { $all = $false; break }
        }
        if ($all) { $null = $hits.Add($t) }
    }
    return , $hits
}

$script:Results = [System.Collections.ArrayList]::new()
function Record {
    param([string]$Ver, [string]$Id, [bool]$Pass, [string]$Detail)
    $recordedAt = Get-Date
    $durationMs = [long][Math]::Max(0, ($recordedAt - $script:LastRecordAt).TotalMilliseconds)
    $script:LastRecordAt = $recordedAt
    $null = $script:Results.Add(@{ Version = $Ver; Id = $Id; Pass = $Pass; Detail = $Detail; DurationMs = $durationMs })
    $color = if ($Pass) { "Green" } else { "Red" }
    $tag   = if ($Pass) { "PASS" } else { "FAIL" }
    Write-Host ("  {0}  [{1}] {2} -- {3}" -f $tag, $Ver, $Id, $Detail) -ForegroundColor $color
}

Write-Host ""
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host " Test-EarlyEndRows -- early-return and diagnostic no-op regressions" -ForegroundColor Cyan
Write-Host ("=" * 70) -ForegroundColor Cyan

foreach ($ver in $Versions) {
    $cfg = $VersionConfig[$ver]
    if (-not $cfg) { Write-Host "  SKIP unknown version $ver"; continue }
    $port  = $cfg.Port
    $label = $cfg.Label
    Write-Host ""
    Write-Host "--- $label (port $port) ---" -ForegroundColor Cyan

    $ping = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query "SELECT 1;" -Timeout 10
    if ($ping.ExitCode -ne 0) {
        Write-Host "  SKIP  [$ver] not reachable" -ForegroundColor DarkGray
        continue
    }

    # ------------------------------------------------------------------
    # SETUP
    # ------------------------------------------------------------------
    if (-not $SkipSetup) {
        Write-Host "  Deploying proc + diag + test DB ..."
        if (-not (Deploy-SqlFile -ServerAddr $Server -Port $port -Pwd $Password -Path $ProcFile))  { Record $ver "SETUP" $false "proc deploy failed"; continue }
        if (-not (Deploy-SqlFile -ServerAddr $Server -Port $port -Pwd $Password -Path $DiagFile))  { Record $ver "SETUP" $false "diag deploy failed"; continue }
        if (-not (Deploy-SqlFile -ServerAddr $Server -Port $port -Pwd $Password -Path $SetupFile)) { Record $ver "SETUP" $false "setup_multiversion_db failed"; continue }
    }

    $null = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
/* Parallel mode requires dbo.Queue to pre-exist (proc auto-creates QueueStatistic only) */
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
IF OBJECT_ID(N'dbo.EmptyStatTestProbe', N'U') IS NOT NULL DROP TABLE dbo.EmptyStatTestProbe;
IF DB_ID(N'EmptyStatTest') IS NULL CREATE DATABASE EmptyStatTest;
IF OBJECT_ID(N'dbo.StatUpdateDiagHistory', N'U') IS NOT NULL DROP TABLE dbo.StatUpdateDiagHistory;
IF OBJECT_ID(N'dbo.QueueStatistic', N'U') IS NOT NULL DELETE FROM dbo.QueueStatistic;
DELETE FROM dbo.Queue;
DELETE FROM dbo.CommandLog;
IF OBJECT_ID(N'dbo.StatUpdateLock', N'U') IS NOT NULL DELETE FROM dbo.StatUpdateLock;
"@

    # ------------------------------------------------------------------
    # E01: NO_QUALIFYING_STATS writes a real END row
    # ------------------------------------------------------------------
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
DECLARE @rc int;
EXEC @rc = dbo.sp_StatUpdate @Databases = N'EmptyStatTest', @Execute = N'Y', @LogToTable = N'Y';
SELECT ReturnCode = @rc;
SELECT
    StartCount = SUM(CASE WHEN CommandType = N'SP_STATUPDATE_START' THEN 1 ELSE 0 END),
    EndCount   = SUM(CASE WHEN CommandType = N'SP_STATUPDATE_END' THEN 1 ELSE 0 END),
    EndStopReason = MAX(CASE WHEN CommandType = N'SP_STATUPDATE_END'
        THEN ExtendedInfo.value(N'(/Summary/StopReason)[1]', N'nvarchar(50)') END)
FROM dbo.CommandLog;
"@
    $rs   = (Find-Tables $r.DataSet @("StopReason", "Status"))[0]
    $pair = (Find-Tables $r.DataSet @("StartCount", "EndCount"))[0]
    $rcT  = (Find-Tables $r.DataSet @("ReturnCode"))[0]
    $ok = ($null -ne $rs) -and ($rs.Rows[0].StopReason -eq "NO_QUALIFYING_STATS") -and
          ($null -ne $pair) -and ([int]$pair.Rows[0].StartCount -eq 1) -and ([int]$pair.Rows[0].EndCount -eq 1) -and
          ($pair.Rows[0].EndStopReason -eq "NO_QUALIFYING_STATS") -and
          ($null -ne $rcT) -and ([int]$rcT.Rows[0].ReturnCode -eq 0)
    $detail = if ($ok) { "START+END paired, END StopReason=NO_QUALIFYING_STATS, rc=0" }
              else { "rs=$($rs.Rows[0].StopReason ?? '?') start=$($pair.Rows[0].StartCount ?? '?') end=$($pair.Rows[0].EndCount ?? '?') endReason=$($pair.Rows[0].EndStopReason ?? '?')" }
    Record $ver "E01-NO_QUALIFYING_STATS" $ok $detail

    # ------------------------------------------------------------------
    # E02: FINGERPRINT_CONFLICT refuses the join, surfaces error, writes END
    # ------------------------------------------------------------------
    # Run A: creates queue + fingerprint, completes.
    # INSERTs (not UPDATEs): modification_counter tracks the leading stats
    # column, so updating Amount/Price bumps nothing -- inserts bump every stat.
    $null = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
USE StatUpdateTest;
INSERT INTO dbo.Orders (CustomerName, Amount)
SELECT TOP 2000 N'E02A', 1.00 FROM sys.all_columns;
INSERT INTO dbo.Products (ProductName, Price)
SELECT TOP 1000 N'E02A', 1.00 FROM sys.all_columns;
"@
    $ra = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
EXEC dbo.sp_StatUpdate @Databases = N'StatUpdateTest', @StatsInParallel = N'Y',
    @ModificationThreshold = 1, @Execute = N'Y', @LogToTable = N'Y';
"@
    $rsA = (Find-Tables $ra.DataSet @("StopReason", "RunLabel"))[0]
    $labelA = if ($rsA) { $rsA.Rows[0].RunLabel } else { "" }
    Start-Sleep -Seconds 2

    # Run B: same parameters_string, different fingerprint (@CriticalTables is fingerprint-only)
    $null = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
USE StatUpdateTest;
INSERT INTO dbo.Orders (CustomerName, Amount)
SELECT TOP 2000 N'E02B', 1.00 FROM sys.all_columns;
INSERT INTO dbo.Products (ProductName, Price)
SELECT TOP 1000 N'E02B', 1.00 FROM sys.all_columns;
"@
    $rb = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
DECLARE @rc int;
EXEC @rc = dbo.sp_StatUpdate @Databases = N'StatUpdateTest', @StatsInParallel = N'Y',
    @ModificationThreshold = 1, @CriticalTables = N'dbo.Orders', @Execute = N'Y', @LogToTable = N'Y';
SELECT ReturnCode = @rc;
"@
    $rsB = (Find-Tables $rb.DataSet @("StopReason", "Status"))[0]
    $rcB = (Find-Tables $rb.DataSet @("ReturnCode"))[0]
    $labelB = if ($rsB) { $rsB.Rows[0].RunLabel } else { "" }
    $errSurfaced = $false
    foreach ($m in $rb.Output) { if ("$m" -like "*Conflicting sp_StatUpdate parameters*") { $errSurfaced = $true } }

    $chk = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
SELECT
    EndConflict = SUM(CASE WHEN CommandType = N'SP_STATUPDATE_END'
        AND ExtendedInfo.value(N'(/Summary/StopReason)[1]', N'nvarchar(50)') = N'FINGERPRINT_CONFLICT' THEN 1 ELSE 0 END),
    BStatRows = SUM(CASE WHEN CommandType = N'UPDATE_STATISTICS'
        AND ExtendedInfo.value(N'(/ExtendedInfo/RunLabel)[1]', N'nvarchar(100)') = N'$labelB' THEN 1 ELSE 0 END)
FROM dbo.CommandLog;
"@
    $cT = (Find-Tables $chk.DataSet @("EndConflict", "BStatRows"))[0]
    $ok = ($null -ne $rsB) -and ($rsB.Rows[0].StopReason -eq "FINGERPRINT_CONFLICT") -and
          ($rsB.Rows[0].Status -eq "ERROR") -and
          ($null -ne $rcB) -and ([int]$rcB.Rows[0].ReturnCode -eq 50001) -and
          $errSurfaced -and
          ($null -ne $cT) -and ([int]$cT.Rows[0].EndConflict -ge 1) -and ([int]$cT.Rows[0].BStatRows -eq 0)
    $detail = if ($ok) { "join refused: rc=50001, error surfaced, END row written, 0 stats processed by worker B" }
              else { "rsB=$($rsB.Rows[0].StopReason ?? 'none') rc=$($rcB.Rows[0].ReturnCode ?? '?') err=$errSurfaced endRow=$($cT.Rows[0].EndConflict ?? '?') bRows=$($cT.Rows[0].BStatRows ?? '?')" }
    Record $ver "E02-FINGERPRINT_CONFLICT" $ok $detail

    # ------------------------------------------------------------------
    # E03: QUEUE_INIT_ERROR writes a real END row
    # ------------------------------------------------------------------
    $null = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
IF OBJECT_ID(N'dbo.QueueStatistic', N'U') IS NOT NULL DELETE FROM dbo.QueueStatistic;
DELETE FROM dbo.Queue;
EXEC(N'CREATE TRIGGER dbo.trg_TestBlockQueueInsert ON dbo.Queue INSTEAD OF INSERT AS BEGIN RAISERROR(N''test-induced queue breakage'', 16, 1); END');
"@
    $null = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
USE StatUpdateTest;
INSERT INTO dbo.Orders (CustomerName, Amount)
SELECT TOP 2000 N'E03', 1.00 FROM sys.all_columns;
"@
    $re = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
DECLARE @rc int;
EXEC @rc = dbo.sp_StatUpdate @Databases = N'StatUpdateTest', @StatsInParallel = N'Y',
    @ModificationThreshold = 1, @Execute = N'Y', @LogToTable = N'Y';
SELECT ReturnCode = @rc;
SELECT EndQIE = COUNT_BIG(*)
FROM dbo.CommandLog
WHERE CommandType = N'SP_STATUPDATE_END'
AND   ExtendedInfo.value(N'(/Summary/StopReason)[1]', N'nvarchar(50)') = N'QUEUE_INIT_ERROR';
"@
    $null = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query "DROP TRIGGER IF EXISTS dbo.trg_TestBlockQueueInsert;"
    $rsE = (Find-Tables $re.DataSet @("StopReason", "Status"))[0]
    $rcE = (Find-Tables $re.DataSet @("ReturnCode"))[0]
    $qT  = (Find-Tables $re.DataSet @("EndQIE"))[0]
    $ok = ($null -ne $rsE) -and ($rsE.Rows[0].StopReason -eq "QUEUE_INIT_ERROR") -and
          ($null -ne $rcE) -and ([int]$rcE.Rows[0].ReturnCode -eq -1) -and
          ($null -ne $qT) -and ([int64]$qT.Rows[0].EndQIE -ge 1)
    $detail = if ($ok) { "StopReason=QUEUE_INIT_ERROR, rc=-1, END row written" }
              else { "rs=$($rsE.Rows[0].StopReason ?? 'none') rc=$($rcE.Rows[0].ReturnCode ?? '?') endRows=$($qT.Rows[0].EndQIE ?? '?')" }
    Record $ver "E03-QUEUE_INIT_ERROR" $ok $detail

    # ------------------------------------------------------------------
    # E04: no orphaned STARTs across all E-tests
    # ------------------------------------------------------------------
    $rp = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
SELECT Orphans = COUNT_BIG(*)
FROM dbo.CommandLog AS s
WHERE s.CommandType = N'SP_STATUPDATE_START'
AND NOT EXISTS (
    SELECT 1 FROM dbo.CommandLog AS e
    WHERE e.CommandType = N'SP_STATUPDATE_END'
    AND   e.ExtendedInfo.value(N'(/Summary/RunLabel)[1]', N'nvarchar(100)') =
          s.ExtendedInfo.value(N'(/Parameters/RunLabel)[1]', N'nvarchar(100)')
);
"@
    $oT = (Find-Tables $rp.DataSet @("Orphans"))[0]
    $ok = ($null -ne $oT) -and ([int64]$oT.Rows[0].Orphans -eq 0)
    Record $ver "E04-NO_ORPHANED_STARTS" $ok ("orphaned STARTs: " + ($oT.Rows[0].Orphans ?? "?"))

    # ------------------------------------------------------------------
    # E05: Phase 3B delta must not double-count after a successful update
    #      (tracking issue).  A successful UPDATE STATISTICS resets the
    #      live modification_counter to 0, so the live counter ALREADY is
    #      mods-since-last-update.  The old Phase 3B formula subtracted the
    #      logged pre-update counter (effective = live - logged), which
    #      double-counts: when the post-update mod volume happens to equal
    #      the logged counter, delta = 0 and the stat was wrongly skipped as
    #      already-current (this is the root cause of the E03 reused-container
    #      flake).  Fix A: a matched SUCCESSFUL CommandLog row proves the
    #      reset, so effective_counter = raw counter.  Self-contained: builds
    #      its own heap table + one stat + a seeded CommandLog row whose
    #      logged ModificationCounter EXACTLY equals the live counter, so the
    #      old formula deterministically yields delta = 0.
    # ------------------------------------------------------------------
    $r5 = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
USE StatUpdateTest;
IF OBJECT_ID(N'dbo.DeltaProbe', N'U') IS NOT NULL DROP TABLE dbo.DeltaProbe;
CREATE TABLE dbo.DeltaProbe (v int NOT NULL);            /* heap, single stat, no PK/auto stats */
INSERT INTO dbo.DeltaProbe (v) SELECT TOP 3000 1 FROM sys.all_columns;
CREATE STATISTICS ST_DeltaProbe_v ON dbo.DeltaProbe (v); /* fresh stat -> modification_counter resets to 0, last_updated set */
INSERT INTO dbo.DeltaProbe (v) SELECT TOP 3000 2 FROM sys.all_columns; /* post-update mods accumulate on the live counter */

/* Seed the logged pre-update counter to EXACTLY the live counter so the old
   subtraction (live - logged) deterministically yields 0 regardless of drift. */
DECLARE @sid int = (SELECT stats_id FROM sys.stats WHERE object_id = OBJECT_ID(N'dbo.DeltaProbe') AND name = N'ST_DeltaProbe_v');
DECLARE @live bigint = (SELECT modification_counter FROM sys.dm_db_stats_properties(OBJECT_ID(N'dbo.DeltaProbe'), @sid));
DELETE FROM dbo.CommandLog;
INSERT INTO dbo.CommandLog (DatabaseName, SchemaName, ObjectName, ObjectType, Command, CommandType, StartTime, EndTime, ErrorNumber, StatisticsName, ExtendedInfo)
VALUES (N'StatUpdateTest', N'dbo', N'DeltaProbe', N'U', N'UPDATE STATISTICS test', N'UPDATE_STATISTICS',
    DATEADD(MINUTE, -60, SYSDATETIME()), DATEADD(MINUTE, -59, SYSDATETIME()), 0, N'ST_DeltaProbe_v',
    CONVERT(xml, N'<ExtendedInfo><ModificationCounter>' + CONVERT(nvarchar(20), @live) + N'</ModificationCounter></ExtendedInfo>'));

DECLARE @sf int = -1;
EXEC dbo.sp_StatUpdate @Databases = N'StatUpdateTest', @Tables = N'dbo.DeltaProbe',
    @Execute = N'N', @LogToTable = N'N', @ModificationThreshold = 1, @StatsFoundOut = @sf OUTPUT;
SELECT DeltaFound = @sf, LiveCounter = @live;
"@
    $d5 = (Find-Tables $r5.DataSet @("DeltaFound"))[0]
    $found5 = if ($d5) { [int]$d5.Rows[0].DeltaFound } else { -1 }
    $live5  = if ($d5) { "$($d5.Rows[0].LiveCounter)" } else { "?" }
    $ok = ($found5 -eq 1)
    $detail = if ($ok) { "effective=raw: stat qualifies (StatsFound=1, live=logged=$live5); double-count fixed" }
              else { "StatsFound=$found5 (pre-fix: delta=$live5-$live5=0 wrongly skipped the stat as current)" }
    Record $ver "E05-DELTA_NO_DOUBLE_COUNT" $ok $detail
    $null = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query "USE StatUpdateTest; IF OBJECT_ID(N'dbo.DeltaProbe', N'U') IS NOT NULL DROP TABLE dbo.DeltaProbe; DELETE FROM dbo.CommandLog;"

    # ------------------------------------------------------------------
    # kzx0 fixture builder
    # ------------------------------------------------------------------
    $fixtureZombie = @"
DELETE FROM dbo.CommandLog;
DECLARE @now datetime2(3) = SYSDATETIME(), @i int = 1, @t0 datetime2(3), @t1 datetime2(3), @lbl nvarchar(100);
WHILE @i <= 3
BEGIN
    SET @t0 = DATEADD(HOUR, -24 * @i, @now);
    SET @t1 = DATEADD(SECOND, 5, @t0);
    SET @lbl = N'zombie_run_' + CONVERT(nvarchar(10), @i);
    INSERT INTO dbo.CommandLog (DatabaseName, SchemaName, ObjectName, ObjectType, Command, CommandType, StartTime, ExtendedInfo)
    VALUES (N'StatUpdateTest', N'dbo', N'sp_StatUpdate', N'P', N'EXECUTE dbo.sp_StatUpdate', N'SP_STATUPDATE_START', @t0,
        CONVERT(xml, N'<Parameters><Version>3.5.7</Version><RunLabel>' + @lbl + N'</RunLabel><SessionID>99</SessionID><Databases>StatUpdateTest</Databases><TimeLimit>3600</TimeLimit><SortOrder>QUERY_STORE</SortOrder><StatsInParallel>Y</StatsInParallel></Parameters>'));
    INSERT INTO dbo.CommandLog (DatabaseName, SchemaName, ObjectName, ObjectType, Command, CommandType, StartTime, EndTime, ExtendedInfo)
    VALUES (N'StatUpdateTest', N'dbo', N'sp_StatUpdate', N'P', N'sp_StatUpdate completed: PARALLEL_COMPLETE', N'SP_STATUPDATE_END', @t0, @t1,
        CONVERT(xml, N'<Summary><Version>3.5.7</Version><RunLabel>' + @lbl + N'</RunLabel><SessionID>99</SessionID><StatsFound>58</StatsFound><StatsProcessed>0</StatsProcessed><StatsSucceeded>0</StatsSucceeded><StatsFailed>0</StatsFailed><StatsRemaining>{REMAINING}</StatsRemaining><DurationSeconds>5</DurationSeconds><StopReason>PARALLEL_COMPLETE</StopReason></Summary>'));
    SET @i += 1;
END;
"@

    # ------------------------------------------------------------------
    # K01: zombie fleet (StatsRemaining > 0) -> C6 + CompletionPct 0
    # tracking issue (maintainer decision 2026-07-30): when EVERY non-killed run in
    # the window confirmed StatsProcessed = 0 (this fixture: all 3), W3
    # STALE_BACKLOG defers to C6 -- throughput advice is nonsense for pure
    # no-op windows.  W3 must now be ABSENT here (pre-tracking issue it was
    # required present via the kzx0 un-suppression, which still applies to
    # mixed windows).
    # ------------------------------------------------------------------
    $null = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query ($fixtureZombie -replace '\{REMAINING\}', '58')
    $rd = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query "EXEC dbo.sp_StatUpdate_Diag @DaysBack = 14, @ExpertMode = 1, @SkipHistory = 1;"
    $recs = (Find-Tables $rd.DataSet @("Severity", "Category", "Finding"))[0]
    $c6row = $null; $w3row = $null
    if ($recs) {
        foreach ($row in $recs.Rows) {
            if ($row.Category -eq "SILENT_NOOP_RUN") { $c6row = $row }
            if ($row.Category -eq "STALE_BACKLOG")   { $w3row = $row }
        }
    }
    $compOk = $false
    foreach ($t in (Find-Tables $rd.DataSet @("RunLabel", "CompletionPct"))) {
        foreach ($row in $t.Rows) {
            if ("$($row.RunLabel)" -like "zombie_run_*" -and $row.CompletionPct -isnot [System.DBNull]) {
                if ([decimal]$row.CompletionPct -eq 0) { $compOk = $true }
                elseif ([decimal]$row.CompletionPct -eq 100) { $compOk = $false; break }
            }
        }
    }
    $ok = ($null -ne $c6row) -and ($c6row.Severity -eq "CRITICAL") -and ($c6row.Finding -like "3 run(s)*") -and
          ($null -eq $w3row) -and $compOk
    $detail = if ($ok) { "C6 CRITICAL fired (3 runs), W3 correctly deferred to C6 (all runs processed 0; tracking issue), zombie CompletionPct=0" }
              else { "c6=$(if ($c6row) { $c6row.Finding } else { 'MISSING' }) w3=$(if ($w3row) { 'present (should defer to C6; tracking issue)' } else { 'absent' }) comp0=$compOk" }
    Record $ver "K01-C6_ZOMBIE_REMAINING" $ok $detail

    # ------------------------------------------------------------------
    # K02: zombie joiners (StatsRemaining = 0) -> C6 + COMPLETION grade hole closed
    # ------------------------------------------------------------------
    $null = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query ($fixtureZombie -replace '\{REMAINING\}', '0')
    $rd2 = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query "EXEC dbo.sp_StatUpdate_Diag @DaysBack = 14, @ExpertMode = 1, @SkipHistory = 1;"
    $recs2 = (Find-Tables $rd2.DataSet @("Severity", "Category", "Finding"))[0]
    $c6b = $null
    if ($recs2) { foreach ($row in $recs2.Rows) { if ($row.Category -eq "SILENT_NOOP_RUN") { $c6b = $row } } }
    $dash = (Find-Tables $rd2.DataSet @("Category", "Grade", "Score"))[0]
    $compScore = $null
    if ($dash) { foreach ($row in $dash.Rows) { if ($row.Category -eq "COMPLETION") { $compScore = [int]$row.Score } } }
    $ok = ($null -ne $c6b) -and ($null -ne $compScore) -and ($compScore -lt 95)
    $detail = if ($ok) { "C6 fired, COMPLETION score=$compScore (<95; pre-fix fast path scored 95)" }
              else { "c6=$(if ($c6b) { 'present' } else { 'MISSING' }) completionScore=$($compScore ?? 'n/a')" }
    Record $ver "K02-C6_ZOMBIE_GRADE_HOLE" $ok $detail

    # ------------------------------------------------------------------
    # K03: healthy fleet + late joiner -> no C6, CompletionPct 100 (3j5l intact)
    # ------------------------------------------------------------------
    $null = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
DELETE FROM dbo.CommandLog;
DECLARE @now datetime2(3) = SYSDATETIME();
DECLARE @t0 datetime2(3) = DATEADD(HOUR, -24, @now);          /* fleet start */
DECLARE @tA datetime2(3) = DATEADD(MINUTE, 30, @t0);          /* worker A end */
DECLARE @tB datetime2(3) = DATEADD(MINUTE, 28, @t0);          /* worker B end */
DECLARE @tC0 datetime2(3) = DATEADD(MINUTE, 10, @t0);         /* late joiner start */
DECLARE @tC1 datetime2(3) = DATEADD(SECOND, 10, @tC0);        /* late joiner end */

/* worker A: processed 30 of 58 */
INSERT INTO dbo.CommandLog (DatabaseName, SchemaName, ObjectName, ObjectType, Command, CommandType, StartTime, ExtendedInfo)
VALUES (N'StatUpdateTest', N'dbo', N'sp_StatUpdate', N'P', N'EXECUTE dbo.sp_StatUpdate', N'SP_STATUPDATE_START', @t0,
    CONVERT(xml, N'<Parameters><Version>3.5.7</Version><RunLabel>healthy_A</RunLabel><Databases>StatUpdateTest</Databases><TimeLimit>3600</TimeLimit><SortOrder>QUERY_STORE</SortOrder><StatsInParallel>Y</StatsInParallel></Parameters>'));
INSERT INTO dbo.CommandLog (DatabaseName, SchemaName, ObjectName, ObjectType, Command, CommandType, StartTime, EndTime, ExtendedInfo)
VALUES (N'StatUpdateTest', N'dbo', N'sp_StatUpdate', N'P', N'sp_StatUpdate completed: PARALLEL_COMPLETE', N'SP_STATUPDATE_END', @t0, @tA,
    CONVERT(xml, N'<Summary><Version>3.5.7</Version><RunLabel>healthy_A</RunLabel><StatsFound>58</StatsFound><StatsProcessed>30</StatsProcessed><StatsSucceeded>30</StatsSucceeded><StatsFailed>0</StatsFailed><StatsRemaining>28</StatsRemaining><DurationSeconds>1800</DurationSeconds><StopReason>PARALLEL_COMPLETE</StopReason></Summary>'));

/* worker B: processed 28 of 58 */
INSERT INTO dbo.CommandLog (DatabaseName, SchemaName, ObjectName, ObjectType, Command, CommandType, StartTime, ExtendedInfo)
VALUES (N'StatUpdateTest', N'dbo', N'sp_StatUpdate', N'P', N'EXECUTE dbo.sp_StatUpdate', N'SP_STATUPDATE_START', DATEADD(SECOND, 3, @t0),
    CONVERT(xml, N'<Parameters><Version>3.5.7</Version><RunLabel>healthy_B</RunLabel><Databases>StatUpdateTest</Databases><TimeLimit>3600</TimeLimit><SortOrder>QUERY_STORE</SortOrder><StatsInParallel>Y</StatsInParallel></Parameters>'));
INSERT INTO dbo.CommandLog (DatabaseName, SchemaName, ObjectName, ObjectType, Command, CommandType, StartTime, EndTime, ExtendedInfo)
VALUES (N'StatUpdateTest', N'dbo', N'sp_StatUpdate', N'P', N'sp_StatUpdate completed: PARALLEL_COMPLETE', N'SP_STATUPDATE_END', DATEADD(SECOND, 3, @t0), @tB,
    CONVERT(xml, N'<Summary><Version>3.5.7</Version><RunLabel>healthy_B</RunLabel><StatsFound>58</StatsFound><StatsProcessed>28</StatsProcessed><StatsSucceeded>28</StatsSucceeded><StatsFailed>0</StatsFailed><StatsRemaining>30</StatsRemaining><DurationSeconds>1680</DurationSeconds><StopReason>PARALLEL_COMPLETE</StopReason></Summary>'));

/* late joiner C: found 58, processed 0, remaining 0 (skip-discovery joiner) */
INSERT INTO dbo.CommandLog (DatabaseName, SchemaName, ObjectName, ObjectType, Command, CommandType, StartTime, ExtendedInfo)
VALUES (N'StatUpdateTest', N'dbo', N'sp_StatUpdate', N'P', N'EXECUTE dbo.sp_StatUpdate', N'SP_STATUPDATE_START', @tC0,
    CONVERT(xml, N'<Parameters><Version>3.5.7</Version><RunLabel>healthy_C</RunLabel><Databases>StatUpdateTest</Databases><TimeLimit>3600</TimeLimit><SortOrder>QUERY_STORE</SortOrder><StatsInParallel>Y</StatsInParallel></Parameters>'));
INSERT INTO dbo.CommandLog (DatabaseName, SchemaName, ObjectName, ObjectType, Command, CommandType, StartTime, EndTime, ExtendedInfo)
VALUES (N'StatUpdateTest', N'dbo', N'sp_StatUpdate', N'P', N'sp_StatUpdate completed: PARALLEL_COMPLETE', N'SP_STATUPDATE_END', @tC0, @tC1,
    CONVERT(xml, N'<Summary><Version>3.5.7</Version><RunLabel>healthy_C</RunLabel><StatsFound>58</StatsFound><StatsProcessed>0</StatsProcessed><StatsSucceeded>0</StatsSucceeded><StatsFailed>0</StatsFailed><StatsRemaining>0</StatsRemaining><DurationSeconds>10</DurationSeconds><StopReason>PARALLEL_COMPLETE</StopReason></Summary>'));

/* UPDATE_STATISTICS activity for workers A and B through the window */
DECLARE @j int = 1;
WHILE @j <= 10
BEGIN
    INSERT INTO dbo.CommandLog (DatabaseName, SchemaName, ObjectName, ObjectType, Command, CommandType, StartTime, EndTime, ErrorNumber, StatisticsName, ExtendedInfo)
    VALUES (N'StatUpdateTest', N'dbo', N'Orders', N'U', N'UPDATE STATISTICS test', N'UPDATE_STATISTICS',
        DATEADD(MINUTE, 3 * @j, @t0), DATEADD(SECOND, 30, DATEADD(MINUTE, 3 * @j, @t0)), 0, N'ST_test_' + CONVERT(nvarchar(10), @j),
        CONVERT(xml, N'<ExtendedInfo><RunLabel>' + CASE WHEN @j % 2 = 0 THEN N'healthy_A' ELSE N'healthy_B' END + N'</RunLabel><Version>3.5.7</Version></ExtendedInfo>'));
    SET @j += 1;
END;
"@
    $rd3 = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query "EXEC dbo.sp_StatUpdate_Diag @DaysBack = 14, @ExpertMode = 1, @SkipHistory = 1;"
    $recs3 = (Find-Tables $rd3.DataSet @("Severity", "Category", "Finding"))[0]
    $c6c = $null
    if ($recs3) { foreach ($row in $recs3.Rows) { if ($row.Category -eq "SILENT_NOOP_RUN") { $c6c = $row } } }
    $comp100 = @{}
    foreach ($t in (Find-Tables $rd3.DataSet @("RunLabel", "CompletionPct"))) {
        foreach ($row in $t.Rows) {
            $lbl = "$($row.RunLabel)"
            if ($lbl -like "healthy_*" -and $row.CompletionPct -isnot [System.DBNull]) {
                $comp100[$lbl] = [decimal]$row.CompletionPct
            }
        }
    }
    $all100 = ($comp100.Count -ge 3) -and (($comp100.Values | Where-Object { $_ -ne 100 }).Count -eq 0)
    $ok = ($null -eq $c6c) -and $all100
    $detail = if ($ok) { "no C6 false positive; CompletionPct=100 for A/B/joiner (3j5l intact)" }
              else { "c6=$(if ($c6c) { 'FALSE POSITIVE' } else { 'none' }) completion=$(($comp100.GetEnumerator() | ForEach-Object { "$($_.Key)=$($_.Value)" }) -join ',')" }
    Record $ver "K03-HEALTHY_NO_FALSE_POSITIVE" $ok $detail

    # ------------------------------------------------------------------
    # K04 (2026-07-30): serial mop-up discovery actually FINDS seeded work.
    # The gh-462 @mop_up_where_sql quote-quadrupling broke serial/leader
    # mop-up discovery for ~3 months while every gate stayed green -- the
    # per-database CATCH swallowed the syntax error and 'no additional
    # stats found' looked normal.  This is a live end-to-end assertion:
    # tiny modifications (below tier thresholds so the priority pass finds
    # nothing) MUST surface via the mop-up sweep (MopUpFound > 0 in the END
    # Summary XML).  Guards both the WHERE-fragment quoting and the
    # empty-discovery @empty_disc_select shape (is_critical omission made
    # the same scenario die earlier with DB_SKIPPED).
    # ------------------------------------------------------------------
    $null = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
DELETE FROM dbo.CommandLog;
USE StatUpdateTest;
UPDATE TOP (10) dbo.Orders SET Amount = Amount;
UPDATE TOP (10) dbo.Products SET Price = Price;
"@
    $rk4 = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query "EXEC master.dbo.sp_StatUpdate @Databases = N'StatUpdateTest', @MopUpPass = N'Y', @TimeLimit = 300;"
    $rk4b = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
SELECT TOP (1)
    K4MopFound = ExtendedInfo.value(N'(Summary/MopUpFound)[1]', N'int'),
    K4Processed = ExtendedInfo.value(N'(Summary/StatsProcessed)[1]', N'int'),
    K4Skipped = CASE WHEN ExtendedInfo.value(N'(Summary/WarningsCodes)[1]', N'nvarchar(max)') LIKE N'%DB_SKIPPED%' THEN 1 ELSE 0 END
FROM dbo.CommandLog
WHERE CommandType = N'SP_STATUPDATE_END'
ORDER BY ID DESC;
"@
    $k4 = (Find-Tables $rk4b.DataSet @("K4MopFound"))[0]
    $k4found = if ($k4 -and $k4.Rows.Count -gt 0 -and $k4.Rows[0].K4MopFound -isnot [System.DBNull]) { [int]$k4.Rows[0].K4MopFound } else { -1 }
    $k4proc  = if ($k4 -and $k4.Rows.Count -gt 0 -and $k4.Rows[0].K4Processed -isnot [System.DBNull]) { [int]$k4.Rows[0].K4Processed } else { -1 }
    $k4skip  = if ($k4 -and $k4.Rows.Count -gt 0) { [int]$k4.Rows[0].K4Skipped } else { 1 }
    $ok = ($k4found -gt 0) -and ($k4proc -gt 0) -and ($k4skip -eq 0)
    $detail = if ($ok) { "mop-up discovered $k4found stat(s), processed $k4proc, no DB_SKIPPED (quote fix + empty-shape fix verified live)" }
              else { "mopUpFound=$k4found processed=$k4proc dbSkipped=$k4skip (mop-up discovery or empty-discovery shape regressed)" }
    Record $ver "K04-MOPUP_DISCOVERY_FINDS_WORK" $ok $detail

    # ------------------------------------------------------------------
    # G01-G04 (tracking issue / gh-555): early-rejection paths publish
    # outcomes BEFORE the severity-16 signal.  T-SQL limit locked in by G02:
    # OUTPUT params are never copied back when the caller's CATCH intercepts
    # the error, so TRY/CATCH callers key off the streamed summary row, the
    # denial CommandLog row, or ERROR_MESSAGE().
    # ------------------------------------------------------------------

    # G01: trancount guard, NO caller TRY/CATCH -> rc=1, OUTPUT params set,
    # summary row present, error message surfaced
    $rg1 = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
DECLARE @sr nvarchar(50), @rc int = -999, @sf int = -999;
BEGIN TRANSACTION;
EXEC @rc = dbo.sp_StatUpdate @Databases = N'StatUpdateTest', @Execute = N'N', @LogToTable = N'N',
    @StopReasonOut = @sr OUTPUT, @StatsFoundOut = @sf OUTPUT;
ROLLBACK TRANSACTION;
SELECT G1Rc = @rc, G1StopReason = @sr, G1Found = @sf;
"@
    $g1s  = (Find-Tables $rg1.DataSet @("Status", "StopReason"))[0]
    $g1o  = (Find-Tables $rg1.DataSet @("G1Rc"))[0]
    $g1msg = ($rg1.Output | Where-Object { "$_" -like "*transaction count is not 0*" }).Count -ge 1
    $ok = ($null -ne $g1s) -and ($g1s.Rows[0].StopReason -eq "PARAMETER_ERROR") -and
          ($null -ne $g1o) -and ([int]$g1o.Rows[0].G1Rc -eq 1) -and
          ("$($g1o.Rows[0].G1StopReason)" -eq "PARAMETER_ERROR") -and ([int]$g1o.Rows[0].G1Found -eq 0) -and $g1msg
    $detail = if ($ok) { "rc=1, @StopReasonOut=PARAMETER_ERROR, summary row streamed, error surfaced" }
              else { "rc=$($g1o.Rows[0].G1Rc ?? '?') srOut=$($g1o.Rows[0].G1StopReason ?? '?') summary=$(if ($g1s) { $g1s.Rows[0].StopReason } else { 'MISSING' }) msg=$g1msg" }
    Record $ver "G01-TRANCOUNT_NO_TRYCATCH" $ok $detail

    # G02: trancount guard WITH caller TRY/CATCH -> summary row still streamed
    # before the error; ERROR_MESSAGE carries the signal
    $rg2 = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
DECLARE @caught nvarchar(500) = N'';
BEGIN TRANSACTION;
BEGIN TRY
    EXEC dbo.sp_StatUpdate @Databases = N'StatUpdateTest', @Execute = N'N', @LogToTable = N'N';
END TRY
BEGIN CATCH
    SET @caught = ERROR_MESSAGE();
END CATCH;
ROLLBACK TRANSACTION;
SELECT G2Caught = @caught;
"@
    $g2s = (Find-Tables $rg2.DataSet @("Status", "StopReason"))[0]
    $g2c = (Find-Tables $rg2.DataSet @("G2Caught"))[0]
    $ok = ($null -ne $g2s) -and ($g2s.Rows[0].StopReason -eq "PARAMETER_ERROR") -and
          ($null -ne $g2c) -and ("$($g2c.Rows[0].G2Caught)" -like "*transaction count is not 0*")
    $detail = if ($ok) { "summary row reached TRY/CATCH caller (StopReason=PARAMETER_ERROR), CATCH got the message" }
              else { "summary=$(if ($g2s) { $g2s.Rows[0].StopReason } else { 'MISSING (pre-fix: suppressed by signal-first ordering)' }) caught=$($g2c.Rows[0].G2Caught ?? '?')" }
    Record $ver "G02-TRANCOUNT_TRYCATCH" $ok $detail

    # G03/G04 need a live lock holder: a second connection with an active
    # request (WAITFOR) whose (SessionID, LoginTime) we plant in StatUpdateLock.
    $holderConn = $null
    $holderReader = $null
    try {
        $holderConnStr = "Server=$Server,$port;Database=master;User Id=sa;Password=$Password;Encrypt=false;TrustServerCertificate=true;Connection Timeout=30;Pooling=false"
        $holderConn = [System.Data.SqlClient.SqlConnection]::new($holderConnStr)
        $holderConn.Open()
        $holderCmd = $holderConn.CreateCommand()
        $holderCmd.CommandTimeout = 300
        $holderCmd.CommandText = "SELECT HolderSid = @@SPID, HolderLt = CONVERT(varchar(23), s.login_time, 121) FROM sys.dm_exec_sessions AS s WHERE s.session_id = @@SPID; WAITFOR DELAY '00:02:00';"
        $holderReader = $holderCmd.ExecuteReader()
        $null = $holderReader.Read()
        $holderSid = [int]$holderReader["HolderSid"]
        $holderLt  = "$($holderReader['HolderLt'])"

        # G03: ALREADY_RUNNING WITH caller TRY/CATCH -> denial row persisted with
        # holder metadata + summary row streamed BEFORE control reaches CATCH
        $rg3 = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
IF OBJECT_ID(N'dbo.StatUpdateLock', N'U') IS NULL
    CREATE TABLE dbo.StatUpdateLock (
        Resource sysname NOT NULL, SessionID smallint NOT NULL,
        LoginTime datetime NOT NULL, AcquiredAt datetime2(3) NOT NULL DEFAULT SYSDATETIME(),
        CONSTRAINT PK_StatUpdateLock PRIMARY KEY CLUSTERED (Resource));
DELETE FROM dbo.StatUpdateLock;
INSERT INTO dbo.StatUpdateLock (Resource, SessionID, LoginTime, AcquiredAt)
VALUES (N'sp_StatUpdate', $holderSid, CONVERT(datetime, '$holderLt', 121), SYSDATETIME());
DELETE FROM dbo.CommandLog WHERE CommandType = N'SP_STATUPDATE_DENIED';
DECLARE @caught nvarchar(500) = N'';
BEGIN TRY
    EXEC dbo.sp_StatUpdate @Databases = N'StatUpdateTest', @Execute = N'N', @LogToTable = N'Y';
END TRY
BEGIN CATCH
    SET @caught = ERROR_MESSAGE();
END CATCH;
SELECT
    G3Caught = @caught,
    G3DeniedRows = (SELECT COUNT_BIG(*) FROM dbo.CommandLog WHERE CommandType = N'SP_STATUPDATE_DENIED'),
    G3HolderSid = (SELECT TOP (1) cl.ExtendedInfo.value(N'(/Denial/HolderSessionID)[1]', N'int')
                   FROM dbo.CommandLog AS cl WHERE cl.CommandType = N'SP_STATUPDATE_DENIED' ORDER BY cl.ID DESC);
"@
        $g3s = (Find-Tables $rg3.DataSet @("Status", "StopReason"))[0]
        $g3d = (Find-Tables $rg3.DataSet @("G3Caught"))[0]
        $ok = ($null -ne $g3d) -and ("$($g3d.Rows[0].G3Caught)" -like "*already running*") -and
              ([long]$g3d.Rows[0].G3DeniedRows -eq 1) -and ([int]$g3d.Rows[0].G3HolderSid -eq $holderSid) -and
              ($null -ne $g3s) -and ($g3s.Rows[0].StopReason -eq "ALREADY_RUNNING")
        $detail = if ($ok) { "denial row persisted (holder sid=$holderSid) + summary row streamed before CATCH" }
                  else { "caught=$($g3d.Rows[0].G3Caught ?? '?') denied=$($g3d.Rows[0].G3DeniedRows ?? '?') holderSid=$($g3d.Rows[0].G3HolderSid ?? '?') (want $holderSid) summary=$(if ($g3s) { $g3s.Rows[0].StopReason } else { 'MISSING' })" }
        Record $ver "G03-ALREADY_RUNNING_TRYCATCH" $ok $detail

        # G04: ALREADY_RUNNING WITHOUT TRY/CATCH -> unchanged contract: OUTPUT
        # params set, summary row, exactly one fresh denial row, error surfaced
        $rg4 = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
DELETE FROM dbo.CommandLog WHERE CommandType = N'SP_STATUPDATE_DENIED';
DECLARE @sr nvarchar(50);
EXEC dbo.sp_StatUpdate @Databases = N'StatUpdateTest', @Execute = N'N', @LogToTable = N'Y',
    @StopReasonOut = @sr OUTPUT;
SELECT
    G4StopReason = @sr,
    G4DeniedRows = (SELECT COUNT_BIG(*) FROM dbo.CommandLog WHERE CommandType = N'SP_STATUPDATE_DENIED');
DELETE FROM dbo.StatUpdateLock;
"@
        $g4s = (Find-Tables $rg4.DataSet @("Status", "StopReason"))[0]
        $g4o = (Find-Tables $rg4.DataSet @("G4StopReason"))[0]
        $g4msg = ($rg4.Output | Where-Object { "$_" -like "*already running*" }).Count -ge 1
        $ok = ($null -ne $g4o) -and ("$($g4o.Rows[0].G4StopReason)" -eq "ALREADY_RUNNING") -and
              ([long]$g4o.Rows[0].G4DeniedRows -eq 1) -and
              ($null -ne $g4s) -and ($g4s.Rows[0].StopReason -eq "ALREADY_RUNNING") -and $g4msg
        $detail = if ($ok) { "@StopReasonOut=ALREADY_RUNNING, summary row, 1 denial row, error surfaced" }
                  else { "srOut=$($g4o.Rows[0].G4StopReason ?? '?') denied=$($g4o.Rows[0].G4DeniedRows ?? '?') summary=$(if ($g4s) { $g4s.Rows[0].StopReason } else { 'MISSING' }) msg=$g4msg" }
        Record $ver "G04-ALREADY_RUNNING_NO_TRYCATCH" $ok $detail
    }
    finally {
        # Cancel the WAITFOR first -- Reader.Dispose alone would block until the
        # delay completes -- then dispose and clear the lock row
        try { if ($holderCmd) { $holderCmd.Cancel() } } catch { }
        try { if ($holderReader) { $holderReader.Dispose() } } catch { }
        try { if ($holderConn) { $holderConn.Dispose() } } catch { }
        $null = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query "IF OBJECT_ID(N'dbo.StatUpdateLock', N'U') IS NOT NULL DELETE FROM dbo.StatUpdateLock; DELETE FROM dbo.CommandLog WHERE CommandType = N'SP_STATUPDATE_DENIED';"
    }

    # Cleanup fixtures
    $null = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query "DELETE FROM dbo.CommandLog; IF DB_ID(N'EmptyStatTest') IS NOT NULL BEGIN ALTER DATABASE EmptyStatTest SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE EmptyStatTest; END"
}

# =====================================================================
# SUMMARY
# =====================================================================
$pass = ($script:Results | Where-Object { $_.Pass }).Count
$fail = ($script:Results | Where-Object { -not $_.Pass }).Count
Write-Host ""
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host ("  Test-EarlyEndRows: {0} passed, {1} failed" -f $pass, $fail) -ForegroundColor $(if ($fail -eq 0) { "Green" } else { "Red" })
Write-Host ("=" * 70) -ForegroundColor Cyan

if ($ResultsPath) {
    if ($Versions.Count -ne 1 -or $script:Results.Count -ne 13) {
        throw "Results output requires one SQL version and all 13 declared checks; got versions=$($Versions.Count), checks=$($script:Results.Count)."
    }
    $sqlVersion = $Versions[0]
    $port = $VersionConfig[$sqlVersion].Port
    $aliveResult = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query "SELECT CONVERT(nvarchar(64), SERVERPROPERTY('ProductVersion'));" -Timeout 10
    $serverAliveAtEnd = $aliveResult.ExitCode -eq 0
    $sqlBuild = if ($serverAliveAtEnd -and $aliveResult.DataSet.Tables.Count -gt 0) { [string]$aliveResult.DataSet.Tables[0].Rows[0][0] } else { "unavailable" }
    $checks = @($script:Results | ForEach-Object {
        $check = [ordered]@{ check = $_.Id; result = $(if ($_.Pass) { "PASS" } else { "FAIL" }); duration_ms = [long]$_.DurationMs }
        if (-not $_.Pass) { $check.reason = [string]$_.Detail }
        $check
    })
    $commit = (& git -C $RepoRoot rev-parse HEAD).Trim()
    if ($LASTEXITCODE -ne 0) { throw "Could not resolve the tested source commit." }
    Import-Module (Join-Path $PSScriptRoot "ReleaseResults.psm1") -Force
    $endedAt = Get-Date
    $runDurationMs = [long](($endedAt - $script:RunStartedAt).TotalMilliseconds)
    [void](Write-ReleaseRunList -Path $ResultsPath -Commit $commit -SqlVersion $sqlVersion -SqlBuild $sqlBuild `
        -ServerAliveAtEnd $serverAliveAtEnd -Suite "EarlyEndRows" -ExpectedCount 13 -Checks $checks `
        -StartedAt $script:RunStartedAt -EndedAt $endedAt -DurationMs $runDurationMs)
}

if ($fail -gt 0) { exit 1 } else { exit 0 }
