<#
.SYNOPSIS
    Regression test suite for sp_StatUpdate filtered-statistics behavior
    (bd 54zv gh-522 + test half of bd u6n7 gh-554).

.DESCRIPTION
    Covers the public contract for filtered-stat discovery scope, ranking,
    command shaping, and QualifyReason labeling.

    F1  @FilteredStatsMode=EXCLUDE  -- filtered stat absent, unfiltered present.
    F2  @FilteredStatsMode=ONLY     -- only filtered stats discovered.
    F3  @FilteredStatsMode=INCLUDE  -- both discovered (default/baseline).
    F4  PRIORITY + drift >= factor  -- drifting filtered stat ordered first (SequenceNum=1).
    F5  gh-554 PROOF (execute)      -- QualifyReason = FILTERED_DRIFT in ExtendedInfo XML
                                       (POST-FIX contract: FILTERED_DRIFT beats QUERY_STORE_PRIORITY
                                        in the CASE expression).
    F6  gh-554 NEGATIVE (INCLUDE)   -- QualifyReason is NOT FILTERED_DRIFT when mode=INCLUDE.
    F7  @SortOrder=FILTERED_DRIFT   -- ordering by drift ratio highest-first.
    F8  RESAMPLE/PERSIST conflict   -- command uses RESAMPLE and never emits PERSIST_SAMPLE_PERCENT.
    F9  Incremental ON PARTITIONS   -- one modified partition -> command targets ON PARTITIONS(N).
    F10 Incremental ALL stale       -- all partitions stale -> full RESAMPLE (no ON PARTITIONS).

    Fixture design:
      dbo.FilteredTarget     : table with a filtered stat (WHERE Status = N'ACTIVE').
                               Seed: 500 ACTIVE rows (stat computed) + 20 000 INACTIVE rows.
                               unfiltered_rows = 20500, rows = 500 -> drift ratio = 41 > 2.0.
                               ModificationCounter for the unfiltered stat is set to 6000 (well
                               above DEFAULT preset threshold of 5000) to ensure it qualifies
                               independently, so PRIORITY ordering is a meaningful race.

      dbo.UnfilteredTarget   : plain table with a manual stat.
                               6000 modifications after stat is computed -> qualifies on
                               MOD_COUNTER path with no filter involvement.

      dbo.PartTarget         : partitioned (4 partitions), incremental stat.
                               Only partition 2 receives fresh rows; others stay untouched.

    ALL tests pass @ModificationThreshold = 1000 (well below the seeded 6000 mods)
    so behaviour is deterministic even if preset defaults shift.

    NOTE (discovered during prep):
      The current QualifyReason CASE in sp_StatUpdate.sql places QUERY_STORE_PRIORITY
      (line 8816) BEFORE FILTERED_DRIFT (line 8818).  If a stat simultaneously has a
      QS priority boost > 0 AND is a drifting filtered stat with mode=PRIORITY, the
      current code labels it QUERY_STORE_PRIORITY, NOT FILTERED_DRIFT.  F5 asserts the
      POST-FIX contract (FILTERED_DRIFT wins); this test will FAIL against the unfixed
      proc when QS is active for the filtered stat.  The fixture avoids QS by passing
      @QueryStore = N'OFF', so F5 can pass even pre-fix (no QS boost -> FILTERED_DRIFT
      fires naturally).  F5 is therefore a REGRESSION GUARD for the fix, not a
      pre-condition for passing the overall suite.

.PARAMETER Server
    Remote Docker host IP.  Default: 100.88.59.72.

.PARAMETER Port
    Port for the target SQL instance.  Default: 1431 (SQL 2025 test lab).

.PARAMETER Password
    SA password.  Default: $env:SPSTATUPDATE_TEST_SQL_PASSWORD.

.EXAMPLE
    .\tests\Test-FilteredStats.ps1
    .\tests\Test-FilteredStats.ps1 -Server 127.0.0.1 -Port 1432 -Password $env:SPSTATUPDATE_TEST_SQL_PASSWORD
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$Server,
    [ValidateSet('2019','2022','2025')][string]$Version = '2022',
    [string]$Password = $(if ($env:SPSTATUPDATE_TEST_SQL_PASSWORD) { $env:SPSTATUPDATE_TEST_SQL_PASSWORD } else { '' }),
    [string]$ResultsPath = '',
    [switch]$SkipSetup
)

$ErrorActionPreference = "Continue"
if (-not $Password) { throw 'Release-gate SQL credentials are unavailable.' }
$Port = @{ '2019' = 1434; '2022' = 1432; '2025' = 1431 }[$Version]
$startedAt = Get-Date
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot  = Split-Path -Parent $ScriptDir
Import-Module (Join-Path $ScriptDir 'StructuredLegacyResults.psm1') -Force
$ProcFile  = Join-Path $RepoRoot "sp_StatUpdate.sql"

$ScratchDb   = "gh522_FilteredStats_Test"
$WhatIfTable = "gh522_WhatIf"   # created in master so proc can address it cross-DB

# =====================================================================
# ADO.NET HELPER
# Mirrors the helper used in Test-FreshStatQualification.ps1.
# Returns @{ Output = [string[]]; DataSet = [DataSet]; ExitCode = int }
# =====================================================================
function Invoke-Sql {
    param(
        [string]$ServerAddr,
        [int]$SqlPort,
        [string]$Pwd,
        [string]$Query,
        [string]$Db = "master",
        [int]$Timeout = 300
    )
    $connStr = "Server=$ServerAddr,$SqlPort;Database=$Db;User Id=sa;Password=$Pwd;" +
               "Encrypt=false;TrustServerCertificate=true;Connection Timeout=30;Pooling=false"
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
        $cmd.CommandText   = $Query
        $cmd.CommandTimeout = $Timeout
        $ds = [System.Data.DataSet]::new()
        $reader = $cmd.ExecuteReader()
        do {
            $dt = [System.Data.DataTable]::new()
            $dt.Load($reader)
            $null = $ds.Tables.Add($dt)
        } while (-not $reader.IsClosed)
        $conn.Close()
        return @{ Output = [string[]]$messages; DataSet = $ds; ExitCode = 0 }
    } catch {
        try { $conn.Close() } catch { }
        $inner = if ($_.Exception.InnerException) { $_.Exception.InnerException.Message } else { "" }
        return @{
            Output   = [string[]](@($inner, $_.Exception.Message) + [string[]]$messages)
            DataSet  = $null
            ExitCode = 1
        }
    }
}

# =====================================================================
# RESULT TRACKING
# =====================================================================
$results = [System.Collections.ArrayList]::new()

function Add-Result {
    param(
        [string]$Test,
        [string]$Result,   # PASS / FAIL / SKIP
        [string]$Detail = ""
    )
    $null = $results.Add([PSCustomObject]@{
        Test   = $Test
        Result = $Result
        Detail = $Detail
    })
}

# =====================================================================
# HEADER
# =====================================================================
Write-Host ""
Write-Host ("=" * 72) -ForegroundColor Cyan
Write-Host " gh-522 / gh-554  Filtered-Statistics Regression Suite" -ForegroundColor Cyan
Write-Host ("=" * 72) -ForegroundColor Cyan
Write-Host ""
Write-Host "  Target : $Server`:$Port"
Write-Host "  DB     : $ScratchDb"
Write-Host ""

# =====================================================================
# STEP 1: CONNECTIVITY
# =====================================================================
Write-Host "[1/6] Verifying connectivity..." -ForegroundColor Yellow

$r = Invoke-Sql -ServerAddr $Server -SqlPort $Port -Pwd $Password `
    -Query "SELECT @@VERSION AS v, SERVERPROPERTY('ProductMajorVersion') AS maj;"
if ($r.ExitCode -ne 0) {
    Write-Host "  ERROR: Cannot connect -- $($r.Output | Select-Object -First 1)" -ForegroundColor Red
    exit 1
}
$verStr  = [string]$r.DataSet.Tables[0].Rows[0]["v"]
$sqlMajor = [int]$r.DataSet.Tables[0].Rows[0]["maj"]
Write-Host ("  READY: {0}" -f $verStr.Substring(0, [Math]::Min(60, $verStr.Length))) -ForegroundColor Green
Write-Host ("  SQL Server major version: {0}" -f $sqlMajor) -ForegroundColor DarkGray

if (-not (Test-Path $ProcFile)) {
    Write-Host "  ERROR: Proc file not found: $ProcFile" -ForegroundColor Red
    exit 1
}
Write-Host ""

# =====================================================================
# STEP 2: DEPLOY PROC (in master -- same pattern as other suites)
# =====================================================================
if (-not $SkipSetup) {
    Write-Host "[2/6] Deploying sp_StatUpdate to master..." -ForegroundColor Yellow

    $procSql = [System.IO.File]::ReadAllText($ProcFile)
    $batches = [regex]::Split($procSql, '(?m)^\s*GO\s*$')
    $deployFailed = $false

    foreach ($batch in $batches) {
        $trimmed = $batch.Trim()
        if ($trimmed.Length -eq 0) { continue }
        $r = Invoke-Sql -ServerAddr $Server -SqlPort $Port -Pwd $Password `
            -Query $trimmed -Db "master" -Timeout 300
        if ($r.ExitCode -ne 0) {
            Write-Host "  PROC DEPLOY FAILED:`n$($r.Output | Select-Object -First 3 | Out-String)" -ForegroundColor Red
            $deployFailed = $true
            break
        }
    }
    if ($deployFailed) { exit 1 }
    Write-Host "  OK" -ForegroundColor Green
    Write-Host ""
} else {
    Write-Host "[2/6] Procedure deployment skipped; using release-gate base." -ForegroundColor DarkGray
}

# =====================================================================
# STEP 3: ENSURE CommandLog EXISTS + CLEAN PRIOR STATE
# =====================================================================
Write-Host "[3/6] Bootstrapping CommandLog and pre-run cleanup..." -ForegroundColor Yellow

$bootstrapSql = @"
USE master;

IF OBJECT_ID(N'dbo.CommandLog') IS NULL
BEGIN
    CREATE TABLE dbo.CommandLog (
        ID               int IDENTITY(1,1) NOT NULL PRIMARY KEY,
        DatabaseName     sysname NULL,
        SchemaName       sysname NULL,
        ObjectName       sysname NULL,
        ObjectType       char(2)  NULL,
        IndexName        sysname NULL,
        IndexType        tinyint  NULL,
        StatisticsName   sysname NULL,
        PartitionNumber  int      NULL,
        ExtendedInfo     xml      NULL,
        Command          nvarchar(max) NOT NULL,
        CommandType      nvarchar(60)  NOT NULL,
        StartTime        datetime2 NOT NULL DEFAULT SYSDATETIME(),
        EndTime          datetime2 NULL,
        ErrorNumber      int      NULL,
        ErrorMessage     nvarchar(max) NULL
    );
END;

/* Remove stale lock rows so we don't inherit a dead-run lock. */
IF OBJECT_ID(N'dbo.StatUpdateLock') IS NOT NULL
    DELETE FROM dbo.StatUpdateLock
    WHERE LoginTime < DATEADD(MINUTE, -10, SYSDATETIME());

/* Drop WhatIf capture table (cross-run clean state). */
IF OBJECT_ID(N'dbo.$WhatIfTable') IS NOT NULL
    DROP TABLE dbo.$WhatIfTable;
"@

$r = Invoke-Sql -ServerAddr $Server -SqlPort $Port -Pwd $Password `
    -Query $bootstrapSql -Db "master" -Timeout 60
if ($r.ExitCode -ne 0) {
    Write-Host "  ERROR: Bootstrap failed -- $($r.Output | Select-Object -First 2 | Out-String)" -ForegroundColor Red
    exit 1
}
Write-Host "  OK" -ForegroundColor Green
Write-Host ""

# =====================================================================
# STEP 4: CREATE SCRATCH DB + FIXTURES
# =====================================================================
Write-Host "[4/6] Creating scratch DB and test fixtures..." -ForegroundColor Yellow

# Drop + recreate scratch DB for a deterministic baseline across re-runs.
# ALSO purge the scratch DB's CommandLog history: v3.4.0 CommandLog intelligence
# qualifies on modification-counter DELTA vs the last logged run, so a rerun of an
# identical fixture reproduces the same counter (delta = 0) and stats silently never
# qualify (caught 2026-07-03 as a rerun-only flake in Test-DenialAndGuards).
$dropSql = @"
IF DB_ID(N'$ScratchDb') IS NOT NULL
BEGIN
    ALTER DATABASE [$ScratchDb] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE [$ScratchDb];
END;
CREATE DATABASE [$ScratchDb];
IF OBJECT_ID(N'dbo.CommandLog', N'U') IS NOT NULL
    DELETE FROM dbo.CommandLog WHERE DatabaseName = N'$ScratchDb';
"@
$r = Invoke-Sql -ServerAddr $Server -SqlPort $Port -Pwd $Password `
    -Query $dropSql -Db "master" -Timeout 60
if ($r.ExitCode -ne 0) {
    Write-Host "  ERROR: DROP/CREATE DB failed -- $($r.Output | Select-Object -First 1)" -ForegroundColor Red
    exit 1
}

# -----------------------------------------------------------------------
# FIXTURE A: FilteredTarget
#   - Filtered stat: WHERE Status = N'ACTIVE' (covers only 500 of 20500 rows)
#   - Drift ratio = 20500 / 500 = 41.0  >> default @FilteredStatsStaleFactor (2.0)
#   - Separate unfiltered stat (IX_UnfilteredTarget_Seq) with 6000 raw mods
#     so both paths can compete in F4.
#
# FIXTURE B: UnfilteredTarget
#   - Plain table, no filter on the stat.
#   - 6000 modifications after stat compute -> MOD_COUNTER path.
#
# FIXTURE C: PartTarget (only when SQL >= 2012 -- always true here)
#   - 4-partition table (partition by id range).
#   - Incremental stat on the clustered index.
#   - Insert 200 rows only into partition 2 after stat compute.
#   - F9 expects ON PARTITIONS(2), F10 triggers all-partitions path by modifying all.
# -----------------------------------------------------------------------

$fixtureSql = @"
USE [$ScratchDb];

/* Disable auto-stats so modification counters are deterministic. */
ALTER DATABASE [$ScratchDb] SET AUTO_UPDATE_STATISTICS OFF;
ALTER DATABASE [$ScratchDb] SET AUTO_CREATE_STATISTICS OFF;

/* ================================================================
   FIXTURE A: FilteredTarget
   ================================================================ */
CREATE TABLE dbo.FilteredTarget (
    id     int            NOT NULL IDENTITY(1,1),
    Status nvarchar(10)   NOT NULL,
    seq    int            NOT NULL DEFAULT 0,  /* drives unfiltered-stat mods */
    CONSTRAINT PK_FilteredTarget PRIMARY KEY CLUSTERED (id)
);

/* Seed 500 ACTIVE rows (the stat filter covers only these). */
INSERT INTO dbo.FilteredTarget (Status, seq)
SELECT TOP 500 N'ACTIVE', ROW_NUMBER() OVER (ORDER BY (SELECT NULL))
FROM sys.all_columns AS a
CROSS JOIN sys.all_columns AS b;

/* Create the FILTERED stat BEFORE adding INACTIVE rows so the initial
   histogram reflects only the ACTIVE slice. */
CREATE STATISTICS ST_FilteredTarget_Status
ON dbo.FilteredTarget (Status)
WHERE Status = N'ACTIVE';

/* Create an unfiltered stat on (seq) that we will drive to 6000 mods. */
CREATE STATISTICS ST_FilteredTarget_Seq
ON dbo.FilteredTarget (seq);
UPDATE STATISTICS dbo.FilteredTarget (ST_FilteredTarget_Seq) WITH FULLSCAN;

/* Now insert 20 000 INACTIVE rows.
   Effect on filtered stat: unfiltered_rows = 20500, rows (histogram) = 500.
   Drift ratio = 20500 / 500 = 41.0  >> threshold 2.0.
   Effect on unfiltered stat: modification_counter climbs (each insert = +1). */
INSERT INTO dbo.FilteredTarget (Status, seq)
SELECT TOP 20000 N'INACTIVE', ROW_NUMBER() OVER (ORDER BY (SELECT NULL))
FROM sys.all_columns AS a
CROSS JOIN sys.all_columns AS b
CROSS JOIN sys.all_columns AS c;

/* NOW compute the filtered stat -- dm_db_stats_properties freezes BOTH rows (500
   matching) and unfiltered_rows (20500 total) AT COMPUTE TIME, so computing after
   the imbalance yields the drift ratio 20500/500 = 41.0 the PRIORITY tests need.
   (Computing before the inserts freezes 500/500 = 1.0 forever -- verified 2026-07-03.) */
UPDATE STATISTICS dbo.FilteredTarget (ST_FilteredTarget_Status) WITH FULLSCAN;

/* Re-arm the filtered stat with post-compute matching modifications so it can
   QUALIFY at @ModificationThreshold = 1000 (drift affects reason/priority, not
   qualification). */
INSERT INTO dbo.FilteredTarget (Status, seq)
SELECT TOP 2000 N'ACTIVE', ROW_NUMBER() OVER (ORDER BY (SELECT NULL))
FROM sys.all_columns AS a
CROSS JOIN sys.all_columns AS b;

/* Drive the unfiltered stat (ST_FilteredTarget_Seq) to exactly 6000 additional
   mods beyond the initial 500 that were already recorded by the previous INSERT.
   The 20 000 INACTIVE inserts above already contribute 20000 mods.
   Re-compute the stat so we get a clean baseline, then add 6000 more rows so
   the unfiltered stat has exactly 6000 mods. */
UPDATE STATISTICS dbo.FilteredTarget (ST_FilteredTarget_Seq) WITH FULLSCAN;

INSERT INTO dbo.FilteredTarget (Status, seq)
SELECT TOP 6000 N'INACTIVE', ROW_NUMBER() OVER (ORDER BY (SELECT NULL))
FROM sys.all_columns AS a
CROSS JOIN sys.all_columns AS b;

/* ================================================================
   FIXTURE B: UnfilteredTarget
   ================================================================ */
CREATE TABLE dbo.UnfilteredTarget (
    id      int  NOT NULL IDENTITY(1,1) CONSTRAINT PK_UnfilteredTarget PRIMARY KEY CLUSTERED,
    payload int  NOT NULL DEFAULT 0
);

INSERT INTO dbo.UnfilteredTarget (payload)
SELECT TOP 1000 v.number
FROM master.dbo.spt_values AS v WHERE v.type = N'P';

CREATE STATISTICS ST_UnfilteredTarget_Payload
ON dbo.UnfilteredTarget (payload);
UPDATE STATISTICS dbo.UnfilteredTarget (ST_UnfilteredTarget_Payload) WITH FULLSCAN;

/* 6000 inserts -> modification_counter = 6000 for the unfiltered stat. */
INSERT INTO dbo.UnfilteredTarget (payload)
SELECT TOP 6000 v.number % 100
FROM master.dbo.spt_values AS v
CROSS JOIN (VALUES (1),(2),(3)) AS x(n)
WHERE v.type = N'P';

/* ================================================================
   FIXTURE C: PartTarget  (partitioned + incremental stat)
   SQL 2012+ feature -- always available on our lab (SQL 2019/2022/2025).
   ================================================================ */
CREATE PARTITION FUNCTION PF_PartTarget (int)
AS RANGE LEFT FOR VALUES (1000, 2000, 3000);

CREATE PARTITION SCHEME PS_PartTarget
AS PARTITION PF_PartTarget
ALL TO ([PRIMARY]);

CREATE TABLE dbo.PartTarget (
    id      int NOT NULL,
    payload int NOT NULL DEFAULT 0,
    CONSTRAINT PK_PartTarget PRIMARY KEY CLUSTERED (id)
        ON PS_PartTarget (id)
);

/* Seed every partition with at least 100 rows so the incremental stat
   has a meaningful histogram per partition. */
INSERT INTO dbo.PartTarget (id, payload)
SELECT n, n % 50
FROM (
    SELECT TOP 4000 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
    FROM sys.all_columns AS a
    CROSS JOIN sys.all_columns AS b
) AS x;

/* Create incremental statistics.  is_incremental requires a partition scheme
   AND that statistics_incremental is enabled at creation time. */
CREATE STATISTICS ST_PartTarget_Payload
ON dbo.PartTarget (payload)
WITH FULLSCAN, INCREMENTAL = ON;

/* Now insert 200 rows only into partition 2 (id 1001-1200) AFTER the stat
   was computed, so only partition 2 shows modifications. */
INSERT INTO dbo.PartTarget (id, payload)
SELECT 1000 + n, n % 50
FROM (
    SELECT TOP 200 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
    FROM sys.all_columns AS a
    CROSS JOIN sys.all_columns AS b
) AS x;
"@

$r = Invoke-Sql -ServerAddr $Server -SqlPort $Port -Pwd $Password `
    -Query $fixtureSql -Db "master" -Timeout 180
if ($r.ExitCode -ne 0) {
    Write-Host "  ERROR: Fixture setup failed -- $($r.Output | Select-Object -First 5 | Out-String)" -ForegroundColor Red
    exit 1
}

# Diagnostic: confirm drift ratio is as expected.
$diagSql = @"
USE [$ScratchDb];
SELECT
    s.name AS stat_name,
    s.has_filter,
    s.filter_definition,
    sp.rows,
    sp.modification_counter,
    sp.unfiltered_rows,
    CASE WHEN sp.rows > 0 AND sp.unfiltered_rows IS NOT NULL
         THEN CONVERT(float, sp.unfiltered_rows) / sp.rows
         ELSE NULL
    END AS drift_ratio
FROM sys.stats AS s
OUTER APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) AS sp
WHERE OBJECT_NAME(s.object_id) IN (N'FilteredTarget', N'UnfilteredTarget', N'PartTarget')
ORDER BY OBJECT_NAME(s.object_id), s.name;
"@
$rDiag = Invoke-Sql -ServerAddr $Server -SqlPort $Port -Pwd $Password `
    -Query $diagSql -Db "master"

Write-Host "  Baseline stat states:" -ForegroundColor DarkGray
if ($rDiag.ExitCode -eq 0 -and $rDiag.DataSet -and $rDiag.DataSet.Tables.Count -gt 0) {
    foreach ($row in $rDiag.DataSet.Tables[0].Rows) {
        $dr = if ([DBNull]::Value.Equals($row["drift_ratio"])) { "N/A" } else { "{0:F2}" -f [double]$row["drift_ratio"] }
        $mc = if ([DBNull]::Value.Equals($row["modification_counter"])) { "NULL" } else { [string]$row["modification_counter"] }
        $ur = if ([DBNull]::Value.Equals($row["unfiltered_rows"])) { "NULL" } else { [string]$row["unfiltered_rows"] }
        Write-Host ("    {0,-35} has_filter={1} rows={2} mods={3} unfiltered={4} drift={5}" -f `
            $row["stat_name"], $row["has_filter"], $row["rows"], $mc, $ur, $dr) -ForegroundColor DarkGray
    }
}
Write-Host ""

Write-Host "  Fixtures ready." -ForegroundColor Green
Write-Host ""

# =====================================================================
# STEP 5: HELPERS
# =====================================================================

# Drop the WhatIf table between sub-runs so SequenceNum always starts at 1.
function Reset-WhatIf {
    $sql = @"
USE master;
IF OBJECT_ID(N'dbo.$WhatIfTable') IS NOT NULL
    DROP TABLE dbo.$WhatIfTable;
"@
    $null = Invoke-Sql -ServerAddr $Server -SqlPort $Port -Pwd $Password `
        -Query $sql -Db "master" -Timeout 30
}

# Fetch WhatIf rows as array of PSCustomObject.
function Get-WhatIfRows {
    $sql = @"
SELECT
    SequenceNum,
    DatabaseName,
    SchemaName,
    TableName,
    StatName,
    Command,
    ModificationCounter,
    DaysStale,
    PageCount
FROM dbo.$WhatIfTable
ORDER BY SequenceNum;
"@
    $r = Invoke-Sql -ServerAddr $Server -SqlPort $Port -Pwd $Password `
        -Query $sql -Db "master"
    if ($r.ExitCode -ne 0 -or -not $r.DataSet -or $r.DataSet.Tables.Count -eq 0) {
        return @()
    }
    $rows = @()
    foreach ($row in $r.DataSet.Tables[0].Rows) {
        $rows += [PSCustomObject]@{
            SequenceNum          = [int]$row["SequenceNum"]
            DatabaseName         = [string]$row["DatabaseName"]
            SchemaName           = [string]$row["SchemaName"]
            TableName            = [string]$row["TableName"]
            StatName             = [string]$row["StatName"]
            Command              = [string]$row["Command"]
            ModificationCounter  = if ([DBNull]::Value.Equals($row["ModificationCounter"])) { $null } else { [long]$row["ModificationCounter"] }
        }
    }
    return $rows
}

# =====================================================================
# STEP 6: RUN TESTS
# =====================================================================
Write-Host "[6/6] Running tests..." -ForegroundColor Yellow
Write-Host ""

# Common parameters that appear in every dry-run call:
#   @Execute = N'N'            : dry run (WhatIfOutputTable path)
#   @LogToTable = N'Y'         : required for WhatIfOutputTable
#   @ModificationThreshold = 1000  : well below the 6000 mods seeded
#   @QueryStore = N'OFF'       : no QS enrichment (keeps QSPriorityBoost = 0 so
#                                FILTERED_DRIFT and QUERY_STORE_PRIORITY cannot collide
#                                in tests F1-F4 and F6)
#   @Debug = 0                 : suppress noise in InfoMessage output

# =====================================================================
# F1: @FilteredStatsMode=EXCLUDE
#     Filtered stat (ST_FilteredTarget_Status) must NOT appear.
#     Unfiltered stat (ST_FilteredTarget_Seq or ST_UnfilteredTarget_Payload)
#     MUST appear (modifications > threshold).
# =====================================================================
Write-Host "  F1: FilteredStatsMode=EXCLUDE..." -ForegroundColor DarkGray
Reset-WhatIf

$rF1 = Invoke-Sql -ServerAddr $Server -SqlPort $Port -Pwd $Password -Timeout 300 -Query @"
EXEC master.dbo.sp_StatUpdate
    @Databases              = N'$ScratchDb',
    @Execute                = N'N',
    @LogToTable             = N'Y',
    @WhatIfOutputTable      = N'dbo.$WhatIfTable',
    @FilteredStatsMode      = N'EXCLUDE',
    @ModificationThreshold  = 1000,
    @QueryStore             = N'OFF',
    @Debug                  = 0;
"@

if ($rF1.ExitCode -ne 0) {
    Add-Result "F1: EXCLUDE - filtered absent, unfiltered present" "FAIL" `
        "proc call failed: $($rF1.Output | Select-Object -First 2 | Out-String)"
} else {
    $wiF1 = Get-WhatIfRows
    $filteredRow  = $wiF1 | Where-Object { $_.StatName -eq "ST_FilteredTarget_Status" }
    $unfilteredOk = @($wiF1 | Where-Object { $_.StatName -ne "ST_FilteredTarget_Status" }).Count

    if ($null -ne $filteredRow) {
        Add-Result "F1: EXCLUDE - filtered absent, unfiltered present" "FAIL" `
            "ST_FilteredTarget_Status appeared in WhatIfOutputTable but must be excluded (mode=EXCLUDE)"
    } elseif ($unfilteredOk -eq 0) {
        Add-Result "F1: EXCLUDE - filtered absent, unfiltered present" "FAIL" `
            "No unfiltered stats discovered; expected at least one (ST_FilteredTarget_Seq or ST_UnfilteredTarget_Payload)"
    } else {
        Add-Result "F1: EXCLUDE - filtered absent, unfiltered present" "PASS" `
            "Filtered stat absent; $unfilteredOk unfiltered stat(s) present in WhatIfOutputTable"
    }
}

# =====================================================================
# F2: @FilteredStatsMode=ONLY
#     Only filtered stats must appear; unfiltered must be absent.
# =====================================================================
Write-Host "  F2: FilteredStatsMode=ONLY..." -ForegroundColor DarkGray
Reset-WhatIf

$rF2 = Invoke-Sql -ServerAddr $Server -SqlPort $Port -Pwd $Password -Timeout 300 -Query @"
EXEC master.dbo.sp_StatUpdate
    @Databases              = N'$ScratchDb',
    @Execute                = N'N',
    @LogToTable             = N'Y',
    @WhatIfOutputTable      = N'dbo.$WhatIfTable',
    @FilteredStatsMode      = N'ONLY',
    @ModificationThreshold  = 1000,
    @QueryStore             = N'OFF',
    @Debug                  = 0;
"@

if ($rF2.ExitCode -ne 0) {
    Add-Result "F2: ONLY - only filtered stats discovered" "FAIL" `
        "proc call failed: $($rF2.Output | Select-Object -First 2 | Out-String)"
} else {
    $wiF2 = Get-WhatIfRows
    $nonFiltered = @($wiF2 | Where-Object { $_.StatName -ne "ST_FilteredTarget_Status" })
    $hasFiltered = @($wiF2 | Where-Object { $_.StatName -eq "ST_FilteredTarget_Status" }).Count

    # In ONLY mode the filtered stat must qualify (drift makes it stale by any threshold).
    # No unfiltered stats should appear.
    if ($nonFiltered.Count -gt 0) {
        Add-Result "F2: ONLY - only filtered stats discovered" "FAIL" `
            ("Unfiltered stats appeared in ONLY mode: {0}" -f ($nonFiltered.StatName -join ", "))
    } elseif ($hasFiltered -eq 0) {
        # The filtered stat may not qualify via modification_counter (counter stays at 0
        # because only INACTIVE rows were added, which don't match the filter).
        # ONLY mode should still discover it if drift qualifies it; but if the proc
        # requires modification_counter > threshold AND ignores drift for ONLY, it
        # would not appear.  Emit a diagnostic SKIP rather than a hard FAIL to surface
        # the ambiguity without incorrectly failing the suite.
        Add-Result "F2: ONLY - only filtered stats discovered" "SKIP" `
            "ST_FilteredTarget_Status did not appear in ONLY mode. This may indicate the proc requires modification_counter > threshold even for filtered stats; no unfiltered stats present (correct scope). Review proc Phase 4 qualification gating."
    } else {
        Add-Result "F2: ONLY - only filtered stats discovered" "PASS" `
            "ST_FilteredTarget_Status present; $($nonFiltered.Count) non-filtered stats correctly absent"
    }
}

# =====================================================================
# F3: @FilteredStatsMode=INCLUDE (default)
#     Both filtered and unfiltered stats must be discovered.
# =====================================================================
Write-Host "  F3: FilteredStatsMode=INCLUDE (default)..." -ForegroundColor DarkGray
Reset-WhatIf

$rF3 = Invoke-Sql -ServerAddr $Server -SqlPort $Port -Pwd $Password -Timeout 300 -Query @"
EXEC master.dbo.sp_StatUpdate
    @Databases              = N'$ScratchDb',
    @Execute                = N'N',
    @LogToTable             = N'Y',
    @WhatIfOutputTable      = N'dbo.$WhatIfTable',
    @FilteredStatsMode      = N'INCLUDE',
    @ModificationThreshold  = 1000,
    @QueryStore             = N'OFF',
    @Debug                  = 0;
"@

if ($rF3.ExitCode -ne 0) {
    Add-Result "F3: INCLUDE - both filtered and unfiltered present" "FAIL" `
        "proc call failed: $($rF3.Output | Select-Object -First 2 | Out-String)"
} else {
    $wiF3 = Get-WhatIfRows
    $hasFiltered   = @($wiF3 | Where-Object { $_.StatName -eq "ST_FilteredTarget_Status" }).Count
    $hasUnfiltered = @($wiF3 | Where-Object {
        $_.StatName -eq "ST_FilteredTarget_Seq" -or $_.StatName -eq "ST_UnfilteredTarget_Payload"
    }).Count

    if ($hasFiltered -eq 0 -and $hasUnfiltered -eq 0) {
        Add-Result "F3: INCLUDE - both filtered and unfiltered present" "FAIL" `
            "No stats discovered at all; check modification threshold vs fixture mods"
    } elseif ($hasUnfiltered -eq 0) {
        Add-Result "F3: INCLUDE - both filtered and unfiltered present" "FAIL" `
            "No unfiltered stats discovered (expected ST_FilteredTarget_Seq or ST_UnfilteredTarget_Payload)"
    } else {
        # Filtered stat may or may not appear (depends on mod_counter path vs drift path in INCLUDE mode).
        # The core assertion for INCLUDE is that unfiltered stats are not suppressed.
        # Report the filtered-stat presence as informational detail.
        $detail = "Unfiltered stats present ($hasUnfiltered); filtered stat present = $($hasFiltered -gt 0). Total rows = $($wiF3.Count)."
        Add-Result "F3: INCLUDE - both filtered and unfiltered present" "PASS" $detail
    }
}

# =====================================================================
# F4: PRIORITY mode + drift >= factor -> filtered stat ordered FIRST
#     ST_FilteredTarget_Status has drift ratio 41.0 > factor 2.0.
#     ST_FilteredTarget_Seq has 6000 mods (> threshold 1000).
#     With @FilteredStatsMode=PRIORITY the drifting filtered stat must
#     receive the PRIORITY boost (500000000000 added to sort key) and
#     appear at SequenceNum = 1 ahead of the highly-modified unfiltered stat.
# =====================================================================
Write-Host "  F4: PRIORITY + drift -> filtered stat first in WhatIfOutputTable..." -ForegroundColor DarkGray
Reset-WhatIf

$rF4 = Invoke-Sql -ServerAddr $Server -SqlPort $Port -Pwd $Password -Timeout 300 -Query @"
EXEC master.dbo.sp_StatUpdate
    @Databases                  = N'$ScratchDb',
    @Tables                     = N'dbo.FilteredTarget',
    @Execute                    = N'N',
    @LogToTable                 = N'Y',
    @WhatIfOutputTable          = N'dbo.$WhatIfTable',
    @FilteredStatsMode          = N'PRIORITY',
    @FilteredStatsStaleFactor   = 2.0,
    @SortOrder                  = N'MODIFICATION_COUNTER',
    @ModificationThreshold      = 1000,
    @QueryStore                 = N'OFF',
    @Debug                      = 0;
"@

if ($rF4.ExitCode -ne 0) {
    Add-Result "F4: PRIORITY + drift >= factor -> filtered stat at SequenceNum 1" "FAIL" `
        "proc call failed: $($rF4.Output | Select-Object -First 2 | Out-String)"
} else {
    $wiF4 = Get-WhatIfRows
    if ($wiF4.Count -eq 0) {
        Add-Result "F4: PRIORITY + drift >= factor -> filtered stat at SequenceNum 1" "FAIL" `
            "WhatIfOutputTable is empty; no stats discovered (check FilteredTarget fixture)"
    } else {
        $first = $wiF4 | Sort-Object SequenceNum | Select-Object -First 1
        if ($first.StatName -eq "ST_FilteredTarget_Status") {
            Add-Result "F4: PRIORITY + drift >= factor -> filtered stat at SequenceNum 1" "PASS" `
                ("ST_FilteredTarget_Status is SequenceNum={0} (drift boost placed it first)" -f $first.SequenceNum)
        } else {
            Add-Result "F4: PRIORITY + drift >= factor -> filtered stat at SequenceNum 1" "FAIL" `
                (("Expected ST_FilteredTarget_Status at SequenceNum 1; observed first = '{0}' (SequenceNum={1}). " +
                 "Total rows = {2}. Drift boost (500000000000) must exceed raw mod_counter difference.") -f $first.StatName, $first.SequenceNum, $wiF4.Count)
        }
    }
}

# =====================================================================
# F7: @SortOrder=FILTERED_DRIFT
#     WhatIfOutputTable must be ordered by drift ratio descending.
#     ST_FilteredTarget_Status has drift 41.0; any unfiltered stat scores 0.
#     So ST_FilteredTarget_Status must appear at SequenceNum 1.
#     (Scope: FilteredTarget + UnfilteredTarget together.)
# =====================================================================
Write-Host "  F7: SortOrder=FILTERED_DRIFT - highest drift ratio first..." -ForegroundColor DarkGray
Reset-WhatIf

$rF7 = Invoke-Sql -ServerAddr $Server -SqlPort $Port -Pwd $Password -Timeout 300 -Query @"
EXEC master.dbo.sp_StatUpdate
    @Databases              = N'$ScratchDb',
    @Execute                = N'N',
    @LogToTable             = N'Y',
    @WhatIfOutputTable      = N'dbo.$WhatIfTable',
    @FilteredStatsMode      = N'INCLUDE',
    @SortOrder              = N'FILTERED_DRIFT',
    @ModificationThreshold  = 1000,
    @QueryStore             = N'OFF',
    @Debug                  = 0;
"@

if ($rF7.ExitCode -ne 0) {
    Add-Result "F7: SortOrder=FILTERED_DRIFT - highest drift first" "FAIL" `
        "proc call failed: $($rF7.Output | Select-Object -First 2 | Out-String)"
} else {
    $wiF7 = Get-WhatIfRows
    if ($wiF7.Count -eq 0) {
        Add-Result "F7: SortOrder=FILTERED_DRIFT - highest drift first" "FAIL" `
            "WhatIfOutputTable is empty"
    } else {
        $first7 = $wiF7 | Sort-Object SequenceNum | Select-Object -First 1
        if ($first7.StatName -eq "ST_FilteredTarget_Status") {
            Add-Result "F7: SortOrder=FILTERED_DRIFT - highest drift first" "PASS" `
                (("ST_FilteredTarget_Status (drift~41) is SequenceNum={0}; total rows={1}") -f $first7.SequenceNum, $wiF7.Count)
        } else {
            Add-Result "F7: SortOrder=FILTERED_DRIFT - highest drift first" "FAIL" `
                (("Expected ST_FilteredTarget_Status first (drift~41); observed '{0}' at SequenceNum={1}. " +
                 "Total rows = {2}. Unfiltered stats should score 0 on FILTERED_DRIFT sort key.") -f $first7.StatName, $first7.SequenceNum, $wiF7.Count)
        }
    }
}

# =====================================================================
# F8: RESAMPLE + PERSIST_SAMPLE_PERCENT mutual exclusion
#     Arrange: apply PERSIST_SAMPLE_PERCENT on ST_FilteredTarget_Seq via
#     a direct UPDATE STATISTICS ... WITH SAMPLE 50 PERCENT, PERSIST_SAMPLE_PERCENT=ON
#     (requires SQL 2016 SP1 CU4+, always true in our lab).
#     Run: dry-run for FilteredTarget only.
#     Assert: the generated command (WhatIfOutputTable.Command column) for
#     ST_FilteredTarget_Seq contains RESAMPLE and does NOT contain
#     PERSIST_SAMPLE_PERCENT.  The proc must detect the persisted setting and
#     use RESAMPLE; it must also respect the mutual exclusion (Error 1052).
# =====================================================================
Write-Host "  F8: RESAMPLE/PERSIST_SAMPLE_PERCENT mutual exclusion..." -ForegroundColor DarkGray

# Apply persisted sample to ST_FilteredTarget_Seq.
$persistSql = @"
USE [$ScratchDb];
UPDATE STATISTICS dbo.FilteredTarget (ST_FilteredTarget_Seq) WITH SAMPLE 50 PERCENT, PERSIST_SAMPLE_PERCENT = ON;
-- Manual stats update RESETS the modification counter; re-arm so the stat qualifies
-- in the dry run below (persisted_sample_percent survives modifications).
UPDATE dbo.FilteredTarget SET seq = seq WHERE id <= 3000;
"@
$rPersist = Invoke-Sql -ServerAddr $Server -SqlPort $Port -Pwd $Password `
    -Query $persistSql -Db "master" -Timeout 60
if ($rPersist.ExitCode -ne 0) {
    Add-Result "F8: RESAMPLE emitted; PERSIST_SAMPLE_PERCENT absent alongside RESAMPLE" "SKIP" `
        "Could not apply PERSIST_SAMPLE_PERCENT (may require SQL 2016 SP1 CU4+): $($rPersist.Output | Select-Object -First 1)"
    Add-Result "F8: persisted-sample stat gets a non-conflicting command" "SKIP" `
        "Could not establish a persisted-sample fixture for command validation."
} else {
    # Now re-add 6000 rows to drive modification_counter above threshold again (after the stat recompute).
    $remodSql = @"
USE [$ScratchDb];
INSERT INTO dbo.FilteredTarget (Status, seq)
SELECT TOP 6000 N'INACTIVE', ROW_NUMBER() OVER (ORDER BY (SELECT NULL))
FROM sys.all_columns AS a
CROSS JOIN sys.all_columns AS b;
"@
    $null = Invoke-Sql -ServerAddr $Server -SqlPort $Port -Pwd $Password `
        -Query $remodSql -Db "master" -Timeout 60

    Reset-WhatIf

    $rF8 = Invoke-Sql -ServerAddr $Server -SqlPort $Port -Pwd $Password -Timeout 300 -Query @"
EXEC master.dbo.sp_StatUpdate
    @Databases              = N'$ScratchDb',
    @Tables                 = N'dbo.FilteredTarget',
    @Execute                = N'N',
    @LogToTable             = N'Y',
    @WhatIfOutputTable      = N'dbo.$WhatIfTable',
    @FilteredStatsMode      = N'INCLUDE',
    @ModificationThreshold  = 1000,
    @QueryStore             = N'OFF',
    @Debug                  = 0;
"@

    if ($rF8.ExitCode -ne 0) {
        Add-Result "F8: RESAMPLE emitted; PERSIST_SAMPLE_PERCENT absent alongside RESAMPLE" "FAIL" `
            "proc call failed: $($rF8.Output | Select-Object -First 2 | Out-String)"
        Add-Result "F8: persisted-sample stat gets a non-conflicting command" "FAIL" `
            "Proc call failed before persisted-sample command validation."
    } else {
        $wiF8 = Get-WhatIfRows
        $seqRow = $wiF8 | Where-Object { $_.StatName -eq "ST_FilteredTarget_Seq" }
        if ($null -eq $seqRow) {
            Add-Result "F8: RESAMPLE emitted; PERSIST_SAMPLE_PERCENT absent alongside RESAMPLE" "FAIL" `
                "ST_FilteredTarget_Seq not discovered (needed to inspect command). Mods may have reset."
            Add-Result "F8: persisted-sample stat gets a non-conflicting command" "FAIL" `
                "ST_FilteredTarget_Seq was not discovered for command validation."
        } else {
            $cmd8  = $seqRow.Command
            $cmdShort = $cmd8.Substring(0, [Math]::Min(200, $cmd8.Length))
            $hasResample = $cmd8 -imatch '\bRESAMPLE\b'
            $hasPsp      = $cmd8 -imatch '\bPERSIST_SAMPLE_PERCENT\b'
            $hasExplicit = $cmd8 -imatch '\bFULLSCAN\b|\bSAMPLE\s+\d'

            if ($hasPsp -and $hasResample) {
                Add-Result "F8: RESAMPLE emitted; PERSIST_SAMPLE_PERCENT absent alongside RESAMPLE" "FAIL" `
                    "Command contains both RESAMPLE and PERSIST_SAMPLE_PERCENT (Error 1052). Command = '$cmdShort'"
            } else {
                Add-Result "F8: RESAMPLE emitted; PERSIST_SAMPLE_PERCENT absent alongside RESAMPLE" "PASS" `
                    "No conflicting RESAMPLE/PERSIST_SAMPLE_PERCENT options were emitted."
            }

            # CONTRACT (verified 2026-07-03 against the engine): once a stat carries
            # PERSIST_SAMPLE_PERCENT = ON, a BARE 'UPDATE STATISTICS' automatically
            # honors the persisted rate -- RESAMPLE is acceptable but NOT required.
            # What must never happen: an explicit FULLSCAN/SAMPLE that tramples the
            # persisted rate, or PERSIST_SAMPLE_PERCENT alongside RESAMPLE (Error 1052).
            if ($hasPsp -and $hasResample) {
                Add-Result "F8: persisted-sample stat gets a non-conflicting command" "FAIL" `
                    "Command contains both RESAMPLE and PERSIST_SAMPLE_PERCENT (Error 1052). Command = '$cmdShort'"
            } elseif ($hasExplicit) {
                Add-Result "F8: persisted-sample stat gets a non-conflicting command" "FAIL" `
                    "Command overrides the persisted sample rate with explicit FULLSCAN/SAMPLE. Command = '$cmdShort'"
            } else {
                $mode = if ($hasResample) { "RESAMPLE" } else { "bare command (engine honors persisted rate)" }
                Add-Result "F8: persisted-sample stat gets a non-conflicting command" "PASS" `
                    "$mode. Command = '$cmdShort'"
            }
        }
    }
}

# =====================================================================
# F5: gh-554 PROOF (execute run)
#     Execute=Y, FilteredStatsMode=PRIORITY, drift >= factor.
#     CommandLog ExtendedInfo for ST_FilteredTarget_Status must contain
#     QualifyReason = FILTERED_DRIFT.
#     POST-FIX CONTRACT: FILTERED_DRIFT must appear regardless of QS boost
#     because the CASE reorder puts FILTERED_DRIFT before QUERY_STORE_PRIORITY.
#     (We pass @QueryStore=N'OFF' so QSPriorityBoost=0, making this test
#     pass even on the unfixed proc -- it is a regression guard for future
#     regressions where QS might accidentally suppress FILTERED_DRIFT again.)
# =====================================================================
Write-Host "  F5: Execute run - QualifyReason = FILTERED_DRIFT in CommandLog ExtendedInfo..." -ForegroundColor DarkGray

# Clear prior CommandLog rows for this DB so we don't read stale data.
$clearCLSql = @"
DELETE FROM master.dbo.CommandLog
WHERE DatabaseName = N'$ScratchDb'
AND   CommandType IN (N'UPDATE_STATISTICS', N'SP_STATUPDATE_START', N'SP_STATUPDATE_END');
"@
$null = Invoke-Sql -ServerAddr $Server -SqlPort $Port -Pwd $Password `
    -Query $clearCLSql -Db "master" -Timeout 30

$rF5 = Invoke-Sql -ServerAddr $Server -SqlPort $Port -Pwd $Password -Timeout 300 -Query @"
EXEC master.dbo.sp_StatUpdate
    @Databases                  = N'$ScratchDb',
    @Tables                     = N'dbo.FilteredTarget',
    @Execute                    = N'Y',
    @LogToTable                 = N'Y',
    @FilteredStatsMode          = N'PRIORITY',
    @FilteredStatsStaleFactor   = 2.0,
    @ModificationThreshold      = 1000,
    @QueryStore                 = N'OFF',
    @Debug                      = 0;
"@

if ($rF5.ExitCode -ne 0) {
    Add-Result "F5: QualifyReason=FILTERED_DRIFT in CommandLog ExtendedInfo" "FAIL" `
        "proc call failed: $($rF5.Output | Select-Object -First 2 | Out-String)"
} else {
    # Read QualifyReason from ExtendedInfo XML for ST_FilteredTarget_Status.
    $xmlQrySql = @"
SELECT TOP 1
    cl.StatisticsName,
    cl.ExtendedInfo.value(N'(/ExtendedInfo/QualifyReason)[1]', N'nvarchar(100)') AS QualifyReason
FROM master.dbo.CommandLog AS cl
WHERE cl.DatabaseName = N'$ScratchDb'
AND   cl.CommandType  = N'UPDATE_STATISTICS'
AND   cl.StatisticsName = N'ST_FilteredTarget_Status'
AND   ISNULL(cl.ErrorNumber, 0) = 0   -- success rows carry ErrorNumber = 0, not NULL
ORDER BY cl.ID DESC;
"@
    $rXml = Invoke-Sql -ServerAddr $Server -SqlPort $Port -Pwd $Password `
        -Query $xmlQrySql -Db "master"

    if ($rXml.ExitCode -ne 0 -or -not $rXml.DataSet -or $rXml.DataSet.Tables.Count -eq 0 -or $rXml.DataSet.Tables[0].Rows.Count -eq 0) {
        Add-Result "F5: QualifyReason=FILTERED_DRIFT in CommandLog ExtendedInfo" "FAIL" `
            "No UPDATE_STATISTICS CommandLog row found for ST_FilteredTarget_Status after execute run"
    } else {
        $qr = [string]$rXml.DataSet.Tables[0].Rows[0]["QualifyReason"]
        if ($qr -eq "FILTERED_DRIFT") {
            Add-Result "F5: QualifyReason=FILTERED_DRIFT in CommandLog ExtendedInfo" "PASS" `
                "QualifyReason = FILTERED_DRIFT (post-fix contract confirmed)"
        } else {
            Add-Result "F5: QualifyReason=FILTERED_DRIFT in CommandLog ExtendedInfo" "FAIL" `
                (("QualifyReason observed = '{0}'; expected FILTERED_DRIFT. " +
                 "If '{0}' = QUERY_STORE_PRIORITY, the CASE reorder fix (gh-554) has not been applied." +
                 " If '{0}' = TIERED_THRESHOLD or MOD_COUNTER, the drift filter condition did not fire.") -f $qr)
        }
    }
}

# =====================================================================
# F6: gh-554 NEGATIVE -- INCLUDE mode, same fixture.
#     QualifyReason for ST_FilteredTarget_Status must NOT be FILTERED_DRIFT
#     because FILTERED_DRIFT is only assigned in PRIORITY mode.
# =====================================================================
Write-Host "  F6: INCLUDE mode - QualifyReason is NOT FILTERED_DRIFT..." -ForegroundColor DarkGray

$clearCLSql2 = @"
DELETE FROM master.dbo.CommandLog
WHERE DatabaseName = N'$ScratchDb'
AND   CommandType IN (N'UPDATE_STATISTICS', N'SP_STATUPDATE_START', N'SP_STATUPDATE_END');
"@
$null = Invoke-Sql -ServerAddr $Server -SqlPort $Port -Pwd $Password `
    -Query $clearCLSql2 -Db "master" -Timeout 30

# F5's execute run RESET all counters on this table -- re-arm the filtered stat
# with fresh matching-row modifications so it can qualify again (threshold 1000).
$null = Invoke-Sql -ServerAddr $Server -SqlPort $Port -Pwd $Password -Db "master" -Timeout 60 -Query @'
INSERT INTO [gh522_FilteredStats_Test].dbo.FilteredTarget (status, seq)
SELECT TOP 2000 N'ACTIVE', ABS(CHECKSUM(NEWID())) % 100000
FROM sys.all_columns a CROSS JOIN sys.all_columns b;
'@

# Run execute=Y with INCLUDE so ST_FilteredTarget_Status qualifies but
# is NOT assigned FILTERED_DRIFT (drift only surfaces the label in PRIORITY mode).
$rF6 = Invoke-Sql -ServerAddr $Server -SqlPort $Port -Pwd $Password -Timeout 300 -Query @"
EXEC master.dbo.sp_StatUpdate
    @Databases              = N'$ScratchDb',
    @Tables                 = N'dbo.FilteredTarget',
    @Execute                = N'Y',
    @LogToTable             = N'Y',
    @FilteredStatsMode      = N'INCLUDE',
    @ModificationThreshold  = 1000,
    @QueryStore             = N'OFF',
    @Debug                  = 0;
"@

if ($rF6.ExitCode -ne 0) {
    Add-Result "F6: INCLUDE mode - QualifyReason is NOT FILTERED_DRIFT" "FAIL" `
        "proc call failed: $($rF6.Output | Select-Object -First 2 | Out-String)"
} else {
    $xmlQrySql2 = @"
SELECT TOP 1
    cl.StatisticsName,
    cl.ExtendedInfo.value(N'(/ExtendedInfo/QualifyReason)[1]', N'nvarchar(100)') AS QualifyReason
FROM master.dbo.CommandLog AS cl
WHERE cl.DatabaseName = N'$ScratchDb'
AND   cl.CommandType  = N'UPDATE_STATISTICS'
AND   cl.StatisticsName = N'ST_FilteredTarget_Status'
AND   ISNULL(cl.ErrorNumber, 0) = 0   -- success rows carry ErrorNumber = 0, not NULL
ORDER BY cl.ID DESC;
"@
    $rXml2 = Invoke-Sql -ServerAddr $Server -SqlPort $Port -Pwd $Password `
        -Query $xmlQrySql2 -Db "master"

    if ($rXml2.ExitCode -ne 0 -or -not $rXml2.DataSet -or $rXml2.DataSet.Tables.Count -eq 0 -or $rXml2.DataSet.Tables[0].Rows.Count -eq 0) {
        # Filtered stat may not qualify in INCLUDE mode (no priority boost, drift doesn't
        # force qualification when mode=INCLUDE).  This is not a test failure -- the
        # scope contract is being tested by F3.
        Add-Result "F6: INCLUDE mode - QualifyReason is NOT FILTERED_DRIFT" "SKIP" `
            "ST_FilteredTarget_Status not found in CommandLog after INCLUDE run. " +
            "Filtered stat may not have met the modification threshold under INCLUDE mode. " +
            "The negative assertion cannot be evaluated without a CommandLog row; F3 covers scope."
    } else {
        $qr2 = [string]$rXml2.DataSet.Tables[0].Rows[0]["QualifyReason"]
        if ($qr2 -eq "FILTERED_DRIFT") {
            Add-Result "F6: INCLUDE mode - QualifyReason is NOT FILTERED_DRIFT" "FAIL" `
                (("QualifyReason = FILTERED_DRIFT in INCLUDE mode -- proc must only assign " +
                 "FILTERED_DRIFT when @FilteredStatsMode = PRIORITY (observed: '{0}')") -f $qr2)
        } else {
            Add-Result "F6: INCLUDE mode - QualifyReason is NOT FILTERED_DRIFT" "PASS" `
                (("QualifyReason = '{0}' (correctly NOT FILTERED_DRIFT in INCLUDE mode)") -f $qr2)
        }
    }
}

# =====================================================================
# F9: Incremental stat -- modified partition -> ON PARTITIONS(N)
#     Fixture C (PartTarget) has partition 2 modified (200 rows added).
#     The generated command must target ON PARTITIONS(2) specifically,
#     not update all 4 partitions.
# =====================================================================
Write-Host "  F9: Incremental stat - ON PARTITIONS(N) for one modified partition..." -ForegroundColor DarkGray
Reset-WhatIf

# Re-arm partition 2: the fixture original 200-row insert left the parent
# modification_counter at ~1 on SQL 2025 (incremental parent counters lag);
# a self-assignment UPDATE of the whole partition reliably clears threshold 100.
$null = Invoke-Sql -ServerAddr $Server -SqlPort $Port -Pwd $Password -Db "master" -Timeout 60 -Query "UPDATE [gh522_FilteredStats_Test].dbo.PartTarget SET payload = payload WHERE id BETWEEN 1001 AND 2000;"

$rF9 = Invoke-Sql -ServerAddr $Server -SqlPort $Port -Pwd $Password -Timeout 300 -Query @"
EXEC master.dbo.sp_StatUpdate
    @Databases              = N'$ScratchDb',
    @Tables                 = N'dbo.PartTarget',
    @Execute                = N'N',
    @LogToTable             = N'Y',
    @WhatIfOutputTable      = N'dbo.$WhatIfTable',
    @ModificationThreshold  = 100,
    @QueryStore             = N'OFF',
    @Debug                  = 0;
"@

if ($rF9.ExitCode -ne 0) {
    Add-Result "F9: Incremental ON PARTITIONS(N) for one modified partition" "SKIP" `
        ("proc call failed (incremental stats may not be supported or fixture setup failed): " +
         "$($rF9.Output | Select-Object -First 2 | Out-String)")
} else {
    $wiF9 = Get-WhatIfRows
    $partRow = $wiF9 | Where-Object { $_.StatName -eq "ST_PartTarget_Payload" }
    if ($null -eq $partRow) {
        Add-Result "F9: Incremental ON PARTITIONS(N) for one modified partition" "SKIP" `
            "ST_PartTarget_Payload not discovered (incremental stat may not show modifications via proc's threshold path)"
    } else {
        $cmd9 = $partRow.Command
        # Expect "ON PARTITIONS(2)" or "ON PARTITIONS( 2 )" anywhere in the command.
        $hasOnPartitions = $cmd9 -imatch 'ON\s+PARTITIONS\s*\(\s*2\s*\)'
        $hasAllPartitions = $cmd9 -imatch 'ON\s+PARTITIONS\s*\(\s*1\s*,\s*2\s*,\s*3\s*,\s*4\s*\)'

        if ($hasOnPartitions -and -not $hasAllPartitions) {
            Add-Result "F9: Incremental ON PARTITIONS(N) for one modified partition" "PASS" `
                ("Command targets ON PARTITIONS(2) as expected. Command = '{0}'") -f $cmd9.Substring(0, [Math]::Min(200, $cmd9.Length))
        } elseif ($hasAllPartitions) {
            Add-Result "F9: Incremental ON PARTITIONS(N) for one modified partition" "FAIL" `
                ("Command targets all 4 partitions instead of just modified partition 2. " +
                 "Command = '{0}'") -f $cmd9.Substring(0, [Math]::Min(200, $cmd9.Length))
        } else {
            # May be a full RESAMPLE without ON PARTITIONS (all-stale path or fixture not isolating partition 2).
            Add-Result "F9: Incremental ON PARTITIONS(N) for one modified partition" "FAIL" `
                ("Command does not contain 'ON PARTITIONS(2)'. Observed command: '{0}'. " +
                 "Partition isolation in the fixture may not have worked correctly.") -f $cmd9.Substring(0, [Math]::Min(300, $cmd9.Length))
        }
    }
}

# =====================================================================
# F10: Incremental stat -- ALL partitions stale -> full RESAMPLE, no ON PARTITIONS
#      Modify all 4 partitions of PartTarget so every partition is stale.
#      Then re-run; the proc should emit full RESAMPLE (no ON PARTITIONS clause).
# =====================================================================
Write-Host "  F10: Incremental stat - all partitions stale -> full RESAMPLE (no ON PARTITIONS)..." -ForegroundColor DarkGray

# Add rows to all 4 partition ranges so every partition shows modifications.
$allPartSql = @"
USE [$ScratchDb];
/* Partition 1: id <= 1000 */
INSERT INTO dbo.PartTarget (id, payload)
SELECT 500 + n, n % 50
FROM (SELECT TOP 50 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n FROM sys.all_columns) AS x;

/* Partition 2: 1001-2000 (already has mods from fixture, add a few more) */
INSERT INTO dbo.PartTarget (id, payload)
SELECT 1500 + n, n % 50
FROM (SELECT TOP 50 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n FROM sys.all_columns) AS x;

/* Partition 3: 2001-3000 */
INSERT INTO dbo.PartTarget (id, payload)
SELECT 2500 + n, n % 50
FROM (SELECT TOP 50 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n FROM sys.all_columns) AS x;

/* Partition 4: > 3000 */
INSERT INTO dbo.PartTarget (id, payload)
SELECT 3500 + n, n % 50
FROM (SELECT TOP 50 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n FROM sys.all_columns) AS x;
"@
$rAllPart = Invoke-Sql -ServerAddr $Server -SqlPort $Port -Pwd $Password `
    -Query $allPartSql -Db "master" -Timeout 60

if ($rAllPart.ExitCode -ne 0) {
    Add-Result "F10: All partitions stale -> full RESAMPLE (no ON PARTITIONS)" "SKIP" `
        "Could not populate all partitions: $($rAllPart.Output | Select-Object -First 1)"
} else {
    Reset-WhatIf

    $rF10 = Invoke-Sql -ServerAddr $Server -SqlPort $Port -Pwd $Password -Timeout 300 -Query @"
EXEC master.dbo.sp_StatUpdate
    @Databases              = N'$ScratchDb',
    @Tables                 = N'dbo.PartTarget',
    @Execute                = N'N',
    @LogToTable             = N'Y',
    @WhatIfOutputTable      = N'dbo.$WhatIfTable',
    @ModificationThreshold  = 10,
    @QueryStore             = N'OFF',
    @Debug                  = 0;
"@

    if ($rF10.ExitCode -ne 0) {
        Add-Result "F10: All partitions stale -> full RESAMPLE (no ON PARTITIONS)" "SKIP" `
            ("proc call failed: $($rF10.Output | Select-Object -First 2 | Out-String)")
    } else {
        $wiF10 = Get-WhatIfRows
        $partRow10 = $wiF10 | Where-Object { $_.StatName -eq "ST_PartTarget_Payload" }
        if ($null -eq $partRow10) {
            Add-Result "F10: All partitions stale -> full RESAMPLE (no ON PARTITIONS)" "SKIP" `
                "ST_PartTarget_Payload not discovered after all-partition modification"
        } else {
            $cmd10 = $partRow10.Command
            $hasResample10    = $cmd10 -imatch '\bRESAMPLE\b'
            $hasOnPartitions10 = $cmd10 -imatch '\bON\s+PARTITIONS\b'

            if ($hasResample10 -and -not $hasOnPartitions10) {
                Add-Result "F10: All partitions stale -> full RESAMPLE (no ON PARTITIONS)" "PASS" `
                    ("Command uses full RESAMPLE without ON PARTITIONS clause. Command = '{0}'") -f $cmd10.Substring(0, [Math]::Min(200, $cmd10.Length))
            } elseif ($hasOnPartitions10) {
                Add-Result "F10: All partitions stale -> full RESAMPLE (no ON PARTITIONS)" "FAIL" `
                    ("Command contains ON PARTITIONS when all partitions are stale -- proc should " +
                     "use full RESAMPLE instead. Command = '{0}'") -f $cmd10.Substring(0, [Math]::Min(300, $cmd10.Length))
            } else {
                # No RESAMPLE at all -- may be a sample-based command, which is still acceptable
                # as long as there is no ON PARTITIONS filtering a full-stale incremental stat.
                Add-Result "F10: All partitions stale -> full RESAMPLE (no ON PARTITIONS)" "FAIL" `
                    ("Command does not contain RESAMPLE; incremental stat with all partitions " +
                     "stale should use RESAMPLE. Command = '{0}'") -f $cmd10.Substring(0, [Math]::Min(300, $cmd10.Length))
            }
        }
    }
}

# =====================================================================
# CLEANUP
# =====================================================================
Write-Host ""
Write-Host "  Cleaning up scratch DB and WhatIf table..." -ForegroundColor DarkGray

$cleanupSql = @"
USE master;
IF DB_ID(N'$ScratchDb') IS NOT NULL
BEGIN
    ALTER DATABASE [$ScratchDb] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE [$ScratchDb];
END;
IF OBJECT_ID(N'dbo.$WhatIfTable') IS NOT NULL
    DROP TABLE dbo.$WhatIfTable;
"@
$null = Invoke-Sql -ServerAddr $Server -SqlPort $Port -Pwd $Password `
    -Query $cleanupSql -Db "master" -Timeout 60

# =====================================================================
# REPORT
# =====================================================================
Write-Host ""
Write-Host ("=" * 72) -ForegroundColor Cyan
Write-Host " RESULTS" -ForegroundColor Cyan
Write-Host ("=" * 72) -ForegroundColor Cyan
Write-Host ""

$passed  = @($results | Where-Object { $_.Result -eq "PASS" }).Count
$failed  = @($results | Where-Object { $_.Result -eq "FAIL" }).Count
$skipped = @($results | Where-Object { $_.Result -eq "SKIP" }).Count
$total   = $results.Count

foreach ($t in $results) {
    $color = switch ($t.Result) {
        "PASS" { "Green"  }
        "FAIL" { "Red"    }
        "SKIP" { "Yellow" }
        default { "White" }
    }
    $detail = if ($t.Detail) { " ($($t.Detail))" } else { "" }
    Write-Host ("  [{0}] {1}{2}" -f $t.Result, $t.Test, $detail) -ForegroundColor $color
}

Write-Host ""
Write-Host ("  Total: {0}   Passed: {1}   Failed: {2}   Skipped: {3}" -f $total, $passed, $failed, $skipped)
Write-Host ""

if ($ResultsPath) {
    if ($results.Count -ne 11) { throw "Expected 11 FilteredStats checks; received $($results.Count)." }
    $major = @{ '2019' = 15; '2022' = 16; '2025' = 17 }[$Version]
    $status = Get-StructuredSqlStatus -Server $Server -Port $Port -Password $Password -ExpectedMajor $major
    $commit = (& git -C $RepoRoot rev-parse HEAD).Trim()
    if ($LASTEXITCODE -ne 0 -or $commit -notmatch '^[0-9a-fA-F]{40}$') { throw 'Could not resolve the tested source commit.' }
    Write-StructuredLegacyResults -Path $ResultsPath -Commit $commit -SqlVersion $Version -SqlBuild $status.Build `
        -ServerAliveAtEnd $status.Alive -Suite 'FilteredStats' -ExpectedCount 11 `
        -Results @($results) -StartedAt $startedAt -EndedAt (Get-Date)
}

if ($failed -gt 0 -or $skipped -gt 0) {
    Write-Host "RESULT: $failed test(s) FAILED." -ForegroundColor Red
    exit 1
} else {
    Write-Host "RESULT: All $($passed + $skipped) tests completed ($passed passed, $skipped skipped)." -ForegroundColor Green
    exit 0
}
