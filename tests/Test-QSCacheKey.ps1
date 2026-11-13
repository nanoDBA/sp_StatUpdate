<#
.SYNOPSIS
    Regression tests for tracking issue (gh-556) + tracking issue: Phase 5B/3B QS score cache
    must preserve cached priority scores and validate the full cache identity
    (schema + metric).  Tests Q01-Q08.

.DESCRIPTION
    rcng: three defects in the gh-503 CommandLog-backed QS score cache:
      1. Phase 6 zero-initialized qs_priority_boost for ALL candidates while
         live enrichment skipped qs_cache_hit=1 rows -- every hit ranked as 0.
      2. Cache lookup never compared QSMetric -- a CPU score could be reused
         for a DURATION/WAITS run (gh-503 listed metric change as an
         invalidation condition).
      3. Cache key was DatabaseName+ObjectName+StatisticsName (no SchemaName)
         -- same-named table+stat pairs in different schemas shared scores.

    Fixture: database QSCacheTest (QUERY_STORE ON) with schemas s1/s2, each
    holding an identical table CacheT with index IX_CacheVal, so the two
    stats collide on every key component except schema.

    Q01  baseline: no seed rows -> Phase 5B reports 0 hits, fixture qualifies
    Q02  valid hit: fresh seed (s1, CPU) -> exactly 1 hit
    Q03  metric-change miss: CPU seed, DURATION run -> 0 hits
    Q04  legacy-row miss: seed without QSMetric element -> 0 hits
    Q05  stale miss: QSLastExecution older than recent-hours window -> 0 hits
    Q06  schema key: seed names s2 only -> exactly 1 hit (pre-fix: 2, both
         schemas matched)
    Q07  Phase 3B schema key (tracking issue): seed a ModificationCounter
         row naming s2 only -> exactly 1 stat delta-enriched and skipped as
         already-current; s1 keeps its raw counter (pre-fix: 2 enriched, both
         schemas skipped).  Runs BEFORE Q08 because Q08 executes updates and
         resets the modification counters this test depends on.
    Q08  value survives Phase 6: execute run with fresh s1 seed (boost 12345,
         @SortOrder=QUERY_STORE) -> new CommandLog row for s1 carries
          QSPriorityBoost=12345 (pre-fix: 0) and is processed first
     Q09  ascending-key rescue with few changes and no QS boost -> ASCENDING_KEY
     Q10  ascending-key rescue with QS boost -> ASCENDING_KEY and boost > 0
     Q11  threshold-qualified stat with QS boost -> its threshold reason
     Q12  mop-up-discovered stat remains MOP_UP
     Q13  Diag reads legacy QUERY_STORE_PRIORITY and new ASCENDING_KEY values

    Seeded timestamps use server-side SYSDATETIME() -- the server's clock
    runs behind real time, so client-side dates would break freshness checks.

.PARAMETER Server
    Remote Docker host IP.  Default: 100.88.59.72.

.PARAMETER Password
    SA password.  Default: $env:SPSTATUPDATE_TEST_SQL_PASSWORD.

.PARAMETER Versions
    Which SQL versions to test.  Default: 2019, 2022, 2025.
    The matching container must already be running (tests/Set-SqlContainer.ps1).

.PARAMETER SkipSetup
    Skip proc deployment and fixture DB rebuild (assumes already deployed).

.EXAMPLE
    .\tests\Test-QSCacheKey.ps1 -Versions @("2022")
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

# Extract "Phase 5B (QS cache): X of Y ..." hit count from a run's messages.
# Returns -1 when the line is absent (Phase 5B did not run).
function Get-CacheHits {
    param($RunResult)
    foreach ($m in $RunResult.Output) {
        if ("$m" -match 'Phase 5B \(QS cache\): (\d+) of (\d+) stats') {
            return [int]$Matches[1]
        }
    }
    return -1
}

# Extract "Phase 3B (CommandLog velocity): X stats have update history, Y with
# 0 modifications" counts from a run's messages (icjq renamed this line when it
# dropped the delta model -- see Q07).  Returns @(-1, -1) when the line is absent.
function Get-Phase3B {
    param($RunResult)
    foreach ($m in $RunResult.Output) {
        if ("$m" -match 'Phase 3B \(CommandLog velocity\): (\d+) stats have update history, (\d+) with 0 modifications') {
            return @([int]$Matches[1], [int]$Matches[2])
        }
    }
    return @(-1, -1)
}

# One CommandLog seed row, all times server-side.  $MetricXml empty = legacy row.
function New-SeedSql {
    param([string]$Schema, [string]$Table, [string]$Stat, [long]$Boost,
          [string]$Metric, [int]$LastExecHoursAgo = 0)
    $metricXml = if ($Metric) { "<QSMetric>$Metric</QSMetric>" } else { "" }
    return @"
INSERT INTO master.dbo.CommandLog
    (DatabaseName, SchemaName, ObjectName, ObjectType, StatisticsName,
     ExtendedInfo, Command, CommandType, StartTime, EndTime, ErrorNumber)
SELECT
    N'QSCacheTest', N'$Schema', N'$Table', N'U', N'$Stat',
    CONVERT(xml,
        N'<ExtendedInfo><QSPriorityBoost>$Boost</QSPriorityBoost>'
        + N'<QSTotalCpuMs>777</QSTotalCpuMs>'
        + N'<QSTotalExecutions>500</QSTotalExecutions>'
        + N'<QSPlanCount>3</QSPlanCount>'
        + N'<QSLastExecution>'
        + CONVERT(nvarchar(30), DATEADD(HOUR, -$LastExecHoursAgo, SYSDATETIME()), 126)
        + N'</QSLastExecution>$metricXml</ExtendedInfo>'),
    N'UPDATE STATISTICS [QSCacheTest].[$Schema].[$Table] ([$Stat]);',
    N'UPDATE_STATISTICS',
    DATEADD(MINUTE, -5, SYSDATETIME()), DATEADD(MINUTE, -4, SYSDATETIME()), 0;
"@
}

$ClearSeeds = "DELETE FROM master.dbo.CommandLog WHERE DatabaseName = N'QSCacheTest';"

# Dry-run invocation; $Metric switches @QueryStore.
function New-RunSql {
    param([string]$Metric)
    return @"
DECLARE @found int;
EXEC master.dbo.sp_StatUpdate
    @Databases = N'QSCacheTest',
    @QueryStore = N'$Metric',
    @ModificationThreshold = 10,
    @Execute = N'N',
    @LogToTable = N'N',
    @Debug = 1,
    @StatsFoundOut = @found OUTPUT;
SELECT TestStatsFound = @found;
"@
}

function New-LoggedReasonRunSql {
    param(
        [string]$Tables,
        [string]$Metric = 'OFF',
        [long]$Threshold = 1000,
        [string]$MopUp = 'N',
        [string]$Execute = 'Y'
    )
    return @"
DECLARE @before int = ISNULL((SELECT MAX(ID) FROM master.dbo.CommandLog), 0);
EXEC master.dbo.sp_StatUpdate
    @Databases = N'QSCacheTest',
    @Tables = N'$Tables',
    @QueryStore = N'$Metric',
    @SortOrder = N'QUERY_STORE',
    @ModificationThreshold = $Threshold,
    @StaleHours = 999999,
    @QueryStoreMinExecutions = 1,
    @MopUpPass = N'$MopUp',
    @MopUpMinRemainingSeconds = 1,
    @TimeLimit = 300,
    @Execute = N'$Execute',
    @LogToTable = N'Y',
    @Debug = 1;
SELECT
    ID,
    StatisticsName,
    QualifyReason = ExtendedInfo.value(N'(/ExtendedInfo/QualifyReason)[1]', N'nvarchar(100)'),
    QSPriorityBoost = ExtendedInfo.value(N'(/ExtendedInfo/QSPriorityBoost)[1]', N'bigint')
FROM master.dbo.CommandLog
WHERE ID > @before
  AND DatabaseName = N'QSCacheTest'
  AND CommandType = N'UPDATE_STATISTICS'
ORDER BY ID;
"@
}

function Get-ReasonRows {
    param($RunResult)
    if (-not $RunResult.DataSet) { return @() }
    foreach ($table in $RunResult.DataSet.Tables) {
        if ($table.Columns.Contains('StatisticsName') -and $table.Columns.Contains('QualifyReason')) {
            return @($table.Rows | ForEach-Object {
                [pscustomobject]@{
                    ID = [int]$_.ID
                    StatisticsName = [string]$_.StatisticsName
                    QualifyReason = if ($_.QualifyReason -is [DBNull]) { '' } else { [string]$_.QualifyReason }
                    QSPriorityBoost = if ($_.QSPriorityBoost -is [DBNull]) { 0L } else { [long]$_.QSPriorityBoost }
                }
            })
        }
    }
    return @()
}

Write-Host ""
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host " Test-QSCacheKey -- Query Store score cache (Q01-Q08)" -ForegroundColor Cyan
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
    # SETUP: proc + CommandLog (master) + QSCacheTest fixture DB
    # ------------------------------------------------------------------
    if (-not $SkipSetup) {
        Write-Host "  Deploying proc + fixture DB ..."
        if (-not (Deploy-SqlFile -ServerAddr $Server -Port $port -Pwd $Password -Path $ProcFile)) {
            Record $ver "SETUP" $false "proc deploy failed"; continue
        }
    }

    $setup = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Timeout 300 -Query @"
IF OBJECT_ID(N'dbo.CommandLog', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.CommandLog (
        ID INT IDENTITY(1,1) PRIMARY KEY,
        DatabaseName NVARCHAR(128), SchemaName NVARCHAR(128),
        ObjectName NVARCHAR(128), ObjectType NVARCHAR(50),
        IndexName NVARCHAR(128), IndexType TINYINT,
        StatisticsName NVARCHAR(128), PartitionNumber INT,
        ExtendedInfo XML, Command NVARCHAR(MAX), CommandType NVARCHAR(60),
        StartTime DATETIME2, EndTime DATETIME2,
        ErrorNumber INT, ErrorMessage NVARCHAR(MAX)
    );
END;
IF DB_ID(N'QSCacheTest') IS NOT NULL
BEGIN
    ALTER DATABASE QSCacheTest SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE QSCacheTest;
END;
CREATE DATABASE QSCacheTest;
ALTER DATABASE QSCacheTest SET QUERY_STORE = ON;
ALTER DATABASE QSCacheTest SET QUERY_STORE (OPERATION_MODE = READ_WRITE);
"@
    if ($setup.ExitCode -ne 0) { Record $ver "SETUP" $false "fixture DB create failed: $($setup.Output -join '; ')"; continue }

    $fixture = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db "QSCacheTest" -Timeout 300 -Query @"
EXEC(N'CREATE SCHEMA s1;');
EXEC(N'CREATE SCHEMA s2;');
CREATE TABLE s1.CacheT (id int IDENTITY(1,1) PRIMARY KEY, val int NOT NULL);
CREATE TABLE s2.CacheT (id int IDENTITY(1,1) PRIMARY KEY, val int NOT NULL);
CREATE INDEX IX_CacheVal ON s1.CacheT (val);
CREATE INDEX IX_CacheVal ON s2.CacheT (val);
INSERT INTO s1.CacheT (val) SELECT TOP (300) ABS(CHECKSUM(NEWID())) % 1000 FROM sys.all_objects;
INSERT INTO s2.CacheT (val) SELECT TOP (300) ABS(CHECKSUM(NEWID())) % 1000 FROM sys.all_objects;
/* CREATE INDEX on an empty table leaves NO stats blob (last_updated NULL,
   modification_counter NULL) -- such stats qualify via the NEVER_UPDATED
   rescue and carry no usable counters for Phase 3B.  Compute real histograms
   on the populated tables, THEN modify, so modification_counter = 300 and
   last_updated is set. */
UPDATE STATISTICS s1.CacheT;
UPDATE STATISTICS s2.CacheT;
INSERT INTO s1.CacheT (val) SELECT TOP (300) ABS(CHECKSUM(NEWID())) % 1000 FROM sys.all_objects;
INSERT INTO s2.CacheT (val) SELECT TOP (300) ABS(CHECKSUM(NEWID())) % 1000 FROM sys.all_objects;

ALTER DATABASE QSCacheTest SET AUTO_UPDATE_STATISTICS OFF;
ALTER DATABASE QSCacheTest SET AUTO_CREATE_STATISTICS OFF;

CREATE TABLE s1.QR_AscNoBoost (
    id int NOT NULL IDENTITY(1,1) CONSTRAINT PK_QR_AscNoBoost PRIMARY KEY CLUSTERED,
    payload int NOT NULL
);
INSERT INTO s1.QR_AscNoBoost (payload) SELECT TOP (100) 1 FROM sys.all_objects;
UPDATE STATISTICS s1.QR_AscNoBoost WITH FULLSCAN;
INSERT INTO s1.QR_AscNoBoost (payload) VALUES (1);

CREATE TABLE s1.QR_AscBoost (
    id int NOT NULL IDENTITY(1,1) CONSTRAINT PK_QR_AscBoost PRIMARY KEY CLUSTERED,
    payload int NOT NULL
);
INSERT INTO s1.QR_AscBoost (payload) SELECT TOP (100) 1 FROM sys.all_objects;
UPDATE STATISTICS s1.QR_AscBoost WITH FULLSCAN;
INSERT INTO s1.QR_AscBoost (payload) VALUES (1);

CREATE TABLE s1.QR_ThresholdBoost (payload int NOT NULL);
INSERT INTO s1.QR_ThresholdBoost (payload) SELECT TOP (100) 1 FROM sys.all_objects;
CREATE STATISTICS ST_QR_Threshold ON s1.QR_ThresholdBoost (payload);
UPDATE STATISTICS s1.QR_ThresholdBoost WITH FULLSCAN;
INSERT INTO s1.QR_ThresholdBoost (payload) SELECT TOP (600) 2 FROM sys.all_objects;

CREATE TABLE s1.QR_MopUp (payload int NOT NULL);
INSERT INTO s1.QR_MopUp (payload) SELECT TOP (100) 1 FROM sys.all_objects;
CREATE STATISTICS ST_QR_MopUp ON s1.QR_MopUp (payload);
UPDATE STATISTICS s1.QR_MopUp WITH FULLSCAN;
INSERT INTO s1.QR_MopUp (payload) VALUES (2);
"@
    if ($fixture.ExitCode -ne 0) { Record $ver "SETUP" $false "fixture tables failed: $($fixture.Output -join '; ')"; continue }
    Record $ver "SETUP" $true "proc deployed, QSCacheTest fixture built"

    # ------------------------------------------------------------------
    # Q01: baseline -- no seeds -> Phase 5B runs with 0 hits, fixture qualifies
    # ------------------------------------------------------------------
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query ($ClearSeeds + (New-RunSql -Metric "CPU"))
    $hits = Get-CacheHits $r
    $found = -1
    if ($r.DataSet) {
        foreach ($t in $r.DataSet.Tables) {
            if ($t.Columns.Contains("TestStatsFound") -and $t.Rows.Count -gt 0 -and $t.Rows[0]["TestStatsFound"] -isnot [DBNull]) {
                $found = [int]$t.Rows[0]["TestStatsFound"]
            }
        }
    }
    Record $ver "Q01-baseline" ($hits -eq 0 -and $found -ge 2) "hits=$hits (want 0), StatsFound=$found (want >=2)"

    # ------------------------------------------------------------------
    # Q02: valid hit -- fresh s1/CPU seed -> exactly 1 hit
    # ------------------------------------------------------------------
    $seed = New-SeedSql -Schema "s1" -Table "CacheT" -Stat "IX_CacheVal" -Boost 12345 -Metric "CPU"
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query ($ClearSeeds + $seed + (New-RunSql -Metric "CPU"))
    $hits = Get-CacheHits $r
    Record $ver "Q02-valid-hit" ($hits -eq 1) "hits=$hits (want 1)"

    # ------------------------------------------------------------------
    # Q03: metric change -- CPU seed, DURATION run -> 0 hits (forced live)
    # ------------------------------------------------------------------
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query ($ClearSeeds + $seed + (New-RunSql -Metric "DURATION"))
    $hits = Get-CacheHits $r
    Record $ver "Q03-metric-miss" ($hits -eq 0) "hits=$hits (want 0; CPU seed must not serve DURATION run)"

    # ------------------------------------------------------------------
    # Q04: legacy row (no QSMetric element) -> 0 hits
    # ------------------------------------------------------------------
    $legacy = New-SeedSql -Schema "s1" -Table "CacheT" -Stat "IX_CacheVal" -Boost 12345 -Metric ""
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query ($ClearSeeds + $legacy + (New-RunSql -Metric "CPU"))
    $hits = Get-CacheHits $r
    Record $ver "Q04-legacy-miss" ($hits -eq 0) "hits=$hits (want 0; row without QSMetric is a miss)"

    # ------------------------------------------------------------------
    # Q05: stale timestamp (200h ago > 168h window) -> 0 hits
    # ------------------------------------------------------------------
    $stale = New-SeedSql -Schema "s1" -Table "CacheT" -Stat "IX_CacheVal" -Boost 12345 -Metric "CPU" -LastExecHoursAgo 200
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query ($ClearSeeds + $stale + (New-RunSql -Metric "CPU"))
    $hits = Get-CacheHits $r
    Record $ver "Q05-stale-miss" ($hits -eq 0) "hits=$hits (want 0; QSLastExecution outside recent-hours window)"

    # ------------------------------------------------------------------
    # Q06: schema key -- seed names s2 only; s1.CacheT.IX_CacheVal must NOT
    # match it (pre-fix: both schemas hit -> 2)
    # ------------------------------------------------------------------
    $seedS2 = New-SeedSql -Schema "s2" -Table "CacheT" -Stat "IX_CacheVal" -Boost 22222 -Metric "CPU"
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query ($ClearSeeds + $seedS2 + (New-RunSql -Metric "CPU"))
    $hits = Get-CacheHits $r
    Record $ver "Q06-schema-key" ($hits -eq 1) "hits=$hits (want 1; pre-fix both schemas matched = 2)"

    # ------------------------------------------------------------------
    # Q07: Phase 3B CommandLog enrichment schema key (tracking issue) --
    # a CommandLog row naming s2 must supply update history to s2 only, never s1.
    # The enrichment count (cl_last_counter IS NOT NULL) is still schema-scoped,
    # so an s2-only seed enriches exactly 1 stat; pre-fix the schema-less key
    # matched BOTH schemas (2 enriched).
    #
    # icjq: Phase 3B no longer subtracts the logged counter (effective = raw), so
    # the seed must NOT make any stat's effective_counter 0 / skip it as current.
    # The "0 modifications" count therefore stays put between baseline and seeded
    # runs (delta 0) -- the pre-icjq formula would have zeroed the matched stat.
    # ------------------------------------------------------------------
    # Hand-rolled rather than New-SeedSql: Phase 3B keys off a ModificationCounter
    # element, and the helper only emits the QS-cache fields.
    $seed3b = @"
INSERT INTO master.dbo.CommandLog
    (DatabaseName, SchemaName, ObjectName, ObjectType, StatisticsName,
     ExtendedInfo, Command, CommandType, StartTime, EndTime, ErrorNumber)
SELECT
    N'QSCacheTest', N's2', N'CacheT', N'U', N'IX_CacheVal',
    CONVERT(xml, N'<ExtendedInfo><ModificationCounter>300</ModificationCounter></ExtendedInfo>'),
    N'UPDATE STATISTICS [QSCacheTest].[s2].[CacheT] ([IX_CacheVal]);',
    N'UPDATE_STATISTICS',
    DATEADD(MINUTE, -5, SYSDATETIME()), DATEADD(MINUTE, -4, SYSDATETIME()), 0;
"@
    # Assert the DELTA between an unseeded baseline and the seeded run, not
    # absolutes -- other candidate stats can contribute update-history noise.
    $rBase = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query ($ClearSeeds + (New-RunSql -Metric "CPU"))
    $base = Get-Phase3B $rBase
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query ($ClearSeeds + $seed3b + (New-RunSql -Metric "CPU"))
    $seeded = Get-Phase3B $r
    $enrichedDelta = $seeded[0] - $base[0]
    $zeroDelta     = $seeded[1] - $base[1]
    Record $ver "Q07-3b-schema-key" ($base[0] -eq 0 -and $enrichedDelta -eq 1 -and $zeroDelta -eq 0) "baseline history=$($base[0])/zero=$($base[1]); seeded history=$($seeded[0])/zero=$($seeded[1]); deltas +$enrichedDelta history / +$zeroDelta zero (want +1 history schema-scoped, +0 zero since icjq effective=raw never skips; pre-fix +2 history)"

    # ------------------------------------------------------------------
    # Q08: value survives Phase 6 -- execute with fresh s1 seed; the new
    # CommandLog row must carry the cached boost (pre-fix: 0) and the boosted
    # stat must be processed first under QUERY_STORE sort order.
    # ------------------------------------------------------------------
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Timeout 300 -Query @"
$ClearSeeds
$seed
DECLARE @before int = ISNULL((SELECT MAX(ID) FROM master.dbo.CommandLog), 0);
EXEC master.dbo.sp_StatUpdate
    @Databases = N'QSCacheTest',
    @Tables = N's1.CacheT',
    @QueryStore = N'CPU',
    @SortOrder = N'QUERY_STORE',
    @ModificationThreshold = 10,
    @Execute = N'Y',
    @LogToTable = N'Y',
    @Debug = 1;
SELECT
    BoostOfSeededStat = (
        SELECT TOP (1) cl.ExtendedInfo.value(N'(/ExtendedInfo/QSPriorityBoost)[1]', N'bigint')
        FROM master.dbo.CommandLog AS cl
        WHERE cl.ID > @before  /* only rows written by this run, not the seed */
        AND   cl.DatabaseName = N'QSCacheTest'
        AND   cl.CommandType = N'UPDATE_STATISTICS'
        AND   cl.SchemaName = N's1'
        AND   cl.ObjectName = N'CacheT'
        AND   cl.StatisticsName = N'IX_CacheVal'
        ORDER BY cl.ID DESC
    ),
    SeededProcessedFirst = CASE WHEN (
        SELECT TOP (1) cl.SchemaName + N'.' + cl.ObjectName + N'.' + cl.StatisticsName
        FROM master.dbo.CommandLog AS cl
        WHERE cl.ID > @before
        AND   cl.DatabaseName = N'QSCacheTest'
        AND   cl.CommandType = N'UPDATE_STATISTICS'
        ORDER BY cl.ID ASC
    ) = N's1.CacheT.IX_CacheVal' THEN 1 ELSE 0 END;
"@
    $boost = -1; $first = -1
    if ($r.DataSet) {
        foreach ($t in $r.DataSet.Tables) {
            if ($t.Columns.Contains("BoostOfSeededStat") -and $t.Rows.Count -gt 0) {
                $boost = if ($t.Rows[0]["BoostOfSeededStat"] -is [DBNull]) { -1 } else { [long]$t.Rows[0]["BoostOfSeededStat"] }
                $first = [int]$t.Rows[0]["SeededProcessedFirst"]
            }
        }
    }
    Record $ver "Q08-boost-survives" ($boost -eq 12345 -and $first -eq 1) "logged boost=$boost (want 12345; pre-fix 0), processedFirst=$first (want 1)"

    # Q09: one change on an identity-leading statistic, below the threshold,
    # without any Query Store boost. The rescue rule is the only qualifier.
    $null = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query $ClearSeeds
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Timeout 300 `
        -Query (New-LoggedReasonRunSql -Tables 's1.QR_AscNoBoost' -Metric 'OFF' -Threshold 1000)
    $reasonRows = @(Get-ReasonRows $r)
    $ascNoBoost = $reasonRows | Where-Object StatisticsName -eq 'PK_QR_AscNoBoost' | Select-Object -Last 1
    Record $ver 'Q09-ascending-no-boost' ($ascNoBoost -and $ascNoBoost.QualifyReason -eq 'ASCENDING_KEY' -and $ascNoBoost.QSPriorityBoost -eq 0) `
        "reason=$(if ($ascNoBoost) { $ascNoBoost.QualifyReason } else { 'missing' }), boost=$(if ($ascNoBoost) { $ascNoBoost.QSPriorityBoost } else { -1 }) (want ASCENDING_KEY, 0)"

    # Q10/Q11: the same run contains a low-change ascending rescue and a
    # threshold-qualified statistic, both with cached boosts on their table.
    $seedAscReason = New-SeedSql -Schema 's1' -Table 'QR_AscBoost' -Stat 'PK_QR_AscBoost' -Boost 12345 -Metric 'CPU'
    $seedThresholdReason = New-SeedSql -Schema 's1' -Table 'QR_ThresholdBoost' -Stat 'ST_QR_Threshold' -Boost 23456 -Metric 'CPU'
    $reasonRunSql = $ClearSeeds + $seedAscReason + $seedThresholdReason + `
        (New-LoggedReasonRunSql -Tables 's1.QR_AscBoost,s1.QR_ThresholdBoost' -Metric 'CPU' -Threshold 10)
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Timeout 300 -Query $reasonRunSql
    $reasonRows = @(Get-ReasonRows $r)
    $ascBoost = $reasonRows | Where-Object StatisticsName -eq 'PK_QR_AscBoost' | Select-Object -Last 1
    $thresholdBoost = $reasonRows | Where-Object StatisticsName -eq 'ST_QR_Threshold' | Select-Object -Last 1
    Record $ver 'Q10-ascending-with-boost' ($ascBoost -and $ascBoost.QualifyReason -eq 'ASCENDING_KEY' -and $ascBoost.QSPriorityBoost -gt 0) `
        "reason=$(if ($ascBoost) { $ascBoost.QualifyReason } else { 'missing' }), boost=$(if ($ascBoost) { $ascBoost.QSPriorityBoost } else { -1 }) (want ASCENDING_KEY, >0)"
    Record $ver 'Q11-threshold-with-boost' ($thresholdBoost -and $thresholdBoost.QualifyReason -eq 'TIERED_THRESHOLD' -and $thresholdBoost.QSPriorityBoost -gt 0) `
        "reason=$(if ($thresholdBoost) { $thresholdBoost.QualifyReason } else { 'missing' }), boost=$(if ($thresholdBoost) { $thresholdBoost.QSPriorityBoost } else { -1 }) (want TIERED_THRESHOLD, >0)"

    # Q13: preserve legacy CommandLog values and pass the new value through Diag.
    if ($ascBoost -and $thresholdBoost) {
        $diagQuery = @"
UPDATE master.dbo.CommandLog
SET ExtendedInfo.modify('replace value of (/ExtendedInfo/QualifyReason/text())[1] with "ASCENDING_KEY"')
WHERE ID = $($ascBoost.ID);
UPDATE master.dbo.CommandLog
SET ExtendedInfo.modify('replace value of (/ExtendedInfo/QualifyReason/text())[1] with "QUERY_STORE_PRIORITY"')
WHERE ID = $($thresholdBoost.ID);
EXEC master.dbo.sp_StatUpdate_Diag @DaysBack = 30, @ExpertMode = 1, @SkipHistory = 1;
"@
        $diag = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Timeout 300 -Query $diagQuery
        $diagReasons = @()
        if ($diag.DataSet) {
            foreach ($table in $diag.DataSet.Tables) {
                if ($table.Columns.Contains('QualifyReason')) {
                    foreach ($row in $table.Rows) {
                        if ($row.QualifyReason -isnot [DBNull]) { $diagReasons += [string]$row.QualifyReason }
                    }
                }
            }
        }
        $diagCompat = $diag.ExitCode -eq 0 -and $diagReasons -contains 'QUERY_STORE_PRIORITY' -and $diagReasons -contains 'ASCENDING_KEY'
        Record $ver 'Q13-diag-legacy-and-new-reasons' $diagCompat "Diag values present: legacy=$($diagReasons -contains 'QUERY_STORE_PRIORITY'), ascending=$($diagReasons -contains 'ASCENDING_KEY')"
    } else {
        Record $ver 'Q13-diag-legacy-and-new-reasons' $false 'Could not produce both boosted CommandLog fixture rows.'
    }

    # Q12: a statistic admitted only by the mop-up pass retains its reason.
    $mopUpSql = New-LoggedReasonRunSql -Tables 's1.QR_MopUp' -Metric 'OFF' -Threshold 10000 -MopUp 'Y' -Execute 'Y'
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Timeout 300 -Query ($ClearSeeds + $mopUpSql)
    $reasonRows = @(Get-ReasonRows $r)
    $mopUpReason = $reasonRows | Where-Object StatisticsName -eq 'ST_QR_MopUp' | Select-Object -Last 1
    Record $ver 'Q12-mop-up-reason' ($mopUpReason -and $mopUpReason.QualifyReason -eq 'MOP_UP') `
        "reason=$(if ($mopUpReason) { $mopUpReason.QualifyReason } else { 'missing' }) (want MOP_UP)"

    # Cleanup fixture rows so later suites see a clean CommandLog
    $null = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query $ClearSeeds
}

# ----------------------------------------------------------------------
# SUMMARY
# ----------------------------------------------------------------------
Write-Host ""
Write-Host ("=" * 70) -ForegroundColor Cyan
$pass = @($script:Results | Where-Object { $_.Pass }).Count
$fail = @($script:Results | Where-Object { -not $_.Pass }).Count
Write-Host (" Test-QSCacheKey: {0} passed, {1} failed" -f $pass, $fail) -ForegroundColor $(if ($fail -eq 0) { "Green" } else { "Red" })

if ($ResultsPath) {
    if ($Versions.Count -ne 1 -or $script:Results.Count -ne 14) {
        throw "Results output requires one SQL version and all 14 declared checks; got versions=$($Versions.Count), checks=$($script:Results.Count)."
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
        -ServerAliveAtEnd $serverAliveAtEnd -Suite "QSCacheKey" -ExpectedCount 14 -Checks $checks `
        -StartedAt $script:RunStartedAt -EndedAt $endedAt -DurationMs $runDurationMs)
}

if ($fail -gt 0) {
    foreach ($f in ($script:Results | Where-Object { -not $_.Pass })) {
        Write-Host ("   FAIL [{0}] {1} -- {2}" -f $f.Version, $f.Id, $f.Detail) -ForegroundColor Red
    }
    exit 1
}
exit 0
