<#
.SYNOPSIS
    Regression test for gh-550: statistics that have NEVER been computed
    (last_updated = NULL) over POPULATED tables must qualify in discovery
    with QualifyReason = NEVER_UPDATED, while fresh stats on EMPTY tables
    must NOT qualify.

.DESCRIPTION
    Before this fix (GitHub gh-550), fresh stats
    on populated tables were silently skipped in Phase 4 qualification.

    Root cause: sys.dm_db_stats_properties returns modification_counter = NULL
    and rows = NULL for never-computed stats.  Phase 2 enrichment coerces
    these to 0 via ISNULL, and Phase 4 gated the entire OR-chain on
    "effective_counter > 0", which is false for fresh stats.

    Fix: Phase 2b now populates a physical_row_count column from sys.partitions
    for candidates with last_updated IS NULL.  Phase 4 outer gate becomes
    "(effective_counter > 0 OR (last_updated IS NULL AND physical_row_count > 0))",
    and a new NEVER_UPDATED inner branch qualifies these stats.

    This test creates a scratch database with:
      - dbo.FreshPopulated:  populated (1000 rows), stat that has NEVER been queried
      - dbo.FreshEmpty:      empty table with a stat
      - dbo.ConventionallyModified: pre-updated stat with modifications since

    Then runs sp_StatUpdate in dry-run + @LogToTable=Y + @WhatIfOutputTable
    discovery and asserts:
      (a) fresh stat on populated table IS discovered/qualifies
      (b) fresh stat on empty table is NOT discovered
      (c) conventionally-modified stat still qualifies (regression guard)
      (d) SP_STATUPDATE_START debug output contains "NEVER_UPDATED rescue
          qualified" text (observable dry-run signal for QualifyReason).

    The QualifyReason = NEVER_UPDATED string itself only surfaces in
    ExtendedInfo XML during Execute=Y (or Diag output), so assertion (d)
    proves the code path fires via the Phase 4 debug message rather than
    the XML tag.

.PARAMETER Server
    Remote Docker host IP.  Default: 100.88.59.72.

.PARAMETER Port
    Port for the target SQL instance.  Default: 1431 (SQL 2025 test lab).

.PARAMETER Password
    SA password.  Default: $env:SPSTATUPDATE_TEST_SQL_PASSWORD.

.EXAMPLE
    .\tests\Test-FreshStatQualification.ps1
    .\tests\Test-FreshStatQualification.ps1 -Server 127.0.0.1 -Port 1432 -Password $env:SPSTATUPDATE_TEST_SQL_PASSWORD
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$Server,
    [ValidateSet('2019','2022','2025')][string]$Version = '2022',
    [string]$Password = $(if ($env:SPSTATUPDATE_TEST_SQL_PASSWORD) { $env:SPSTATUPDATE_TEST_SQL_PASSWORD } else { '' }),
    [string]$ResultsPath = ''
)

$ErrorActionPreference = "Continue"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot  = Split-Path -Parent $ScriptDir
$Port = @{ '2019' = 1434; '2022' = 1432; '2025' = 1431 }[$Version]
$startedAt = Get-Date
Import-Module (Join-Path $ScriptDir 'StructuredLegacyResults.psm1') -Force
if (-not $Password) { throw 'Release-gate SQL credentials are unavailable.' }
$ProcFile  = Join-Path $RepoRoot "sp_StatUpdate.sql"

$ScratchDb   = "gh550_FreshStat_Test"
$WhatIfTable = "gh550_WhatIf"

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
    $connStr = "Server=$ServerAddr,$Port;Database=$Db;User Id=sa;Password=$Pwd;Encrypt=false;TrustServerCertificate=true;Connection Timeout=30;Pooling=false"
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

# =====================================================================
# HEADER
# =====================================================================
Write-Host ""
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host " gh-550 Fresh-Stat Qualification Regression Test" -ForegroundColor Cyan
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host ""
Write-Host "  Target: $Server`:$Port"
Write-Host "  DB:     $ScratchDb"
Write-Host ""

# =====================================================================
# STEP 1: VERIFY CONNECTIVITY
# =====================================================================
Write-Host "[1/5] Verifying connectivity..." -ForegroundColor Yellow

$r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password `
    -Query "SELECT @@VERSION AS v;"
if ($r.ExitCode -ne 0) {
    Write-Host "  ERROR: Cannot connect -- $($r.Output | Select-Object -First 1)" -ForegroundColor Red
    exit 1
}
$verStr = if ($r.DataSet.Tables.Count -gt 0) {
    ([string]$r.DataSet.Tables[0].Rows[0]["v"]).Substring(0, [Math]::Min(60, ([string]$r.DataSet.Tables[0].Rows[0]["v"]).Length))
} else { "Connected" }
Write-Host "  READY: $verStr" -ForegroundColor Green
Write-Host ""

if (-not (Test-Path $ProcFile)) {
    Write-Host "ERROR: Proc file not found: $ProcFile" -ForegroundColor Red
    exit 1
}

# =====================================================================
# STEP 2: DEPLOY PROC (in master)
# =====================================================================
Write-Host "[2/5] Deploying sp_StatUpdate to master..." -ForegroundColor Yellow

$procSql = [System.IO.File]::ReadAllText($ProcFile)
$batches = [regex]::Split($procSql, '(?m)^\s*GO\s*$')
$deployFailed = $false

foreach ($batch in $batches) {
    $trimmed = $batch.Trim()
    if ($trimmed.Length -eq 0) { continue }
    $r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query $trimmed -Db "master" -Timeout 300
    if ($r.ExitCode -ne 0) {
        Write-Host "  PROC DEPLOY FAILED: $($r.Output | Select-Object -First 3)" -ForegroundColor Red
        $deployFailed = $true
        break
    }
}
if ($deployFailed) { exit 1 }
Write-Host "  OK" -ForegroundColor Green
Write-Host ""

# =====================================================================
# STEP 3: CREATE SCRATCH DB + TEST TABLES
# =====================================================================
Write-Host "[3/5] Creating scratch DB and test fixtures..." -ForegroundColor Yellow

# Drop + recreate scratch DB so state is deterministic across re-runs.
$dropSql = @"
IF DB_ID(N'$ScratchDb') IS NOT NULL
BEGIN
    ALTER DATABASE [$ScratchDb] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE [$ScratchDb];
END;
CREATE DATABASE [$ScratchDb];
"@
$r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query $dropSql -Db "master" -Timeout 60
if ($r.ExitCode -ne 0) {
    Write-Host "  ERROR creating DB: $($r.Output | Select-Object -First 1)" -ForegroundColor Red
    exit 1
}

# Create three tables with distinct stat states:
#   FreshPopulated: 1000 rows, PK stat has NEVER been computed
#      -> Fresh CREATE + populated INSERT + NO query touching indexed col
#         + AUTO_UPDATE_STATISTICS is OFF for this DB to prevent implicit
#         compute during INSERT/probe.
#      NOTE: on CREATE TABLE with PRIMARY KEY, SQL Server does NOT
#      compute the stat until either an UPDATE STATISTICS runs, a query
#      references the column, or auto-update fires past threshold.  We
#      keep this table quiet -- no SELECT touches the PK.
#   FreshEmpty:    0 rows, PK stat has never been computed.
#   ConventionallyModified: populated, PK stat was UPDATE STATISTICS'd
#      after INSERT (so last_updated IS NOT NULL), then extra rows added
#      to drive modification_counter above @ModificationThreshold.
$setupSql = @"
USE [$ScratchDb];

-- Disable auto-update so we can observe the truly "never computed" state.
ALTER DATABASE [$ScratchDb] SET AUTO_UPDATE_STATISTICS OFF;
ALTER DATABASE [$ScratchDb] SET AUTO_CREATE_STATISTICS OFF;

-- (1) FreshPopulated: fresh stat, populated table.
CREATE TABLE dbo.FreshPopulated (
    id int NOT NULL IDENTITY(1,1) PRIMARY KEY CLUSTERED,
    payload nvarchar(50) NOT NULL
);

INSERT INTO dbo.FreshPopulated (payload)
SELECT TOP 1000 REPLICATE(N'x', 50)
FROM sys.all_columns AS a
CROSS JOIN sys.all_columns AS b;

-- Intentionally do NOT query the PK column -- keeps sys.dm_db_stats_properties
-- returning NULL for last_updated/modification_counter/rows.

-- (2) FreshEmpty: fresh stat, empty table.
CREATE TABLE dbo.FreshEmpty (
    id int NOT NULL IDENTITY(1,1) PRIMARY KEY CLUSTERED,
    payload nvarchar(50) NOT NULL
);

-- (3) ConventionallyModified: pre-computed stat, then modifications.
CREATE TABLE dbo.ConventionallyModified (
    id int NOT NULL IDENTITY(1,1) PRIMARY KEY CLUSTERED,
    payload nvarchar(50) NOT NULL
);

INSERT INTO dbo.ConventionallyModified (payload)
SELECT TOP 1000 REPLICATE(N'y', 50)
FROM sys.all_columns AS a
CROSS JOIN sys.all_columns AS b;

-- Force stat computation so last_updated IS NOT NULL.
UPDATE STATISTICS dbo.ConventionallyModified WITH FULLSCAN;

-- Add modifications above default @ModificationThreshold (5000).
INSERT INTO dbo.ConventionallyModified (payload)
SELECT TOP 6000 REPLICATE(N'z', 50)
FROM sys.all_columns AS a
CROSS JOIN sys.all_columns AS b;
"@

$r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query $setupSql -Db "master" -Timeout 120
if ($r.ExitCode -ne 0) {
    Write-Host "  ERROR seeding data: $($r.Output | Select-Object -First 3)" -ForegroundColor Red
    exit 1
}

# Confirm baseline: FreshPopulated.PK stat is truly never-computed.
$baselineQry = @"
USE [$ScratchDb];
SELECT
    t.name AS table_name,
    s.name AS stat_name,
    sp.last_updated,
    sp.rows AS stat_rows,
    sp.modification_counter,
    (SELECT SUM(p.rows) FROM sys.partitions AS p
     WHERE p.object_id = t.object_id AND p.index_id IN (0,1)) AS physical_rows
FROM sys.tables AS t
JOIN sys.stats AS s ON s.object_id = t.object_id
OUTER APPLY sys.dm_db_stats_properties(t.object_id, s.stats_id) AS sp
WHERE t.name IN (N'FreshPopulated', N'FreshEmpty', N'ConventionallyModified')
ORDER BY t.name, s.stats_id;
"@
$rBase = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query $baselineQry -Db "master"
if ($rBase.ExitCode -ne 0 -or -not $rBase.DataSet -or $rBase.DataSet.Tables.Count -eq 0) {
    Write-Host "  ERROR checking baseline: $($rBase.Output | Select-Object -First 1)" -ForegroundColor Red
    exit 1
}

Write-Host "  Baseline stat states:" -ForegroundColor DarkGray
foreach ($row in $rBase.DataSet.Tables[0].Rows) {
    $lu = if ([DBNull]::Value.Equals($row["last_updated"])) { "NULL" } else { [string]$row["last_updated"] }
    $mc = if ([DBNull]::Value.Equals($row["modification_counter"])) { "NULL" } else { [string]$row["modification_counter"] }
    $sr = if ([DBNull]::Value.Equals($row["stat_rows"])) { "NULL" } else { [string]$row["stat_rows"] }
    $pr = if ([DBNull]::Value.Equals($row["physical_rows"])) { "NULL" } else { [string]$row["physical_rows"] }
    Write-Host ("    {0,-27} {1,-25} last_updated={2} rows={3} mod={4} physical={5}" -f `
        $row["table_name"], $row["stat_name"], $lu, $sr, $mc, $pr) -ForegroundColor DarkGray
}
Write-Host ""

# =====================================================================
# STEP 4: RUN sp_StatUpdate (dry-run + WhatIfOutputTable + Debug)
# =====================================================================
Write-Host "[4/5] Running sp_StatUpdate discovery (dry-run)..." -ForegroundColor Yellow

# Ensure CommandLog exists in master (proc requires it for LogToTable=Y).
$clSql = @"
USE master;
IF OBJECT_ID(N'dbo.CommandLog') IS NULL
BEGIN
    CREATE TABLE dbo.CommandLog (
        ID int IDENTITY(1,1) NOT NULL PRIMARY KEY,
        DatabaseName sysname NULL,
        SchemaName sysname NULL,
        ObjectName sysname NULL,
        ObjectType char(2) NULL,
        IndexName sysname NULL,
        IndexType tinyint NULL,
        StatisticsName sysname NULL,
        PartitionNumber int NULL,
        ExtendedInfo xml NULL,
        Command nvarchar(max) NOT NULL,
        CommandType nvarchar(60) NOT NULL,
        StartTime datetime2 NOT NULL DEFAULT SYSDATETIME(),
        EndTime datetime2 NULL,
        ErrorNumber int NULL,
        ErrorMessage nvarchar(max) NULL
    );
END;

-- Clear any prior StatUpdateLock from a stale run.
IF OBJECT_ID(N'dbo.StatUpdateLock') IS NOT NULL
    DELETE FROM dbo.StatUpdateLock WHERE LoginTime < DATEADD(MINUTE, -10, SYSDATETIME());

-- Drop the WhatIf capture table so we start clean.
IF OBJECT_ID(N'dbo.$WhatIfTable') IS NOT NULL
    DROP TABLE dbo.$WhatIfTable;
"@
$r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query $clSql -Db "master" -Timeout 60
if ($r.ExitCode -ne 0) {
    Write-Host "  ERROR setting up CommandLog: $($r.Output | Select-Object -First 1)" -ForegroundColor Red
    exit 1
}

# Discovery run.  @Debug=1 emits the "NEVER_UPDATED rescue qualified N fresh stat(s)"
# message.  @WhatIfOutputTable captures which stats would be updated.
$discoverySql = @"
EXEC master.dbo.sp_StatUpdate
    @Databases = N'$ScratchDb',
    @Execute = N'N',
    @LogToTable = N'Y',
    @WhatIfOutputTable = N'dbo.$WhatIfTable',
    @Debug = 1;
"@
$rDisc = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query $discoverySql -Db "master" -Timeout 300
if ($rDisc.ExitCode -ne 0) {
    Write-Host "  ERROR during discovery: $($rDisc.Output | Select-Object -First 3)" -ForegroundColor Red
    exit 1
}
$discOut = ($rDisc.Output -join "`n")
Write-Host "  Discovery run complete." -ForegroundColor Green
Write-Host ""

# Fetch WhatIfOutputTable rows so we know exactly which stats would be updated.
$rWhatIf = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password `
    -Query "SELECT DatabaseName, SchemaName, TableName, StatName FROM dbo.$WhatIfTable ORDER BY SequenceNum;" `
    -Db "master"
$whatIfRows = @()
if ($rWhatIf.ExitCode -eq 0 -and $rWhatIf.DataSet -and $rWhatIf.DataSet.Tables.Count -gt 0) {
    $whatIfRows = @($rWhatIf.DataSet.Tables[0].Rows)
}

Write-Host "  Stats that qualified (captured via WhatIfOutputTable):" -ForegroundColor DarkGray
if ($whatIfRows.Count -eq 0) {
    Write-Host "    (none)" -ForegroundColor DarkGray
} else {
    foreach ($w in $whatIfRows) {
        Write-Host ("    {0}.{1}.{2}.{3}" -f $w["DatabaseName"], $w["SchemaName"], $w["TableName"], $w["StatName"]) -ForegroundColor DarkGray
    }
}
Write-Host ""

# =====================================================================
# STEP 5: ASSERT
# =====================================================================
Write-Host "[5/5] Running assertions..." -ForegroundColor Yellow
Write-Host ""

$results = [System.Collections.ArrayList]::new()
function Add-Result {
    param([string]$Test, [string]$Result, [string]$Detail = "")
    $null = $results.Add([PSCustomObject]@{ Test = $Test; Result = $Result; Detail = $Detail })
}

# Helper: does WhatIfOutputTable have a row for this table?
function Test-TableInWhatIf {
    param([string]$TableName)
    foreach ($w in $whatIfRows) {
        if (([string]$w["TableName"]) -eq $TableName) { return $true }
    }
    return $false
}

# ---- (a) Fresh stat on populated table IS discovered/qualifies ----
if (Test-TableInWhatIf -TableName "FreshPopulated") {
    Add-Result "A: Fresh stat on populated table qualifies" "PASS" "FreshPopulated present in WhatIfOutputTable"
} else {
    Add-Result "A: Fresh stat on populated table qualifies" "FAIL" "FreshPopulated NOT present in WhatIfOutputTable -- bug gh-550 present"
}

# ---- (b) Fresh stat on empty table does NOT qualify ----
if (Test-TableInWhatIf -TableName "FreshEmpty") {
    Add-Result "B: Fresh stat on EMPTY table does NOT qualify" "FAIL" "FreshEmpty appeared in WhatIfOutputTable (should not)"
} else {
    Add-Result "B: Fresh stat on EMPTY table does NOT qualify" "PASS" "FreshEmpty correctly excluded"
}

# ---- (c) Regression: conventionally-modified stat still qualifies ----
if (Test-TableInWhatIf -TableName "ConventionallyModified") {
    Add-Result "C: Regression: modified stat still qualifies" "PASS" "ConventionallyModified present in WhatIfOutputTable"
} else {
    Add-Result "C: Regression: modified stat still qualifies" "FAIL" "ConventionallyModified missing -- pre-existing threshold path broken"
}

# ---- (d) Phase 4 debug output confirms NEVER_UPDATED rescue fired ----
# Look for the RAISERROR message we emit when @Debug=1 and >= 1 fresh stat qualifies.
if ($discOut -match "NEVER_UPDATED rescue qualified\s+(\d+)\s+fresh stat") {
    $rescueCount = [int]$Matches[1]
    if ($rescueCount -ge 1) {
        Add-Result "D: Phase 4 debug shows NEVER_UPDATED rescue" "PASS" "Phase 4 rescued $rescueCount fresh stat(s)"
    } else {
        Add-Result "D: Phase 4 debug shows NEVER_UPDATED rescue" "FAIL" "Phase 4 message matched but count is $rescueCount"
    }
} else {
    Add-Result "D: Phase 4 debug shows NEVER_UPDATED rescue" "FAIL" "Phase 4 debug output did not include 'NEVER_UPDATED rescue qualified' message"
}

# =====================================================================
# REPORT
# =====================================================================
Write-Host ""
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host " RESULTS" -ForegroundColor Cyan
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host ""

$passed = @($results | Where-Object { $_.Result -eq "PASS" }).Count
$failed = @($results | Where-Object { $_.Result -eq "FAIL" }).Count
$total  = $results.Count

foreach ($t in $results) {
    $tc = switch ($t.Result) {
        "PASS" { "Green" }
        "FAIL" { "Red"   }
        default { "White" }
    }
    $detail = if ($t.Detail) { " ($($t.Detail))" } else { "" }
    Write-Host "  [$($t.Result)] $($t.Test)$detail" -ForegroundColor $tc
}

Write-Host ""
Write-Host ("  Total: {0}   Passed: {1}   Failed: {2}" -f $total, $passed, $failed)
Write-Host ""

# Cleanup: drop scratch DB so re-runs start fresh.
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
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query $cleanupSql -Db "master" -Timeout 60 | Out-Null

if ($ResultsPath) {
    $major = @{ '2019' = 15; '2022' = 16; '2025' = 17 }[$Version]
    $sqlStatus = Get-StructuredSqlStatus -Server $Server -Port $Port -Password $Password -ExpectedMajor $major
    $commit = (& git -C $RepoRoot rev-parse HEAD).Trim()
    if ($LASTEXITCODE -ne 0 -or $commit -notmatch '^[0-9a-fA-F]{40}$') { throw 'Could not resolve the tested source commit.' }
    Write-StructuredLegacyResults -Path $ResultsPath -Commit $commit -SqlVersion $Version -SqlBuild $sqlStatus.Build `
        -ServerAliveAtEnd $sqlStatus.Alive -Suite 'FreshStatQualification' -ExpectedCount 4 `
        -Results @($results) -StartedAt $startedAt -EndedAt (Get-Date)
}

if ($failed -gt 0) {
    Write-Host "RESULT: $failed test(s) FAILED." -ForegroundColor Red
    exit 1
} else {
    Write-Host "RESULT: All $total tests PASSED." -ForegroundColor Green
    exit 0
}
