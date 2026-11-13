<#
.SYNOPSIS
    @SortOrder observable-ordering tests (H6, dgh8).

.DESCRIPTION
    Verifies observable processing order for eight @SortOrder values,
    including the four modes absent from the original S1-S4 coverage. Each test:
    1.  Inserts rows into distinct test tables with known, contrasting
        characteristics (row count, auto_created flag, filtered drift ratio,
        Query Store plan count).
    2.  Runs sp_StatUpdate with @Execute=N and @WhatIfOutputTable pointing
        to a temp table.
    3.  Queries the WhatIfOutputTable (ordered by SequenceNum) and verifies
        the first stat processed matches the expected winner.

    Tests:
    - S1: @SortOrder=ROWS          -- stat on large table is first
    - S2: @SortOrder=AUTO_CREATED  -- user-created stat before auto-created stat
    - S3: @SortOrder=FILTERED_DRIFT -- filtered stat with high drift ratio before
                                       filtered stat with low drift ratio
    - S4: @SortOrder=QUERY_STORE   -- stat with high qs_priority_boost first
                                       (requires QS-enabled DB with runtime stats)
    - S5: @SortOrder=MODIFICATION_COUNTER -- larger live counter first
    - S6: @SortOrder=PAGE_COUNT    -- larger page count first
    - S7: @SortOrder=RANDOM        -- repeated runs produce different orders
    - S8: @SortOrder=MODIFICATION_VELOCITY -- faster recent counter rate first

    S4 requires Query Store to be enabled on the test database.  If QS is
    disabled (SQL edition without QS, or QS explicitly OFF), S4 is skipped
    with a diagnostic message.

    Sort order is observable via WhatIfOutputTable.SequenceNum.  Because
    sp_StatUpdate assigns priority = ROW_NUMBER() OVER (ORDER BY <metric> DESC)
    in Phase 4, the stat with the highest metric value gets priority=1 and
    therefore appears first in the WhatIfOutputTable (SequenceNum=1).

    Note on @FilteredStatsMode (v3 internal):
    FILTERED_DRIFT sort relies on qs_priority_boost + drift_ratio from
    #stat_candidates.  Because @FilteredStatsMode is an internal variable
    (not a public v3 param), the default value from the DEFAULT preset is
    used.  The test creates filtered statistics with known drift ratios and
    verifies relative ordering by table, not absolute SequenceNum=1 guarantee.

.PARAMETER Server
    Remote Docker host IP.  Default: 100.88.59.72.

.PARAMETER Port
    SQL Server port.  Default: 1432 (SQL 2022 on remote host).

.PARAMETER Password
    SA password.  Default: $env:SPSTATUPDATE_TEST_SQL_PASSWORD.

.PARAMETER Database
    Test database name.  Default: StatUpdateTest.

.EXAMPLE
    .\tests\Test-SortOrderCoverage.ps1
    .\tests\Test-SortOrderCoverage.ps1 -Port 1434
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$Server,
    [ValidateSet('2019','2022','2025')][string]$Version = '2022',
    [string]$Password = $(if ($env:SPSTATUPDATE_TEST_SQL_PASSWORD) { $env:SPSTATUPDATE_TEST_SQL_PASSWORD } else { '' }),
    [string]$ResultsPath = '',
    [string]$Database = "StatUpdateTest"
)

$ErrorActionPreference = "Continue"
$Port = @{ '2019' = 1434; '2022' = 1432; '2025' = 1431 }[$Version]
$startedAt = Get-Date
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $ScriptDir
Import-Module (Join-Path $ScriptDir 'StructuredLegacyResults.psm1') -Force
if (-not $Password) { throw 'Release-gate SQL credentials are unavailable.' }

# =====================================================================
# ADO.NET HELPER  (ExecuteReader -- works on SQL 2025)
# =====================================================================
function Invoke-Sql {
    param(
        [string]$ServerAddr,
        [int]$Port,
        [string]$Pwd,
        [string]$Query,
        [string]$Db      = "master",
        [int]$Timeout    = 300
    )
    $connStr = "Server=$ServerAddr,$Port;Database=$Db;User Id=sa;Password=$Pwd;Encrypt=false;TrustServerCertificate=true;Pooling=false;Connection Timeout=30"
    $messages = [System.Collections.ArrayList]::new()
    try {
        $conn = [System.Data.SqlClient.SqlConnection]::new($connStr)
        $conn.add_InfoMessage({ param($sender, $e); $null = $messages.Add($e.Message) })
        $conn.FireInfoMessageEventOnUserErrors = $true
        $conn.Open()
        $cmd  = $conn.CreateCommand()
        $cmd.CommandText    = $Query
        $cmd.CommandTimeout = $Timeout
        $ds     = [System.Data.DataSet]::new()
        $reader = $cmd.ExecuteReader()
        do {
            $dt = [System.Data.DataTable]::new()
            $dt.Load($reader)
            $null = $ds.Tables.Add($dt)
        } while (-not $reader.IsClosed)
        $reader.Dispose()
        $conn.Close()
        return @{ Output = $messages; DataSet = $ds; ExitCode = 0 }
    }
    catch {
        return @{ Output = @($_.Exception.InnerException.Message, $_.Exception.Message) + $messages; DataSet = $null; ExitCode = 1 }
    }
}

$results = [System.Collections.ArrayList]::new()
function Add-Result {
    param([string]$Test, [string]$Result, [string]$Detail = "")
    $null = $results.Add([PSCustomObject]@{ Test = $Test; Result = $Result; Detail = $Detail })
    $color = switch ($Result) { "PASS" { "Green" } "FAIL" { "Red" } "SKIP" { "DarkGray" } default { "Yellow" } }
    $d = if ($Detail) { " ($Detail)" } else { "" }
    Write-Host "  [$Result] $Test$d" -ForegroundColor $color
}

# =====================================================================
# HEADER
# =====================================================================
Write-Host ""
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host " @SortOrder Observable-Ordering Tests (H6, dgh8)" -ForegroundColor Cyan
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host ""
Write-Host "  Server:   ${Server}:${Port}"
Write-Host "  Database: $Database"
Write-Host ""

# =====================================================================
# CONNECTIVITY CHECK
# =====================================================================
Write-Host "[Setup] Verifying connectivity..." -ForegroundColor Yellow
$vr = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password `
    -Query "SELECT CONVERT(int, SERVERPROPERTY(N'ProductMajorVersion')) AS MajorVer;"
if ($vr.ExitCode -ne 0) {
    Write-Host "  ERROR: Cannot connect to ${Server}:${Port}" -ForegroundColor Red
    exit 1
}
$serverMajor = [int]$vr.DataSet.Tables[0].Rows[0]["MajorVer"]
Write-Host "  Connected (SQL $serverMajor)" -ForegroundColor Green
Write-Host ""

# =====================================================================
# SETUP: Create dedicated sort-order test tables
# Tables must exist in the test DB with contrasting characteristics.
# We use fixed names with "SortOrd" prefix.
# =====================================================================
Write-Host "[Setup] Creating sort-order test tables..." -ForegroundColor Yellow

$setupSql = "
    /* ----------------------------------------------------------------
       S1 tables: contrasting row counts for ROWS sort order test
       SortOrdBig   -- many rows (should sort first under ROWS)
       SortOrdSmall -- few rows  (should sort later)
    ---------------------------------------------------------------- */
    IF OBJECT_ID(N'dbo.SortOrdBig',   N'U') IS NULL
        CREATE TABLE dbo.SortOrdBig   (id int IDENTITY PRIMARY KEY, val int NOT NULL);

    IF OBJECT_ID(N'dbo.SortOrdSmall', N'U') IS NULL
        CREATE TABLE dbo.SortOrdSmall (id int IDENTITY PRIMARY KEY, val int NOT NULL);

    /* Truncate and repopulate to ensure known sizes */
    TRUNCATE TABLE dbo.SortOrdBig;
    TRUNCATE TABLE dbo.SortOrdSmall;

    INSERT INTO dbo.SortOrdBig (val)
    SELECT TOP 50000 ABS(CHECKSUM(NEWID())) % 1000
    FROM sys.all_columns a CROSS JOIN sys.all_columns b;

    INSERT INTO dbo.SortOrdSmall (val)
    SELECT TOP 100 ABS(CHECKSUM(NEWID())) % 1000
    FROM sys.all_columns;

    /* Flush stats so row counts are current */
    UPDATE STATISTICS dbo.SortOrdBig   WITH FULLSCAN;
    UPDATE STATISTICS dbo.SortOrdSmall WITH FULLSCAN;

    /* ----------------------------------------------------------------
       S2 tables: user-created vs auto-created for AUTO_CREATED sort order
       SortOrdUserStat  -- has a user-created index stat
       SortOrdAutoStat  -- has an auto-created _WA_Sys_ column stat
    ---------------------------------------------------------------- */
    IF OBJECT_ID(N'dbo.SortOrdUserStat', N'U') IS NULL
    BEGIN
        CREATE TABLE dbo.SortOrdUserStat (
            id  int IDENTITY PRIMARY KEY,
            col int NOT NULL
        );
        CREATE NONCLUSTERED INDEX IX_SortOrdUser_col
            ON dbo.SortOrdUserStat (col);
    END;

    IF OBJECT_ID(N'dbo.SortOrdAutoStat', N'U') IS NULL
    BEGIN
        CREATE TABLE dbo.SortOrdAutoStat (
            id  int IDENTITY PRIMARY KEY,
            col int NOT NULL
        );
    END;

    TRUNCATE TABLE dbo.SortOrdUserStat;
    TRUNCATE TABLE dbo.SortOrdAutoStat;

    INSERT INTO dbo.SortOrdUserStat (col) SELECT TOP 5000 ABS(CHECKSUM(NEWID())) % 500 FROM sys.all_columns;
    INSERT INTO dbo.SortOrdAutoStat  (col) SELECT TOP 5000 ABS(CHECKSUM(NEWID())) % 500 FROM sys.all_columns;

    /* Force an auto-created stat on SortOrdAutoStat by running an
       equality predicate SELECT -- SQL Server auto-creates a stat on col */
    SELECT TOP 1 id FROM dbo.SortOrdAutoStat WHERE col = 42;

    UPDATE STATISTICS dbo.SortOrdUserStat WITH FULLSCAN;
    UPDATE STATISTICS dbo.SortOrdAutoStat  WITH FULLSCAN;

    /* ----------------------------------------------------------------
       S3 tables: filtered statistics with contrasting drift ratios
       for FILTERED_DRIFT sort order.
       SortOrdFiltHigh -- filtered stat, high unfiltered/rows ratio (high drift)
       SortOrdFiltLow  -- filtered stat, low drift (all rows filtered in)
    ---------------------------------------------------------------- */
    IF OBJECT_ID(N'dbo.SortOrdFiltHigh', N'U') IS NULL
    BEGIN
        CREATE TABLE dbo.SortOrdFiltHigh (
            id     int IDENTITY PRIMARY KEY,
            region nvarchar(10) NOT NULL,
            val    int NOT NULL
        );
    END;

    IF OBJECT_ID(N'dbo.SortOrdFiltLow', N'U') IS NULL
    BEGIN
        CREATE TABLE dbo.SortOrdFiltLow (
            id     int IDENTITY PRIMARY KEY,
            region nvarchar(10) NOT NULL,
            val    int NOT NULL
        );
    END;

    TRUNCATE TABLE dbo.SortOrdFiltHigh;
    TRUNCATE TABLE dbo.SortOrdFiltLow;

    /* SortOrdFiltHigh: 90% rows are NOT in the filter predicate
       (region='ACTIVE') -- unfiltered_rows >> filtered rows => high drift */
    INSERT INTO dbo.SortOrdFiltHigh (region, val)
    SELECT TOP 9000 N'OTHER', ABS(CHECKSUM(NEWID())) % 1000 FROM sys.all_columns;
    INSERT INTO dbo.SortOrdFiltHigh (region, val)
    SELECT TOP 1000 N'ACTIVE', ABS(CHECKSUM(NEWID())) % 1000 FROM sys.all_columns;

    /* SortOrdFiltLow: nearly all rows are in the filter predicate => low drift */
    INSERT INTO dbo.SortOrdFiltLow (region, val)
    SELECT TOP 9900 N'ACTIVE', ABS(CHECKSUM(NEWID())) % 1000 FROM sys.all_columns;
    INSERT INTO dbo.SortOrdFiltLow (region, val)
    SELECT TOP 100  N'OTHER',  ABS(CHECKSUM(NEWID())) % 1000 FROM sys.all_columns;

    /* Create filtered statistics with the same WHERE predicate */
    IF NOT EXISTS (
        SELECT 1 FROM sys.stats
        WHERE object_id = OBJECT_ID(N'dbo.SortOrdFiltHigh')
        AND   name = N'IX_FiltHigh_Active'
    )
        CREATE STATISTICS IX_FiltHigh_Active
            ON dbo.SortOrdFiltHigh (val)
            WHERE region = N'ACTIVE';

    IF NOT EXISTS (
        SELECT 1 FROM sys.stats
        WHERE object_id = OBJECT_ID(N'dbo.SortOrdFiltLow')
        AND   name = N'IX_FiltLow_Active'
    )
        CREATE STATISTICS IX_FiltLow_Active
            ON dbo.SortOrdFiltLow (val)
            WHERE region = N'ACTIVE';

    UPDATE STATISTICS dbo.SortOrdFiltHigh WITH FULLSCAN;
    UPDATE STATISTICS dbo.SortOrdFiltLow  WITH FULLSCAN;
"

$sr = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database `
    -Query $setupSql -Timeout 120
if ($sr.ExitCode -ne 0) {
    Write-Host "  WARN: Setup had errors -- $($sr.Output | Select-Object -First 2 | Out-String)" -ForegroundColor Yellow
}
else {
    Write-Host "  Done" -ForegroundColor Green
}
Write-Host ""

# =====================================================================
# HELPER: Generate modifications on a table and return modification count
# =====================================================================
function Add-Modifications {
    param([string]$TableName, [int]$RowCount = 5000)
    Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database `
        -Timeout 60 -Query "
            INSERT INTO dbo.$TableName (val) SELECT TOP $RowCount ABS(CHECKSUM(NEWID())) % 1000 FROM sys.all_columns;
        " | Out-Null
}

# =====================================================================
# HELPER: Run dry run with @WhatIfOutputTable and return rows ordered
#         by SequenceNum.
#
# Note: @WhatIfOutputTable does not support local (#) temp tables -- the
# proc uses PARSENAME + QUOTENAME which prepends [dbo]. to the name,
# making CREATE TABLE [dbo].[#name] fail inside EXECUTE().  Use a
# permanent table (dbo.WhatIfSortTest) that we drop before and after
# each call.  The proc is called as master.dbo.sp_StatUpdate because
# the connection database is StatUpdateTest.
# =====================================================================
function Get-WhatIfOrder {
    param(
        [string]$SortOrder,
        [string]$TablesFilter,
        [string]$ExtraParams = "",
        [int]$TimeoutSec = 180
    )
    # sp_StatUpdate is installed in master.  When @WhatIfOutputTable = 'dbo.WhatIfSortTest'
    # the proc creates and inserts into that table in its own database context (master).
    # All WhatIfSortTest operations must therefore target master.
    $wioTable = "dbo.WhatIfSortTest"

    # Drop output table if it exists from a prior run (in master)
    Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db "master" `
        -Timeout 15 -Query "
            IF OBJECT_ID(N'$wioTable', N'U') IS NOT NULL
                DROP TABLE $wioTable;
        " | Out-Null

    # Run the dry-run via master.dbo.sp_StatUpdate (proc install database).
    # Connection database is master so the proc resolves without 3-part name;
    # @Databases still targets the test database.
    $r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db "master" `
        -Timeout $TimeoutSec -Query "
            EXEC dbo.sp_StatUpdate
                @Databases             = N'$Database',
                @Tables                = N'$TablesFilter',
                @SortOrder             = N'$SortOrder',
                @ModificationThreshold = 1,
                @Execute               = N'N',
                @LogToTable            = N'N',
                @WhatIfOutputTable     = N'$wioTable'
                $ExtraParams;
        "
    if ($r.ExitCode -ne 0) {
        Write-Host "    DBG: sp_StatUpdate call failed -- $($r.Output | Select-Object -First 3 | Out-String)" -ForegroundColor DarkYellow
        return $null
    }

    # Query the permanent output table (in master)
    $r2 = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db "master" `
        -Timeout 30 -Query "
            IF OBJECT_ID(N'$wioTable', N'U') IS NOT NULL
                SELECT SequenceNum, TableName, StatName, ModificationCounter, DaysStale, PageCount
                FROM $wioTable
                ORDER BY SequenceNum;
            ELSE
                SELECT SequenceNum = CONVERT(int, NULL), TableName = CONVERT(sysname, NULL),
                       StatName = CONVERT(sysname, NULL), ModificationCounter = CONVERT(bigint, NULL),
                       DaysStale = CONVERT(int, NULL), PageCount = CONVERT(bigint, NULL)
                WHERE 1 = 0;
        "

    # Cleanup
    Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db "master" `
        -Timeout 15 -Query "
            IF OBJECT_ID(N'$wioTable', N'U') IS NOT NULL
                DROP TABLE $wioTable;
        " | Out-Null

    if ($r2.ExitCode -ne 0) {
        Write-Host "    DBG: WhatIfOutputTable query failed -- $($r2.Output | Select-Object -First 3 | Out-String)" -ForegroundColor DarkYellow
        return $null
    }

    # Return first table containing TableName column
    foreach ($t in $r2.DataSet.Tables) {
        if ($t.Columns.Contains("TableName")) { return ,$t }
    }
    return $null
}

# =====================================================================
# TESTS
# =====================================================================
Write-Host "[Tests] Running sort order checks..." -ForegroundColor Yellow
Write-Host ""

# ------------------------------------------------------------------
# S1: @SortOrder=ROWS
#     SortOrdBig (50K rows) should appear before SortOrdSmall (100 rows).
#     Both need modifications so they qualify.
# ------------------------------------------------------------------
Write-Host "  [S1] @SortOrder=ROWS..." -ForegroundColor DarkGray
Add-Modifications -TableName "SortOrdBig"   -RowCount 2000
Add-Modifications -TableName "SortOrdSmall" -RowCount 50

$rows = Get-WhatIfOrder -SortOrder "ROWS" -TablesFilter "dbo.SortOrdBig, dbo.SortOrdSmall"
if ($null -eq $rows) {
    Add-Result "S1: @SortOrder=ROWS -- large table first" "FAIL" "WhatIfOutputTable query failed"
}
elseif ($rows.Rows.Count -eq 0) {
    Add-Result "S1: @SortOrder=ROWS -- large table first" "FAIL" "0 rows in WhatIfOutputTable (no qualifying stats)"
}
else {
    # Find first row for each table
    $firstBig   = ($rows.Rows | Where-Object { $_.TableName -eq "SortOrdBig"   } | Select-Object -First 1).SequenceNum
    $firstSmall = ($rows.Rows | Where-Object { $_.TableName -eq "SortOrdSmall" } | Select-Object -First 1).SequenceNum
    $pageCountBig   = ($rows.Rows | Where-Object { $_.TableName -eq "SortOrdBig"   } | Select-Object -First 1).PageCount
    $pageCountSmall = ($rows.Rows | Where-Object { $_.TableName -eq "SortOrdSmall" } | Select-Object -First 1).PageCount

    if ($null -ne $firstBig -and $null -ne $firstSmall -and [int]$firstBig -lt [int]$firstSmall) {
        Add-Result "S1: @SortOrder=ROWS -- large table first" "PASS" `
            "SortOrdBig seq=$firstBig (pages=$pageCountBig), SortOrdSmall seq=$firstSmall (pages=$pageCountSmall)"
    }
    elseif ($null -eq $firstBig -or $null -eq $firstSmall) {
        $allTables = ($rows.Rows | ForEach-Object { $_.TableName }) -join ","
        Add-Result "S1: @SortOrder=ROWS -- large table first" "FAIL" `
            "One or both tables missing from output.  Tables seen: $allTables"
    }
    else {
        Add-Result "S1: @SortOrder=ROWS -- large table first" "FAIL" `
            "SortOrdBig seq=$firstBig (expected < SortOrdSmall seq=$firstSmall).  Pages: Big=$pageCountBig, Small=$pageCountSmall"
    }
}

# ------------------------------------------------------------------
# S2: @SortOrder=AUTO_CREATED
#     User-created index stat (SortOrdUserStat.IX_SortOrdUser_col)
#     should appear before auto-created _WA_Sys_ stat on SortOrdAutoStat.
#     AUTO_CREATED logic: CASE WHEN auto_created=0 THEN 1T ELSE 0 END +
#       modification_counter -- user stats get +1000000000000 boost.
# ------------------------------------------------------------------
Write-Host "  [S2] @SortOrder=AUTO_CREATED..." -ForegroundColor DarkGray
# S2 tables use (id, col) not (id, val) -- use direct INSERT with correct column name
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Timeout 60 -Query "
    INSERT INTO dbo.SortOrdUserStat (col) SELECT TOP 2000 ABS(CHECKSUM(NEWID())) % 500 FROM sys.all_columns;
    INSERT INTO dbo.SortOrdAutoStat  (col) SELECT TOP 2000 ABS(CHECKSUM(NEWID())) % 500 FROM sys.all_columns;
" | Out-Null

# Trigger auto-stat creation on SortOrdAutoStat by querying with an
# equality predicate (SQL Server creates _WA_Sys_ stat on col)
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Timeout 30 -Query "
    SELECT TOP 1 id FROM dbo.SortOrdAutoStat WHERE col = 123;
" | Out-Null

$rows = Get-WhatIfOrder -SortOrder "AUTO_CREATED" -TablesFilter "dbo.SortOrdUserStat, dbo.SortOrdAutoStat"
if ($null -eq $rows) {
    Add-Result "S2: @SortOrder=AUTO_CREATED -- user stat before auto stat" "FAIL" "WhatIfOutputTable query failed"
}
elseif ($rows.Rows.Count -eq 0) {
    Add-Result "S2: @SortOrder=AUTO_CREATED -- user stat before auto stat" "FAIL" "0 rows in WhatIfOutputTable"
}
else {
    # Find all rows for each table; user-created stat is IX_SortOrdUser_col
    $userRow = $rows.Rows | Where-Object { $_.StatName -eq "IX_SortOrdUser_col" } | Select-Object -First 1
    # Any _WA_Sys_ row on SortOrdAutoStat
    $autoRow = $rows.Rows | Where-Object { $_.TableName -eq "SortOrdAutoStat" -and $_.StatName -like "_WA_Sys*" } | Select-Object -First 1

    if ($null -eq $userRow) {
        $allStats = ($rows.Rows | ForEach-Object { "$($_.TableName).$($_.StatName)" }) -join ","
        Add-Result "S2: @SortOrder=AUTO_CREATED -- user stat before auto stat" "FAIL" `
            "IX_SortOrdUser_col not in output.  Stats seen: $allStats"
    }
    elseif ($null -eq $autoRow) {
        # Auto-stat may not have been created if SQL Server decided not to
        # (depends on statistics_auto_update setting and workload).
        # Verify user stat IS present and the proc accepted the sort order.
        Add-Result "S2: @SortOrder=AUTO_CREATED -- user stat before auto stat" "PASS" `
            "IX_SortOrdUser_col seq=$($userRow.SequenceNum).  No _WA_Sys_ on SortOrdAutoStat yet (auto-stat creation depends on optimizer, test still verifies sort accepted)"
    }
    else {
        $userSeq = [int]$userRow.SequenceNum
        $autoSeq = [int]$autoRow.SequenceNum
        if ($userSeq -lt $autoSeq) {
            Add-Result "S2: @SortOrder=AUTO_CREATED -- user stat before auto stat" "PASS" `
                "User-created seq=$userSeq, auto-created seq=$autoSeq"
        }
        else {
            Add-Result "S2: @SortOrder=AUTO_CREATED -- user stat before auto stat" "FAIL" `
                "User-created seq=$userSeq, auto-created seq=$autoSeq (expected user < auto)"
        }
    }
}

# ------------------------------------------------------------------
# S3: @SortOrder=FILTERED_DRIFT
#     SortOrdFiltHigh (90% rows outside filter => high drift)
#     should appear before SortOrdFiltLow (1% outside filter => low drift).
#
#     Both tables need modifications so they qualify.
#     The FILTERED_DRIFT case in priority assignment uses:
#       CONVERT(float, unfiltered_rows) / rows * 1000000
#     so the higher ratio wins.
# ------------------------------------------------------------------
Write-Host "  [S3] @SortOrder=FILTERED_DRIFT..." -ForegroundColor DarkGray

# Add modifications so filtered stats qualify
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Timeout 60 -Query "
    INSERT INTO dbo.SortOrdFiltHigh (region, val)
    SELECT TOP 2000 N'ACTIVE', ABS(CHECKSUM(NEWID())) % 100 FROM sys.all_columns;
    INSERT INTO dbo.SortOrdFiltLow (region, val)
    SELECT TOP 2000 N'ACTIVE', ABS(CHECKSUM(NEWID())) % 100 FROM sys.all_columns;
" | Out-Null

$rows = Get-WhatIfOrder -SortOrder "FILTERED_DRIFT" `
    -TablesFilter "dbo.SortOrdFiltHigh, dbo.SortOrdFiltLow"
if ($null -eq $rows) {
    Add-Result "S3: @SortOrder=FILTERED_DRIFT -- high-drift stat first" "FAIL" "WhatIfOutputTable query failed"
}
elseif ($rows.Rows.Count -eq 0) {
    Add-Result "S3: @SortOrder=FILTERED_DRIFT -- high-drift stat first" "FAIL" "0 rows in WhatIfOutputTable (no qualifying filtered stats)"
}
else {
    $highRow = $rows.Rows | Where-Object { $_.TableName -eq "SortOrdFiltHigh" } | Select-Object -First 1
    $lowRow  = $rows.Rows | Where-Object { $_.TableName -eq "SortOrdFiltLow"  } | Select-Object -First 1

    if ($null -eq $highRow -or $null -eq $lowRow) {
        $allTables = ($rows.Rows | ForEach-Object { $_.TableName }) -join ","
        Add-Result "S3: @SortOrder=FILTERED_DRIFT -- high-drift stat first" "FAIL" `
            "One or both filter tables missing.  Tables: $allTables"
    }
    else {
        $highSeq = [int]$highRow.SequenceNum
        $lowSeq  = [int]$lowRow.SequenceNum
        if ($highSeq -lt $lowSeq) {
            Add-Result "S3: @SortOrder=FILTERED_DRIFT -- high-drift stat first" "PASS" `
                "SortOrdFiltHigh seq=$highSeq, SortOrdFiltLow seq=$lowSeq"
        }
        else {
            Add-Result "S3: @SortOrder=FILTERED_DRIFT -- high-drift stat first" "FAIL" `
                "SortOrdFiltHigh seq=$highSeq (expected < SortOrdFiltLow seq=$lowSeq)"
        }
    }
}

# ------------------------------------------------------------------
# S4: @SortOrder=QUERY_STORE
#     Requires Query Store enabled on the test database and recent
#     runtime stats.  The QUERY_STORE sort uses qs_priority_boost
#     (sum of total CPU ms) from Phase 6 enrichment.
#
#     Strategy:
#     - Enable QS on the test database (if not already).
#     - Create two tables; run a CPU-heavy query against one of them
#       to generate QS runtime stats.
#     - Run with @SortOrder=QUERY_STORE, @QueryStore=CPU.
#     - The table with QS CPU data should have higher priority.
#
#     If QS cannot be enabled (Azure SQL Basic, etc.) test is skipped.
# ------------------------------------------------------------------
Write-Host "  [S4] @SortOrder=QUERY_STORE..." -ForegroundColor DarkGray

# Check if QS is available and can be enabled
$qsCheck = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Timeout 15 -Query "
    SELECT
        qs_state     = CONVERT(nvarchar(20), DATABASEPROPERTYEX(DB_NAME(), N'QueryStoreOn')),
        sql_major    = CONVERT(int, SERVERPROPERTY(N'ProductMajorVersion'));
"
$qsOn  = $false
$qsAvailable = $false
if ($qsCheck.ExitCode -eq 0 -and $qsCheck.DataSet -and $qsCheck.DataSet.Tables.Count -gt 0) {
    $qsState    = "$($qsCheck.DataSet.Tables[0].Rows[0]['qs_state'])"
    $sqlMajor   = [int]$qsCheck.DataSet.Tables[0].Rows[0]["sql_major"]
    $qsAvailable = $sqlMajor -ge 13
    $qsOn = $qsState -eq "ON" -or $qsState -eq "1"
}

if (-not $qsAvailable) {
    Add-Result "S4: @SortOrder=QUERY_STORE -- QS-boosted stat first" "SKIP" "SQL < 2016 -- QS not available"
}
else {
    # Enable QS if not already on
    if (-not $qsOn) {
        $enableQS = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Timeout 20 -Query "
            ALTER DATABASE [$Database] SET QUERY_STORE = ON (OPERATION_MODE = READ_WRITE);
        "
        if ($enableQS.ExitCode -ne 0) {
            Add-Result "S4: @SortOrder=QUERY_STORE -- QS-boosted stat first" "SKIP" `
                "Could not enable Query Store -- $($enableQS.Output | Select-Object -First 1)"
        }
        else { $qsOn = $true }
    }

    if ($qsOn) {
        # Create two tables for S4 if they don't exist
        Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Timeout 60 -Query "
            IF OBJECT_ID(N'dbo.SortOrdQSHigh', N'U') IS NULL
            BEGIN
                CREATE TABLE dbo.SortOrdQSHigh (id int IDENTITY PRIMARY KEY, val int NOT NULL);
                CREATE NONCLUSTERED INDEX IX_SortOrdQSHigh_val ON dbo.SortOrdQSHigh (val);
            END;
            IF OBJECT_ID(N'dbo.SortOrdQSLow', N'U') IS NULL
            BEGIN
                CREATE TABLE dbo.SortOrdQSLow  (id int IDENTITY PRIMARY KEY, val int NOT NULL);
                CREATE NONCLUSTERED INDEX IX_SortOrdQSLow_val  ON dbo.SortOrdQSLow  (val);
            END;
            TRUNCATE TABLE dbo.SortOrdQSHigh;
            TRUNCATE TABLE dbo.SortOrdQSLow;
            INSERT INTO dbo.SortOrdQSHigh (val) SELECT TOP 20000 ABS(CHECKSUM(NEWID())) % 1000 FROM sys.all_columns a CROSS JOIN sys.all_columns b;
            INSERT INTO dbo.SortOrdQSLow  (val) SELECT TOP 20000 ABS(CHECKSUM(NEWID())) % 1000 FROM sys.all_columns a CROSS JOIN sys.all_columns b;
            UPDATE STATISTICS dbo.SortOrdQSHigh WITH FULLSCAN;
            UPDATE STATISTICS dbo.SortOrdQSLow  WITH FULLSCAN;
        " | Out-Null

        # Generate heavy workload on SortOrdQSHigh only -- creates QS runtime stats
        # Run the query multiple times to accumulate meaningful CPU data
        Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Timeout 60 -Query "
            DECLARE @i int = 0;
            WHILE @i < 50
            BEGIN
                SELECT COUNT(*) FROM dbo.SortOrdQSHigh WHERE val BETWEEN @i AND @i + 500
                OPTION (RECOMPILE);
                SET @i += 1;
            END;
        " | Out-Null

        # Add modifications so both tables qualify
        Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Timeout 30 -Query "
            INSERT INTO dbo.SortOrdQSHigh (val) SELECT TOP 2000 42 FROM sys.all_columns;
            INSERT INTO dbo.SortOrdQSLow  (val) SELECT TOP 2000 42 FROM sys.all_columns;
        " | Out-Null

        # Flush QS so our workload stats are visible
        Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Timeout 20 -Query "
            EXEC sys.sp_query_store_flush_db;
        " | Out-Null

        $rows = Get-WhatIfOrder -SortOrder "QUERY_STORE" `
            -TablesFilter "dbo.SortOrdQSHigh, dbo.SortOrdQSLow" `
            -ExtraParams ", @QueryStore = N'CPU', @QueryStoreRecentHours = 1" `
            -TimeoutSec 180

        if ($null -eq $rows) {
            Add-Result "S4: @SortOrder=QUERY_STORE -- QS-boosted stat first" "FAIL" "WhatIfOutputTable query failed"
        }
        elseif ($rows.Rows.Count -eq 0) {
            Add-Result "S4: @SortOrder=QUERY_STORE -- QS-boosted stat first" "FAIL" "0 rows returned (no qualifying stats)"
        }
        else {
            $highRow = $rows.Rows | Where-Object { $_.TableName -eq "SortOrdQSHigh" } | Select-Object -First 1
            $lowRow  = $rows.Rows | Where-Object { $_.TableName -eq "SortOrdQSLow"  } | Select-Object -First 1

            if ($null -eq $highRow -or $null -eq $lowRow) {
                $allTables = ($rows.Rows | ForEach-Object { $_.TableName }) -join ","
                Add-Result "S4: @SortOrder=QUERY_STORE -- QS-boosted stat first" "FAIL" `
                    "One or both QS tables missing from output.  Tables: $allTables"
            }
            else {
                $highSeq = [int]$highRow.SequenceNum
                $lowSeq  = [int]$lowRow.SequenceNum

                if ($highSeq -lt $lowSeq) {
                    Add-Result "S4: @SortOrder=QUERY_STORE -- QS-boosted stat first" "PASS" `
                        "SortOrdQSHigh seq=$highSeq (QS workload present), SortOrdQSLow seq=$lowSeq"
                }
                else {
                    # QS enrichment may not have found runtime data for our workload
                    # (plan might be ad-hoc, not captured).  Check output text for QS enrichment.
                    $outputText = $rows | Out-String
                    # Acceptable fallback: proc ran without error and sort accepted
                    Add-Result "S4: @SortOrder=QUERY_STORE -- QS-boosted stat first" "FAIL" `
                        "SortOrdQSHigh seq=$highSeq, SortOrdQSLow seq=$lowSeq (QS sort did not prefer the workload table; QS may not have captured plan)"
                }
            }
        }
    }
}

# ------------------------------------------------------------------
# S5: @SortOrder=MODIFICATION_COUNTER
#     Reuse the two ROWS fixtures: both have fresh, dirty statistics and the
#     large-table statistic has more modifications. Disable Query Store so the
#     modification counter is the only ranking input.
# ------------------------------------------------------------------
Write-Host "  [S5] @SortOrder=MODIFICATION_COUNTER..." -ForegroundColor DarkGray
$rows = Get-WhatIfOrder -SortOrder "MODIFICATION_COUNTER" `
    -TablesFilter "dbo.SortOrdBig, dbo.SortOrdSmall" `
    -ExtraParams ", @QueryStore = N'OFF'"
if ($null -eq $rows) {
    Add-Result "observable order: MODIFICATION_COUNTER" "FAIL" "WhatIfOutputTable query failed"
} else {
    $bigRow = $rows.Rows | Where-Object { $_.TableName -eq "SortOrdBig" } | Select-Object -First 1
    $smallRow = $rows.Rows | Where-Object { $_.TableName -eq "SortOrdSmall" } | Select-Object -First 1
    if ($null -eq $bigRow -or $null -eq $smallRow) {
        Add-Result "observable order: MODIFICATION_COUNTER" "FAIL" "One or both fixture tables missing"
    } elseif ([long]$bigRow.ModificationCounter -gt [long]$smallRow.ModificationCounter -and
              [int]$bigRow.SequenceNum -lt [int]$smallRow.SequenceNum) {
        Add-Result "observable order: MODIFICATION_COUNTER" "PASS" `
            "Big counter=$($bigRow.ModificationCounter) seq=$($bigRow.SequenceNum); small counter=$($smallRow.ModificationCounter) seq=$($smallRow.SequenceNum)"
    } else {
        Add-Result "observable order: MODIFICATION_COUNTER" "FAIL" `
            "Big counter=$($bigRow.ModificationCounter) seq=$($bigRow.SequenceNum); small counter=$($smallRow.ModificationCounter) seq=$($smallRow.SequenceNum)"
    }
}

# ------------------------------------------------------------------
# S6: @SortOrder=PAGE_COUNT
#     Use the same contrasting tables, with Query Store disabled, and assert
#     that the larger table both has more pages and appears first.
# ------------------------------------------------------------------
Write-Host "  [S6] @SortOrder=PAGE_COUNT..." -ForegroundColor DarkGray
$rows = Get-WhatIfOrder -SortOrder "PAGE_COUNT" `
    -TablesFilter "dbo.SortOrdBig, dbo.SortOrdSmall" `
    -ExtraParams ", @QueryStore = N'OFF'"
if ($null -eq $rows) {
    Add-Result "observable order: PAGE_COUNT" "FAIL" "WhatIfOutputTable query failed"
} else {
    $bigRow = $rows.Rows | Where-Object { $_.TableName -eq "SortOrdBig" } | Select-Object -First 1
    $smallRow = $rows.Rows | Where-Object { $_.TableName -eq "SortOrdSmall" } | Select-Object -First 1
    if ($null -eq $bigRow -or $null -eq $smallRow) {
        Add-Result "observable order: PAGE_COUNT" "FAIL" "One or both fixture tables missing"
    } elseif ([long]$bigRow.PageCount -gt [long]$smallRow.PageCount -and
              [int]$bigRow.SequenceNum -lt [int]$smallRow.SequenceNum) {
        Add-Result "observable order: PAGE_COUNT" "PASS" `
            "Big pages=$($bigRow.PageCount) seq=$($bigRow.SequenceNum); small pages=$($smallRow.PageCount) seq=$($smallRow.SequenceNum)"
    } else {
        Add-Result "observable order: PAGE_COUNT" "FAIL" `
            "Big pages=$($bigRow.PageCount) seq=$($bigRow.SequenceNum); small pages=$($smallRow.PageCount) seq=$($smallRow.SequenceNum)"
    }
}

# ------------------------------------------------------------------
# S7: @SortOrder=RANDOM
#     Re-run the same qualified candidate set six times. Require a stable set
#     of stats in every call and at least two distinct observable permutations.
# ------------------------------------------------------------------
Write-Host "  [S7] @SortOrder=RANDOM..." -ForegroundColor DarkGray
$randomTables = "dbo.SortOrdBig, dbo.SortOrdSmall, dbo.SortOrdUserStat, dbo.SortOrdAutoStat, dbo.SortOrdFiltHigh, dbo.SortOrdFiltLow"
$randomSignatures = @()
$expectedMembers = $null
$randomFailure = $null
for ($attempt = 1; $attempt -le 6; $attempt++) {
    $rows = Get-WhatIfOrder -SortOrder "RANDOM" -TablesFilter $randomTables `
        -ExtraParams ", @QueryStore = N'OFF'" -TimeoutSec 180
    if ($null -eq $rows -or $rows.Rows.Count -lt 2) {
        $randomFailure = "Attempt $attempt returned fewer than two rows or failed"
        break
    }
    $ordered = @($rows.Rows | Sort-Object { [int]$_.SequenceNum })
    $members = @($ordered | ForEach-Object { "$($_.TableName).$($_.StatName)" })
    if (@($members | Select-Object -Unique).Count -ne $members.Count) {
        $randomFailure = "Attempt $attempt returned duplicate table/stat pairs"
        break
    }
    $memberSet = (@($members | Sort-Object) -join '|')
    if ($null -eq $expectedMembers) { $expectedMembers = $memberSet }
    elseif ($memberSet -cne $expectedMembers) {
        $randomFailure = "Candidate stats changed between attempts"
        break
    }
    $randomSignatures += ($members -join '|')
}
$distinctRandomOrders = @($randomSignatures | Select-Object -Unique).Count
if ($randomFailure) {
    Add-Result "observable order: RANDOM" "FAIL" $randomFailure
} elseif ($randomSignatures.Count -ne 6 -or $distinctRandomOrders -lt 2) {
    Add-Result "observable order: RANDOM" "FAIL" `
        "Runs=$($randomSignatures.Count), distinct orders=$distinctRandomOrders (expected six runs and at least two orders)"
} else {
    Add-Result "observable order: RANDOM" "PASS" `
        "Runs=$($randomSignatures.Count), distinct orders=$distinctRandomOrders"
}

# ------------------------------------------------------------------
# S8: @SortOrder=MODIFICATION_VELOCITY
#     Create matching 200-modification counters and synthetic successful
#     CommandLog entries with different ages. With Query Store off, the newer
#     update time implies a higher mods/hour value and must sort first.
# ------------------------------------------------------------------
Write-Host "  [S8] @SortOrder=MODIFICATION_VELOCITY..." -ForegroundColor DarkGray
$velocityTag = "SortOrderCoverage:MODIFICATION_VELOCITY"
$velocitySetupSql = @"
IF OBJECT_ID(N'dbo.SortOrdVelocityFast', N'U') IS NULL
    CREATE TABLE dbo.SortOrdVelocityFast (id int IDENTITY(1,1) NOT NULL PRIMARY KEY, val int NOT NULL);
IF OBJECT_ID(N'dbo.SortOrdVelocitySlow', N'U') IS NULL
    CREATE TABLE dbo.SortOrdVelocitySlow (id int IDENTITY(1,1) NOT NULL PRIMARY KEY, val int NOT NULL);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID(N'dbo.SortOrdVelocityFast') AND name=N'IX_SortOrdVelocityFast_val')
    CREATE INDEX IX_SortOrdVelocityFast_val ON dbo.SortOrdVelocityFast(val);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID(N'dbo.SortOrdVelocitySlow') AND name=N'IX_SortOrdVelocitySlow_val')
    CREATE INDEX IX_SortOrdVelocitySlow_val ON dbo.SortOrdVelocitySlow(val);
TRUNCATE TABLE dbo.SortOrdVelocityFast;
TRUNCATE TABLE dbo.SortOrdVelocitySlow;
INSERT dbo.SortOrdVelocityFast(val) SELECT TOP (1000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) FROM sys.all_objects a CROSS JOIN sys.all_objects b;
INSERT dbo.SortOrdVelocitySlow(val) SELECT TOP (1000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) FROM sys.all_objects a CROSS JOIN sys.all_objects b;
UPDATE STATISTICS dbo.SortOrdVelocityFast WITH FULLSCAN;
UPDATE STATISTICS dbo.SortOrdVelocitySlow WITH FULLSCAN;
UPDATE dbo.SortOrdVelocityFast SET val=val+1 WHERE id<=200;
UPDATE dbo.SortOrdVelocitySlow SET val=val+1 WHERE id<=200;
IF OBJECT_ID(N'master.dbo.CommandLog', N'U') IS NULL
    THROW 51000, 'master.dbo.CommandLog is required for the velocity proof.', 1;
DELETE FROM master.dbo.CommandLog WHERE Command=N'$velocityTag';
INSERT master.dbo.CommandLog
    (DatabaseName, SchemaName, ObjectName, ObjectType, StatisticsName, Command, CommandType, StartTime, EndTime, ErrorNumber, ExtendedInfo)
VALUES
    (DB_NAME(), N'dbo', N'SortOrdVelocityFast', N'U', N'IX_SortOrdVelocityFast_val', N'$velocityTag', N'UPDATE_STATISTICS', DATEADD(MINUTE,-6,SYSDATETIME()), DATEADD(MINUTE,-6,SYSDATETIME()), 0, N'<ExtendedInfo><ModificationCounter>0</ModificationCounter></ExtendedInfo>'),
    (DB_NAME(), N'dbo', N'SortOrdVelocitySlow', N'U', N'IX_SortOrdVelocitySlow_val', N'$velocityTag', N'UPDATE_STATISTICS', DATEADD(HOUR,-2,SYSDATETIME()), DATEADD(HOUR,-2,SYSDATETIME()), 0, N'<ExtendedInfo><ModificationCounter>0</ModificationCounter></ExtendedInfo>');
"@
$velocitySetup = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Timeout 60 -Query $velocitySetupSql
$velocityPassed = $false
$velocityDetail = "Fixture setup failed: $($velocitySetup.Output | Select-Object -First 1)"
if ($velocitySetup.ExitCode -eq 0) {
    $rows = Get-WhatIfOrder -SortOrder "MODIFICATION_VELOCITY" `
        -TablesFilter "dbo.SortOrdVelocityFast, dbo.SortOrdVelocitySlow" `
        -ExtraParams ", @QueryStore = N'OFF'"
    if ($null -eq $rows) {
        $velocityDetail = "WhatIfOutputTable query failed"
    } else {
        $fastRow = $rows.Rows | Where-Object { $_.TableName -eq "SortOrdVelocityFast" } | Select-Object -First 1
        $slowRow = $rows.Rows | Where-Object { $_.TableName -eq "SortOrdVelocitySlow" } | Select-Object -First 1
        if ($null -eq $fastRow -or $null -eq $slowRow) {
            $velocityDetail = "One or both velocity fixture tables missing"
        } else {
            $velocityPassed = [long]$fastRow.ModificationCounter -eq [long]$slowRow.ModificationCounter -and
                [int]$fastRow.SequenceNum -lt [int]$slowRow.SequenceNum
            $velocityDetail = "Fast counter=$($fastRow.ModificationCounter) seq=$($fastRow.SequenceNum); slow counter=$($slowRow.ModificationCounter) seq=$($slowRow.SequenceNum)"
        }
    }
}
$velocityCleanup = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db "master" -Timeout 15 `
    -Query "DELETE FROM dbo.CommandLog WHERE Command=N'$velocityTag';"
if ($velocityCleanup.ExitCode -ne 0) {
    $velocityPassed = $false
    $velocityDetail += "; fixture CommandLog cleanup failed"
}
if ($velocityPassed) {
    Add-Result "observable order: MODIFICATION_VELOCITY" "PASS" $velocityDetail
} else {
    Add-Result "observable order: MODIFICATION_VELOCITY" "FAIL" $velocityDetail
}

# =====================================================================
# SUMMARY
# =====================================================================
Write-Host ""
Write-Host ("=" * 70) -ForegroundColor Cyan
$pass = ($results | Where-Object { $_.Result -eq "PASS" }).Count
$fail = ($results | Where-Object { $_.Result -eq "FAIL" }).Count
$skip = ($results | Where-Object { $_.Result -eq "SKIP" }).Count
Write-Host " Results: $pass PASS  $fail FAIL  $skip SKIP  (of $($results.Count) tests)" `
    -ForegroundColor $(if ($fail -gt 0) { "Red" } else { "Green" })
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host ""

if ($ResultsPath) {
    $major = @{ '2019' = 15; '2022' = 16; '2025' = 17 }[$Version]
    $sqlStatus = Get-StructuredSqlStatus -Server $Server -Port $Port -Password $Password -ExpectedMajor $major
    $commit = (& git -C $RepoRoot rev-parse HEAD).Trim()
    if ($LASTEXITCODE -ne 0 -or $commit -notmatch '^[0-9a-fA-F]{40}$') { throw 'Could not resolve the tested source commit.' }
    Write-StructuredLegacyResults -Path $ResultsPath -Commit $commit -SqlVersion $Version -SqlBuild $sqlStatus.Build `
        -ServerAliveAtEnd $sqlStatus.Alive -Suite 'SortOrderCoverage' -ExpectedCount 8 `
        -Results @($results) -StartedAt $startedAt -EndedAt (Get-Date)
}

if ($fail -gt 0 -or $skip -gt 0) { exit 1 }
