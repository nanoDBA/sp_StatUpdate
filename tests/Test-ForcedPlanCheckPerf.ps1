<#
.SYNOPSIS
    Benchmark: forced-plan-check query strategy comparison (v3.3.4 vs v3.3.5).

.DESCRIPTION
    Measures performance of two QS forced-plan-check strategies:

      Strategy A (v3.3.4) -- CHARINDEX text scan:
        Joins query_store_plan -> query_store_query -> query_store_query_text,
        then uses CHARINDEX to match table names inside the SQL text.  Simple
        but scales as O(plans * tables * text_length).

      Strategy B (v3.3.5) -- sql_expression_dependencies integer join:
        For compiled (object_id IS NOT NULL) plans, joins through
        sys.sql_expression_dependencies by integer object_id -- no text scanning.
        Falls back to CHARINDEX only for ad-hoc (object_id IS NULL) plans.
        Expected to be significantly faster when most plans are compiled SPs.

    The script:
      1. Creates database PerfTestQS with Query Store in FORCED_PLAN mode.
      2. Creates 5 test tables (10,000 rows each).
      3. Creates $NumStoredProcs stored procedures (SP workload -- tests SED path).
      4. Runs each SP to populate query_store_query / runtime_stats.
      5. Forces the plan for every SP query.
      6. Creates $NumAdHocPlans ad-hoc plans and forces those too (tests CHARINDEX fallback).
      7. Populates #stats_to_process with all 5 tables, processed = 1.
      8. Runs $Iterations timed iterations of Strategy A then B (with cache flush between).
      9. Reports per-iteration timings, averages, and speedup ratio.

    Connection uses ADO.NET with Encrypt=false;Pooling=false (same pattern as all
    test scripts in this repo).  The #stats_to_process temp table must be visible
    to both strategy queries, so both run on the same open connection.

.PARAMETER Server
    SQL Server host. Default: 100.88.59.72.

.PARAMETER Port
    SQL Server port. Default: 1432 (SQL 2022 on remote host).

.PARAMETER Password
    SA password. Default: $env:SPSTATUPDATE_TEST_SQL_PASSWORD.

.PARAMETER Iterations
    Number of timed runs per strategy. Default: 5.

.PARAMETER NumStoredProcs
    Number of stored procedures to create (simulates real SP workload --
    exercises the sql_expression_dependencies fast path). Default: 20.

.PARAMETER NumAdHocPlans
    Number of ad-hoc query plans to force (exercises the CHARINDEX fallback
    path in Strategy B). Default: 10.

.PARAMETER SkipSetup
    Skip DB/table/SP creation and workload generation.  Use when the database
    already exists from a prior run.  #stats_to_process is always recreated.

.PARAMETER SkipTeardown
    Leave PerfTestQS database intact after the benchmark finishes.

.EXAMPLE
    # Full run (create DB, run benchmark, drop DB)
    .\tests\Test-ForcedPlanCheckPerf.ps1

    # Larger workload for a more meaningful signal
    .\tests\Test-ForcedPlanCheckPerf.ps1 -NumStoredProcs 100 -NumAdHocPlans 50 -Iterations 10

    # Re-run benchmark on existing DB (skip slow setup)
    .\tests\Test-ForcedPlanCheckPerf.ps1 -SkipSetup -SkipTeardown

    # Test against SQL 2025
    .\tests\Test-ForcedPlanCheckPerf.ps1 -Port 1431
#>
[CmdletBinding()]
param(
    [string]$Server          = "127.0.0.1",
    [int]   $Port            = 1432,
    [string]$Password        = $env:SPSTATUPDATE_TEST_SQL_PASSWORD,
    [int]   $Iterations      = 5,
    [int]   $NumStoredProcs  = 20,
    [int]   $NumAdHocPlans   = 10,
    [switch]$SkipSetup,
    [switch]$SkipTeardown
)

$ErrorActionPreference = "Continue"
if (-not $Password) { throw 'Set SPSTATUPDATE_TEST_SQL_PASSWORD or pass -Password.' }

# =====================================================================
# CONSTANTS
# =====================================================================
$TestDb     = "PerfTestQS"
$TableNames = @("Orders", "Customers", "Products", "Inventory", "Shipments")

# =====================================================================
# ADO.NET HELPER
# Returns: @{ Output=[ArrayList]; DataSet=[DataSet]; ExitCode=int }
# Uses ExecuteReader + DataTable.Load so InfoMessage events fire
# synchronously (required for SQL 2025 compatibility).
# =====================================================================
function Invoke-Sql {
    param(
        [string]$Query,
        [string]$Db      = "master",
        [int]   $Timeout = 300
    )
    $connStr = "Server=$Server,$Port;Database=$Db;User Id=sa;Password=$Password;" +
               "Encrypt=false;TrustServerCertificate=true;Pooling=false;Connection Timeout=30"
    $messages = [System.Collections.ArrayList]::new()
    $conn = $null
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
        $ds     = [System.Data.DataSet]::new()
        $reader = $cmd.ExecuteReader()
        do {
            $dt = [System.Data.DataTable]::new()
            $dt.Load($reader)
            $null = $ds.Tables.Add($dt)
        } while (-not $reader.IsClosed)
        return @{ Output = $messages; DataSet = $ds; ExitCode = 0 }
    }
    catch {
        $inner = if ($_.Exception.InnerException) { $_.Exception.InnerException.Message } else { "" }
        return @{
            Output   = [System.Collections.ArrayList]@($inner, $_.Exception.Message) + $messages
            DataSet  = $null
            ExitCode = 1
        }
    }
    finally {
        if ($conn -and $conn.State -ne [System.Data.ConnectionState]::Closed) {
            try { $conn.Close() } catch { }
        }
    }
}

# Invoke-Sql variant that returns the raw open SqlConnection so the
# caller can reuse it across multiple commands (needed so #stats_to_process
# is visible to both strategy queries in the benchmark loop).
function Open-SqlConnection {
    $connStr = "Server=$Server,$Port;Database=$TestDb;User Id=sa;Password=$Password;" +
               "Encrypt=false;TrustServerCertificate=true;Pooling=false;Connection Timeout=30"
    $conn = [System.Data.SqlClient.SqlConnection]::new($connStr)
    $conn.Open()
    return $conn
}

function Invoke-SqlOnConn {
    param(
        [System.Data.SqlClient.SqlConnection]$Conn,
        [string]$Query,
        [int]   $Timeout = 300
    )
    $cmd = $Conn.CreateCommand()
    $cmd.CommandText    = $Query
    $cmd.CommandTimeout = $Timeout
    $ds     = [System.Data.DataSet]::new()
    $reader = $cmd.ExecuteReader()
    do {
        $dt = [System.Data.DataTable]::new()
        $dt.Load($reader)
        $null = $ds.Tables.Add($dt)
    } while (-not $reader.IsClosed)
    return $ds
}

# =====================================================================
# HEADER
# =====================================================================
Write-Host ""
Write-Host ("=" * 72) -ForegroundColor Cyan
Write-Host "  Forced-Plan-Check Strategy Benchmark" -ForegroundColor Cyan
Write-Host "  Strategy A: v3.3.4 CHARINDEX text scan" -ForegroundColor Cyan
Write-Host "  Strategy B: v3.3.5 sql_expression_dependencies + CHARINDEX fallback" -ForegroundColor Cyan
Write-Host ("=" * 72) -ForegroundColor Cyan
Write-Host ""
Write-Host "  Server:        ${Server}:${Port}"
Write-Host "  Test database: $TestDb"
Write-Host "  Iterations:    $Iterations"
Write-Host "  Stored procs:  $NumStoredProcs  (exercises SED integer-join path)"
Write-Host "  Ad-hoc plans:  $NumAdHocPlans  (exercises CHARINDEX fallback path)"
Write-Host ""

# =====================================================================
# STEP 1: CONNECTIVITY + VERSION
# =====================================================================
Write-Host "[1/5] Verifying connectivity..." -ForegroundColor Yellow

$vr = Invoke-Sql -Query "SELECT CONVERT(nvarchar(256), @@VERSION) AS v, CONVERT(int, SERVERPROPERTY(N'ProductMajorVersion')) AS MajorVer;"
if ($vr.ExitCode -ne 0) {
    Write-Host "  ERROR: Cannot connect to ${Server}:${Port}" -ForegroundColor Red
    Write-Host "  $($vr.Output | Select-Object -First 1)" -ForegroundColor Red
    exit 1
}

$sqlMajor  = [int]$vr.DataSet.Tables[0].Rows[0]["MajorVer"]
$sqlVerStr = [string]$vr.DataSet.Tables[0].Rows[0]["v"]
$sqlVerStr = $sqlVerStr.Substring(0, [Math]::Min(80, $sqlVerStr.Length))
Write-Host "  Connected: $sqlVerStr" -ForegroundColor Green

# Warn if Query Store isn't well-supported (SQL 2016 = major 13)
if ($sqlMajor -lt 13) {
    Write-Host "  WARN: SQL major version $sqlMajor -- Query Store requires SQL 2016+." -ForegroundColor Yellow
}
Write-Host ""

# =====================================================================
# STEP 2: SETUP -- database, tables, SPs, workload, forced plans
# =====================================================================
if (-not $SkipSetup) {
    Write-Host "[2/5] Creating test database and workload..." -ForegroundColor Yellow

    # ------------------------------------------------------------------
    # 2a. Create (or recreate) PerfTestQS
    # ------------------------------------------------------------------
    Write-Host "  Creating database $TestDb..." -NoNewline

    # Drop if it already exists so setup is idempotent
    $r = Invoke-Sql -Query "
        IF DB_ID(N'$TestDb') IS NOT NULL
        BEGIN
            ALTER DATABASE [$TestDb] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
            DROP DATABASE [$TestDb];
        END;
        CREATE DATABASE [$TestDb];
    " -Timeout 60
    if ($r.ExitCode -ne 0) {
        Write-Host " FAILED" -ForegroundColor Red
        Write-Host "  $($r.Output | Select-Object -First 1)" -ForegroundColor Red
        exit 1
    }

    # Enable Query Store in FORCED_PLAN mode with a generous capture threshold
    $r = Invoke-Sql -Db $TestDb -Query "
        ALTER DATABASE CURRENT SET QUERY_STORE = ON (
            OPERATION_MODE              = READ_WRITE,
            QUERY_CAPTURE_MODE          = ALL,
            MAX_STORAGE_SIZE_MB         = 256,
            CLEANUP_POLICY              = (STALE_QUERY_THRESHOLD_DAYS = 30),
            SIZE_BASED_CLEANUP_MODE     = OFF
        );
        -- Wait for QS to come online
        DECLARE @tries int = 0;
        WHILE (SELECT actual_state FROM sys.database_query_store_options) <> 1
        BEGIN
            WAITFOR DELAY '00:00:01';
            SET @tries += 1;
            IF @tries > 10 BREAK;
        END;
    " -Timeout 30
    if ($r.ExitCode -ne 0) {
        # Non-fatal: QS may be unavailable on this edition (Express)
        Write-Host " (QS enable warning)" -ForegroundColor Yellow
        Write-Host "  WARN: $($r.Output | Select-Object -First 1)" -ForegroundColor Yellow
    } else {
        Write-Host " OK" -ForegroundColor Green
    }

    # ------------------------------------------------------------------
    # 2b. Create 5 test tables (10,000 rows each)
    # ------------------------------------------------------------------
    Write-Host "  Creating test tables (5 x 10,000 rows)..." -NoNewline

    # Build a single batch that creates and populates all tables
    $tablesSql = @"
USE [$TestDb];
GO
IF OBJECT_ID(N'dbo.Orders')    IS NOT NULL DROP TABLE dbo.Orders;
IF OBJECT_ID(N'dbo.Customers') IS NOT NULL DROP TABLE dbo.Customers;
IF OBJECT_ID(N'dbo.Products')  IS NOT NULL DROP TABLE dbo.Products;
IF OBJECT_ID(N'dbo.Inventory') IS NOT NULL DROP TABLE dbo.Inventory;
IF OBJECT_ID(N'dbo.Shipments') IS NOT NULL DROP TABLE dbo.Shipments;

CREATE TABLE dbo.Orders (
    OrderId    int          IDENTITY(1,1) PRIMARY KEY,
    CustomerId int          NOT NULL,
    ProductId  int          NOT NULL,
    OrderDate  datetime2(0) NOT NULL DEFAULT SYSDATETIME(),
    Amount     decimal(12,2) NOT NULL
);

CREATE TABLE dbo.Customers (
    CustomerId   int           IDENTITY(1,1) PRIMARY KEY,
    CustomerName nvarchar(100) NOT NULL,
    Region       nvarchar(50)  NOT NULL,
    CreatedDate  datetime2(0)  NOT NULL DEFAULT SYSDATETIME()
);

CREATE TABLE dbo.Products (
    ProductId   int           IDENTITY(1,1) PRIMARY KEY,
    ProductName nvarchar(100) NOT NULL,
    Category    nvarchar(50)  NOT NULL,
    Price       decimal(10,2) NOT NULL
);

CREATE TABLE dbo.Inventory (
    InventoryId int           IDENTITY(1,1) PRIMARY KEY,
    ProductId   int           NOT NULL,
    WarehouseId int           NOT NULL,
    Quantity    int           NOT NULL
);

CREATE TABLE dbo.Shipments (
    ShipmentId int           IDENTITY(1,1) PRIMARY KEY,
    OrderId    int           NOT NULL,
    ShippedDate datetime2(0) NOT NULL DEFAULT SYSDATETIME(),
    TrackingNo  nvarchar(50) NULL
);
GO
"@

    # Execute batches split on GO
    $tableBatches = [regex]::Split($tablesSql, '(?m)^\s*GO\s*$')
    $failed = $false
    $currentDb = "master"
    foreach ($batch in $tableBatches) {
        $trimmed = $batch.Trim()
        if ($trimmed.Length -eq 0) { continue }
        if ($trimmed -match '(?mi)^\s*USE\s+\[?(\w+)\]?') { $currentDb = $Matches[1] }
        $r = Invoke-Sql -Db $currentDb -Query $trimmed -Timeout 60
        if ($r.ExitCode -ne 0) {
            Write-Host " FAILED" -ForegroundColor Red
            Write-Host "  $($r.Output | Select-Object -First 1)" -ForegroundColor Red
            $failed = $true; break
        }
    }
    if ($failed) { exit 1 }

    # Populate each table with 10,000 rows using sys.all_columns as a row source
    $populateSql = @"
INSERT INTO dbo.Customers (CustomerName, Region)
SELECT TOP 10000
    N'Customer_' + CONVERT(nvarchar(20), ROW_NUMBER() OVER (ORDER BY c1.column_id)),
    CASE (ROW_NUMBER() OVER (ORDER BY c1.column_id)) % 4
        WHEN 0 THEN N'North' WHEN 1 THEN N'South'
        WHEN 2 THEN N'East'  ELSE N'West'
    END
FROM sys.all_columns c1 CROSS JOIN sys.all_columns c2;

INSERT INTO dbo.Products (ProductName, Category, Price)
SELECT TOP 10000
    N'Product_' + CONVERT(nvarchar(20), ROW_NUMBER() OVER (ORDER BY c1.column_id)),
    CASE (ROW_NUMBER() OVER (ORDER BY c1.column_id)) % 5
        WHEN 0 THEN N'Electronics'  WHEN 1 THEN N'Clothing'
        WHEN 2 THEN N'Food'         WHEN 3 THEN N'Books'
        ELSE N'Misc'
    END,
    CONVERT(decimal(10,2), (ROW_NUMBER() OVER (ORDER BY c1.column_id)) % 500) + 0.99
FROM sys.all_columns c1 CROSS JOIN sys.all_columns c2;

INSERT INTO dbo.Orders (CustomerId, ProductId, Amount)
SELECT TOP 10000
    ((ROW_NUMBER() OVER (ORDER BY c1.column_id) - 1) % 10000) + 1,
    ((ROW_NUMBER() OVER (ORDER BY c1.column_id) - 1) % 10000) + 1,
    CONVERT(decimal(12,2), (ROW_NUMBER() OVER (ORDER BY c1.column_id)) % 1000) + 0.50
FROM sys.all_columns c1 CROSS JOIN sys.all_columns c2;

INSERT INTO dbo.Inventory (ProductId, WarehouseId, Quantity)
SELECT TOP 10000
    ((ROW_NUMBER() OVER (ORDER BY c1.column_id) - 1) % 10000) + 1,
    ((ROW_NUMBER() OVER (ORDER BY c1.column_id) - 1) % 5) + 1,
    (ROW_NUMBER() OVER (ORDER BY c1.column_id)) % 1000
FROM sys.all_columns c1 CROSS JOIN sys.all_columns c2;

INSERT INTO dbo.Shipments (OrderId, TrackingNo)
SELECT TOP 10000
    ((ROW_NUMBER() OVER (ORDER BY c1.column_id) - 1) % 10000) + 1,
    N'TRACK' + CONVERT(nvarchar(20), ROW_NUMBER() OVER (ORDER BY c1.column_id))
FROM sys.all_columns c1 CROSS JOIN sys.all_columns c2;
"@
    $r = Invoke-Sql -Db $TestDb -Query $populateSql -Timeout 120
    if ($r.ExitCode -ne 0) {
        Write-Host " FAILED (populate)" -ForegroundColor Red
        Write-Host "  $($r.Output | Select-Object -First 1)" -ForegroundColor Red
        exit 1
    }
    Write-Host " OK" -ForegroundColor Green

    # ------------------------------------------------------------------
    # 2c. Create $NumStoredProcs stored procedures
    #     Each SP does a parameterized SELECT against one of the 5 tables.
    #     sql_expression_dependencies will record these object -> table refs,
    #     which is what Strategy B's fast path uses.
    # ------------------------------------------------------------------
    Write-Host "  Creating $NumStoredProcs stored procedures..." -NoNewline

    $spBatch = [System.Text.StringBuilder]::new()
    for ($i = 1; $i -le $NumStoredProcs; $i++) {
        $tableIdx = ($i - 1) % $TableNames.Count
        $tbl      = $TableNames[$tableIdx]

        # Each SP does a simple SELECT; vary the predicate column slightly
        # so QS treats them as distinct queries with distinct plans.
        $spSql = switch ($tbl) {
            "Orders"    { "SELECT TOP 100 OrderId, CustomerId, Amount FROM dbo.Orders WHERE CustomerId = @Id ORDER BY OrderId;" }
            "Customers" { "SELECT TOP 100 CustomerId, CustomerName, Region FROM dbo.Customers WHERE Region = @Name ORDER BY CustomerId;" }
            "Products"  { "SELECT TOP 100 ProductId, ProductName, Price FROM dbo.Products WHERE Category = @Name ORDER BY ProductId;" }
            "Inventory" { "SELECT TOP 100 InventoryId, ProductId, Quantity FROM dbo.Inventory WHERE ProductId = @Id ORDER BY InventoryId;" }
            "Shipments" { "SELECT TOP 100 ShipmentId, OrderId, TrackingNo FROM dbo.Shipments WHERE OrderId = @Id ORDER BY ShipmentId;" }
        }

        $paramDef = if ($spSql -match '@Name') { "@Name nvarchar(100) = N'North'" } else { "@Id int = 1" }

        $null = $spBatch.AppendLine("IF OBJECT_ID(N'dbo.BenchSP_$i') IS NOT NULL DROP PROCEDURE dbo.BenchSP_$i;")
        $null = $spBatch.AppendLine("GO")
        $null = $spBatch.AppendLine("CREATE PROCEDURE dbo.BenchSP_$i ($paramDef)")
        $null = $spBatch.AppendLine("AS")
        $null = $spBatch.AppendLine("BEGIN")
        $null = $spBatch.AppendLine("    SET NOCOUNT ON;")
        $null = $spBatch.AppendLine("    $spSql")
        $null = $spBatch.AppendLine("END;")
        $null = $spBatch.AppendLine("GO")
    }

    $spBatches = [regex]::Split($spBatch.ToString(), '(?m)^\s*GO\s*$')
    $failed = $false
    foreach ($batch in $spBatches) {
        $trimmed = $batch.Trim()
        if ($trimmed.Length -eq 0) { continue }
        $r = Invoke-Sql -Db $TestDb -Query $trimmed -Timeout 60
        if ($r.ExitCode -ne 0) {
            Write-Host " FAILED (SP $i)" -ForegroundColor Red
            Write-Host "  $($r.Output | Select-Object -First 1)" -ForegroundColor Red
            $failed = $true; break
        }
    }
    if ($failed) { exit 1 }
    Write-Host " OK" -ForegroundColor Green

    # ------------------------------------------------------------------
    # 2d. Execute each SP 10 times to populate query_store_query
    #     and query_store_runtime_stats.
    # ------------------------------------------------------------------
    Write-Host "  Running each SP 10 times to populate QS..." -NoNewline

    for ($i = 1; $i -le $NumStoredProcs; $i++) {
        $execSql = [System.Text.StringBuilder]::new()
        for ($run = 1; $run -le 10; $run++) {
            $null = $execSql.AppendLine("EXEC dbo.BenchSP_$i;")
        }
        $r = Invoke-Sql -Db $TestDb -Query $execSql.ToString() -Timeout 120
        if ($r.ExitCode -ne 0) {
            Write-Host " WARN (SP $i exec)" -ForegroundColor Yellow
            # Non-fatal: proceed even if some SPs fail to populate QS
        }
    }
    Write-Host " OK" -ForegroundColor Green

    # Flush QS to make runtime_stats visible
    Invoke-Sql -Db $TestDb -Query "
        BEGIN TRY EXEC sp_query_store_flush_db; END TRY BEGIN CATCH END CATCH;
    " -Timeout 30 | Out-Null

    # ------------------------------------------------------------------
    # 2e. Force the plan for every SP in QS
    #     sp_query_store_force_plan requires (query_id, plan_id).
    # ------------------------------------------------------------------
    Write-Host "  Forcing plans for SP queries in QS..." -NoNewline

    # Retrieve all query_id / plan_id pairs for our SPs.
    # QS stores the SP's object_id in query_store_query.object_id.
    $r = Invoke-Sql -Db $TestDb -Query "
        SELECT DISTINCT
            qsq.query_id,
            qsp.plan_id
        FROM sys.query_store_plan AS qsp
        INNER JOIN sys.query_store_query AS qsq ON qsq.query_id = qsp.query_id
        INNER JOIN sys.objects AS o ON o.object_id = qsq.object_id
        WHERE o.name LIKE N'BenchSP[_]%'
        AND   qsp.is_forced_plan = 0;
    "
    if ($r.ExitCode -ne 0) {
        Write-Host " WARN (QS query)" -ForegroundColor Yellow
    } else {
        $forcedCount = 0
        foreach ($row in $r.DataSet.Tables[0].Rows) {
            $qid = [long]$row["query_id"]
            $pid = [long]$row["plan_id"]
            $fr = Invoke-Sql -Db $TestDb -Query "EXEC sys.sp_query_store_force_plan @query_id = $qid, @plan_id = $pid;" -Timeout 30
            if ($fr.ExitCode -eq 0) { $forcedCount++ }
        }
        Write-Host " OK ($forcedCount plans forced)" -ForegroundColor Green
    }

    # ------------------------------------------------------------------
    # 2f. Create $NumAdHocPlans ad-hoc forced plans
    #     These are direct SELECT statements (object_id IS NULL in QS).
    #     They exercise the CHARINDEX fallback branch in Strategy B.
    # ------------------------------------------------------------------
    Write-Host "  Creating $NumAdHocPlans ad-hoc forced plans..." -NoNewline

    $adHocForced = 0
    for ($i = 1; $i -le $NumAdHocPlans; $i++) {
        $tableIdx = ($i - 1) % $TableNames.Count
        $tbl = $TableNames[$tableIdx]

        # Vary the literal so QS doesn't deduplicate identical queries.
        # Each literal is unique enough to get its own query_id.
        $adHocSql = switch ($tbl) {
            "Orders"    { "SELECT /* AdHocBench$i */ TOP 50 OrderId, Amount FROM dbo.Orders WHERE OrderId > $i;" }
            "Customers" { "SELECT /* AdHocBench$i */ TOP 50 CustomerId, Region FROM dbo.Customers WHERE CustomerId > $i;" }
            "Products"  { "SELECT /* AdHocBench$i */ TOP 50 ProductId, Price FROM dbo.Products WHERE ProductId > $i;" }
            "Inventory" { "SELECT /* AdHocBench$i */ TOP 50 InventoryId, Quantity FROM dbo.Inventory WHERE InventoryId > $i;" }
            "Shipments" { "SELECT /* AdHocBench$i */ TOP 50 ShipmentId, TrackingNo FROM dbo.Shipments WHERE ShipmentId > $i;" }
        }

        # Run the ad-hoc query to get it into QS
        Invoke-Sql -Db $TestDb -Query $adHocSql -Timeout 30 | Out-Null
    }

    # Flush and then force plans for all ad-hoc queries
    Invoke-Sql -Db $TestDb -Query "BEGIN TRY EXEC sp_query_store_flush_db; END TRY BEGIN CATCH END CATCH;" -Timeout 30 | Out-Null

    $r = Invoke-Sql -Db $TestDb -Query "
        SELECT DISTINCT
            qsq.query_id,
            qsp.plan_id
        FROM sys.query_store_plan AS qsp
        INNER JOIN sys.query_store_query AS qsq ON qsq.query_id = qsp.query_id
        WHERE qsq.object_id IS NULL                  -- ad-hoc plans only
        AND   qsp.is_forced_plan = 0
        AND   EXISTS (
            SELECT 1 FROM sys.query_store_query_text AS qsqt
            WHERE qsqt.query_text_id = qsq.query_text_id
            AND   qsqt.query_sql_text LIKE N'%AdHocBench%'
        );
    "
    if ($r.ExitCode -eq 0 -and $r.DataSet.Tables.Count -gt 0) {
        foreach ($row in $r.DataSet.Tables[0].Rows) {
            $qid = [long]$row["query_id"]
            $pid = [long]$row["plan_id"]
            $fr  = Invoke-Sql -Db $TestDb -Query "EXEC sys.sp_query_store_force_plan @query_id = $qid, @plan_id = $pid;" -Timeout 30
            if ($fr.ExitCode -eq 0) { $adHocForced++ }
        }
    }
    Write-Host " OK ($adHocForced plans forced)" -ForegroundColor Green

    Write-Host ""
} else {
    Write-Host "[2/5] Skipping setup (-SkipSetup)" -ForegroundColor DarkGray
    Write-Host ""
}

# =====================================================================
# STEP 3: VERIFY QS STATE
# =====================================================================
Write-Host "[3/5] Verifying Query Store state..." -ForegroundColor Yellow

$qsCheck = Invoke-Sql -Db $TestDb -Query "
    SELECT
        o.actual_state_desc,
        o.readonly_reason,
        o.current_storage_size_mb,
        (SELECT COUNT(*) FROM sys.query_store_plan  WHERE is_forced_plan = 1)    AS ForcedPlanCount,
        (SELECT COUNT(*) FROM sys.query_store_query WHERE object_id IS NULL)      AS AdHocQueryCount,
        (SELECT COUNT(*) FROM sys.query_store_query WHERE object_id IS NOT NULL)  AS CompiledQueryCount
    FROM sys.database_query_store_options AS o;
"
if ($qsCheck.ExitCode -ne 0) {
    Write-Host "  WARN: Could not query QS state -- $($qsCheck.Output | Select-Object -First 1)" -ForegroundColor Yellow
} else {
    $row = $qsCheck.DataSet.Tables[0].Rows[0]
    $qsState         = [string]$row["actual_state_desc"]
    $qsStorageMb     = [string]$row["current_storage_size_mb"]
    $forcedPlanTotal = [int]$row["ForcedPlanCount"]
    $adHocTotal      = [int]$row["AdHocQueryCount"]
    $compiledTotal   = [int]$row["CompiledQueryCount"]

    $qsColor = if ($qsState -eq "READ_WRITE") { "Green" } else { "Yellow" }
    Write-Host "  QS state:       $qsState  (storage: ${qsStorageMb} MB)" -ForegroundColor $qsColor
    Write-Host "  Forced plans:   $forcedPlanTotal total"
    Write-Host "  Ad-hoc queries: $adHocTotal  |  Compiled queries: $compiledTotal"

    if ($qsState -ne "READ_WRITE") {
        Write-Host "  WARN: QS is not READ_WRITE -- benchmark results may not reflect" -ForegroundColor Yellow
        Write-Host "        real workloads.  Check if this is SQL Express edition." -ForegroundColor Yellow
        $r = Invoke-Sql -Query "SELECT CONVERT(nvarchar(100), SERVERPROPERTY(N'Edition')) AS Ed;"
        if ($r.ExitCode -eq 0) {
            $edition = [string]$r.DataSet.Tables[0].Rows[0]["Ed"]
            Write-Host "  Edition: $edition" -ForegroundColor Yellow
            if ($edition -match "Express") {
                Write-Host "  NOTE: SQL Express limits QS storage to 1 MB -- this reduces the" -ForegroundColor Yellow
                Write-Host "        forced plan count and may compress the timing difference." -ForegroundColor Yellow
            }
        }
    }

    if ($forcedPlanTotal -eq 0) {
        Write-Host ""
        Write-Host "  WARN: No forced plans found.  Strategy A will always return 0 quickly" -ForegroundColor Yellow
        Write-Host "        and the benchmark will not be meaningful.  Possible causes:" -ForegroundColor Yellow
        Write-Host "    - QS was off or in READ_ONLY mode during setup" -ForegroundColor Yellow
        Write-Host "    - Re-run without -SkipSetup" -ForegroundColor Yellow
    }
}
Write-Host ""

# =====================================================================
# STEP 4: BENCHMARK LOOP
#
# Both strategy queries run on a single persistent SqlConnection so
# #stats_to_process (a true temp table) is visible to both.
# Between each A/B pair we flush the plan cache and buffer pool to
# prevent Strategy B from benefiting from Strategy A's I/O warm-up.
# =====================================================================
Write-Host "[4/5] Running benchmark ($Iterations iterations)..." -ForegroundColor Yellow
Write-Host ""

# The timing SQL uses SYSDATETIME() stored in a local #perf_scratch temp table
# to avoid any overhead from PowerShell round-trips between start/stop calls.
# One batch: create scratch, record start, run query, record stop, return ms.
$strategyA_sql = @"
/* ============================================================
   Strategy A (v3.3.4): CHARINDEX text scan
   Matches forced plans to updated tables by scanning query_sql_text.
   ============================================================ */
IF OBJECT_ID(N'tempdb..#perf_scratch') IS NOT NULL DROP TABLE #perf_scratch;
CREATE TABLE #perf_scratch (t0 datetime2(7), t1 datetime2(7), result_count int);

INSERT INTO #perf_scratch (t0, t1, result_count) VALUES (SYSDATETIME(), NULL, 0);

DECLARE @result_a int;
SELECT @result_a = COUNT(DISTINCT qsp.plan_id)
FROM sys.query_store_plan AS qsp
INNER JOIN sys.query_store_query AS qsq
    ON qsq.query_id = qsp.query_id
INNER JOIN sys.query_store_query_text AS qsqt
    ON qsqt.query_text_id = qsq.query_text_id
WHERE qsp.is_forced_plan = 1
AND EXISTS (
    SELECT 1
    FROM #stats_to_process AS stp
    WHERE stp.database_name = DB_NAME() COLLATE DATABASE_DEFAULT
    AND   stp.processed     = 1
    AND   CHARINDEX(
              stp.table_name COLLATE DATABASE_DEFAULT,
              qsqt.query_sql_text COLLATE DATABASE_DEFAULT
          ) > 0
);

UPDATE #perf_scratch SET t1 = SYSDATETIME(), result_count = @result_a;

SELECT
    DATEDIFF(MILLISECOND, t0, t1) AS ElapsedMs,
    result_count                   AS ForcedPlanCount
FROM #perf_scratch;

DROP TABLE #perf_scratch;
"@

$strategyB_sql = @"
/* ============================================================
   Strategy B (v3.3.5): sys.sql_expression_dependencies + CHARINDEX fallback
   For compiled plans (object_id IS NOT NULL): integer join through SED.
   For ad-hoc plans  (object_id IS NULL):     falls back to CHARINDEX.
   ============================================================ */
IF OBJECT_ID(N'tempdb..#perf_scratch') IS NOT NULL DROP TABLE #perf_scratch;
CREATE TABLE #perf_scratch (t0 datetime2(7), t1 datetime2(7), result_count int);

INSERT INTO #perf_scratch (t0, t1, result_count) VALUES (SYSDATETIME(), NULL, 0);

DECLARE @result_b int;
SELECT @result_b = COUNT(DISTINCT qsp.plan_id)
FROM sys.query_store_plan AS qsp
INNER JOIN sys.query_store_query AS qsq
    ON qsq.query_id = qsp.query_id
WHERE qsp.is_forced_plan = 1
AND (
    /* Fast path: compiled SP/query with known object_id */
    EXISTS (
        SELECT 1
        FROM sys.sql_expression_dependencies AS sed
        INNER JOIN #stats_to_process AS stp
            ON stp.object_id = sed.referenced_id
        WHERE sed.referencing_id      = qsq.object_id
        AND   sed.referenced_minor_id = 0
        AND   stp.database_name = DB_NAME() COLLATE DATABASE_DEFAULT
        AND   stp.processed     = 1
    )
    OR
    /* Fallback: ad-hoc query (object_id IS NULL) -- must scan text */
    (qsq.object_id IS NULL
     AND EXISTS (
         SELECT 1
         FROM sys.query_store_query_text AS qsqt
         INNER JOIN #stats_to_process AS stp
             ON CHARINDEX(
                    stp.table_name COLLATE DATABASE_DEFAULT,
                    qsqt.query_sql_text COLLATE DATABASE_DEFAULT
                ) > 0
         WHERE qsqt.query_text_id = qsq.query_text_id
         AND   stp.database_name  = DB_NAME() COLLATE DATABASE_DEFAULT
         AND   stp.processed      = 1
     ))
);

UPDATE #perf_scratch SET t1 = SYSDATETIME(), result_count = @result_b;

SELECT
    DATEDIFF(MILLISECOND, t0, t1) AS ElapsedMs,
    result_count                   AS ForcedPlanCount
FROM #perf_scratch;

DROP TABLE #perf_scratch;
"@

# SQL to build (or rebuild) #stats_to_process for the 5 test tables.
# schema matches what sp_StatUpdate creates internally.
$buildStpSql = @"
IF OBJECT_ID(N'tempdb..#stats_to_process') IS NOT NULL DROP TABLE #stats_to_process;
CREATE TABLE #stats_to_process (
    database_name sysname     NOT NULL,
    table_name    sysname     NOT NULL,
    object_id     int         NOT NULL,
    processed     bit         NOT NULL DEFAULT 0
);

DECLARE @db sysname = DB_NAME();

INSERT INTO #stats_to_process (database_name, table_name, object_id, processed)
SELECT
    @db,
    o.name,
    o.object_id,
    CONVERT(bit, 1)
FROM sys.objects AS o
WHERE o.name IN (N'Orders', N'Customers', N'Products', N'Inventory', N'Shipments')
AND   o.type = N'U';
"@

# Cache-flush SQL (requires sysadmin).  We run this between each strategy
# to ensure neither benefits from the other's warm buffer pool.
$cacheFlushSql = @"
DBCC FREEPROCCACHE WITH NO_INFOMSGS;
DBCC DROPCLEANBUFFERS WITH NO_INFOMSGS;
"@

# Results collection
$timingsA = [System.Collections.ArrayList]::new()
$timingsB = [System.Collections.ArrayList]::new()

Write-Host "  Iter  |  Strategy A (ms)  |  Strategy B (ms)  |  Delta (ms)  |  Winner" -ForegroundColor Cyan
Write-Host "  ------+-------------------+-------------------+--------------+--------" -ForegroundColor Cyan

for ($iter = 1; $iter -le $Iterations; $iter++) {

    # Open a persistent connection for this iteration so #stats_to_process
    # survives across both strategy queries.
    $benchConn = $null
    try {
        $benchConn = Open-SqlConnection

        # Build #stats_to_process on this connection (visible to all cmds below)
        $null = Invoke-SqlOnConn -Conn $benchConn -Query $buildStpSql

        # -------------------------------------------------------
        # Flush cache before Strategy A
        # -------------------------------------------------------
        try {
            $null = Invoke-SqlOnConn -Conn $benchConn -Query $cacheFlushSql -Timeout 60
        }
        catch {
            # Non-fatal: may lack sysadmin.  Warn once.
            if ($iter -eq 1) {
                Write-Host "  WARN: Cache flush failed (needs sysadmin) -- timings may include cache effects" -ForegroundColor Yellow
            }
        }

        # -------------------------------------------------------
        # STRATEGY A
        # -------------------------------------------------------
        $msA = -1
        try {
            $dsA = Invoke-SqlOnConn -Conn $benchConn -Query $strategyA_sql -Timeout 120
            if ($dsA.Tables.Count -gt 0 -and $dsA.Tables[0].Rows.Count -gt 0) {
                $msA = [int]$dsA.Tables[0].Rows[0]["ElapsedMs"]
            }
        }
        catch {
            Write-Host ("  {0,5}  |  TIMEOUT/ERROR    |  -                |  -           |  -" -f $iter) -ForegroundColor Red
            Write-Host "         Strategy A error: $_" -ForegroundColor Red
            continue
        }

        # -------------------------------------------------------
        # Flush cache between strategies
        # -------------------------------------------------------
        try { $null = Invoke-SqlOnConn -Conn $benchConn -Query $cacheFlushSql -Timeout 60 }
        catch { }

        # -------------------------------------------------------
        # STRATEGY B
        # -------------------------------------------------------
        $msB = -1
        try {
            # Must rebuild #stats_to_process -- Strategy A's batch dropped
            # #perf_scratch but left #stats_to_process intact.  Rebuild anyway
            # to be safe (Strategy A may have ended the batch scope).
            $null = Invoke-SqlOnConn -Conn $benchConn -Query $buildStpSql
            $dsB = Invoke-SqlOnConn -Conn $benchConn -Query $strategyB_sql -Timeout 120
            if ($dsB.Tables.Count -gt 0 -and $dsB.Tables[0].Rows.Count -gt 0) {
                $msB = [int]$dsB.Tables[0].Rows[0]["ElapsedMs"]
            }
        }
        catch {
            Write-Host ("  {0,5}  |  {1,-17}  |  TIMEOUT/ERROR    |  -           |  -" -f $iter, "${msA} ms") -ForegroundColor Red
            Write-Host "         Strategy B error: $_" -ForegroundColor Red
            continue
        }

        if ($msA -ge 0) { $null = $timingsA.Add($msA) }
        if ($msB -ge 0) { $null = $timingsB.Add($msB) }

        $delta  = $msA - $msB
        $winner = if    ($msA -lt $msB)  { "A" }
                  elseif ($msB -lt $msA) { "B" }
                  else                    { "TIE" }
        $winnerColor = switch ($winner) {
            "A"   { "Yellow" }   # A won: unexpected, highlight
            "B"   { "Green"  }   # B won: expected improvement
            "TIE" { "DarkGray" }
        }

        Write-Host ("  {0,5}  |  {1,15} ms  |  {2,15} ms  |  {3,10} ms  |  {4}" -f `
            $iter, $msA, $msB, $delta, $winner) -ForegroundColor $winnerColor
    }
    finally {
        if ($benchConn -and $benchConn.State -ne [System.Data.ConnectionState]::Closed) {
            try { $benchConn.Close() } catch { }
        }
    }
}

# =====================================================================
# STEP 5: RESULTS SUMMARY
# =====================================================================
Write-Host ""
Write-Host ("=" * 72) -ForegroundColor Cyan
Write-Host "  BENCHMARK RESULTS" -ForegroundColor Cyan
Write-Host ("=" * 72) -ForegroundColor Cyan
Write-Host ""

if ($timingsA.Count -eq 0 -or $timingsB.Count -eq 0) {
    Write-Host "  ERROR: No valid timing data collected." -ForegroundColor Red
    Write-Host "  Check that the database and forced plans exist (-SkipSetup requires prior run)." -ForegroundColor Red
    exit 1
}

$validIter  = [Math]::Min($timingsA.Count, $timingsB.Count)
$avgA       = [Math]::Round(($timingsA | Measure-Object -Average).Average, 1)
$avgB       = [Math]::Round(($timingsB | Measure-Object -Average).Average, 1)
$minA       = ($timingsA | Measure-Object -Minimum).Minimum
$minB       = ($timingsB | Measure-Object -Minimum).Minimum
$maxA       = ($timingsA | Measure-Object -Maximum).Maximum
$maxB       = ($timingsB | Measure-Object -Maximum).Maximum
$avgDelta   = [Math]::Round($avgA - $avgB, 1)
$speedup    = if ($avgB -gt 0) { [Math]::Round($avgA / $avgB, 2) } else { "N/A" }

# Which strategy was faster on most iterations?
$aWins = 0; $bWins = 0
for ($i = 0; $i -lt $validIter; $i++) {
    if ($timingsA[$i] -lt $timingsB[$i]) { $aWins++ }
    elseif ($timingsB[$i] -lt $timingsA[$i]) { $bWins++ }
}
$overallWinner = if ($bWins -gt $aWins) { "Strategy B (v3.3.5)" }
                 elseif ($aWins -gt $bWins) { "Strategy A (v3.3.4)" }
                 else { "TIE" }
$summaryColor  = if ($overallWinner -match "B") { "Green" }
                 elseif ($overallWinner -match "A") { "Yellow" }
                 else { "DarkGray" }

Write-Host "  Metric              Strategy A (v3.3.4)   Strategy B (v3.3.5)" -ForegroundColor Cyan
Write-Host "  ------------------  --------------------  --------------------"
Write-Host ("  Average (ms):       {0,20}  {1,20}"  -f $avgA,   $avgB)
Write-Host ("  Min (ms):           {0,20}  {1,20}"  -f $minA,   $minB)
Write-Host ("  Max (ms):           {0,20}  {1,20}"  -f $maxA,   $maxB)
Write-Host ""
Write-Host ("  Valid iterations:   {0}" -f $validIter)
Write-Host ("  Avg delta (A-B):    {0} ms  (positive = B is faster)" -f $avgDelta)
Write-Host ("  Speedup ratio (A/B): {0}x" -f $speedup)
Write-Host ("  Iteration wins:     A={0}  B={1}  TIE={2}" -f $aWins, $bWins, ($validIter - $aWins - $bWins))
Write-Host ""
Write-Host ("  OVERALL WINNER:  $overallWinner") -ForegroundColor $summaryColor
Write-Host ""

# Interpretation guidance
if ($speedup -is [double]) {
    if ($speedup -gt 1.5) {
        Write-Host "  INTERPRETATION: Strategy B is meaningfully faster (>1.5x).  The" -ForegroundColor Green
        Write-Host "  sql_expression_dependencies integer join delivers expected benefit" -ForegroundColor Green
        Write-Host "  on this workload ($NumStoredProcs compiled SPs, $NumAdHocPlans ad-hoc plans)." -ForegroundColor Green
    } elseif ($speedup -gt 1.1) {
        Write-Host "  INTERPRETATION: Strategy B is slightly faster (1.1x to 1.5x).  The" -ForegroundColor Yellow
        Write-Host "  improvement may be masked by the ad-hoc CHARINDEX fallback or by a" -ForegroundColor Yellow
        Write-Host "  small total plan count.  Try increasing -NumStoredProcs." -ForegroundColor Yellow
    } elseif ($speedup -lt 0.9) {
        Write-Host "  INTERPRETATION: Strategy A is faster -- unexpected.  Possible causes:" -ForegroundColor Red
        Write-Host "    - sys.sql_expression_dependencies is large or unindexed on this server" -ForegroundColor Red
        Write-Host "    - Forced plan count is very small (optimizer picks trivial plan either way)" -ForegroundColor Red
        Write-Host "    - QS not READ_WRITE (forced plan count = 0, both strategies trivially fast)" -ForegroundColor Red
    } else {
        Write-Host "  INTERPRETATION: Strategies are roughly equivalent on this workload." -ForegroundColor DarkGray
        Write-Host "  Consider increasing -NumStoredProcs and -Iterations for a larger signal." -ForegroundColor DarkGray
    }
}
Write-Host ""

# Edition / QS caveat note
$edCheck = Invoke-Sql -Query "SELECT CONVERT(nvarchar(100), SERVERPROPERTY(N'Edition')) AS Ed;"
if ($edCheck.ExitCode -eq 0) {
    $edition = [string]$edCheck.DataSet.Tables[0].Rows[0]["Ed"]
    if ($edition -match "Express") {
        Write-Host "  NOTE (Express Edition): QS storage is capped at 1 MB.  Forced plan" -ForegroundColor Yellow
        Write-Host "  counts will be low and timing differences may not be representative." -ForegroundColor Yellow
        Write-Host ""
    } elseif ($edition -match "Developer|Enterprise|Standard") {
        Write-Host "  Edition: $edition  (QS fully supported)" -ForegroundColor DarkGray
        Write-Host ""
    }
}

# =====================================================================
# TEARDOWN
# =====================================================================
if (-not $SkipTeardown) {
    Write-Host "[5/5] Tearing down test database..." -ForegroundColor Yellow
    $r = Invoke-Sql -Query "
        IF DB_ID(N'$TestDb') IS NOT NULL
        BEGIN
            ALTER DATABASE [$TestDb] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
            DROP DATABASE [$TestDb];
        END;
    " -Timeout 60
    if ($r.ExitCode -eq 0) {
        Write-Host "  Dropped database $TestDb" -ForegroundColor Green
    } else {
        Write-Host "  WARN: Teardown failed -- $($r.Output | Select-Object -First 1)" -ForegroundColor Yellow
    }
} else {
    Write-Host "[5/5] Keeping test database (-SkipTeardown)" -ForegroundColor DarkGray
    Write-Host "  Re-run with -SkipSetup to benchmark again without recreating workload." -ForegroundColor DarkGray
}
Write-Host ""
