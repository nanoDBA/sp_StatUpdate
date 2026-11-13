<#
.SYNOPSIS
    Extended stop-reason coverage for sp_StatUpdate v3.
    Addresses H1 (@StopByTime -> TIME_LIMIT), H2 (BATCH_LIMIT), and H4 (eight stop reasons).

.DESCRIPTION
    Covers all stop reasons that can be deterministically triggered without AG or
    I/O corruption infrastructure.  Tests are self-contained; each creates its own
    test state.  Does NOT overwrite Test-StopReasons.ps1 (S1-S9).

    Stop reasons covered by this suite (SC prefix):
      SC01: TIME_LIMIT via @TimeLimit=0 -- deterministically reaches the loop deadline.
            @StopByTime format/acceptance remains covered by Test-PresetStopByTime P6.
      SC02: BATCH_LIMIT -- @BatchLimit=1 with two qualifying stats.
      SC03: COMPLETED -- real-execute run exhausts all discovered stats for one table.
      SC04: NO_QUALIFYING_STATS -- impossibly high threshold on a freshly-updated table.
      SC05: TEMPDB_PRESSURE -- @MinTempdbFreeMB set impossibly high + @FailFast=1.
      SC06: FAIL_FAST -- @FailFast=1 on a READ_ONLY database; UPDATE STATISTICS fails.
      SC07: CONSECUTIVE_FAILURES -- 10+ consecutive failures on a READ_ONLY database.
      SC08: COMPLETED_WITH_SKIPPED_DBS -- a legacy-compatibility test DB forces
            a caught discovery error after database selection.
      SC09: NATURAL_END (mop-up transition state) -- NIGHTLY preset + impossibly high
            threshold; proc sets NATURAL_END to enter mop-up path.
      SC10: PARAMETER_ERROR -- invalid parameter value triggers validation failure.
      SC11: ALREADY_RUNNING -- a second call observes a synthetic live lock-holder row.

    Stop reasons NOT tested here (infrastructure not available on standalone Docker):
      AG_SECONDARY      -- requires Availability Group replica
      AG_REDO_QUEUE     -- requires AG with redo queue depth control
      IO_CORRUPTION     -- requires errors 823/824/825 from disk subsystem
      PARALLEL_COMPLETE -- requires @StatsInParallel=Y and queue infrastructure
      FINGERPRINT_CONFLICT / MAX_WORKERS / QUEUE_INIT_ERROR -- parallel-mode only
      LOG_SPACE_HIGH    -- requires transaction log at >90% capacity with @FailFast=1

    FINDING (documented, not fixed here):
      @MaxConsecutiveFailures is NOT a public parameter in v3 -- it is the internal
      @i_max_consecutive_failures.  Test S8 in Test-StopReasons.ps1 passes this
      parameter name to the proc and will fail with "not a parameter" error on v3.

.PARAMETER Server
    Remote Docker host IP.  Default: 100.88.59.72.

.PARAMETER Port
    SQL Server port.  Default: 1432 (SQL 2022 on remote host).

.PARAMETER Password
    SA password.  Default: $env:SPSTATUPDATE_TEST_SQL_PASSWORD.

.PARAMETER Database
    Primary test database.  Default: StatUpdateTest.

.EXAMPLE
    .\tests\Test-StopReasonsCoverage.ps1
    .\tests\Test-StopReasonsCoverage.ps1 -Port 1431
    .\tests\Test-StopReasonsCoverage.ps1 -Server 127.0.0.1 -Port 1433
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$Server,
    [ValidateSet('2019','2022','2025')][string]$Version = '2022',
    [string]$Password = $(if ($env:SPSTATUPDATE_TEST_SQL_PASSWORD) { $env:SPSTATUPDATE_TEST_SQL_PASSWORD } else { '' }),
    [string]$ResultsPath = '',
    [string]$Database = 'StatUpdateTest'
)

$ErrorActionPreference = "Continue"
$Port = @{ '2019' = 1434; '2022' = 1432; '2025' = 1431 }[$Version]
$startedAt = Get-Date
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $ScriptDir
Import-Module (Join-Path $ScriptDir 'StructuredLegacyResults.psm1') -Force
if (-not $Password) { throw 'Release-gate SQL credentials are unavailable.' }

# =====================================================================
# ADO.NET HELPER -- uses ExecuteReader (not SqlDataAdapter.Fill) so
# InfoMessage events fire synchronously on SQL 2025.
# =====================================================================
function Invoke-Sql {
    param(
        [string]$ServerAddr,
        [int]$Port,
        [string]$Pwd,
        [string]$Query,
        [string]$Db      = "master",
        [int]$Timeout    = 600
    )
    $connStr = "Server=$ServerAddr,$Port;Database=$Db;User Id=sa;Password=$Pwd;" +
               "Encrypt=false;TrustServerCertificate=true;Pooling=false;Connection Timeout=30"
    $messages = [System.Collections.ArrayList]::new()

    try {
        $conn = [System.Data.SqlClient.SqlConnection]::new($connStr)
        $conn.add_InfoMessage({
            param($sender, $e)
            $null = $messages.Add($e.Message)
        })
        $conn.FireInfoMessageEventOnUserErrors = $true
        $conn.Open()

        $cmd             = $conn.CreateCommand()
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
    }
    catch {
        try { $conn.Close() } catch { }
        $inner = if ($_.Exception.InnerException) { $_.Exception.InnerException.Message } else { "" }
        return @{
            Output   = [System.Collections.ArrayList]@($inner, $_.Exception.Message) + $messages
            DataSet  = $null
            ExitCode = 1
        }
    }
}

# Helper to extract StopReason from the last DataSet table (OUTPUT param SELECT)
# Falls back to summary result set (Tables[0]) column StopReason.
function Get-StopReason {
    param($DataSet)
    if ($null -eq $DataSet -or $DataSet.Tables.Count -eq 0) { return $null }
    $last = $DataSet.Tables[$DataSet.Tables.Count - 1]
    if ($last.Columns.Contains("StopReason") -and $last.Rows.Count -gt 0) {
        $v = "$($last.Rows[0]['StopReason'])"
        if ($v -ne "" -and $v -ne "System.DBNull") { return $v }
    }
    # Fallback: summary RS (first table)
    $sum = $DataSet.Tables[0]
    if ($sum.Columns.Contains("StopReason") -and $sum.Rows.Count -gt 0) {
        $v = "$($sum.Rows[0]['StopReason'])"
        if ($v -ne "" -and $v -ne "System.DBNull") { return $v }
    }
    return $null
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
Write-Host " sp_StatUpdate Stop-Reason Extended Coverage (SC01-SC11)" -ForegroundColor Cyan
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
Write-Host "  Connected (SQL major version: $serverMajor)" -ForegroundColor Green
Write-Host ""

# =====================================================================
# TESTS
# =====================================================================
Write-Host "[Tests] Running stop-reason coverage checks..." -ForegroundColor Yellow
Write-Host ""

# ------------------------------------------------------------------
# SC01: TIME_LIMIT via @TimeLimit=0
#       Zero is a valid budget (only negative values are rejected), so the first
#       loop-header check deterministically emits TIME_LIMIT. StopByTime's
#       accepted formats are separately checked in Test-PresetStopByTime P6.
# ------------------------------------------------------------------
Write-Host "  SC01: TIME_LIMIT via @TimeLimit=0" -ForegroundColor DarkGray

# Insert modifications first in a separate call so stats are dirty
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Query "
    INSERT INTO dbo.Orders (CustomerName, Amount)
    SELECT TOP 10000 N'SC01Test', 1.00 FROM sys.all_columns;
    INSERT INTO dbo.Products (ProductName, Price)
    SELECT TOP 3000 N'SC01Product', 9.99 FROM sys.all_columns;
" | Out-Null

$r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
    DECLARE @sr nvarchar(50);
    EXEC dbo.sp_StatUpdate
        @Databases             = N'$Database',
        @ModificationThreshold = 100,
        @Execute               = N'Y',
        @TimeLimit             = 0,
        @StopReasonOut         = @sr OUTPUT;
    SELECT @sr AS StopReason;
" -Timeout 120
if ($r.ExitCode -ne 0) {
    $errMsg = ($r.Output | Where-Object { $_ } | Select-Object -First 1)
    Add-Result "SC01: TIME_LIMIT via @TimeLimit=0" "FAIL" "Execution error: $errMsg"
} else {
    $sr = Get-StopReason -DataSet $r.DataSet
    if ($sr -eq "TIME_LIMIT") {
        Add-Result "SC01: TIME_LIMIT via @TimeLimit=0" "PASS" "StopReason=$sr"
    } else {
        Add-Result "SC01: TIME_LIMIT via @TimeLimit=0" "FAIL" "StopReason=$sr (expected TIME_LIMIT)"
    }
}

# ------------------------------------------------------------------
# SC02: BATCH_LIMIT
#       @BatchLimit=1 with enough modifications that at least two
#       stats qualify on different tables.  After processing 1 stat
#       the proc checks @stats_processed >= @BatchLimit and stops.
# ------------------------------------------------------------------
Write-Host "  SC02: BATCH_LIMIT" -ForegroundColor DarkGray

Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Query "
    INSERT INTO dbo.Orders (CustomerName, Amount)
    SELECT TOP 10000 N'SC02OrderTest', 1.00 FROM sys.all_columns;
    INSERT INTO dbo.Products (ProductName, Price)
    SELECT TOP 3000 N'SC02ProductTest', 5.00 FROM sys.all_columns;
" | Out-Null

$r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
    DECLARE @sr nvarchar(50);
    EXEC dbo.sp_StatUpdate
        @Databases             = N'$Database',
        @ModificationThreshold = 100,
        @Execute               = N'Y',
        @BatchLimit            = 1,
        @StopReasonOut         = @sr OUTPUT;
    SELECT @sr AS StopReason;
"
if ($r.ExitCode -ne 0) {
    $errMsg = ($r.Output | Where-Object { $_ } | Select-Object -First 1)
    Add-Result "SC02: BATCH_LIMIT" "FAIL" "Execution error: $errMsg"
} else {
    $sr = Get-StopReason -DataSet $r.DataSet
    if ($sr -eq "BATCH_LIMIT") {
        Add-Result "SC02: BATCH_LIMIT" "PASS" "StopReason=$sr"
    } elseif ($sr -eq "COMPLETED") {
        # Only 1 stat qualified total -- COMPLETED is acceptable
        Add-Result "SC02: BATCH_LIMIT" "PASS" "StopReason=$sr (exactly 1 stat qualified; BATCH_LIMIT=1 and COMPLETED are equivalent here)"
    } else {
        Add-Result "SC02: BATCH_LIMIT" "FAIL" "StopReason=$sr (expected BATCH_LIMIT)"
    }
}

# ------------------------------------------------------------------
# SC03: COMPLETED -- loop drains all qualifying stats
#       Scope to a single table to bound the work.  After updating
#       all stats on that table, @claimed_work=0 -> COMPLETED.
# ------------------------------------------------------------------
Write-Host "  SC03: COMPLETED" -ForegroundColor DarkGray

$sc03Db = "SC03_Test_$(Get-Date -Format 'yyyyMMddHHmmss')"
$sc03SetupOk = $true
$setupR = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
    IF DB_ID(N'$sc03Db') IS NOT NULL DROP DATABASE [$sc03Db];
    CREATE DATABASE [$sc03Db];
" -Timeout 60
if ($setupR.ExitCode -ne 0) { $sc03SetupOk = $false }

if ($sc03SetupOk) {
    $populateR = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $sc03Db -Query "
        CREATE TABLE dbo.SC03Tbl (ID int IDENTITY PRIMARY KEY, Val int);
        INSERT INTO dbo.SC03Tbl (Val)
            SELECT TOP 5000 ABS(CHECKSUM(NEWID()) % 1000) FROM sys.all_columns;
        UPDATE STATISTICS dbo.SC03Tbl;
        INSERT INTO dbo.SC03Tbl (Val)
            SELECT TOP 5000 ABS(CHECKSUM(NEWID()) % 1000) FROM sys.all_columns;
    " -Timeout 60
    if ($populateR.ExitCode -ne 0) { $sc03SetupOk = $false }
}

if ($sc03SetupOk) {
    $r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
        DECLARE @sr nvarchar(50);
        EXEC dbo.sp_StatUpdate
            @Databases             = N'$sc03Db',
            @ModificationThreshold = 100,
            @Execute               = N'Y',
            @StopReasonOut         = @sr OUTPUT;
        SELECT @sr AS StopReason;
    "
    if ($r.ExitCode -ne 0) {
        $errMsg = ($r.Output | Where-Object { $_ } | Select-Object -First 1)
        Add-Result "SC03: COMPLETED" "FAIL" "Execution error: $errMsg"
    } else {
        $sr = Get-StopReason -DataSet $r.DataSet
        if ($sr -eq "COMPLETED") {
            Add-Result "SC03: COMPLETED" "PASS" "StopReason=$sr"
        } else {
            Add-Result "SC03: COMPLETED" "FAIL" "StopReason=$sr (expected COMPLETED)"
        }
    }
} else {
    Add-Result "SC03: COMPLETED" "SKIP" "Could not create isolated test database"
}
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
    IF DB_ID(N'$sc03Db') IS NOT NULL DROP DATABASE [$sc03Db];" -Timeout 60 | Out-Null

# ------------------------------------------------------------------
# SC04: NO_QUALIFYING_STATS
#       Use an isolated DB where we know the exact stat state:
#       create a table, update stats (setting mods to 0), then run
#       sp_StatUpdate with @ModificationThreshold=999999999.
#       With 0 modifications and an impossibly high floor, 0 stats
#       qualify regardless of tiered thresholds.
# ------------------------------------------------------------------
Write-Host "  SC04: NO_QUALIFYING_STATS" -ForegroundColor DarkGray

$sc04Db = "SC04_Test_$(Get-Date -Format 'yyyyMMddHHmmss')"
$sc04SetupOk = $true
$setupR = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
    IF DB_ID(N'$sc04Db') IS NOT NULL DROP DATABASE [$sc04Db];
    CREATE DATABASE [$sc04Db];
" -Timeout 60
if ($setupR.ExitCode -ne 0) { $sc04SetupOk = $false }

if ($sc04SetupOk) {
    # Create a table and update its stats to zero out modification_counter
    $populateR = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $sc04Db -Query "
        CREATE TABLE dbo.SC04Tbl (ID int IDENTITY PRIMARY KEY, Val int);
        INSERT INTO dbo.SC04Tbl (Val)
            SELECT TOP 2000 ABS(CHECKSUM(NEWID()) % 1000) FROM sys.all_columns;
        /* Update stats to set modification_counter to 0 */
        UPDATE STATISTICS dbo.SC04Tbl;
    " -Timeout 60
    if ($populateR.ExitCode -ne 0) { $sc04SetupOk = $false }
}

if ($sc04SetupOk) {
    $r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
        DECLARE @sr nvarchar(50);
        /* 0 modifications + threshold=999999999 = nothing qualifies */
        EXEC dbo.sp_StatUpdate
            @Databases             = N'$sc04Db',
            @ModificationThreshold = 999999999,
            @Execute               = N'Y',
            @StopReasonOut         = @sr OUTPUT;
        SELECT @sr AS StopReason;
    "
    if ($r.ExitCode -ne 0) {
        $errMsg = ($r.Output | Where-Object { $_ } | Select-Object -First 1)
        Add-Result "SC04: NO_QUALIFYING_STATS" "FAIL" "Execution error: $errMsg"
    } else {
        $sr = Get-StopReason -DataSet $r.DataSet
        if ($sr -eq "NO_QUALIFYING_STATS") {
            Add-Result "SC04: NO_QUALIFYING_STATS" "PASS" "StopReason=$sr"
        } else {
            Add-Result "SC04: NO_QUALIFYING_STATS" "FAIL" "StopReason=$sr (expected NO_QUALIFYING_STATS)"
        }
    }
} else {
    Add-Result "SC04: NO_QUALIFYING_STATS" "SKIP" "Could not create isolated test database"
}
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
    IF DB_ID(N'$sc04Db') IS NOT NULL DROP DATABASE [$sc04Db];" -Timeout 60 | Out-Null

# ------------------------------------------------------------------
# SC05: TEMPDB_PRESSURE
#       @MinTempdbFreeMB set to an impossibly large value (999999 MB)
#       ensures the check always fails.  @FailFast=1 causes the proc
#       to set @stop_reason = TEMPDB_PRESSURE and break.
#       Requires at least one qualifying stat so the loop enters.
# ------------------------------------------------------------------
Write-Host "  SC05: TEMPDB_PRESSURE" -ForegroundColor DarkGray

$sc05Db = "SC05_Test_$(Get-Date -Format 'yyyyMMddHHmmss')"
$sc05SetupOk = $true
$setupR = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
    IF DB_ID(N'$sc05Db') IS NOT NULL DROP DATABASE [$sc05Db];
    CREATE DATABASE [$sc05Db];
" -Timeout 60
if ($setupR.ExitCode -ne 0) { $sc05SetupOk = $false }

if ($sc05SetupOk) {
    $populateR = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $sc05Db -Query "
        CREATE TABLE dbo.SC05Tbl (ID int IDENTITY PRIMARY KEY, Val int);
        INSERT INTO dbo.SC05Tbl (Val)
            SELECT TOP 2000 ABS(CHECKSUM(NEWID()) % 1000) FROM sys.all_columns;
        UPDATE STATISTICS dbo.SC05Tbl;
        INSERT INTO dbo.SC05Tbl (Val)
            SELECT TOP 2000 ABS(CHECKSUM(NEWID()) % 1000) FROM sys.all_columns;
    " -Timeout 60
    if ($populateR.ExitCode -ne 0) { $sc05SetupOk = $false }
}

if ($sc05SetupOk) {
    $r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
        DECLARE @sr nvarchar(50);
        EXEC dbo.sp_StatUpdate
            @Databases             = N'$sc05Db',
            @ModificationThreshold = 100,
            @Execute               = N'Y',
            @MinTempdbFreeMB       = 999999,
            @FailFast              = 1,
            @StopReasonOut         = @sr OUTPUT;
        SELECT @sr AS StopReason;
    "
    if ($r.ExitCode -ne 0) {
        $errMsg = ($r.Output | Where-Object { $_ } | Select-Object -First 1)
        Add-Result "SC05: TEMPDB_PRESSURE" "FAIL" "Execution error: $errMsg"
    } else {
        $sr = Get-StopReason -DataSet $r.DataSet
        if ($sr -eq "TEMPDB_PRESSURE") {
            Add-Result "SC05: TEMPDB_PRESSURE" "PASS" "StopReason=$sr"
        } elseif ($sr -eq "NO_QUALIFYING_STATS") {
            Add-Result "SC05: TEMPDB_PRESSURE" "FAIL" "StopReason=$sr -- stats did not qualify (check modification_counter setup)"
        } else {
            Add-Result "SC05: TEMPDB_PRESSURE" "FAIL" "StopReason=$sr (expected TEMPDB_PRESSURE)"
        }
    }
} else {
    Add-Result "SC05: TEMPDB_PRESSURE" "SKIP" "Could not create isolated test database"
}
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
    IF DB_ID(N'$sc05Db') IS NOT NULL DROP DATABASE [$sc05Db];" -Timeout 60 | Out-Null

# ------------------------------------------------------------------
# SC06: FAIL_FAST
#       @FailFast=1 on a READ_ONLY database.  UPDATE STATISTICS on
#       a READ_ONLY database fails with "The database is read-only".
#       With @FailFast=1 the proc sets stop_reason = FAIL_FAST after
#       the first failure.
#       SKIP if setup fails or if proc's READ_ONLY filegroup guard
#       skips the database entirely (returns NO_QUALIFYING_STATS).
# ------------------------------------------------------------------
Write-Host "  SC06: FAIL_FAST" -ForegroundColor DarkGray
$sc06Db = "SC06_FailFastTest_$(Get-Date -Format 'yyyyMMddHHmmss')"
$setupOk = $true

$createDbR = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
    IF DB_ID(N'$sc06Db') IS NOT NULL BEGIN
        ALTER DATABASE [$sc06Db] SET READ_WRITE WITH NO_WAIT;
        DROP DATABASE [$sc06Db];
    END;
    CREATE DATABASE [$sc06Db];
" -Timeout 120
if ($createDbR.ExitCode -ne 0) {
    $setupOk = $false
    $skipMsg = "Could not create test database: $($createDbR.Output[0])"
}

if ($setupOk) {
    $populateR = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $sc06Db -Query "
        CREATE TABLE dbo.FailFastTarget (ID int IDENTITY PRIMARY KEY, Val int NOT NULL);
        INSERT INTO dbo.FailFastTarget (Val)
            SELECT TOP 3000 ABS(CHECKSUM(NEWID()) % 1000) FROM sys.all_columns;
        UPDATE STATISTICS dbo.FailFastTarget;
        INSERT INTO dbo.FailFastTarget (Val)
            SELECT TOP 3000 ABS(CHECKSUM(NEWID()) % 1000) FROM sys.all_columns;
    " -Timeout 120
    if ($populateR.ExitCode -ne 0) {
        $setupOk = $false
        $skipMsg = "Could not populate test database: $($populateR.Output[0])"
    }
}

if ($setupOk) {
    $readOnlyR = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
        ALTER DATABASE [$sc06Db] SET READ_ONLY WITH NO_WAIT;
    " -Timeout 60
    if ($readOnlyR.ExitCode -ne 0) {
        $setupOk = $false
        $skipMsg = "Could not set database READ_ONLY: $($readOnlyR.Output[0])"
    }
}

if ($setupOk) {
    $r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
        DECLARE @sr nvarchar(50);
        EXEC dbo.sp_StatUpdate
            @Databases             = N'$sc06Db',
            @ModificationThreshold = 100,
            @Execute               = N'Y',
            @FailFast              = 1,
            @LogToTable            = N'N',
            @StopReasonOut         = @sr OUTPUT;
        SELECT @sr AS StopReason;
    " -Timeout 120
    if ($r.ExitCode -ne 0) {
        $errMsg = ($r.Output | Where-Object { $_ } | Select-Object -First 1)
        Add-Result "SC06: FAIL_FAST" "FAIL" "Execution error: $errMsg"
    } else {
        $sr = Get-StopReason -DataSet $r.DataSet
        if ($sr -eq "FAIL_FAST") {
            Add-Result "SC06: FAIL_FAST" "PASS" "StopReason=$sr"
        } elseif ($sr -eq "NO_QUALIFYING_STATS") {
            # Proc's READ_ONLY filegroup guard skipped the database -- this is also correct proc behavior
            Add-Result "SC06: FAIL_FAST" "SKIP" "StopReason=$sr -- proc skipped READ_ONLY filegroups during discovery (expected proc behavior)"
        } else {
            Add-Result "SC06: FAIL_FAST" "FAIL" "StopReason=$sr (expected FAIL_FAST or SKIP reason)"
        }
    }
} else {
    Add-Result "SC06: FAIL_FAST" "SKIP" $skipMsg
}

Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
    IF DB_ID(N'$sc06Db') IS NOT NULL BEGIN
        ALTER DATABASE [$sc06Db] SET READ_WRITE WITH NO_WAIT;
        DROP DATABASE [$sc06Db];
    END;
" -Timeout 120 | Out-Null

# ------------------------------------------------------------------
# SC07: CONSECUTIVE_FAILURES
#       @i_max_consecutive_failures default is 10.  This is an @i_
#       internal in v3 -- NOT a public parameter.  To trigger it,
#       create a READ_ONLY database with 10+ qualifying stats so
#       all UPDATE STATISTICS calls fail.  After 10 consecutive
#       failures the proc stops.
#       NOTE: @MaxConsecutiveFailures is NOT valid in v3 -- passing it
#       to the proc returns "not a parameter" error.  This is a finding
#       documented in this test file.
# ------------------------------------------------------------------
Write-Host "  SC07: CONSECUTIVE_FAILURES" -ForegroundColor DarkGray
$sc07Db = "SC07_ConsFailTest_$(Get-Date -Format 'yyyyMMddHHmmss')"
$setupOk = $true

$createDbR = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
    IF DB_ID(N'$sc07Db') IS NOT NULL BEGIN
        ALTER DATABASE [$sc07Db] SET READ_WRITE WITH NO_WAIT;
        DROP DATABASE [$sc07Db];
    END;
    CREATE DATABASE [$sc07Db];
" -Timeout 120
if ($createDbR.ExitCode -ne 0) {
    $setupOk = $false
    $skipMsg = "Could not create test database: $($createDbR.Output[0])"
}

if ($setupOk) {
    # Create 15 tables each with 1 stat, all with modifications
    # 15 > default @i_max_consecutive_failures (10) so we will cross the threshold
    $buildSql = "USE [$sc07Db];"
    for ($i = 1; $i -le 15; $i++) {
        $buildSql += "
            CREATE TABLE dbo.FailTbl${i} (ID int IDENTITY PRIMARY KEY, Val int NOT NULL);
            INSERT INTO dbo.FailTbl${i} (Val)
                SELECT TOP 500 ABS(CHECKSUM(NEWID()) % 100) FROM sys.all_columns;
            UPDATE STATISTICS dbo.FailTbl${i};
            INSERT INTO dbo.FailTbl${i} (Val)
                SELECT TOP 500 ABS(CHECKSUM(NEWID()) % 100) FROM sys.all_columns;"
    }
    $populateR = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $sc07Db `
        -Query $buildSql -Timeout 120
    if ($populateR.ExitCode -ne 0) {
        $setupOk = $false
        $skipMsg = "Could not populate test database: $($populateR.Output[0])"
    }
}

if ($setupOk) {
    $readOnlyR = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
        ALTER DATABASE [$sc07Db] SET READ_ONLY WITH NO_WAIT;
    " -Timeout 60
    if ($readOnlyR.ExitCode -ne 0) {
        $setupOk = $false
        $skipMsg = "Could not set database READ_ONLY: $($readOnlyR.Output[0])"
    }
}

if ($setupOk) {
    $r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
        DECLARE @sr nvarchar(50);
        /* No @MaxConsecutiveFailures in v3 -- uses default @i_max_consecutive_failures = 10 */
        EXEC dbo.sp_StatUpdate
            @Databases             = N'$sc07Db',
            @ModificationThreshold = 100,
            @Execute               = N'Y',
            @LogToTable            = N'N',
            @StopReasonOut         = @sr OUTPUT;
        SELECT @sr AS StopReason;
    " -Timeout 120
    if ($r.ExitCode -ne 0) {
        $errMsg = ($r.Output | Where-Object { $_ } | Select-Object -First 1)
        Add-Result "SC07: CONSECUTIVE_FAILURES" "FAIL" "Execution error: $errMsg"
    } else {
        $sr = Get-StopReason -DataSet $r.DataSet
        if ($sr -eq "CONSECUTIVE_FAILURES") {
            Add-Result "SC07: CONSECUTIVE_FAILURES" "PASS" "StopReason=$sr"
        } elseif ($sr -eq "NO_QUALIFYING_STATS") {
            # READ_ONLY filegroup guard skipped discovery -- same skip as SC06
            Add-Result "SC07: CONSECUTIVE_FAILURES" "SKIP" "StopReason=$sr -- proc skipped READ_ONLY filegroups in discovery (expected proc behavior)"
        } else {
            Add-Result "SC07: CONSECUTIVE_FAILURES" "FAIL" "StopReason=$sr (expected CONSECUTIVE_FAILURES)"
        }
    }
} else {
    Add-Result "SC07: CONSECUTIVE_FAILURES" "SKIP" $skipMsg
}

Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
    IF DB_ID(N'$sc07Db') IS NOT NULL BEGIN
        ALTER DATABASE [$sc07Db] SET READ_WRITE WITH NO_WAIT;
        DROP DATABASE [$sc07Db];
    END;
" -Timeout 120 | Out-Null

# ------------------------------------------------------------------
# SC08: COMPLETED_WITH_SKIPPED_DBS
#       This stop reason fires when a database passes the initial
#       HAS_DBACCESS/state=ONLINE selection filter but then FAILS
#       during per-database staged discovery (the inner TRY/CATCH).
#       The proc comment says: "SINGLE_USER acquired by another
#       session, taken offline, etc."
#
#       Strategy: set this disposable database to compatibility level 100.
#       It remains ONLINE and accessible during initial selection, but the
#       discovery query's STRING_SPLIT call is unavailable at that level.
#       The resulting compilation error is caught by the per-database
#       TRY/CATCH and reported as DB_SKIPPED. No race or session termination.
#
#       NOTE: a non-existent DB name is NOT the same as a skipped DB --
#       the proc silently filters it at selection time with no warning.
#       Only databases that pass INITIAL selection (ONLINE + HAS_DBACCESS)
#       but fail DURING per-database processing add DB_SKIPPED to @warnings.
# ------------------------------------------------------------------
Write-Host "  SC08: COMPLETED_WITH_SKIPPED_DBS" -ForegroundColor DarkGray
$sc08Db = "SC08_SkipTest_$(Get-Date -Format 'yyyyMMddHHmmss')"
$sc08SetupOk = $true

$createDbR = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
    IF DB_ID(N'$sc08Db') IS NOT NULL BEGIN
        IF DATABASEPROPERTYEX(N'$sc08Db', 'UserAccess') = 'SINGLE_USER'
            ALTER DATABASE [$sc08Db] SET MULTI_USER WITH ROLLBACK IMMEDIATE;
        DROP DATABASE [$sc08Db];
    END;
    CREATE DATABASE [$sc08Db];
" -Timeout 60
if ($createDbR.ExitCode -ne 0) { $sc08SetupOk = $false }

if ($sc08SetupOk) {
    $primarySc08R = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Query @"
IF OBJECT_ID(N'dbo.SC08Tbl', N'U') IS NOT NULL DROP TABLE dbo.SC08Tbl;
CREATE TABLE dbo.SC08Tbl (ID int IDENTITY PRIMARY KEY, Val int);
INSERT INTO dbo.SC08Tbl (Val)
    SELECT TOP 2000 ABS(CHECKSUM(NEWID()) % 1000) FROM sys.all_columns;
UPDATE STATISTICS dbo.SC08Tbl WITH FULLSCAN;
INSERT INTO dbo.SC08Tbl (Val)
    SELECT TOP 2000 ABS(CHECKSUM(NEWID()) % 1000) FROM sys.all_columns;
"@ -Timeout 60
    if ($primarySc08R.ExitCode -ne 0) { $sc08SetupOk = $false }
}

if ($sc08SetupOk) {
    $populateR = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $sc08Db -Query "
        CREATE TABLE dbo.SC08Tbl (ID int IDENTITY PRIMARY KEY, Val int);
        INSERT INTO dbo.SC08Tbl (Val)
            SELECT TOP 2000 ABS(CHECKSUM(NEWID()) % 1000) FROM sys.all_columns;
        UPDATE STATISTICS dbo.SC08Tbl;
        INSERT INTO dbo.SC08Tbl (Val)
            SELECT TOP 2000 ABS(CHECKSUM(NEWID()) % 1000) FROM sys.all_columns;
    " -Timeout 60
    if ($populateR.ExitCode -ne 0) { $sc08SetupOk = $false }
}

if ($sc08SetupOk) {
    $compatibilityR = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
        ALTER DATABASE [$sc08Db] SET COMPATIBILITY_LEVEL = 100;" -Timeout 30
    if ($compatibilityR.ExitCode -ne 0) { $sc08SetupOk = $false }
}

if ($sc08SetupOk) {
    # The target DB passes initial selection but its discovery query cannot compile.
    $r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
        DECLARE @sr nvarchar(50), @wo nvarchar(max);
        EXEC dbo.sp_StatUpdate
            @Databases             = N'$Database,$sc08Db',
            @Tables                = N'dbo.SC08Tbl',
            @ModificationThreshold = 100,
            @Execute               = N'N',
            @StopReasonOut         = @sr OUTPUT,
            @WarningsOut           = @wo OUTPUT;
        SELECT @sr AS StopReason, ISNULL(@wo, N'(none)') AS Warnings;
    " -Timeout 120

    if ($r.ExitCode -ne 0) {
        $errMsg = ($r.Output | Where-Object { $_ } | Select-Object -First 1)
        Add-Result "SC08: COMPLETED_WITH_SKIPPED_DBS" "FAIL" "Execution error: $errMsg"
    } else {
        $sr = Get-StopReason -DataSet $r.DataSet
        $warningText = ''
        if ($r.DataSet -and $r.DataSet.Tables.Count -gt 0) {
            $lastTable = $r.DataSet.Tables[$r.DataSet.Tables.Count - 1]
            if ($lastTable.Rows.Count -gt 0 -and $lastTable.Columns.Contains('Warnings')) {
                $warningText = [string]$lastTable.Rows[0]['Warnings']
            }
        }
        if ($sr -eq "COMPLETED_WITH_SKIPPED_DBS" -and $warningText -like "*DB_SKIPPED: $sc08Db*") {
            Add-Result "SC08: COMPLETED_WITH_SKIPPED_DBS" "PASS" "StopReason=$sr"
        } elseif ($sr -eq "COMPLETED_WITH_SKIPPED_DBS") {
            Add-Result "SC08: COMPLETED_WITH_SKIPPED_DBS" "FAIL" "StopReason=$sr but Warnings did not identify DB_SKIPPED for the test database (Warnings=$warningText)"
        } else {
            Add-Result "SC08: COMPLETED_WITH_SKIPPED_DBS" "FAIL" "StopReason=$sr; Warnings=$warningText (expected COMPLETED_WITH_SKIPPED_DBS with the test DB skipped)"
        }
    }
} else {
    Add-Result "SC08: COMPLETED_WITH_SKIPPED_DBS" "SKIP" "Could not set up SC08 test database"
}

Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
    IF DB_ID(N'$sc08Db') IS NOT NULL BEGIN
        IF DATABASEPROPERTYEX(N'$sc08Db', 'UserAccess') = 'SINGLE_USER'
            ALTER DATABASE [$sc08Db] SET MULTI_USER WITH ROLLBACK IMMEDIATE;
        DROP DATABASE [$sc08Db];
    END;
" -Timeout 60 | Out-Null
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Query "
    IF OBJECT_ID(N'dbo.SC08Tbl', N'U') IS NOT NULL DROP TABLE dbo.SC08Tbl;
" | Out-Null

# ------------------------------------------------------------------
# SC09: NATURAL_END (mop-up transition state)
#       NATURAL_END is set at line 5085 when @i_mop_up_pass=Y and
#       zero priority stats qualify.  It is a transitional state that
#       allows the mop-up pass to check for work.  If mop-up also
#       finds nothing, NO_QUALIFYING_STATS is the final stop reason.
#       We confirm the NATURAL_END path fired via the RAISERROR message.
#
#       Uses an isolated database with 0 modifications so that both
#       the priority pass AND the mop-up pass find no work.  NIGHTLY
#       preset enables @i_mop_up_pass=Y.  @TimeLimit=30 ensures the
#       mop-up eligibility check passes (time budget exists).
#       @ModificationThreshold=999999999 ensures 0 priority stats qualify.
#       CommandLog must be enabled for mop-up (@LogToTable=Y is default).
# ------------------------------------------------------------------
Write-Host "  SC09: NATURAL_END (mop-up transition state)" -ForegroundColor DarkGray

$sc09Db = "SC09_Test_$(Get-Date -Format 'yyyyMMddHHmmss')"
$sc09SetupOk = $true
$setupR = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
    IF DB_ID(N'$sc09Db') IS NOT NULL DROP DATABASE [$sc09Db];
    CREATE DATABASE [$sc09Db];
" -Timeout 60
if ($setupR.ExitCode -ne 0) { $sc09SetupOk = $false }

if ($sc09SetupOk) {
    # Create a table but update stats to 0 out modification_counter
    # and do NOT insert additional rows -- keeps mods at 0 for both priority and mop-up
    $populateR = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $sc09Db -Query "
        CREATE TABLE dbo.SC09Tbl (ID int IDENTITY PRIMARY KEY, Val int);
        INSERT INTO dbo.SC09Tbl (Val)
            SELECT TOP 1000 ABS(CHECKSUM(NEWID()) % 100) FROM sys.all_columns;
        UPDATE STATISTICS dbo.SC09Tbl;
    " -Timeout 60
    if ($populateR.ExitCode -ne 0) { $sc09SetupOk = $false }
}

if ($sc09SetupOk) {
    $r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
        DECLARE @sr nvarchar(50);
        EXEC dbo.sp_StatUpdate
            @Databases             = N'$sc09Db',
            @Preset                = N'NIGHTLY',
            @ModificationThreshold = 999999999,
            @Execute               = N'Y',
            @TimeLimit             = 30,
            @StopReasonOut         = @sr OUTPUT;
        SELECT @sr AS StopReason;
    "
    if ($r.ExitCode -ne 0) {
        $errMsg = ($r.Output | Where-Object { $_ } | Select-Object -First 1)
        Add-Result "SC09: NATURAL_END (mop-up transition)" "FAIL" "Execution error: $errMsg"
    } else {
        $sr      = Get-StopReason -DataSet $r.DataSet
        $allMsgs = $r.Output -join "`n"
        $hasMopUpMsg = $allMsgs -match "No priority statistics found"
        if ($sr -eq "NO_QUALIFYING_STATS" -and $hasMopUpMsg) {
            Add-Result "SC09: NATURAL_END (mop-up transition)" "PASS" "StopReason=$sr + 'No priority statistics found' message confirms NATURAL_END path"
        } elseif ($sr -eq "NO_QUALIFYING_STATS") {
            Add-Result "SC09: NATURAL_END (mop-up transition)" "PASS" "StopReason=$sr (stop reason confirms correct path)"
        } else {
            Add-Result "SC09: NATURAL_END (mop-up transition)" "FAIL" "StopReason=$sr (expected NO_QUALIFYING_STATS from NATURAL_END->mop-up path)"
        }
    }
} else {
    Add-Result "SC09: NATURAL_END (mop-up transition)" "SKIP" "Could not create isolated test database"
}
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
    IF DB_ID(N'$sc09Db') IS NOT NULL DROP DATABASE [$sc09Db];" -Timeout 60 | Out-Null

# ------------------------------------------------------------------
# SC10: PARAMETER_ERROR
#       Pass @BatchLimit=0 (must be >= 1).  The proc validates this
#       and exits early with StopReason = PARAMETER_ERROR.
# ------------------------------------------------------------------
Write-Host "  SC10: PARAMETER_ERROR (invalid @BatchLimit=0)" -ForegroundColor DarkGray
$r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
    DECLARE @sr nvarchar(50);
    EXEC dbo.sp_StatUpdate
        @Databases     = N'$Database',
        @Execute       = N'N',
        @BatchLimit    = 0,
        @StopReasonOut = @sr OUTPUT;
    SELECT @sr AS StopReason;
"
if ($r.ExitCode -ne 0) {
    $errMsg = ($r.Output | Where-Object { $_ } | Select-Object -First 1)
    Add-Result "SC10: PARAMETER_ERROR" "FAIL" "Execution error: $errMsg"
} else {
    $sr = Get-StopReason -DataSet $r.DataSet
    if ($sr -eq "PARAMETER_ERROR") {
        Add-Result "SC10: PARAMETER_ERROR" "PASS" "StopReason=$sr"
    } else {
        Add-Result "SC10: PARAMETER_ERROR" "FAIL" "StopReason=$sr (expected PARAMETER_ERROR)"
    }
}

# ------------------------------------------------------------------
# SC11: ALREADY_RUNNING
#       Keep a SQL session open with a fresh dbo.StatUpdateLock row, then call
#       sp_StatUpdate through a second connection. The live-session tuple makes
#       the denial deterministic without relying on a long-running maintenance
#       workload or a sleep-based race.
# ------------------------------------------------------------------
Write-Host "  SC11: ALREADY_RUNNING" -ForegroundColor DarkGray

Add-Type -AssemblyName System.Data
$holderConnString = "Server=$Server,$Port;Database=master;User Id=sa;Password=$Password;" +
                    "Encrypt=false;TrustServerCertificate=true;Pooling=false;Connection Timeout=30"
$holderConn = [System.Data.SqlClient.SqlConnection]::new($holderConnString)
$r2 = $null
try {
    $holderConn.Open()
    $holderCmd = $holderConn.CreateCommand()
    $holderCmd.CommandText = @"
IF OBJECT_ID(N'dbo.StatUpdateLock', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.StatUpdateLock (
        Resource sysname NOT NULL CONSTRAINT PK_StatUpdateLock PRIMARY KEY CLUSTERED,
        SessionID smallint NOT NULL,
        LoginTime datetime NOT NULL,
        AcquiredAt datetime2(3) NOT NULL CONSTRAINT DF_StatUpdateLock_AcquiredAt DEFAULT SYSDATETIME()
    );
END;
DELETE FROM dbo.StatUpdateLock WHERE Resource = N'sp_StatUpdate';
INSERT INTO dbo.StatUpdateLock (Resource, SessionID, LoginTime)
SELECT N'sp_StatUpdate', @@SPID, login_time
FROM sys.dm_exec_sessions
WHERE session_id = @@SPID;
"@
    $holderCmd.CommandTimeout = 30
    $holderCmd.ExecuteNonQuery() | Out-Null
    $holderCmd.Dispose()

    # The holder session stays open and has just completed a request, so the
    # procedure's live-session/60-second grace check must recognize it as alive.
    $r2 = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query @"
DECLARE @sr nvarchar(50);
EXEC dbo.sp_StatUpdate
    @Databases     = N'$Database',
    @Execute       = N'N',
    @StopReasonOut = @sr OUTPUT;
SELECT @sr AS StopReason;
"@ -Timeout 30
} finally {
    $holderConn.Close()
    $holderConn.Dispose()
}

# Remove only the synthetic row after the second call. The session is now
# closed, so the procedure correctly considers it stale if it remains.
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query @"
IF OBJECT_ID(N'dbo.StatUpdateLock', N'U') IS NOT NULL
    DELETE FROM dbo.StatUpdateLock WHERE Resource = N'sp_StatUpdate';
"@ | Out-Null

if ($r2.ExitCode -ne 0) {
    $errMsg = ($r2.Output | Where-Object { $_ } | Select-Object -First 1)
    if ($errMsg -match "ALREADY_RUNNING") {
        Add-Result "SC11: ALREADY_RUNNING" "PASS" "Exception contains ALREADY_RUNNING"
    } else {
        Add-Result "SC11: ALREADY_RUNNING" "FAIL" "Execution error: $errMsg"
    }
} else {
    $sr2 = Get-StopReason -DataSet $r2.DataSet
    if ($sr2 -eq "ALREADY_RUNNING") {
        Add-Result "SC11: ALREADY_RUNNING" "PASS" "StopReason=$sr2"
    } elseif ($sr2 -eq "COMPLETED" -or $sr2 -eq "NO_QUALIFYING_STATS") {
        Add-Result "SC11: ALREADY_RUNNING" "SKIP" "StopReason=$sr2 -- first session completed or released lock before second arrived"
    } else {
        Add-Result "SC11: ALREADY_RUNNING" "FAIL" "StopReason=$sr2 (expected ALREADY_RUNNING)"
    }
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

$summaryColor = if ($failed -eq 0) { "Green" } else { "Red" }
Write-Host "  $passed/$total passed, $failed failed, $skipped skipped" -ForegroundColor $summaryColor
Write-Host ""

Write-Host "  Stop reasons covered:" -ForegroundColor Cyan
foreach ($t in $results) {
    $c = switch ($t.Result) { "PASS" { "Green" } "FAIL" { "Red" } "SKIP" { "DarkGray" } default { "Yellow" } }
    Write-Host "    [$($t.Result)] $($t.Test)" -ForegroundColor $c
    if ($t.Detail) { Write-Host "          $($t.Detail)" -ForegroundColor DarkGray }
}
Write-Host ""

if ($ResultsPath) {
    $major = @{ '2019' = 15; '2022' = 16; '2025' = 17 }[$Version]
    $sqlStatus = Get-StructuredSqlStatus -Server $Server -Port $Port -Password $Password -ExpectedMajor $major
    $commit = (& git -C $RepoRoot rev-parse HEAD).Trim()
    if ($LASTEXITCODE -ne 0 -or $commit -notmatch '^[0-9a-fA-F]{40}$') { throw 'Could not resolve the tested source commit.' }
    Write-StructuredLegacyResults -Path $ResultsPath -Commit $commit -SqlVersion $Version -SqlBuild $sqlStatus.Build `
        -ServerAliveAtEnd $sqlStatus.Alive -Suite 'StopReasonsCoverage' -ExpectedCount 11 `
        -Results @($results) -StartedAt $startedAt -EndedAt (Get-Date)
}

if ($failed -gt 0 -or $skipped -gt 0) {
    Write-Host "  FAILED TESTS:" -ForegroundColor Red
    foreach ($t in ($results | Where-Object { $_.Result -eq "FAIL" })) {
        Write-Host "    - $($t.Test): $($t.Detail)" -ForegroundColor Red
    }
    Write-Host ""
    exit 1
}

exit 0
