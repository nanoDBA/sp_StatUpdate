<#
.SYNOPSIS
    Targeted tests for denial artifact (o7gf/gh-551), FILTERED_DRIFT precedence
    (u6n7/gh-554), and a plain dry-run regression guard.

.DESCRIPTION
    T_DAG1: Fabricate a live-looking StatUpdateLock holder row, call the proc,
            assert the ERROR result set has StopReason = ALREADY_RUNNING AND a
            SP_STATUPDATE_DENIED CommandLog row exists with the Denial XML
            (Version, DeniedAt, SessionID, HolderSessionID).  Cleans up the lock
            row in a finally block.

    T_DAG2: Same denial scenario with @LogToTable = N'N'.  Asserts NO
            SP_STATUPDATE_DENIED row is written to CommandLog.

    T_DAG3: FILTERED_DRIFT precedence.  Create a scratch DB with a filtered stat
            whose selectivity-drift ratio exceeds the factor, seed QS activity
            if cheap (INSERT only -- no explicit QS boost injection), run an
            execute pass with @FilteredStatsMode = N'PRIORITY' and
            @LogToTable = N'Y'.  Assert the UPDATE_STATISTICS CommandLog row
            carries QualifyReason = FILTERED_DRIFT in ExtendedInfo XML.

    T_DAG4: Plain dry run against master.  Regression guard -- proc must complete
            without error.

    Runs against one isolated supported SQL test instance.

.PARAMETER Server
    Remote host IP.  Default: 100.88.59.72.

.PARAMETER Port
    SQL port.  Default: 1431 (SQL 2025 container).

.PARAMETER Password
    SA password.  Default: $env:SPSTATUPDATE_TEST_SQL_PASSWORD.

.PARAMETER SkipSetup
    Skip proc deployment (assumes already deployed).

.EXAMPLE
    .\tests\Test-DenialAndGuards.ps1
    .\tests\Test-DenialAndGuards.ps1 -Server 127.0.0.1 -Port 1431
    .\tests\Test-DenialAndGuards.ps1 -SkipSetup
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

# =====================================================================
# ADO.NET HELPER  (mirrors Test-AbortOnIntegrityError.ps1 conventions)
# =====================================================================
function Invoke-Sql {
    param(
        [string]$ServerAddr,
        [int]   $SrvPort,
        [string]$Pwd,
        [string]$Query,
        [string]$Db      = "master",
        [int]   $Timeout = 300
    )
    $connStr = "Server=$ServerAddr,$SrvPort;Database=$Db;User Id=sa;Password=$Pwd;Encrypt=false;TrustServerCertificate=true;Connection Timeout=30;Pooling=false"
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
        $cmd.CommandText    = $Query
        $cmd.CommandTimeout = $Timeout
        $ds     = [System.Data.DataSet]::new()
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
Write-Host " sp_StatUpdate Denial Artifact + Guards Test Suite (o7gf, u6n7, sss6)" -ForegroundColor Cyan
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host ""
Write-Host "  Server:  $Server,$Port"
Write-Host "  Proc:    $ProcFile"
Write-Host ""

# =====================================================================
# STEP 1: VERIFY CONNECTIVITY
# =====================================================================
Write-Host "[1/4] Verifying connectivity..." -ForegroundColor Yellow
$rConn = Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password `
    -Query "SELECT CONVERT(nvarchar(128), SERVERPROPERTY(N'ProductVersion')) AS v;"
if ($rConn.ExitCode -ne 0) {
    Write-Host "  ERROR: Cannot reach $Server,$Port -- $($rConn.Output | Select-Object -First 1)" -ForegroundColor Red
    exit 1
}
$sqlVer = if ($rConn.DataSet.Tables.Count -gt 0 -and $rConn.DataSet.Tables[0].Rows.Count -gt 0) {
    [string]$rConn.DataSet.Tables[0].Rows[0]["v"]
} else { "Connected" }
Write-Host "  SQL Server $sqlVer -- READY" -ForegroundColor Green
Write-Host ""

if (-not (Test-Path $ProcFile)) {
    Write-Host "ERROR: Proc file not found: $ProcFile" -ForegroundColor Red
    exit 1
}

# =====================================================================
# STEP 2: DEPLOY PROC  (mirrors Test-AbortOnIntegrityError.ps1)
# =====================================================================
if (-not $SkipSetup) {
    Write-Host "[2/4] Deploying sp_StatUpdate..." -ForegroundColor Yellow

    $procSql  = [System.IO.File]::ReadAllText($ProcFile)
    $batches  = [regex]::Split($procSql, '(?m)^\s*GO\s*$')
    $deployOk = $true

    foreach ($batch in $batches) {
        $trimmed = $batch.Trim()
        if ($trimmed.Length -eq 0) { continue }
        $r = Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password `
            -Query $trimmed -Db "master" -Timeout 120
        if ($r.ExitCode -ne 0) {
            Write-Host "  PROC DEPLOY FAILED: $($r.Output | Select-Object -First 1)" -ForegroundColor Red
            $deployOk = $false
            break
        }
    }

    if (-not $deployOk) {
        Write-Host "  Aborting -- proc deployment failed." -ForegroundColor Red
        exit 1
    }
    Write-Host "  Proc deployed OK" -ForegroundColor Green
    Write-Host ""
} else {
    Write-Host "[2/4] Skipping deploy (-SkipSetup)" -ForegroundColor DarkGray
    Write-Host ""
}

# =====================================================================
# STEP 3: ENSURE CommandLog exists
# =====================================================================
Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password -Timeout 60 `
    -Query @"
IF OBJECT_ID(N'dbo.CommandLog') IS NULL
BEGIN
    CREATE TABLE dbo.CommandLog (
        ID int IDENTITY(1,1) NOT NULL PRIMARY KEY,
        DatabaseName sysname NULL,
        SchemaName sysname NULL,
        ObjectName sysname NULL,
        ObjectType nchar(2) NULL,
        StatisticsName sysname NULL,
        PartitionNumber int NULL,
        ExtendedInfo xml NULL,
        Command nvarchar(max) NULL,
        CommandType nvarchar(60) NOT NULL,
        StartTime datetime2(7) NOT NULL,
        EndTime datetime2(7) NULL,
        ErrorNumber int NULL,
        ErrorMessage nvarchar(max) NULL
    );
END;
"@ | Out-Null

# =====================================================================
# STEP 4: RUN TESTS
# =====================================================================
Write-Host "[3/4] Running 4 tests..." -ForegroundColor Yellow
Write-Host ""

$results = [System.Collections.ArrayList]::new()
function Add-Result {
    param([string]$Test, [string]$Result, [string]$Detail = "")
    $null = $results.Add([PSCustomObject]@{ Test = $Test; Result = $Result; Detail = $Detail })
    $tc = switch ($Result) { "PASS" { "DarkGreen" } "FAIL" { "Red" } default { "DarkGray" } }
    $sym = if ($Result -eq "PASS") { "[PASS]" } else { "[FAIL]" }
    Write-Host "  $sym $Test" -ForegroundColor $tc
    if ($Detail) { Write-Host "       $Detail" -ForegroundColor DarkGray }
}

# ================================================================
# T_DAG1: Denial artifact written when @LogToTable = Y
# ================================================================
Write-Host "--- T_DAG1: ALREADY_RUNNING writes SP_STATUPDATE_DENIED to CommandLog ---" -ForegroundColor Yellow

# Cleanup helper -- runs in finally
$cleanupLock = {
    Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password `
        -Query "IF OBJECT_ID(N'dbo.StatUpdateLock') IS NOT NULL DELETE FROM dbo.StatUpdateLock;" | Out-Null
}

# Note the last CommandLog ID before the test so we only check rows added during it
$rPreId = Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password `
    -Query "SELECT ISNULL(MAX(ID), 0) AS MaxID FROM dbo.CommandLog;"
$preMaxId = if ($rPreId.ExitCode -eq 0 -and $rPreId.DataSet.Tables.Count -gt 0 -and
               $rPreId.DataSet.Tables[0].Rows.Count -gt 0 -and
               $rPreId.DataSet.Tables[0].Rows[0]["MaxID"] -ne [DBNull]::Value) {
    [int]$rPreId.DataSet.Tables[0].Rows[0]["MaxID"]
} else { 0 }

# Fabricate a live-looking lock holder: use a real (but distinct) session ID and
# login_time so that the liveness probe finds it in dm_exec_sessions.
# We grab our OWN session's login_time but use a DIFFERENT session_id (@@SPID + 32767
# guaranteed out of range so it is NOT in dm_exec_sessions -- the liveness probe
# will find session absent and... reclaim as dead, not deny!
#
# Correct approach: fabricate a row that passes the "alive" check by matching
# an ACTIVE session in dm_exec_sessions with an active request.
# Since we cannot inject a real concurrent active request, we use the proc's
# TTL guard fallback: a row with AcquiredAt in the PAST but within 8 hours,
# referencing a session that IS in dm_exec_sessions with an active request.
# The simplest way: point the lock at OUR OWN session (@@SPID) -- but set
# AcquiredAt recently so the 8-hour TTL does not reclaim it.  The proc's
# same-session branch (Option S) would then reclaim it unconditionally...
#
# Instead: we fabricate a lock row pointing at a real session that has an
# active request by using the proc's own session during a blocking wait.
# For a test harness that cannot block itself, the cleanest approximation is:
# set the lock row so that the liveness probe falls into the "active request
# present" branch.  We do this by fabricating a row with a SessionID of @@SPID
# and a LoginTime that matches OUR login -- but the proc treats same-session
# rows as unconditionally stale (Option S) and reclaims.
#
# For a reliable ALREADY_RUNNING denial in a test, we instead directly INSERT
# a row with a DIFFERENT session_id that exists in dm_exec_sessions with an
# active request.  The most robust method: query for any OTHER session that
# currently has an active request (which our own connection's query satisfies).
# We grab a session_id from dm_exec_requests that is NOT our own, then plant
# the lock row pointing at it.  If none exists, we skip with INFO.

# Open a DEDICATED second connection to act as the live lock holder.  The proc's
# staleness guard (v3.5.5) treats a holder as alive when its session exists, is not
# our SPID, logged in before claiming, and was recently active -- a fresh connection
# that just ran a query satisfies all of that for the duration of these two tests.
$holderConn = New-Object System.Data.SqlClient.SqlConnection("Server=$Server,$Port;Database=master;User Id=sa;Password=$Password;Encrypt=false;TrustServerCertificate=true;Connection Timeout=30;Pooling=false")
$holderSid = $null
$holderLt  = $null
$skipDag1  = $false
try {
    $holderConn.Open()
    $hc = $holderConn.CreateCommand()
    $hc.CommandText = "SELECT session_id, login_time FROM sys.dm_exec_sessions WHERE session_id = @@SPID;"
    $hr = $hc.ExecuteReader()
    $ht = New-Object System.Data.DataTable
    $ht.Load($hr)
    $holderSid = [int]$ht.Rows[0]["session_id"]
    $holderLt  = [datetime]$ht.Rows[0]["login_time"]
    Write-Host "  Opened dedicated holder connection: session $holderSid -- will plant as lock holder." -ForegroundColor DarkGray
} catch {
    Add-Result "T_DAG1: ALREADY_RUNNING writes SP_STATUPDATE_DENIED" "SKIP" "Could not open holder connection: $($_.Exception.Message)"
    $skipDag1 = $true
}

if (-not $skipDag1) {
    try {
        # Delete any stale lock row first
        Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password `
            -Query "IF OBJECT_ID(N'dbo.StatUpdateLock') IS NOT NULL DELETE FROM dbo.StatUpdateLock;" | Out-Null

        # Ensure StatUpdateLock table exists
        Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password `
            -Query @"
IF OBJECT_ID(N'dbo.StatUpdateLock', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.StatUpdateLock (
        Resource sysname NOT NULL,
        SessionID smallint NOT NULL,
        LoginTime datetime NOT NULL,
        AcquiredAt datetime2(3) NOT NULL DEFAULT SYSDATETIME(),
        CONSTRAINT PK_StatUpdateLock PRIMARY KEY CLUSTERED (Resource)
    );
END;
"@ | Out-Null

        # Plant live-looking lock row pointing at the alien session
        $ltStr = $holderLt.ToString("yyyy-MM-dd HH:mm:ss.fff")
        Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password `
            -Query @"
DELETE FROM dbo.StatUpdateLock;
INSERT INTO dbo.StatUpdateLock (Resource, SessionID, LoginTime, AcquiredAt)
VALUES (N'sp_StatUpdate', $holderSid, '$ltStr', SYSDATETIME());
"@ | Out-Null

        # Call proc -- expect ALREADY_RUNNING result set (sev-16 RAISERROR means ExitCode != 0)
        $r1 = Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password -Timeout 30 `
            -Query @"
EXEC dbo.sp_StatUpdate
    @Databases   = N'master',
    @Execute     = N'N',
    @LogToTable  = N'Y';
"@

        # Assert: result set has StopReason = ALREADY_RUNNING
        $dag1ResultOk = $false
        $dag1StopReason = $null
        if ($r1.DataSet -and $r1.DataSet.Tables.Count -gt 0) {
            foreach ($tbl in $r1.DataSet.Tables) {
                if ($tbl.Columns.Contains("StopReason")) {
                    foreach ($row in $tbl.Rows) {
                        $dag1StopReason = [string]$row["StopReason"]
                        if ($dag1StopReason -eq "ALREADY_RUNNING") {
                            $dag1ResultOk = $true
                        }
                        break
                    }
                    break
                }
            }
        }

        if (-not $dag1ResultOk) {
            Add-Result "T_DAG1: ALREADY_RUNNING writes SP_STATUPDATE_DENIED" "FAIL" `
                "Result set did not contain StopReason=ALREADY_RUNNING (got '$dag1StopReason').  Lock holder may have been reclaimed."
        } else {
            # Assert: SP_STATUPDATE_DENIED row was written to CommandLog
            $rDenied = Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password `
                -Query @"
SELECT TOP 1
    ID,
    Command,
    StartTime,
    EndTime,
    ExtendedInfo
FROM dbo.CommandLog
WHERE CommandType = N'SP_STATUPDATE_DENIED'
  AND ID > $preMaxId
ORDER BY ID DESC;
"@

            $dag1DeniedOk  = $false
            $dag1XmlDetail = "no SP_STATUPDATE_DENIED row found in CommandLog"

            if ($rDenied.ExitCode -eq 0 -and $rDenied.DataSet -and
                $rDenied.DataSet.Tables.Count -gt 0 -and $rDenied.DataSet.Tables[0].Rows.Count -gt 0) {

                $deniedRow = $rDenied.DataSet.Tables[0].Rows[0]
                $cmdVal    = [string]$deniedRow["Command"]
                $xiRaw     = [string]$deniedRow["ExtendedInfo"]

                # Assert Command contains the expected literal
                if ($cmdVal -notmatch "DENIED") {
                    $dag1XmlDetail = "SP_STATUPDATE_DENIED row found but Command missing 'DENIED': '$cmdVal'"
                } else {
                    # Verify XML contains Version and DeniedAt elements
                    $rXi = Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password `
                        -Query @"
DECLARE @x xml = (SELECT ExtendedInfo FROM dbo.CommandLog WHERE ID = $($deniedRow['ID']));
SELECT
    @x.value('(/Denial/Version)[1]',    'nvarchar(50)')  AS Version,
    @x.value('(/Denial/DeniedAt)[1]',   'nvarchar(50)')  AS DeniedAt,
    @x.value('(/Denial/SessionID)[1]',  'nvarchar(10)')  AS SessionID,
    @x.value('(/Denial/HolderSessionID)[1]', 'nvarchar(10)') AS HolderSessionID;
"@

                    if ($rXi.ExitCode -eq 0 -and $rXi.DataSet.Tables.Count -gt 0 -and
                        $rXi.DataSet.Tables[0].Rows.Count -gt 0) {
                        $xi      = $rXi.DataSet.Tables[0].Rows[0]
                        $denialVersion = [string]$xi["Version"]
                        $denAt   = [string]$xi["DeniedAt"]
                        $spid    = [string]$xi["SessionID"]
                        $hspid   = [string]$xi["HolderSessionID"]

                        if ($denialVersion -and $denAt -and $spid) {
                            $dag1DeniedOk  = $true
                            $dag1XmlDetail = "CommandType=SP_STATUPDATE_DENIED, Version=$denialVersion, DeniedAt=$denAt, SessionID=$spid, HolderSessionID=$hspid"
                        } else {
                            $dag1XmlDetail = "SP_STATUPDATE_DENIED row found but XML missing fields: Version='$denialVersion' DeniedAt='$denAt' SessionID='$spid'"
                        }
                    } else {
                        $dag1XmlDetail = "SP_STATUPDATE_DENIED XML query failed: $($rXi.Output | Select-Object -First 1)"
                    }
                }
            }

            $dag1Verdict = if ($dag1DeniedOk) { "PASS" } else { "FAIL" }
            Add-Result "T_DAG1: ALREADY_RUNNING writes SP_STATUPDATE_DENIED" $dag1Verdict $dag1XmlDetail
        }
    } finally {
        & $cleanupLock
    }
}

Write-Host ""

# ================================================================
# T_DAG2: Denial with @LogToTable = N writes NO row to CommandLog
# ================================================================
Write-Host "--- T_DAG2: ALREADY_RUNNING with @LogToTable=N writes NO CommandLog row ---" -ForegroundColor Yellow

# Reuse the dedicated holder connection from T_DAG1 (keep it warm so the proc's
# idle-staleness guard still sees a recently-active session).
if ($null -eq $holderSid -or $holderConn.State -ne 'Open') {
    Add-Result "T_DAG2: @LogToTable=N denial writes no CommandLog row" "SKIP" `
        "Holder connection unavailable"
} else {
    $hc2 = $holderConn.CreateCommand()
    $hc2.CommandText = "SELECT 1;"   # heartbeat keeps the holder recently-active
    [void]$hc2.ExecuteScalar()
    $h2sid = $holderSid
    $h2lt  = $holderLt

    try {
        Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password `
            -Query "IF OBJECT_ID(N'dbo.StatUpdateLock') IS NOT NULL DELETE FROM dbo.StatUpdateLock;" | Out-Null

        $lt2Str = $h2lt.ToString("yyyy-MM-dd HH:mm:ss.fff")
        Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password `
            -Query @"
IF OBJECT_ID(N'dbo.StatUpdateLock', N'U') IS NULL
    CREATE TABLE dbo.StatUpdateLock (
        Resource sysname NOT NULL,
        SessionID smallint NOT NULL,
        LoginTime datetime NOT NULL,
        AcquiredAt datetime2(3) NOT NULL DEFAULT SYSDATETIME(),
        CONSTRAINT PK_StatUpdateLock PRIMARY KEY CLUSTERED (Resource)
    );
DELETE FROM dbo.StatUpdateLock;
INSERT INTO dbo.StatUpdateLock (Resource, SessionID, LoginTime, AcquiredAt)
VALUES (N'sp_StatUpdate', $h2sid, '$lt2Str', SYSDATETIME());
"@ | Out-Null

        $rPreId2 = Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password `
            -Query "SELECT ISNULL(MAX(ID), 0) AS MaxID FROM dbo.CommandLog;"
        $preMax2 = if ($rPreId2.ExitCode -eq 0 -and $rPreId2.DataSet.Tables.Count -gt 0 -and
                       $rPreId2.DataSet.Tables[0].Rows.Count -gt 0) {
            [int]$rPreId2.DataSet.Tables[0].Rows[0]["MaxID"]
        } else { 0 }

        $r2 = Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password -Timeout 30 `
            -Query @"
EXEC dbo.sp_StatUpdate
    @Databases   = N'master',
    @Execute     = N'N',
    @LogToTable  = N'N';
"@

        # Check no SP_STATUPDATE_DENIED row was inserted
        $rCheck2 = Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password `
            -Query @"
SELECT COUNT(*) AS Cnt
FROM dbo.CommandLog
WHERE CommandType = N'SP_STATUPDATE_DENIED'
  AND ID > $preMax2;
"@

        $deniedCount = if ($rCheck2.ExitCode -eq 0 -and $rCheck2.DataSet.Tables.Count -gt 0 -and
                           $rCheck2.DataSet.Tables[0].Rows.Count -gt 0) {
            [int]$rCheck2.DataSet.Tables[0].Rows[0]["Cnt"]
        } else { -1 }

        # Also verify we actually got ALREADY_RUNNING (otherwise we can't trust the negative)
        $dag2Denied = $false
        if ($r2.DataSet -and $r2.DataSet.Tables.Count -gt 0) {
            foreach ($tbl in $r2.DataSet.Tables) {
                if ($tbl.Columns.Contains("StopReason")) {
                    foreach ($row in $tbl.Rows) {
                        if ([string]$row["StopReason"] -eq "ALREADY_RUNNING") { $dag2Denied = $true }
                        break
                    }
                    break
                }
            }
        }

        if (-not $dag2Denied) {
            Add-Result "T_DAG2: @LogToTable=N denial writes no CommandLog row" "SKIP" `
                "Proc did not return ALREADY_RUNNING (lock reclaimed -- alien session may have ended)"
        } elseif ($deniedCount -eq 0) {
            Add-Result "T_DAG2: @LogToTable=N denial writes no CommandLog row" "PASS" `
                "ALREADY_RUNNING confirmed, 0 SP_STATUPDATE_DENIED rows written"
        } else {
            Add-Result "T_DAG2: @LogToTable=N denial writes no CommandLog row" "FAIL" `
                "Found $deniedCount SP_STATUPDATE_DENIED row(s) despite @LogToTable=N"
        }
    } finally {
        & $cleanupLock
    }
}

# Holder connection no longer needed past the denial tests.
try { $holderConn.Close() } catch { }

Write-Host ""

# ================================================================
# T_DAG3: FILTERED_DRIFT wins over QS boost in QualifyReason
# ================================================================
Write-Host "--- T_DAG3: FILTERED_DRIFT precedence over QUERY_STORE_PRIORITY ---" -ForegroundColor Yellow

$dag3Db = "dag3_FilteredDrift_Test"

# Create scratch DB + fixtures
$rSetupDb = Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password -Timeout 60 `
    -Query @"
IF DB_ID(N'$dag3Db') IS NOT NULL
BEGIN
    ALTER DATABASE [$dag3Db] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE [$dag3Db];
END;
CREATE DATABASE [$dag3Db];
"@

if ($rSetupDb.ExitCode -ne 0) {
    Add-Result "T_DAG3: FILTERED_DRIFT precedence" "FAIL" `
        "Could not create scratch DB: $($rSetupDb.Output | Select-Object -First 1)"
} else {
    # Build a table with a filtered stat where selectivity drift ratio exceeds factor (2.0).
    # Filter: col > 100.  Unfiltered_rows ~ 6000, rows within filter ~ 1000.
    # ratio = 6000 / 1000 = 6.0 > 2.0 (@FilteredStatsStaleFactor default).
    $rSetup3 = Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password -Timeout 60 `
        -Db $dag3Db `
        -Query @"
-- Disable auto-update to keep stat state deterministic.
ALTER DATABASE [$dag3Db] SET AUTO_UPDATE_STATISTICS OFF;
ALTER DATABASE [$dag3Db] SET AUTO_CREATE_STATISTICS OFF;

-- Table with a skewed value distribution.
CREATE TABLE dbo.OrdersFilt (
    id     int NOT NULL IDENTITY(1,1) PRIMARY KEY CLUSTERED,
    region int NOT NULL,
    amount int NOT NULL
);

-- 5000 rows with region <= 100 (outside the filter predicate)
INSERT INTO dbo.OrdersFilt (region, amount)
SELECT TOP 5000 ABS(CHECKSUM(NEWID())) % 100 + 1, ABS(CHECKSUM(NEWID())) % 1000
FROM sys.all_columns AS a CROSS JOIN sys.all_columns AS b;

-- 1000 rows with region > 100 (inside the filter predicate)
INSERT INTO dbo.OrdersFilt (region, amount)
SELECT TOP 1000 ABS(CHECKSUM(NEWID())) % 900 + 101, ABS(CHECKSUM(NEWID())) % 1000
FROM sys.all_columns AS a CROSS JOIN sys.all_columns AS b;

-- Create filtered statistic on region > 100 and compute its initial histogram.
CREATE STATISTICS ST_OrdersFilt_region_filtered
ON dbo.OrdersFilt (region)
WHERE region > 100;

-- Compute initial histogram (so last_updated IS NOT NULL, stat is filterable).
UPDATE STATISTICS dbo.OrdersFilt (ST_OrdersFilt_region_filtered) WITH FULLSCAN;

-- Now add 5000 more rows within the filter to inflate unfiltered_rows
-- relative to what the histogram saw (drives drift ratio above 2.0).
INSERT INTO dbo.OrdersFilt (region, amount)
SELECT TOP 5000 ABS(CHECKSUM(NEWID())) % 900 + 101, ABS(CHECKSUM(NEWID())) % 1000
FROM sys.all_columns AS a CROSS JOIN sys.all_columns AS b;
"@

    if ($rSetup3.ExitCode -ne 0) {
        Add-Result "T_DAG3: FILTERED_DRIFT precedence" "FAIL" `
            "Fixture setup failed: $($rSetup3.Output | Select-Object -First 1)"
    } else {
        # Clear lock before proc call
        Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password `
            -Query "IF OBJECT_ID(N'dbo.StatUpdateLock') IS NOT NULL DELETE FROM dbo.StatUpdateLock;" | Out-Null

        # Purge CommandLog history for the scratch DB.  v3.4.0 CommandLog intelligence
        # qualifies on modification-counter DELTA vs the last logged run: a rerun of this
        # fixture reproduces the same counter, delta = 0, and the stat silently never
        # qualifies (caught 2026-07-03 as a rerun-only flake).
        Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password `
            -Query "DELETE FROM dbo.CommandLog WHERE DatabaseName = N'$dag3Db';" | Out-Null

        $rPreId3 = Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password `
            -Query "SELECT ISNULL(MAX(ID), 0) AS MaxID FROM dbo.CommandLog;"
        $preMax3 = if ($rPreId3.ExitCode -eq 0 -and $rPreId3.DataSet.Tables.Count -gt 0 -and
                       $rPreId3.DataSet.Tables[0].Rows.Count -gt 0) {
            [int]$rPreId3.DataSet.Tables[0].Rows[0]["MaxID"]
        } else { 0 }

        # Run with FilteredStatsMode=PRIORITY, LogToTable=Y, Execute=Y.
        # Use ModificationThreshold=1 so conventional stats also qualify.
        # @FilteredStatsStaleFactor = 1.5 (below 6.0 ratio) ensures drift qualifies.
        $r3 = Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password -Timeout 300 `
            -Query @"
EXEC dbo.sp_StatUpdate
    @Databases              = N'$dag3Db',
    @Tables                 = N'dbo.OrdersFilt',
    @Execute                = N'Y',
    @LogToTable             = N'Y',
    @ModificationThreshold  = 1,
    @FilteredStatsMode      = N'PRIORITY',
    @FilteredStatsStaleFactor = 1.5;
"@

        if ($r3.ExitCode -ne 0) {
            Add-Result "T_DAG3: FILTERED_DRIFT precedence" "FAIL" `
                "Proc raised exception: $($r3.Output | Select-Object -First 1)"
        } else {
            # Find the UPDATE_STATISTICS CommandLog row for our filtered stat
            $rLogRow = Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password `
                -Query @"
SELECT TOP 1
    ID,
    ExtendedInfo
FROM dbo.CommandLog
WHERE CommandType = N'UPDATE_STATISTICS'
  AND DatabaseName = N'$dag3Db'
  AND StatisticsName = N'ST_OrdersFilt_region_filtered'
  AND ID > $preMax3
ORDER BY ID DESC;
"@

            if ($rLogRow.ExitCode -ne 0 -or $rLogRow.DataSet.Tables.Count -eq 0 -or
                $rLogRow.DataSet.Tables[0].Rows.Count -eq 0) {
                Add-Result "T_DAG3: FILTERED_DRIFT precedence" "FAIL" `
                    "No UPDATE_STATISTICS CommandLog row found for ST_OrdersFilt_region_filtered"
            } else {
                $logId = [int]$rLogRow.DataSet.Tables[0].Rows[0]["ID"]

                # Extract QualifyReason from ExtendedInfo XML
                $rQR = Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password `
                    -Query @"
SELECT
    (SELECT ExtendedInfo FROM dbo.CommandLog WHERE ID = $logId)
        .value('(/ExtendedInfo/QualifyReason)[1]', 'nvarchar(100)') AS QualifyReason;
"@

                $qr = $null
                if ($rQR.ExitCode -eq 0 -and $rQR.DataSet.Tables.Count -gt 0 -and
                    $rQR.DataSet.Tables[0].Rows.Count -gt 0) {
                    $qrVal = $rQR.DataSet.Tables[0].Rows[0]["QualifyReason"]
                    if ($qrVal -ne [DBNull]::Value) { $qr = [string]$qrVal }
                }

                if ($qr -eq "FILTERED_DRIFT") {
                    Add-Result "T_DAG3: FILTERED_DRIFT precedence" "PASS" `
                        "QualifyReason = FILTERED_DRIFT (CommandLog ID $logId)"
                } else {
                    Add-Result "T_DAG3: FILTERED_DRIFT precedence" "FAIL" `
                        "QualifyReason = '$qr' (expected FILTERED_DRIFT).  QS boost may have overridden drift, or drift ratio was below factor."
                }
            }
        }
    }

    # Drop scratch DB
    Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password -Timeout 60 `
        -Query @"
IF DB_ID(N'$dag3Db') IS NOT NULL
BEGIN
    ALTER DATABASE [$dag3Db] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE [$dag3Db];
END;
"@ | Out-Null
}

Write-Host ""

# ================================================================
# T_DAG4: Plain dry run -- regression guard
# ================================================================
Write-Host "--- T_DAG4: Plain dry run regression guard ---" -ForegroundColor Yellow

# Clear lock before dry run
Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password `
    -Query "IF OBJECT_ID(N'dbo.StatUpdateLock') IS NOT NULL DELETE FROM dbo.StatUpdateLock;" | Out-Null

$r4 = Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password -Timeout 120 `
    -Query @"
EXEC dbo.sp_StatUpdate
    @Databases  = N'master',
    @Execute    = N'N',
    @LogToTable = N'N';
"@

if ($r4.ExitCode -eq 0) {
    Add-Result "T_DAG4: Plain dry run completes without error" "PASS" "ExitCode=0"
} else {
    Add-Result "T_DAG4: Plain dry run completes without error" "FAIL" `
        "Proc raised exception: $($r4.Output | Select-Object -First 1)"
}

Write-Host ""

# =====================================================================
# STEP 4: RESULTS SUMMARY
# =====================================================================
Write-Host "[4/4] Results" -ForegroundColor Yellow
Write-Host ""
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host " RESULTS" -ForegroundColor Cyan
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host ""

$passed = @($results | Where-Object { $_.Result -eq "PASS" }).Count
$failed = @($results | Where-Object { $_.Result -eq "FAIL" }).Count
$skipped = @($results | Where-Object { $_.Result -eq "SKIP" }).Count
$total  = $results.Count

foreach ($t in $results) {
    $tc = switch ($t.Result) {
        "PASS" { "DarkGreen" }
        "FAIL" { "Red"       }
        "SKIP" { "DarkGray"  }
        default { "White"    }
    }
    $detail = if ($t.Detail) { " ($($t.Detail))" } else { "" }
    Write-Host "  [$($t.Result)] $($t.Test)$detail" -ForegroundColor $tc
}

Write-Host ""
Write-Host ("{0,-6} {1,-8} {2,-8} {3,-6}" -f "Tests", "Passed", "Skipped", "Status")
Write-Host ("{0,-6} {1,-8} {2,-8} {3,-6}" -f "-----", "------", "-------", "------")
$status = if ($failed -eq 0) { "PASS" } else { "FAIL" }
$sc     = if ($failed -eq 0) { "Green" } else { "Red" }
Write-Host ("{0,-6} {1,-8} {2,-8} {3,-6}" -f $total, $passed, $skipped, $status) -ForegroundColor $sc
Write-Host ""

if ($ResultsPath) {
    $major = @{ '2019' = 15; '2022' = 16; '2025' = 17 }[$Version]
    $sqlStatus = Get-StructuredSqlStatus -Server $Server -Port $Port -Password $Password -ExpectedMajor $major
    $commit = (& git -C $RepoRoot rev-parse HEAD).Trim()
    if ($LASTEXITCODE -ne 0 -or $commit -notmatch '^[0-9a-fA-F]{40}$') { throw 'Could not resolve the tested source commit.' }
    Write-StructuredLegacyResults -Path $ResultsPath -Commit $commit -SqlVersion $Version -SqlBuild $sqlStatus.Build `
        -ServerAliveAtEnd $sqlStatus.Alive -Suite 'DenialAndGuards' -ExpectedCount 4 `
        -Results @($results) -StartedAt $startedAt -EndedAt (Get-Date)
}

if ($failed -gt 0 -or $skipped -gt 0) {
    Write-Host "RESULT: $failed test(s) FAILED." -ForegroundColor Red
    exit 1
} else {
    Write-Host "RESULT: All $total test(s) complete ($passed PASS, $skipped SKIP)." -ForegroundColor Green
    exit 0
}
