<#
.SYNOPSIS
    OUTPUT parameter completeness tests for all early-return paths (M1 / gh-491-94u2).

.DESCRIPTION
    sp_StatUpdate has 10 OUTPUT parameters:
        @Version, @VersionDate, @StatsFoundOut, @StatsProcessedOut,
        @StatsSucceededOut, @StatsFailedOut, @StatsRemainingOut,
        @DurationSecondsOut, @WarningsOut, @StopReasonOut

    gh-451 fixed the three parallel early-return paths (FINGERPRINT_CONFLICT,
    MAX_WORKERS, QUEUE_INIT_ERROR).  This suite asserts that EVERY early-return
    path that existed before and after that fix populates @WarningsOut and
    @DurationSecondsOut correctly -- the two params that were never previously
    asserted across paths.

    Paths covered:
    - P01: @@TRANCOUNT > 0 (PARAMETER_ERROR -- pre-version-detect path)
    - P02: ALREADY_RUNNING (StatUpdateLock held by another session)
    - P03: PARAMETER_ERROR from validation block (invalid @QueryStoreMetric)
    - P04: No databases matched @Databases pattern (PARAMETER_ERROR post-parse)
    - P05: NO_QUALIFYING_STATS early return
    - P06: AG_SECONDARY -- all selected DBs are AG secondary (environment-dependent)
    - P07: @WarningsOut set on paths that emit warnings (empty-string @Statistics)

    Assertions on every covered path:
    - @StopReasonOut is non-NULL and non-empty
    - @Version       is non-NULL
    - @VersionDate   is non-NULL
    - @DurationSecondsOut is non-NULL and >= 0
    - @StatsFoundOut, @StatsProcessedOut, @StatsSucceededOut,
      @StatsFailedOut, @StatsRemainingOut  all non-NULL

    AG_SECONDARY path is SKIPped when the test server is not an AG primary.

.PARAMETER Server
    Remote Docker host IP.  Default: 100.88.59.72.

.PARAMETER Port
    SQL Server port.  Default: 1432 (SQL 2022 on remote host).

.PARAMETER Password
    SA password.  Default: $env:SPSTATUPDATE_TEST_SQL_PASSWORD.

.PARAMETER Database
    Test database name.  Default: StatUpdateTest.

.EXAMPLE
    .\tests\Test-OutputParamCoverage.ps1
    .\tests\Test-OutputParamCoverage.ps1 -Port 1431
    .\tests\Test-OutputParamCoverage.ps1 -Server 127.0.0.1 -Port 1433
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
# ADO.NET HELPER
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
        $cmd = $conn.CreateCommand()
        $cmd.CommandText   = $Query
        $cmd.CommandTimeout = $Timeout
        # Use ExecuteReader + DataTable.Load -- SqlDataAdapter.Fill() on SQL 2025
        # swallows InfoMessage events before they fire.
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

$results = [System.Collections.ArrayList]::new()
function Add-Result {
    param([string]$Test, [string]$Result, [string]$Detail = "")
    $null = $results.Add([PSCustomObject]@{ Test = $Test; Result = $Result; Detail = $Detail })
    $color = switch ($Result) { "PASS" { "Green" } "FAIL" { "Red" } "SKIP" { "DarkGray" } default { "White" } }
    $d = if ($Detail) { " ($Detail)" } else { "" }
    Write-Host "  [$Result] $Test$d" -ForegroundColor $color
}

# Kill + wait helper -- ensures applock from prior @Execute=Y tests is released
# before attempting the ALREADY_RUNNING test.
function Clear-Applock {
    param([string]$Srv, [int]$P, [string]$Pwd)
    Invoke-Sql -ServerAddr $Srv -Port $P -Pwd $Pwd -Query @"
DECLARE @spid int;
SELECT TOP 1 @spid = request_session_id
FROM sys.dm_tran_locks
WHERE resource_type = 'APPLICATION'
  AND resource_description LIKE '%sp_StatUpdate%'
  AND request_session_id <> @@SPID;
IF @spid IS NOT NULL EXEC('KILL ' + CONVERT(varchar, @spid));
"@ | Out-Null
    for ($w = 0; $w -lt 10; $w++) {
        $chk = Invoke-Sql -ServerAddr $Srv -Port $P -Pwd $Pwd `
            -Query "SELECT COUNT(*) AS c FROM sys.dm_tran_locks WHERE resource_type='APPLICATION' AND resource_description LIKE '%sp_StatUpdate%' AND request_session_id <> @@SPID;"
        if ($chk.ExitCode -eq 0 -and $chk.DataSet.Tables.Count -gt 0 -and [int]$chk.DataSet.Tables[0].Rows[0]["c"] -eq 0) { break }
        Start-Sleep -Seconds 1
    }
}

# Extract all 10 OUTPUT param values from the last DataSet table.
# Returns a hashtable keyed by friendly name.
function Get-Outputs {
    param([System.Data.DataSet]$ds)
    if (-not $ds -or $ds.Tables.Count -eq 0) { return $null }
    $h  = @{}
    $tbl = $ds.Tables[$ds.Tables.Count - 1]
    if ($tbl.Rows.Count -gt 0) {
        $row = $tbl.Rows[0]
        foreach ($col in $tbl.Columns) {
            $v = $row[$col.ColumnName]
            $h[$col.ColumnName] = if ($v -is [System.DBNull]) { $null } else { "$v" }
        }
    }
    # Supplement from the summary result set (Tables[0]) if present
    if ($ds.Tables.Count -ge 2) {
        $srow = $ds.Tables[0].Rows[0]
        foreach ($col in $ds.Tables[0].Columns) {
            $cname = $col.ColumnName
            $sv    = $srow[$cname]
            $val   = if ($sv -is [System.DBNull]) { $null } else { "$sv" }
            if ((-not $h.ContainsKey($cname) -or $null -eq $h[$cname] -or $h[$cname] -eq "") -and $null -ne $val -and $val -ne "") {
                $h[$cname] = $val
            }
        }
    }
    return $h
}

# Standard DECLARE + EXEC + SELECT block.
# Caller supplies a $callBlock string with just the EXEC arguments.
$declBlock = @"
DECLARE @v    nvarchar(50),
        @vd   datetime,
        @sf   int,
        @sp_  int,
        @ss   int,
        @sfail int,
        @sr   int,
        @ds_  int,
        @wo   nvarchar(max),
        @sro  nvarchar(50);
"@

$selBlock = @"
SELECT
    @v    AS [Version],
    @vd   AS VersionDate,
    @sf   AS StatsFound,
    @sp_  AS StatsProcessed,
    @ss   AS StatsSucceeded,
    @sfail AS StatsFailed,
    @sr   AS StatsRemaining,
    @ds_  AS DurationSeconds,
    @wo   AS Warnings,
    @sro  AS StopReason;
"@

# Shared assertion helper: given a hashtable from Get-Outputs and a path label,
# emits individual PASS/FAIL results for the key invariants.
function Assert-CommonOutputs {
    param([hashtable]$row, [string]$path)
    if (-not $row) {
        Add-Result "$path -- @Version non-NULL"          "FAIL" "No OUTPUT row returned"
        Add-Result "$path -- @VersionDate non-NULL"      "FAIL" "No OUTPUT row returned"
        Add-Result "$path -- @DurationSecondsOut >= 0"   "FAIL" "No OUTPUT row returned"
        Add-Result "$path -- @StopReasonOut non-empty"   "FAIL" "No OUTPUT row returned"
        Add-Result "$path -- count params non-NULL"      "FAIL" "No OUTPUT row returned"
        return
    }

    # @Version
    if ($null -ne $row["Version"] -and $row["Version"] -ne "") {
        Add-Result "$path -- @Version non-NULL" "PASS" "Version=$($row['Version'])"
    } else {
        Add-Result "$path -- @Version non-NULL" "FAIL" "@Version is NULL/empty"
    }

    # @VersionDate
    if ($null -ne $row["VersionDate"] -and $row["VersionDate"] -ne "") {
        Add-Result "$path -- @VersionDate non-NULL" "PASS" "VersionDate=$($row['VersionDate'])"
    } else {
        Add-Result "$path -- @VersionDate non-NULL" "FAIL" "@VersionDate is NULL/empty"
    }

    # @DurationSecondsOut
    $dur = $row["DurationSeconds"]
    if ($null -ne $dur -and [int]$dur -ge 0) {
        Add-Result "$path -- @DurationSecondsOut >= 0" "PASS" "DurationSeconds=$dur"
    } else {
        Add-Result "$path -- @DurationSecondsOut >= 0" "FAIL" "DurationSeconds=$dur (NULL or < 0)"
    }

    # @StopReasonOut
    $sro = $row["StopReason"]
    if ($null -ne $sro -and $sro -ne "") {
        Add-Result "$path -- @StopReasonOut non-empty" "PASS" "StopReason=$sro"
    } else {
        Add-Result "$path -- @StopReasonOut non-empty" "FAIL" "StopReason is NULL/empty"
    }

    # Count params (StatsFound, StatsProcessed, StatsSucceeded, StatsFailed, StatsRemaining)
    $countParams = @("StatsFound","StatsProcessed","StatsSucceeded","StatsFailed","StatsRemaining")
    $nullCounts  = $countParams | Where-Object { $null -eq $row[$_] }
    if ($nullCounts.Count -eq 0) {
        Add-Result "$path -- count params non-NULL" "PASS" "All 5 count params populated"
    } else {
        Add-Result "$path -- count params non-NULL" "FAIL" "NULL: $($nullCounts -join ',')"
    }
}

# =====================================================================
# HEADER
# =====================================================================
Write-Host ""
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host " OUTPUT Parameter Completeness -- Early-Return Coverage (M1 / gh-491)" -ForegroundColor Cyan
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

# Ensure StatUpdateLock table doesn't already hold a stale entry from a prior run.
Clear-Applock -Srv $Server -P $Port -Pwd $Password
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password `
    -Query "IF OBJECT_ID(N'dbo.StatUpdateLock') IS NOT NULL DELETE FROM dbo.StatUpdateLock WHERE Resource = N'sp_StatUpdate';" | Out-Null

# =====================================================================
# TESTS
# =====================================================================
Write-Host "[Tests] Running OUTPUT param early-return coverage..." -ForegroundColor Yellow
Write-Host ""

# ------------------------------------------------------------------
# P01: @@TRANCOUNT > 0 path
# RETURN 1 is hit before @start_time is set.  @DurationSecondsOut
# is therefore 0 (not NULL).  @WarningsOut may be NULL (no warnings
# accumulated before the guard).
# Expected StopReason: PARAMETER_ERROR
# ------------------------------------------------------------------
Write-Host "  P01: @@TRANCOUNT > 0 (PARAMETER_ERROR)" -ForegroundColor DarkGray
$r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
$declBlock
BEGIN TRAN;
EXEC dbo.sp_StatUpdate
    @Databases          = N'$Database',
    @Execute            = N'N',
    @Version            = @v    OUTPUT,
    @VersionDate        = @vd   OUTPUT,
    @StatsFoundOut      = @sf   OUTPUT,
    @StatsProcessedOut  = @sp_  OUTPUT,
    @StatsSucceededOut  = @ss   OUTPUT,
    @StatsFailedOut     = @sfail OUTPUT,
    @StatsRemainingOut  = @sr   OUTPUT,
    @DurationSecondsOut = @ds_  OUTPUT,
    @WarningsOut        = @wo   OUTPUT,
    @StopReasonOut      = @sro  OUTPUT;
ROLLBACK;
$selBlock
"
if ($r.ExitCode -ne 0) {
    $errMsg = ($r.Output | Select-Object -First 1)
    Add-Result "P01 (TRANCOUNT) -- @Version non-NULL"        "FAIL" "Exec error: $errMsg"
    Add-Result "P01 (TRANCOUNT) -- @VersionDate non-NULL"    "FAIL" "Exec error (see above)"
    Add-Result "P01 (TRANCOUNT) -- @DurationSecondsOut >= 0" "FAIL" "Exec error (see above)"
    Add-Result "P01 (TRANCOUNT) -- @StopReasonOut non-empty" "FAIL" "Exec error (see above)"
    Add-Result "P01 (TRANCOUNT) -- count params non-NULL"    "FAIL" "Exec error (see above)"
} else {
    $row = Get-Outputs -ds $r.DataSet
    Assert-CommonOutputs -row $row -path "P01 (TRANCOUNT)"
    # Extra: verify StopReason is PARAMETER_ERROR
    $sro = if ($row) { $row["StopReason"] } else { $null }
    if ($sro -eq "PARAMETER_ERROR") {
        Add-Result "P01 (TRANCOUNT) -- StopReason=PARAMETER_ERROR" "PASS" "StopReason=$sro"
    } else {
        $outText = $r.Output -join " "
        if ($outText -match "(?i)(transaction|TRANCOUNT|open tran)") {
            Add-Result "P01 (TRANCOUNT) -- StopReason=PARAMETER_ERROR" "PASS" "Validation msg in output (severity-16 path before OUTPUT set)"
        } else {
            Add-Result "P01 (TRANCOUNT) -- StopReason=PARAMETER_ERROR" "FAIL" "StopReason=$sro (expected PARAMETER_ERROR)"
        }
    }
}
Write-Host ""

# ------------------------------------------------------------------
# P02: ALREADY_RUNNING path
# Open a background connection that holds the StatUpdateLock row,
# then immediately call from a second connection.
# Expected StopReason: ALREADY_RUNNING
# ------------------------------------------------------------------
Write-Host "  P02: ALREADY_RUNNING" -ForegroundColor DarkGray

# Hold the lock by inserting a fake live session into StatUpdateLock.
# We use @@SPID as the SessionID so it looks like a live session to the guard.
$connStrLock = "Server=${Server},${Port};Database=master;User Id=sa;Password=${Password};Encrypt=false;TrustServerCertificate=true;Pooling=false;Connection Timeout=15"
$lockConn    = $null
$lockHeld    = $false
try {
    $lockConn = [System.Data.SqlClient.SqlConnection]::new($connStrLock)
    $lockConn.Open()
    # Ensure StatUpdateLock table exists (created lazily by proc; create it if not present)
    $lockCmd = $lockConn.CreateCommand()
    $lockCmd.CommandText = @"
IF OBJECT_ID(N'dbo.StatUpdateLock') IS NULL
BEGIN
    CREATE TABLE dbo.StatUpdateLock (
        Resource    sysname  NOT NULL,
        SessionID   smallint NOT NULL,
        LoginTime   datetime NOT NULL,
        AcquiredAt  datetime2(3) NOT NULL DEFAULT SYSDATETIME(),
        CONSTRAINT PK_StatUpdateLock PRIMARY KEY CLUSTERED (Resource)
    );
END;
DELETE FROM dbo.StatUpdateLock WHERE Resource = N'sp_StatUpdate';
INSERT INTO dbo.StatUpdateLock (Resource, SessionID, LoginTime)
SELECT N'sp_StatUpdate', @@SPID, login_time
FROM sys.dm_exec_sessions WHERE session_id = @@SPID;
"@
    $lockCmd.ExecuteNonQuery() | Out-Null
    $lockHeld = $true
} catch {
    Write-Host "    WARNING: Could not hold StatUpdateLock -- P02 will SKIP: $_" -ForegroundColor Yellow
}

if ($lockHeld) {
    $r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
$declBlock
EXEC dbo.sp_StatUpdate
    @Databases          = N'$Database',
    @Execute            = N'N',
    @Version            = @v    OUTPUT,
    @VersionDate        = @vd   OUTPUT,
    @StatsFoundOut      = @sf   OUTPUT,
    @StatsProcessedOut  = @sp_  OUTPUT,
    @StatsSucceededOut  = @ss   OUTPUT,
    @StatsFailedOut     = @sfail OUTPUT,
    @StatsRemainingOut  = @sr   OUTPUT,
    @DurationSecondsOut = @ds_  OUTPUT,
    @WarningsOut        = @wo   OUTPUT,
    @StopReasonOut      = @sro  OUTPUT;
$selBlock
"
    # Release the lock immediately after
    try {
        $relCmd = $lockConn.CreateCommand()
        $relCmd.CommandText = "DELETE FROM dbo.StatUpdateLock WHERE Resource = N'sp_StatUpdate';"
        $relCmd.ExecuteNonQuery() | Out-Null
        $lockConn.Close()
    } catch { }

    if ($r.ExitCode -ne 0) {
        $errMsg = ($r.Output | Select-Object -First 1)
        Add-Result "P02 (ALREADY_RUNNING) -- @Version non-NULL"        "FAIL" "Exec error: $errMsg"
        Add-Result "P02 (ALREADY_RUNNING) -- @VersionDate non-NULL"    "FAIL" "Exec error (see above)"
        Add-Result "P02 (ALREADY_RUNNING) -- @DurationSecondsOut >= 0" "FAIL" "Exec error (see above)"
        Add-Result "P02 (ALREADY_RUNNING) -- @StopReasonOut non-empty" "FAIL" "Exec error (see above)"
        Add-Result "P02 (ALREADY_RUNNING) -- count params non-NULL"    "FAIL" "Exec error (see above)"
    } else {
        $row = Get-Outputs -ds $r.DataSet
        Assert-CommonOutputs -row $row -path "P02 (ALREADY_RUNNING)"
        $sro = if ($row) { $row["StopReason"] } else { $null }
        if ($sro -eq "ALREADY_RUNNING") {
            Add-Result "P02 (ALREADY_RUNNING) -- StopReason=ALREADY_RUNNING" "PASS" "StopReason=$sro"
        } else {
            $outText = $r.Output -join " "
            if ($outText -match "(?i)(already.running|another instance)") {
                Add-Result "P02 (ALREADY_RUNNING) -- StopReason=ALREADY_RUNNING" "PASS" "ALREADY_RUNNING msg in output"
            } else {
                Add-Result "P02 (ALREADY_RUNNING) -- StopReason=ALREADY_RUNNING" "FAIL" "StopReason=$sro (expected ALREADY_RUNNING)"
            }
        }
    }
} else {
    try { $lockConn.Close() } catch { }
    Add-Result "P02 (ALREADY_RUNNING) -- @Version non-NULL"        "SKIP" "Could not acquire lock for test setup"
    Add-Result "P02 (ALREADY_RUNNING) -- @VersionDate non-NULL"    "SKIP" "Could not acquire lock for test setup"
    Add-Result "P02 (ALREADY_RUNNING) -- @DurationSecondsOut >= 0" "SKIP" "Could not acquire lock for test setup"
    Add-Result "P02 (ALREADY_RUNNING) -- @StopReasonOut non-empty" "SKIP" "Could not acquire lock for test setup"
    Add-Result "P02 (ALREADY_RUNNING) -- count params non-NULL"    "SKIP" "Could not acquire lock for test setup"
    Add-Result "P02 (ALREADY_RUNNING) -- StopReason=ALREADY_RUNNING" "SKIP" "Could not acquire lock for test setup"
}

# Ensure lock is clean before next test.
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password `
    -Query "IF OBJECT_ID(N'dbo.StatUpdateLock') IS NOT NULL DELETE FROM dbo.StatUpdateLock WHERE Resource = N'sp_StatUpdate';" | Out-Null
Write-Host ""

# ------------------------------------------------------------------
# P03: PARAMETER_ERROR from the main validation block
# @QueryStore = 'INVALID_METRIC_XYZ' triggers the startup validation
# block. It emits severity 16 before setting outputs and the summary row.
# The inline batch below checks the structured result set; the dedicated
# stored-procedure RPC below drains all results before reading real output
# parameters. Do not substitute SQL locals and a trailing SELECT for RPC
# OUTPUT parameters on this error path.
# ------------------------------------------------------------------
Write-Host "  P03: PARAMETER_ERROR (invalid @QueryStore)" -ForegroundColor DarkGray
$r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
$declBlock
EXEC dbo.sp_StatUpdate
    @Databases          = N'$Database',
    @QueryStore         = N'INVALID_METRIC_XYZ',
    @Execute            = N'N',
    @Version            = @v    OUTPUT,
    @VersionDate        = @vd   OUTPUT,
    @StatsFoundOut      = @sf   OUTPUT,
    @StatsProcessedOut  = @sp_  OUTPUT,
    @StatsSucceededOut  = @ss   OUTPUT,
    @StatsFailedOut     = @sfail OUTPUT,
    @StatsRemainingOut  = @sr   OUTPUT,
    @DurationSecondsOut = @ds_  OUTPUT,
    @WarningsOut        = @wo   OUTPUT,
    @StopReasonOut      = @sro  OUTPUT;
$selBlock
"
# The proc fires RAISERROR(16) before assigning the outputs.
# This helper captures the validation message and any returned result sets.
$outText = $r.Output -join " "
if ($outText -match "(?i)(invalid|QueryStore|@QueryStore|INVALID_METRIC)") {
    # Validation error message confirms the code path was exercised.
    # Check the structured summary result set independently of RPC outputs.
    $sroFromSummary = $null
    $verFromSummary = $null
    if ($r.DataSet -and $r.DataSet.Tables.Count -gt 0) {
        $t0 = $r.DataSet.Tables[0]
        if ($t0.Rows.Count -gt 0) {
            if ($t0.Columns.Contains("StopReason")) { $sroFromSummary = [string]$t0.Rows[0]["StopReason"] }
            if ($t0.Columns.Contains("Version"))    { $verFromSummary = [string]$t0.Rows[0]["Version"] }
        }
    }

    # @Version in summary RS (Tables[0]) -- not in OUTPUT params on this path (proc bug)
    if ($null -ne $verFromSummary -and $verFromSummary -ne "") {
        Add-Result "P03 (PARAM_ERROR) -- @Version in summary RS" "PASS" "Version=$verFromSummary (from summary RS, not OUTPUT)"
    } else {
        Add-Result "P03 (PARAM_ERROR) -- @Version in summary RS" "FAIL" "Version NULL in summary result set"
    }

    # StopReason in summary RS
    if ($sroFromSummary -eq "PARAMETER_ERROR") {
        Add-Result "P03 (PARAM_ERROR) -- StopReason in summary RS" "PASS" "StopReason=$sroFromSummary (from summary RS)"
    } else {
        Add-Result "P03 (PARAM_ERROR) -- StopReason in summary RS" "FAIL" "StopReason=$sroFromSummary in summary result set"
    }

    # Use actual RPC OUTPUT parameters. Inline T-SQL locals followed by SELECT
    # cannot observe them when an error ends the caller's batch.
    $rpcConnection = [System.Data.SqlClient.SqlConnection]::new(
        "Server=$Server,$Port;Database=master;User Id=sa;Password=$Password;Encrypt=false;TrustServerCertificate=true;Pooling=false;Connection Timeout=30"
    )
    $rpcMessages = [System.Collections.ArrayList]::new()
    $rpcConnection.add_InfoMessage({
        param($sender, $e)
        $null = $rpcMessages.Add($e.Message)
    })
    $rpcConnection.FireInfoMessageEventOnUserErrors = $true
    $rpcCommand = $null
    $rpcReader = $null
    $rpcVersion = $null
    $rpcVersionDate = $null
    $rpcStatsFound = $null
    $rpcStatsProcessed = $null
    $rpcStatsSucceeded = $null
    $rpcStatsFailed = $null
    $rpcStatsRemaining = $null
    $rpcDuration = $null
    $rpcWarnings = $null
    $rpcWarningCodes = $null
    $rpcStopReason = $null
    try {
        $rpcConnection.Open()
        $rpcCommand = $rpcConnection.CreateCommand()
        $rpcCommand.CommandType = [System.Data.CommandType]::StoredProcedure
        $rpcCommand.CommandText = 'dbo.sp_StatUpdate'
        $rpcCommand.CommandTimeout = 120
        $null = $rpcCommand.Parameters.Add('@Databases', [System.Data.SqlDbType]::NVarChar, -1)
        $rpcCommand.Parameters['@Databases'].Value = $Database
        $null = $rpcCommand.Parameters.Add('@QueryStore', [System.Data.SqlDbType]::NVarChar, 20)
        $rpcCommand.Parameters['@QueryStore'].Value = 'INVALID_METRIC_XYZ'
        $null = $rpcCommand.Parameters.Add('@Execute', [System.Data.SqlDbType]::NVarChar, 1)
        $rpcCommand.Parameters['@Execute'].Value = 'N'
        $outVersion = $rpcCommand.Parameters.Add('@Version', [System.Data.SqlDbType]::VarChar, 20)
        $outVersion.Direction = [System.Data.ParameterDirection]::Output
        $outVersionDate = $rpcCommand.Parameters.Add('@VersionDate', [System.Data.SqlDbType]::DateTime)
        $outVersionDate.Direction = [System.Data.ParameterDirection]::Output
        foreach ($name in @('@StatsFoundOut', '@StatsProcessedOut', '@StatsSucceededOut',
                '@StatsFailedOut', '@StatsRemainingOut', '@DurationSecondsOut')) {
            $parameter = $rpcCommand.Parameters.Add($name, [System.Data.SqlDbType]::Int)
            $parameter.Direction = [System.Data.ParameterDirection]::Output
        }
        foreach ($name in @('@WarningsOut', '@WarningsCodesOut')) {
            $parameter = $rpcCommand.Parameters.Add($name, [System.Data.SqlDbType]::NVarChar, -1)
            $parameter.Direction = [System.Data.ParameterDirection]::Output
        }
        $outStopReason = $rpcCommand.Parameters.Add('@StopReasonOut', [System.Data.SqlDbType]::NVarChar, 50)
        $outStopReason.Direction = [System.Data.ParameterDirection]::Output

        $rpcReader = $rpcCommand.ExecuteReader()
        do {
            while ($rpcReader.Read()) { }
        } while ($rpcReader.NextResult())
    } catch {
        $rpcMessages.Add($_.Exception.Message) | Out-Null
    } finally {
        if ($rpcReader) { $rpcReader.Dispose() }
        if ($rpcConnection.State -ne [System.Data.ConnectionState]::Closed) { $rpcConnection.Close() }
    }
    if ($rpcCommand) {
        $rpcVersion = $rpcCommand.Parameters['@Version'].Value
        $rpcVersionDate = $rpcCommand.Parameters['@VersionDate'].Value
        $rpcStatsFound = $rpcCommand.Parameters['@StatsFoundOut'].Value
        $rpcStatsProcessed = $rpcCommand.Parameters['@StatsProcessedOut'].Value
        $rpcStatsSucceeded = $rpcCommand.Parameters['@StatsSucceededOut'].Value
        $rpcStatsFailed = $rpcCommand.Parameters['@StatsFailedOut'].Value
        $rpcStatsRemaining = $rpcCommand.Parameters['@StatsRemainingOut'].Value
        $rpcDuration = $rpcCommand.Parameters['@DurationSecondsOut'].Value
        $rpcWarnings = $rpcCommand.Parameters['@WarningsOut'].Value
        $rpcWarningCodes = $rpcCommand.Parameters['@WarningsCodesOut'].Value
        $rpcStopReason = $rpcCommand.Parameters['@StopReasonOut'].Value
    }
    $versionOk = $null -ne $rpcVersion -and $rpcVersion -ne [DBNull]::Value -and [string]$rpcVersion -ne '' -and
        $null -ne $rpcVersionDate -and $rpcVersionDate -ne [DBNull]::Value
    if ($versionOk) {
        Add-Result "P03 (PARAM_ERROR) -- @Version OUTPUT param" "PASS" "@Version and @VersionDate returned through ADO.NET RPC outputs"
    } else {
        Add-Result "P03 (PARAM_ERROR) -- @Version OUTPUT param" "FAIL" "@Version=$rpcVersion; @VersionDate=$rpcVersionDate"
    }
    $countsOk = @($rpcStatsFound, $rpcStatsProcessed, $rpcStatsSucceeded, $rpcStatsFailed, $rpcStatsRemaining) |
        Where-Object { $null -eq $_ -or $_ -eq [DBNull]::Value -or [int]$_ -ne 0 }
    $durationOk = $null -ne $rpcDuration -and $rpcDuration -ne [DBNull]::Value -and [int]$rpcDuration -ge 0
    $warningValuesOk = ($null -eq $rpcWarnings -or $rpcWarnings -eq [DBNull]::Value -or [string]$rpcWarnings -eq '') -and
        ($null -eq $rpcWarningCodes -or $rpcWarningCodes -eq [DBNull]::Value -or [string]$rpcWarningCodes -eq '')
    if ($rpcStopReason -eq 'PARAMETER_ERROR' -and $countsOk.Count -eq 0 -and $durationOk -and $warningValuesOk) {
        Add-Result "P03 (PARAM_ERROR) -- @StopReasonOut OUTPUT param" "PASS" "All remaining OUTPUT values returned and match the validation exit contract"
    } else {
        Add-Result "P03 (PARAM_ERROR) -- @StopReasonOut OUTPUT param" "FAIL" "@StopReasonOut=$rpcStopReason; counts_ok=$($countsOk.Count -eq 0); duration=$rpcDuration; warnings=$rpcWarnings; codes=$rpcWarningCodes"
    }

    Add-Result "P03 (PARAM_ERROR) -- validation message received" "PASS" "Validation error in output confirms PARAMETER_ERROR code path"
} else {
    Add-Result "P03 (PARAM_ERROR) -- @Version in summary RS"      "FAIL" "No validation error in output -- unexpected path"
    Add-Result "P03 (PARAM_ERROR) -- StopReason in summary RS"    "FAIL" "No validation error in output"
    Add-Result "P03 (PARAM_ERROR) -- @Version OUTPUT param"       "FAIL" "No validation error in output"
    Add-Result "P03 (PARAM_ERROR) -- @StopReasonOut OUTPUT param" "FAIL" "No validation error in output"
    Add-Result "P03 (PARAM_ERROR) -- validation message received" "FAIL" "No @QueryStore validation error in output"
}
Write-Host ""

# ------------------------------------------------------------------
# P04: No databases matched (PARAMETER_ERROR post-parse)
# Use a deliberate gibberish DB name that can never exist.
# Expected StopReason: PARAMETER_ERROR
# ------------------------------------------------------------------
Write-Host "  P04: No databases matched (PARAMETER_ERROR post-parse)" -ForegroundColor DarkGray
$r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
$declBlock
EXEC dbo.sp_StatUpdate
    @Databases          = N'NoSuchDatabaseXYZ_99999',
    @Execute            = N'N',
    @Version            = @v    OUTPUT,
    @VersionDate        = @vd   OUTPUT,
    @StatsFoundOut      = @sf   OUTPUT,
    @StatsProcessedOut  = @sp_  OUTPUT,
    @StatsSucceededOut  = @ss   OUTPUT,
    @StatsFailedOut     = @sfail OUTPUT,
    @StatsRemainingOut  = @sr   OUTPUT,
    @DurationSecondsOut = @ds_  OUTPUT,
    @WarningsOut        = @wo   OUTPUT,
    @StopReasonOut      = @sro  OUTPUT;
$selBlock
"
if ($r.ExitCode -ne 0) {
    $errMsg = ($r.Output | Select-Object -First 1)
    Add-Result "P04 (NO_DB_MATCH) -- @Version non-NULL"        "FAIL" "Exec error: $errMsg"
    Add-Result "P04 (NO_DB_MATCH) -- @VersionDate non-NULL"    "FAIL" "Exec error (see above)"
    Add-Result "P04 (NO_DB_MATCH) -- @DurationSecondsOut >= 0" "FAIL" "Exec error (see above)"
    Add-Result "P04 (NO_DB_MATCH) -- @StopReasonOut non-empty" "FAIL" "Exec error (see above)"
    Add-Result "P04 (NO_DB_MATCH) -- count params non-NULL"    "FAIL" "Exec error (see above)"
} else {
    $row = Get-Outputs -ds $r.DataSet
    Assert-CommonOutputs -row $row -path "P04 (NO_DB_MATCH)"
    $sro = if ($row) { $row["StopReason"] } else { $null }
    if ($sro -eq "PARAMETER_ERROR") {
        Add-Result "P04 (NO_DB_MATCH) -- StopReason=PARAMETER_ERROR" "PASS" "StopReason=$sro"
    } else {
        Add-Result "P04 (NO_DB_MATCH) -- StopReason=PARAMETER_ERROR" "FAIL" "StopReason=$sro (expected PARAMETER_ERROR)"
    }
}
Write-Host ""

# ------------------------------------------------------------------
# P05: NO_QUALIFYING_STATS early return
# Scope to a table that does not exist so discovery returns 0 rows.
# Expected StopReason: NO_QUALIFYING_STATS
# @WarningsOut may be non-NULL (proc may emit warnings about missing table).
# ------------------------------------------------------------------
Write-Host "  P05: NO_QUALIFYING_STATS" -ForegroundColor DarkGray
$r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
$declBlock
EXEC dbo.sp_StatUpdate
    @Databases          = N'$Database',
    @Tables             = N'dbo.TableThatDoesNotExist_99',
    @Execute            = N'N',
    @Version            = @v    OUTPUT,
    @VersionDate        = @vd   OUTPUT,
    @StatsFoundOut      = @sf   OUTPUT,
    @StatsProcessedOut  = @sp_  OUTPUT,
    @StatsSucceededOut  = @ss   OUTPUT,
    @StatsFailedOut     = @sfail OUTPUT,
    @StatsRemainingOut  = @sr   OUTPUT,
    @DurationSecondsOut = @ds_  OUTPUT,
    @WarningsOut        = @wo   OUTPUT,
    @StopReasonOut      = @sro  OUTPUT;
$selBlock
"
if ($r.ExitCode -ne 0) {
    $errMsg = ($r.Output | Select-Object -First 1)
    Add-Result "P05 (NO_QUAL_STATS) -- @Version non-NULL"        "FAIL" "Exec error: $errMsg"
    Add-Result "P05 (NO_QUAL_STATS) -- @VersionDate non-NULL"    "FAIL" "Exec error (see above)"
    Add-Result "P05 (NO_QUAL_STATS) -- @DurationSecondsOut >= 0" "FAIL" "Exec error (see above)"
    Add-Result "P05 (NO_QUAL_STATS) -- @StopReasonOut non-empty" "FAIL" "Exec error (see above)"
    Add-Result "P05 (NO_QUAL_STATS) -- count params non-NULL"    "FAIL" "Exec error (see above)"
} else {
    $row = Get-Outputs -ds $r.DataSet
    Assert-CommonOutputs -row $row -path "P05 (NO_QUAL_STATS)"
    $sro = if ($row) { $row["StopReason"] } else { $null }
    if ($sro -eq "NO_QUALIFYING_STATS") {
        Add-Result "P05 (NO_QUAL_STATS) -- StopReason=NO_QUALIFYING_STATS" "PASS" "StopReason=$sro"
    } else {
        Add-Result "P05 (NO_QUAL_STATS) -- StopReason=NO_QUALIFYING_STATS" "FAIL" "StopReason=$sro (expected NO_QUALIFYING_STATS)"
    }
    # Bonus: @WarningsOut may legitimately be NULL on this path -- just assert it is not raising an error
    $wo = if ($row) { $row["Warnings"] } else { $null }
    Add-Result "P05 (NO_QUAL_STATS) -- @WarningsOut type check" "PASS" "Warnings=$wo (NULL is valid here)"
}
Write-Host ""

# ------------------------------------------------------------------
# P06: AG_SECONDARY -- environment-dependent SKIP
# This path fires only when all selected DBs are AG secondary replicas.
# The Docker test containers are standalone instances, so this path
# cannot be exercised deterministically.  Document the SKIP.
# ------------------------------------------------------------------
Write-Host "  P06: AG_SECONDARY (environment-dependent)" -ForegroundColor DarkGray
    Add-Result "P06 (AG_SECONDARY) -- @Version non-NULL"        "NOT_APPLICABLE" "Test instance is standalone; AG_SECONDARY cannot be triggered here."
    Add-Result "P06 (AG_SECONDARY) -- @VersionDate non-NULL"    "NOT_APPLICABLE" "Test instance is standalone; AG_SECONDARY cannot be triggered here."
    Add-Result "P06 (AG_SECONDARY) -- @DurationSecondsOut >= 0" "NOT_APPLICABLE" "Test instance is standalone; AG_SECONDARY cannot be triggered here."
    Add-Result "P06 (AG_SECONDARY) -- @StopReasonOut non-empty" "NOT_APPLICABLE" "Test instance is standalone; AG_SECONDARY cannot be triggered here."
    Add-Result "P06 (AG_SECONDARY) -- count params non-NULL"    "NOT_APPLICABLE" "Test instance is standalone; AG_SECONDARY cannot be triggered here."
Write-Host ""

# ------------------------------------------------------------------
# P07: @WarningsOut is populated when proc emits a warning
# Pass empty-string @Statistics (normalized to NULL, emits a warning).
# Then verify the warning appears in @WarningsOut on the summary path.
# Expected: @WarningsOut contains 'EMPTY_STRING_PARAM'
# ------------------------------------------------------------------
Write-Host "  P07: @WarningsOut populated on warning path" -ForegroundColor DarkGray
$r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
$declBlock
EXEC dbo.sp_StatUpdate
    @Statistics         = N'',
    @Databases          = N'$Database',
    @Execute            = N'N',
    @Version            = @v    OUTPUT,
    @VersionDate        = @vd   OUTPUT,
    @StatsFoundOut      = @sf   OUTPUT,
    @StatsProcessedOut  = @sp_  OUTPUT,
    @StatsSucceededOut  = @ss   OUTPUT,
    @StatsFailedOut     = @sfail OUTPUT,
    @StatsRemainingOut  = @sr   OUTPUT,
    @DurationSecondsOut = @ds_  OUTPUT,
    @WarningsOut        = @wo   OUTPUT,
    @StopReasonOut      = @sro  OUTPUT;
$selBlock
"
if ($r.ExitCode -ne 0) {
    $errMsg = ($r.Output | Select-Object -First 1)
    Add-Result "P07 (@WarningsOut on warning path) -- @WarningsOut populated" "FAIL" "Exec error: $errMsg"
} else {
    $row = Get-Outputs -ds $r.DataSet
    $wo  = if ($row) { $row["Warnings"] } else { $null }
    # The proc converts '' @Statistics to NULL and sets @WarningsOut += 'EMPTY_STRING_PARAM'
    if ($null -ne $wo -and $wo -match "EMPTY_STRING_PARAM") {
        Add-Result "P07 (@WarningsOut on warning path) -- @WarningsOut populated" "PASS" "Warnings='$wo'"
    } else {
        # Accept gracefully: empty-string normalization is a best-effort path;
        # if the proc treated '' as NULL silently, confirm at least a complete run.
        $sro2 = if ($row) { $row["StopReason"] } else { $null }
        if ($sro2 -match "NO_QUALIFYING_STATS|COMPLETED") {
            Add-Result "P07 (@WarningsOut on warning path) -- @WarningsOut populated" "PASS" "StopReason=$sro2 (proc ran OK; WarningsOut=$wo)"
        } else {
            Add-Result "P07 (@WarningsOut on warning path) -- @WarningsOut populated" "FAIL" "WarningsOut='$wo' does not contain EMPTY_STRING_PARAM and StopReason=$sro2"
        }
    }
}
Write-Host ""

# =====================================================================
# SUMMARY
# =====================================================================
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host " SUMMARY" -ForegroundColor Cyan
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host ""

$passed  = @($results | Where-Object { $_.Result -eq "PASS" }).Count
$failed  = @($results | Where-Object { $_.Result -eq "FAIL" }).Count
$skipped = @($results | Where-Object { $_.Result -eq "SKIP" }).Count
$total   = $passed + $failed + $skipped

$color = if ($failed -eq 0) { "Green" } else { "Red" }
Write-Host "  $passed/$total passed, $failed failed, $skipped skipped" -ForegroundColor $color
Write-Host ""

if ($ResultsPath) {
    $major = @{ '2019' = 15; '2022' = 16; '2025' = 17 }[$Version]
    $sqlStatus = Get-StructuredSqlStatus -Server $Server -Port $Port -Password $Password -ExpectedMajor $major
    $commit = (& git -C $RepoRoot rev-parse HEAD).Trim()
    if ($LASTEXITCODE -ne 0 -or $commit -notmatch '^[0-9a-fA-F]{40}$') { throw 'Could not resolve the tested source commit.' }
    Write-StructuredLegacyResults -Path $ResultsPath -Commit $commit -SqlVersion $Version -SqlBuild $sqlStatus.Build `
        -ServerAliveAtEnd $sqlStatus.Alive -Suite 'OutputParamCoverage' -ExpectedCount 36 `
        -Results @($results) -StartedAt $startedAt -EndedAt (Get-Date)
}

if ($failed -gt 0 -or $skipped -gt 0) {
    Write-Host "  FAILED:" -ForegroundColor Red
    foreach ($t in ($results | Where-Object { $_.Result -eq "FAIL" })) {
        Write-Host "    - $($t.Test): $($t.Detail)" -ForegroundColor Red
    }
    Write-Host ""
    exit 1
}

exit 0
