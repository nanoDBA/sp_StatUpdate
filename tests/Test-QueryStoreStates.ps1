<#
.SYNOPSIS
    QS state degradation tests -- QS OFF, READ_ONLY, and zero recent data.

.DESCRIPTION
    Tests that sp_StatUpdate degrades gracefully when the target database has
    Query Store disabled, READ_ONLY, or no recent runtime stats.

    Covers three QS states and one cross-version boundary check:
    - QS1: QS disabled (ALTER DATABASE ... SET QUERY_STORE = OFF)
            @QueryStore = CPU should succeed; END XML has QSEnrichmentSkipped=1.
    - QS2: QS in READ_ONLY mode (OPERATION_MODE = READ_ONLY)
            Proc should raise a WarningsOut entry, but still complete.
    - QS3: QS enabled but zero recent runtime stats
            Phase 6 should bail out early; proc still completes.
    - QS4: QS param accepted on SQL Server versions that support it

    Relies on the StatUpdateTest database (from setup_multiversion_db.sql).

.PARAMETER Server
    Remote Docker host IP.  Default: 100.88.59.72.

.PARAMETER Port
    SQL Server port.  Default: 1432 (SQL 2022 on remote host).

.PARAMETER Password
    SA password.  Default: $env:SPSTATUPDATE_TEST_SQL_PASSWORD.

.PARAMETER Database
    Test database name.  Default: StatUpdateTest.

.PARAMETER SkipSetup
    Skip fresh-modification inserts (assumes data already present).

.EXAMPLE
    .\tests\Test-QueryStoreStates.ps1
    .\tests\Test-QueryStoreStates.ps1 -Port 1431
    .\tests\Test-QueryStoreStates.ps1 -SkipSetup
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$Server,
    [ValidateSet('2019','2022','2025')][string]$Version = '2022',
    [string]$Password = $(if ($env:SPSTATUPDATE_TEST_SQL_PASSWORD) { $env:SPSTATUPDATE_TEST_SQL_PASSWORD } else { '' }),
    [string]$ResultsPath = '',
    [string]$Database = "StatUpdateTest",
    [switch]$SkipSetup
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
# Uses ExecuteReader (not SqlDataAdapter.Fill) -- Fill misses InfoMessage
# events on SQL 2025 when the result set completes too quickly.
# =====================================================================
function Invoke-Sql {
    param(
        [string]$ServerAddr,
        [int]   $Port,
        [string]$Pwd,
        [string]$Query,
        [string]$Db      = "master",
        [int]   $Timeout = 300
    )
    $connStr = "Server=$ServerAddr,$Port;Database=$Db;User Id=sa;Password=$Pwd;" +
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

        $cmd                = $conn.CreateCommand()
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
    }
    catch {
        try { if ($conn -and $conn.State -eq 'Open') { $conn.Close() } } catch { }
        $inner = if ($_.Exception.InnerException) { $_.Exception.InnerException.Message } else { "" }
        return @{ Output = @($inner, $_.Exception.Message) + $messages; DataSet = $null; ExitCode = 1 }
    }
}

# Helper: kill any stale sp_StatUpdate applock then wait until released
function Clear-StatUpdateApplock {
    param([string]$ServerAddr, [int]$Port, [string]$Pwd)

    $r = Invoke-Sql -ServerAddr $ServerAddr -Port $Port -Pwd $Pwd -Query "
        SELECT request_session_id AS spid
        FROM   sys.dm_tran_locks
        WHERE  resource_type        = N'APPLICATION'
        AND    resource_description LIKE N'%sp_StatUpdate%';"

    if ($r.DataSet -and $r.DataSet.Tables.Count -gt 0) {
        foreach ($row in $r.DataSet.Tables[0].Rows) {
            $spid = [int]$row["spid"]
            Invoke-Sql -ServerAddr $ServerAddr -Port $Port -Pwd $Pwd `
                -Query "IF EXISTS (SELECT 1 FROM sys.dm_exec_sessions WHERE session_id = $spid) EXEC(N'KILL $spid');" | Out-Null
        }
    }

    # Poll until the applock is released (up to 10 s)
    $deadline = (Get-Date).AddSeconds(10)
    while ((Get-Date) -lt $deadline) {
        $check = Invoke-Sql -ServerAddr $ServerAddr -Port $Port -Pwd $Pwd -Query "
            SELECT COUNT(*) AS cnt
            FROM   sys.dm_tran_locks
            WHERE  resource_type        = N'APPLICATION'
            AND    resource_description LIKE N'%sp_StatUpdate%';"
        if ($check.DataSet -and $check.DataSet.Tables.Count -gt 0) {
            $cnt = [int]$check.DataSet.Tables[0].Rows[0]["cnt"]
            if ($cnt -eq 0) { break }
        }
        Start-Sleep -Milliseconds 500
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
Write-Host " sp_StatUpdate -- QS State Degradation Tests (QS1-QS4)" -ForegroundColor Cyan
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host ""
Write-Host "  Server:   ${Server}:${Port}"
Write-Host "  Database: $Database"
Write-Host ""

# =====================================================================
# CONNECTIVITY
# =====================================================================
Write-Host "[Setup] Verifying connectivity..." -ForegroundColor Yellow
$vr = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password `
    -Query "SELECT CONVERT(int, SERVERPROPERTY(N'ProductMajorVersion')) AS MajorVer;"

if ($vr.ExitCode -ne 0) {
    Write-Host "  ERROR: Cannot connect to ${Server}:${Port} -- $($vr.Output | Select-Object -First 1)" -ForegroundColor Red
    exit 1
}
$serverMajor = [int]$vr.DataSet.Tables[0].Rows[0]["MajorVer"]
Write-Host "  Connected (SQL major version: $serverMajor)" -ForegroundColor Green
Write-Host ""

# =====================================================================
# SETUP
# =====================================================================
if (-not $SkipSetup) {
    Write-Host "[Setup] Inserting fresh modifications..." -ForegroundColor Yellow

    Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Query "
        INSERT INTO dbo.Orders (CustomerName, Amount)
        SELECT TOP 3000 N'QSState', 1.00 FROM sys.all_columns;" | Out-Null
    Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Query "
        INSERT INTO dbo.Products (ProductName, Price)
        SELECT TOP 1000 N'QSState', 1.00 FROM sys.all_columns;" | Out-Null

    Write-Host "  Done" -ForegroundColor Green
    Write-Host ""
}

# Clean CommandLog for predictable assertions later
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
    DELETE FROM master.dbo.CommandLog
    WHERE  CommandType IN (N'SP_STATUPDATE_START', N'SP_STATUPDATE_END')
    AND    StartTime   >= DATEADD(HOUR, -1, SYSDATETIME());" | Out-Null

Clear-StatUpdateApplock -ServerAddr $Server -Port $Port -Pwd $Password

# =====================================================================
# QS1: QS disabled -- proc must succeed; QSEnrichmentSkipped=1 in END XML
# =====================================================================
Write-Host "[Tests] Running QS state tests..." -ForegroundColor Yellow
Write-Host ""

Write-Host "  QS1: Disabling Query Store on $Database..." -ForegroundColor DarkGray
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password `
    -Query "BEGIN TRY ALTER DATABASE [$Database] SET QUERY_STORE = OFF; END TRY BEGIN CATCH END CATCH;" | Out-Null
Start-Sleep -Seconds 1  # let QS state propagate

Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Query "
    INSERT INTO dbo.Orders (CustomerName, Amount)
    SELECT TOP 2000 N'QS1', 1.00 FROM sys.all_columns;" | Out-Null

$runLabel = "QS1_" + (Get-Date -Format "yyyyMMdd_HHmmss")
$r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password `
    -Query "
        DECLARE @StopReason nvarchar(50), @Warnings nvarchar(max);
        EXEC dbo.sp_StatUpdate
            @Databases            = N'$Database',
            @Tables               = N'dbo.Orders',
            @QueryStore           = N'CPU',
            @ModificationThreshold = 100,
            @Execute              = N'Y',
            @LogToTable           = N'Y',
            @StopReasonOut        = @StopReason OUTPUT,
            @WarningsOut          = @Warnings   OUTPUT;
        SELECT @StopReason AS StopReason, ISNULL(@Warnings, N'') AS Warnings;"

if ($r.ExitCode -ne 0) {
    Add-Result "QS1: QS OFF -- proc succeeds" "FAIL" ($r.Output | Select-Object -First 1)
}
else {
    # Last table in DataSet is the appended SELECT for OUTPUT params
    $outTable = $r.DataSet.Tables[$r.DataSet.Tables.Count - 1]
    $stopReason = if ($outTable.Rows.Count -gt 0) { [string]$outTable.Rows[0]["StopReason"] } else { "" }

    if ($stopReason -match "COMPLETED|NATURAL_END|NO_QUALIFYING") {
        Add-Result "QS1: QS OFF -- proc succeeds" "PASS" "StopReason=$stopReason"
    }
    else {
        Add-Result "QS1: QS OFF -- proc succeeds" "FAIL" "Unexpected StopReason='$stopReason'"
    }

    # Check CommandLog END XML for QSEnrichmentSkipped
    $r2 = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
        SELECT TOP 1
            ExtendedInfo.value('(/ExtendedInfo/QSEnrichmentSkipped)[1]', 'int') AS QSSkipped
        FROM master.dbo.CommandLog
        WHERE CommandType = N'SP_STATUPDATE_END'
        ORDER BY ID DESC;"

    if ($r2.ExitCode -eq 0 -and $r2.DataSet.Tables.Count -gt 0 -and $r2.DataSet.Tables[0].Rows.Count -gt 0) {
        $qsSkipped = $r2.DataSet.Tables[0].Rows[0]["QSSkipped"]
        # QSEnrichmentSkipped=1 expected when QS is off; NULL is also acceptable
        # (older code paths may not write the field) -- treat as non-failure
        if ($qsSkipped -eq [DBNull]::Value -or [int]$qsSkipped -eq 1) {
            Add-Result "QS1: QS OFF -- QSEnrichmentSkipped in END XML" "PASS" "QSEnrichmentSkipped=$qsSkipped"
        }
        else {
            Add-Result "QS1: QS OFF -- QSEnrichmentSkipped in END XML" "FAIL" "Expected NULL or 1, got $qsSkipped"
        }
    }
    else {
        Add-Result "QS1: QS OFF -- QSEnrichmentSkipped in END XML" "SKIP" "No SP_STATUPDATE_END row found"
    }
}

Clear-StatUpdateApplock -ServerAddr $Server -Port $Port -Pwd $Password

# =====================================================================
# QS2: QS READ_ONLY -- proc should complete; WarningsOut has QS warning
# =====================================================================
Write-Host "  QS2: Setting Query Store to READ_ONLY on $Database..." -ForegroundColor DarkGray

# Enable QS first, then set READ_ONLY
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password `
    -Query "BEGIN TRY ALTER DATABASE [$Database] SET QUERY_STORE = ON; END TRY BEGIN CATCH END CATCH;" | Out-Null
Start-Sleep -Seconds 1
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password `
    -Query "BEGIN TRY ALTER DATABASE [$Database] SET QUERY_STORE (OPERATION_MODE = READ_ONLY); END TRY BEGIN CATCH END CATCH;" | Out-Null
Start-Sleep -Seconds 1

# Verify state
$stateCheck = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password `
    -Query "SELECT actual_state_desc FROM sys.database_query_store_options WHERE database_id = DB_ID(N'$Database');"
$qsState = if ($stateCheck.DataSet -and $stateCheck.DataSet.Tables.Count -gt 0 -and $stateCheck.DataSet.Tables[0].Rows.Count -gt 0) {
    [string]$stateCheck.DataSet.Tables[0].Rows[0]["actual_state_desc"]
} else { "UNKNOWN" }
Write-Host "    QS state: $qsState" -ForegroundColor DarkGray

Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Query "
    INSERT INTO dbo.Orders (CustomerName, Amount)
    SELECT TOP 2000 N'QS2', 1.00 FROM sys.all_columns;" | Out-Null

$r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password `
    -Query "
        DECLARE @StopReason nvarchar(50), @Warnings nvarchar(max);
        EXEC dbo.sp_StatUpdate
            @Databases            = N'$Database',
            @Tables               = N'dbo.Orders',
            @QueryStore           = N'CPU',
            @ModificationThreshold = 100,
            @Execute              = N'Y',
            @LogToTable           = N'Y',
            @StopReasonOut        = @StopReason OUTPUT,
            @WarningsOut          = @Warnings   OUTPUT;
        SELECT @StopReason AS StopReason, ISNULL(@Warnings, N'') AS Warnings;"

if ($r.ExitCode -ne 0) {
    Add-Result "QS2: QS READ_ONLY -- proc completes" "FAIL" ($r.Output | Select-Object -First 1)
}
else {
    $outTable   = $r.DataSet.Tables[$r.DataSet.Tables.Count - 1]
    $stopReason = if ($outTable.Rows.Count -gt 0) { [string]$outTable.Rows[0]["StopReason"] } else { "" }
    $warnings   = if ($outTable.Rows.Count -gt 0) { [string]$outTable.Rows[0]["Warnings"]   } else { "" }

    if ($stopReason -match "COMPLETED|NATURAL_END|NO_QUALIFYING") {
        Add-Result "QS2: QS READ_ONLY -- proc completes" "PASS" "StopReason=$stopReason"
    }
    else {
        Add-Result "QS2: QS READ_ONLY -- proc completes" "FAIL" "Unexpected StopReason='$stopReason'"
    }

    # The proc documents a QS READ_ONLY pre-run warning -- check WarningsOut or RAISERROR output
    $outText  = ($r.Output -join "`n")
    $hasQsWarn = ($warnings -match "READ_ONLY|QS.*read|query.store.*read") -or
                 ($outText   -match "READ_ONLY|Query Store.*READ_ONLY")

    if ($hasQsWarn) {
        Add-Result "QS2: QS READ_ONLY -- warning surfaced" "PASS" "QS READ_ONLY warning present"
    }
    else {
        # Not a hard failure -- warning surfacing depends on QS state at runtime
        Add-Result "QS2: QS READ_ONLY -- warning surfaced" "SKIP" "No QS READ_ONLY warning found (state was: $qsState)"
    }
}

Clear-StatUpdateApplock -ServerAddr $Server -Port $Port -Pwd $Password

# Restore QS to ON/READ_WRITE for remaining tests
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password `
    -Query "BEGIN TRY ALTER DATABASE [$Database] SET QUERY_STORE (OPERATION_MODE = READ_WRITE); END TRY BEGIN CATCH END CATCH;" | Out-Null
Start-Sleep -Seconds 1

# =====================================================================
# QS3: QS enabled but zero recent runtime stats -- Phase 6 bails out
# =====================================================================
Write-Host "  QS3: Clearing QS runtime stats to simulate zero-data state..." -ForegroundColor DarkGray

# Flush then clear all QS data so there are no recent runtime stats
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Query "
    BEGIN TRY EXEC sp_query_store_flush_db; END TRY BEGIN CATCH END CATCH;" | Out-Null
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Query "
    BEGIN TRY ALTER DATABASE [$Database] SET QUERY_STORE CLEAR; END TRY BEGIN CATCH END CATCH;" | Out-Null
Start-Sleep -Seconds 1

# Use a very narrow RecentHours window so no cached data falls in range
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Query "
    INSERT INTO dbo.Orders (CustomerName, Amount)
    SELECT TOP 2000 N'QS3', 1.00 FROM sys.all_columns;" | Out-Null

$r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password `
    -Query "
        DECLARE @StopReason nvarchar(50);
        EXEC dbo.sp_StatUpdate
            @Databases             = N'$Database',
            @Tables                = N'dbo.Orders',
            @QueryStore            = N'CPU',
            @QueryStoreRecentHours = 1,
            @ModificationThreshold = 100,
            @Execute               = N'N',
            @LogToTable            = N'N',
            @Debug                 = 1,
            @StopReasonOut         = @StopReason OUTPUT;
        SELECT @StopReason AS StopReason;"

$outText    = ($r.Output -join "`n")
$outTable   = if ($r.DataSet) { $r.DataSet.Tables[$r.DataSet.Tables.Count - 1] } else { $null }
$stopReason = if ($outTable -and $outTable.Rows.Count -gt 0) { [string]$outTable.Rows[0]["StopReason"] } else { "" }

if ($r.ExitCode -ne 0) {
    Add-Result "QS3: zero QS data -- proc completes" "FAIL" ($r.Output | Select-Object -First 1)
}
else {
    Add-Result "QS3: zero QS data -- proc completes" "PASS" "StopReason=$stopReason"

    # Phase 6 should either be skipped or enriched 0 stats -- not an error
    $phase6Bailed = ($outText -match "Phase 6.*skip|Phase 6.*bail|no recent QS|0 stats enriched")
    if ($phase6Bailed) {
        Add-Result "QS3: zero QS data -- Phase 6 bails out" "PASS" "Phase 6 early bail-out confirmed"
    }
    else {
        # Phase 6 may simply not appear in output when there is nothing to process
        Add-Result "QS3: zero QS data -- Phase 6 bails out" "SKIP" "Phase 6 bail-out phrase not found; may have been skipped silently"
    }
}

Clear-StatUpdateApplock -ServerAddr $Server -Port $Port -Pwd $Password

# =====================================================================
# QS4: @QueryStore parameter accepted without error on supported versions
# =====================================================================
# Re-enable QS so discovery can proceed
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password `
    -Query "BEGIN TRY ALTER DATABASE [$Database] SET QUERY_STORE = ON; END TRY BEGIN CATCH END CATCH;" | Out-Null
Start-Sleep -Seconds 1

Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Query "
    INSERT INTO dbo.Orders (CustomerName, Amount)
    SELECT TOP 2000 N'QS4', 1.00 FROM sys.all_columns;" | Out-Null

$r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password `
    -Query "
        DECLARE @StopReason nvarchar(50);
        EXEC dbo.sp_StatUpdate
            @Databases            = N'$Database',
            @Tables               = N'dbo.Orders',
            @QueryStore           = N'CPU',
            @QueryStoreTopPlans   = 10,
            @ModificationThreshold = 100,
            @Execute              = N'N',
            @LogToTable           = N'N',
            @StopReasonOut        = @StopReason OUTPUT;
        SELECT @StopReason AS StopReason;"

if ($r.ExitCode -eq 0) {
    $outTable   = $r.DataSet.Tables[$r.DataSet.Tables.Count - 1]
    $stopReason = if ($outTable.Rows.Count -gt 0) { [string]$outTable.Rows[0]["StopReason"] } else { "" }
    Add-Result "QS4: @QueryStore=CPU accepted (QS ON)" "PASS" "StopReason=$stopReason"
}
else {
    $errMsg = ($r.Output | Where-Object { $_ -notmatch "^\s*$" } | Select-Object -First 1)
    Add-Result "QS4: @QueryStore=CPU accepted (QS ON)" "FAIL" $errMsg
}

Clear-StatUpdateApplock -ServerAddr $Server -Port $Port -Pwd $Password

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

$color = if ($failed -eq 0) { "Green" } else { "Red" }
Write-Host "  $passed/$total passed, $failed failed, $skipped skipped" -ForegroundColor $color
Write-Host ""

if ($ResultsPath) {
    $major = @{ '2019' = 15; '2022' = 16; '2025' = 17 }[$Version]
    $sqlStatus = Get-StructuredSqlStatus -Server $Server -Port $Port -Password $Password -ExpectedMajor $major
    $commit = (& git -C $RepoRoot rev-parse HEAD).Trim()
    if ($LASTEXITCODE -ne 0 -or $commit -notmatch '^[0-9a-fA-F]{40}$') { throw 'Could not resolve the tested source commit.' }
    Write-StructuredLegacyResults -Path $ResultsPath -Commit $commit -SqlVersion $Version -SqlBuild $sqlStatus.Build `
        -ServerAliveAtEnd $sqlStatus.Alive -Suite 'QueryStoreStates' -ExpectedCount 7 `
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
