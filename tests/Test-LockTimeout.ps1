<#
.SYNOPSIS
    @LockTimeout regression guard -- ensures the proc restores the caller's LOCK_TIMEOUT.

.DESCRIPTION
    Tests that sp_StatUpdate does not leak its internal @LockTimeout setting into the
    caller's session.  Covers:
    - L1: Custom @LockTimeout does not persist (session had 3000, proc used 5)
    - L2: @LockTimeout=NULL preserves session default (-1 / infinite)
    - L3: Custom session LOCK_TIMEOUT preserved after real execution
    - L4: @LockTimeout accepted in dry run (no error)

    Each test sets LOCK_TIMEOUT and calls the proc in a single batch so the
    session-level setting is visible before and after.

.PARAMETER Server
    Remote Docker host IP. Default: 100.88.59.72.

.PARAMETER Port
    SQL Server port. Default: 1432 (SQL 2022 on remote host).

.PARAMETER Password
    SA password. Default: $env:SPSTATUPDATE_TEST_SQL_PASSWORD.

.PARAMETER Database
    Test database name. Default: StatUpdateTest.

.EXAMPLE
    .\tests\Test-LockTimeout.ps1
    .\tests\Test-LockTimeout.ps1 -Port 1431
    .\tests\Test-LockTimeout.ps1 -Server 127.0.0.1 -Port 1433
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
        [string]$Db = "master",
        [int]$Timeout = 300
    )
    $connStr = "Server=$ServerAddr,$Port;Database=$Db;User Id=sa;Password=$Pwd;Encrypt=false;TrustServerCertificate=true;Pooling=false;Connection Timeout=30"
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
        $cmd.CommandText = $Query
        $cmd.CommandTimeout = $Timeout

        $adapter = [System.Data.SqlClient.SqlDataAdapter]::new($cmd)
        $ds = [System.Data.DataSet]::new()
        $null = $adapter.Fill($ds)

        $conn.Close()

        return @{
            Output   = $messages
            DataSet  = $ds
            ExitCode = 0
        }
    }
    catch {
        return @{
            Output   = @($_.Exception.InnerException.Message, $_.Exception.Message) + $messages
            DataSet  = $null
            ExitCode = 1
        }
    }
}

$results = [System.Collections.ArrayList]::new()

function Add-Result {
    param([string]$Test, [string]$Result, [string]$Detail = "")
    $null = $results.Add([PSCustomObject]@{ Test = $Test; Result = $Result; Detail = $Detail })
    $color = switch ($Result) { "PASS" { "Green" } "FAIL" { "Red" } "SKIP" { "DarkGray" } }
    $d = if ($Detail) { " ($Detail)" } else { "" }
    Write-Host "  [$Result] $Test$d" -ForegroundColor $color
}

# =====================================================================
# HEADER
# =====================================================================
Write-Host ""
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host " @LockTimeout Regression Guard" -ForegroundColor Cyan
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host ""
Write-Host "  Server:   ${Server}:${Port}"
Write-Host "  Database: $Database"
Write-Host ""

# =====================================================================
# CONNECTIVITY CHECK
# =====================================================================
Write-Host "[Setup] Verifying connectivity..." -ForegroundColor Yellow
$vr = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "SELECT CONVERT(int, SERVERPROPERTY(N'ProductMajorVersion')) AS MajorVer;"
if ($vr.ExitCode -ne 0) {
    Write-Host "  ERROR: Cannot connect to ${Server}:${Port}" -ForegroundColor Red
    exit 1
}
$serverMajor = [int]$vr.DataSet.Tables[0].Rows[0]["MajorVer"]
Write-Host "  Connected (SQL major version: $serverMajor)" -ForegroundColor Green
Write-Host ""

# =====================================================================
# SETUP: Generate modifications for L3 (real execution)
# =====================================================================
Write-Host "[Setup] Generating modifications for L3..." -ForegroundColor Yellow
Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Db $Database -Query "
    INSERT INTO dbo.Orders (CustomerName, Amount)
    SELECT TOP 5000 N'LockTest', 1.00 FROM sys.all_columns;
" | Out-Null
Write-Host "  Done" -ForegroundColor Green
Write-Host ""

# =====================================================================
# TESTS
# =====================================================================
Write-Host "[Tests] Running @LockTimeout regression checks..." -ForegroundColor Yellow
Write-Host ""

# ------------------------------------------------------------------
# L1: @LockTimeout does not persist to session
#     Session sets 3000ms, proc uses 5ms.  After proc returns,
#     session should still be 3000ms.
# ------------------------------------------------------------------
$r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
    SET LOCK_TIMEOUT 3000;
    DECLARE @before int = @@LOCK_TIMEOUT;
    EXEC dbo.sp_StatUpdate
        @Databases = N'$Database',
        @LockTimeout = 5,
        @Execute = N'N';
    DECLARE @after int = @@LOCK_TIMEOUT;
    SELECT @before AS BeforeTimeout, @after AS AfterTimeout;
"
if ($r.ExitCode -ne 0) {
    $errMsg = ($r.Output | Select-Object -First 1)
    Add-Result "L1: @LockTimeout does not persist to session" "FAIL" "Execution error: $errMsg"
} elseif ($r.DataSet -and $r.DataSet.Tables.Count -gt 0) {
    $lastTbl = $r.DataSet.Tables[$r.DataSet.Tables.Count - 1]
    if ($lastTbl.Columns.Contains("BeforeTimeout") -and $lastTbl.Rows.Count -gt 0) {
        $row = $lastTbl.Rows[0]
        $before = [int]$row["BeforeTimeout"]
        $after  = [int]$row["AfterTimeout"]
        if ($before -eq 3000 -and $after -eq 3000) {
            Add-Result "L1: @LockTimeout does not persist to session" "PASS" "Before=$before, After=$after"
        } else {
            Add-Result "L1: @LockTimeout does not persist to session" "FAIL" "Before=$before, After=$after (expected 3000/3000)"
        }
    } else {
        Add-Result "L1: @LockTimeout does not persist to session" "FAIL" "BeforeTimeout column not found in last result set"
    }
} else {
    Add-Result "L1: @LockTimeout does not persist to session" "FAIL" "No result set returned"
}

# ------------------------------------------------------------------
# L2: @LockTimeout=NULL preserves session default
#     Session default is -1 (infinite).  Proc called without
#     @LockTimeout (NULL default).  Session should stay -1.
# ------------------------------------------------------------------
$r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
    SET LOCK_TIMEOUT -1;
    DECLARE @before int = @@LOCK_TIMEOUT;
    EXEC dbo.sp_StatUpdate
        @Databases = N'$Database',
        @Execute = N'N';
    DECLARE @after int = @@LOCK_TIMEOUT;
    SELECT @before AS BeforeTimeout, @after AS AfterTimeout;
"
if ($r.ExitCode -ne 0) {
    $errMsg = ($r.Output | Select-Object -First 1)
    Add-Result "L2: @LockTimeout=NULL preserves session default" "FAIL" "Execution error: $errMsg"
} elseif ($r.DataSet -and $r.DataSet.Tables.Count -gt 0) {
    $lastTbl = $r.DataSet.Tables[$r.DataSet.Tables.Count - 1]
    if ($lastTbl.Columns.Contains("BeforeTimeout") -and $lastTbl.Rows.Count -gt 0) {
        $row = $lastTbl.Rows[0]
        $before = [int]$row["BeforeTimeout"]
        $after  = [int]$row["AfterTimeout"]
        if ($before -eq -1 -and $after -eq -1) {
            Add-Result "L2: @LockTimeout=NULL preserves session default" "PASS" "Before=$before, After=$after"
        } else {
            Add-Result "L2: @LockTimeout=NULL preserves session default" "FAIL" "Before=$before, After=$after (expected -1/-1)"
        }
    } else {
        Add-Result "L2: @LockTimeout=NULL preserves session default" "FAIL" "BeforeTimeout column not found in last result set"
    }
} else {
    Add-Result "L2: @LockTimeout=NULL preserves session default" "FAIL" "No result set returned"
}

# ------------------------------------------------------------------
# L3: Custom session LOCK_TIMEOUT preserved after execution
#     Session sets 5000ms, proc uses 60ms, actual execution happens.
#     After proc returns, session should still be 5000ms.
# ------------------------------------------------------------------
$r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
    SET LOCK_TIMEOUT 5000;
    DECLARE @before int = @@LOCK_TIMEOUT;
    EXEC dbo.sp_StatUpdate
        @Databases = N'$Database',
        @LockTimeout = 60,
        @ModificationThreshold = 100,
        @Execute = N'Y';
    DECLARE @after int = @@LOCK_TIMEOUT;
    SELECT @before AS BeforeTimeout, @after AS AfterTimeout;
"
if ($r.ExitCode -ne 0) {
    $errMsg = ($r.Output | Select-Object -First 1)
    Add-Result "L3: Custom LOCK_TIMEOUT preserved after execution" "FAIL" "Execution error: $errMsg"
} elseif ($r.DataSet -and $r.DataSet.Tables.Count -gt 0) {
    $lastTbl = $r.DataSet.Tables[$r.DataSet.Tables.Count - 1]
    if ($lastTbl.Columns.Contains("BeforeTimeout") -and $lastTbl.Rows.Count -gt 0) {
        $row = $lastTbl.Rows[0]
        $before = [int]$row["BeforeTimeout"]
        $after  = [int]$row["AfterTimeout"]
        if ($after -eq 5000) {
            Add-Result "L3: Custom LOCK_TIMEOUT preserved after execution" "PASS" "Before=$before, After=$after"
        } else {
            Add-Result "L3: Custom LOCK_TIMEOUT preserved after execution" "FAIL" "Before=$before, After=$after (expected After=5000)"
        }
    } else {
        Add-Result "L3: Custom LOCK_TIMEOUT preserved after execution" "FAIL" "BeforeTimeout column not found in last result set"
    }
} else {
    Add-Result "L3: Custom LOCK_TIMEOUT preserved after execution" "FAIL" "No result set returned"
}

# ------------------------------------------------------------------
# L4: @LockTimeout accepted in dry run
#     Verifies @LockTimeout=1 compiles and runs without error in
#     dry-run mode.
# ------------------------------------------------------------------
$r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
    EXEC dbo.sp_StatUpdate
        @Databases = N'$Database',
        @LockTimeout = 1,
        @Execute = N'N';
"
if ($r.ExitCode -eq 0) {
    Add-Result "L4: @LockTimeout accepted in dry run" "PASS" ""
} else {
    $errMsg = ($r.Output | Select-Object -First 1)
    Add-Result "L4: @LockTimeout accepted in dry run" "FAIL" $errMsg
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

$color = if ($failed -eq 0) { "Green" } else { "Red" }
Write-Host "  $passed/$total passed, $failed failed, $skipped skipped" -ForegroundColor $color
Write-Host ""

if ($ResultsPath) {
    $major = @{ '2019' = 15; '2022' = 16; '2025' = 17 }[$Version]
    $sqlStatus = Get-StructuredSqlStatus -Server $Server -Port $Port -Password $Password -ExpectedMajor $major
    $commit = (& git -C $RepoRoot rev-parse HEAD).Trim()
    if ($LASTEXITCODE -ne 0 -or $commit -notmatch '^[0-9a-fA-F]{40}$') { throw 'Could not resolve the tested source commit.' }
    Write-StructuredLegacyResults -Path $ResultsPath -Commit $commit -SqlVersion $Version -SqlBuild $sqlStatus.Build `
        -ServerAliveAtEnd $sqlStatus.Alive -Suite 'LockTimeout' -ExpectedCount 4 `
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
