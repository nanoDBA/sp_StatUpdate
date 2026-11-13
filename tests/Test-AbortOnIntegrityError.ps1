<#
.SYNOPSIS
    Targeted test suite for sp_StatUpdate @AbortOnIntegrityError parameter (f14h, gh-549).

.DESCRIPTION
    Tests the @AbortOnIntegrityError bit parameter introduced in gh-549.
    Integrity errors (823/824/825) cannot be induced in a normal container
    environment, so this suite tests the plumbing:

    T_AIE1: Proc accepts @AbortOnIntegrityError = 1 (default) in a dry run
            without raising an error.
    T_AIE2: Proc accepts @AbortOnIntegrityError = 0 in a dry run without
            raising an error.
    T_AIE3: SP_STATUPDATE_START CommandLog ExtendedInfo XML contains
            <AbortOnIntegrityError>1</AbortOnIntegrityError> when called
            with default (omitted) value.
    T_AIE4: SP_STATUPDATE_START CommandLog ExtendedInfo XML contains
            <AbortOnIntegrityError>0</AbortOnIntegrityError> when called
            with @AbortOnIntegrityError = 0.
    T_AIE5: @Help = 1 output includes a row for @AbortOnIntegrityError in
            the RS 3 parameter reference table.

    Target: SQL 2025 at 127.0.0.1,1431 the SQL login.
    DO NOT run this script -- the orchestrator runs it.

.PARAMETER Server
    Remote host IP.  Default: 100.88.59.72.

.PARAMETER Port
    SQL port.  Default: 1431 (SQL 2025 container).

.PARAMETER Password
    SA password.  Default: $env:SPSTATUPDATE_TEST_SQL_PASSWORD.

.PARAMETER SkipSetup
    Skip proc deployment (assumes already deployed).

.EXAMPLE
    .\tests\Test-AbortOnIntegrityError.ps1
    .\tests\Test-AbortOnIntegrityError.ps1 -Server 127.0.0.1 -Port 1431
    .\tests\Test-AbortOnIntegrityError.ps1 -SkipSetup
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
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot  = Split-Path -Parent $ScriptDir
$Port = @{ '2019' = 1434; '2022' = 1432; '2025' = 1431 }[$Version]
$startedAt = Get-Date
Import-Module (Join-Path $ScriptDir 'StructuredLegacyResults.psm1') -Force
if (-not $Password) { throw 'Release-gate SQL credentials are unavailable.' }
$ProcFile  = Join-Path $RepoRoot "sp_StatUpdate.sql"

# =====================================================================
# ADO.NET HELPER
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
        $cmd.CommandText   = $Query
        $cmd.CommandTimeout = $Timeout
        # Use ExecuteReader -- Fill() on SQL 2025 swallows InfoMessage events before they fire.
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
Write-Host " sp_StatUpdate @AbortOnIntegrityError Test Suite (f14h / gh-549)" -ForegroundColor Cyan
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
# STEP 2: DEPLOY PROC (optional)
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
# STEP 3: RUN TESTS
# =====================================================================
Write-Host "[3/4] Running 5 tests..." -ForegroundColor Yellow
Write-Host ""

$results = [System.Collections.ArrayList]::new()
function Add-Result {
    param([string]$Test, [string]$Result, [string]$Detail = "")
    $null = $results.Add([PSCustomObject]@{ Test = $Test; Result = $Result; Detail = $Detail })
}

$db = "StatUpdateTest"

# Ensure test DB exists (best-effort -- tests use dry-run against master if not)
Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password `
    -Query "IF DB_ID(N'StatUpdateTest') IS NULL CREATE DATABASE StatUpdateTest;" | Out-Null

# Ensure stat candidate exists for dry runs that need discovery to produce rows.
# Insert a small batch into a known table so at least one stat qualifies.
# Ignore failure -- master DB always has qualifying stats via @ModificationThreshold=1.
Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password -Db $db `
    -Query "IF OBJECT_ID(N'dbo.Orders') IS NOT NULL INSERT INTO dbo.Orders (CustomerName, Amount) SELECT TOP 100 N'AIEseed', 1.00 FROM sys.all_columns;" | Out-Null

# Clear any stale lock row
Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password `
    -Query "IF OBJECT_ID(N'dbo.StatUpdateLock') IS NOT NULL DELETE FROM dbo.StatUpdateLock WHERE LoginTime < DATEADD(MINUTE, -10, SYSDATETIME());" | Out-Null

# ================================================================
# T_AIE1: @AbortOnIntegrityError = 1 accepted without error (dry run)
# ================================================================
$r1 = Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password -Timeout 300 `
    -Query @"
EXEC dbo.sp_StatUpdate
    @Databases = N'master',
    @Execute   = N'N',
    @LogToTable = N'N',
    @AbortOnIntegrityError = 1;
"@

if ($r1.ExitCode -eq 0) {
    Add-Result "T_AIE1: @AbortOnIntegrityError=1 accepted in dry run" "PASS" "Proc completed without exception (ExitCode=0)"
} else {
    Add-Result "T_AIE1: @AbortOnIntegrityError=1 accepted in dry run" "FAIL" "Proc raised exception: $($r1.Output | Select-Object -First 1)"
}

# Clear lock between runs
Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password `
    -Query "IF OBJECT_ID(N'dbo.StatUpdateLock') IS NOT NULL DELETE FROM dbo.StatUpdateLock WHERE LoginTime < DATEADD(MINUTE, -10, SYSDATETIME());" | Out-Null

# ================================================================
# T_AIE2: @AbortOnIntegrityError = 0 accepted without error (dry run)
# ================================================================
$r2 = Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password -Timeout 300 `
    -Query @"
EXEC dbo.sp_StatUpdate
    @Databases = N'master',
    @Execute   = N'N',
    @LogToTable = N'N',
    @AbortOnIntegrityError = 0;
"@

if ($r2.ExitCode -eq 0) {
    Add-Result "T_AIE2: @AbortOnIntegrityError=0 accepted in dry run" "PASS" "Proc completed without exception (ExitCode=0)"
} else {
    Add-Result "T_AIE2: @AbortOnIntegrityError=0 accepted in dry run" "FAIL" "Proc raised exception: $($r2.Output | Select-Object -First 1)"
}

# Seed modifications for the CommandLog tests (T_AIE3 / T_AIE4) which use @Execute=Y.
Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password -Db $db `
    -Query "IF OBJECT_ID(N'dbo.Orders') IS NOT NULL INSERT INTO dbo.Orders (CustomerName, Amount) SELECT TOP 500 N'AIEseed3', 1.00 FROM sys.all_columns;" | Out-Null

# Clear lock
Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password `
    -Query "IF OBJECT_ID(N'dbo.StatUpdateLock') IS NOT NULL DELETE FROM dbo.StatUpdateLock WHERE LoginTime < DATEADD(MINUTE, -10, SYSDATETIME());" | Out-Null

# ================================================================
# T_AIE3: SP_STATUPDATE_START ExtendedInfo XML contains
#         <AbortOnIntegrityError>1</AbortOnIntegrityError>
#         when called with default value (omitted = 1).
# ================================================================
$r3 = Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password -Timeout 600 `
    -Query @"
EXEC dbo.sp_StatUpdate
    @Databases        = N'$db',
    @ModificationThreshold = 1,
    @LogToTable       = N'Y',
    @Execute          = N'Y';
"@

$t3Ok     = $false
$t3Detail = "SP_STATUPDATE_START row not found in CommandLog"

if ($r3.ExitCode -eq 0) {
    # Extract RunLabel from InfoMessage ("Run: <label> (logged to CommandLog)")
    $rOut3    = ($r3.Output -join "`n")
    $runLabel3 = if ($rOut3 -match 'Run:\s*(\S+)\s*\(logged') { $Matches[1] } else { $null }

    $whereClause3 = if ($runLabel3) {
        $safe3 = $runLabel3 -replace "'", "''"
        "WHERE CommandType = N'SP_STATUPDATE_START' AND ExtendedInfo.value('(/Parameters/RunLabel)[1]','nvarchar(100)') = N'$safe3'"
    } else {
        "WHERE CommandType = N'SP_STATUPDATE_START' ORDER BY StartTime DESC"
    }

    $rXml3 = Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password `
        -Query "SELECT TOP 1 ExtendedInfo FROM dbo.CommandLog $whereClause3;"

    if ($rXml3.ExitCode -eq 0 -and $rXml3.DataSet -and
        $rXml3.DataSet.Tables.Count -gt 0 -and $rXml3.DataSet.Tables[0].Rows.Count -gt 0) {

        $raw3     = [string]$rXml3.DataSet.Tables[0].Rows[0]["ExtendedInfo"]
        $safeRaw3 = $raw3 -replace "'", "''"
        $rVal3    = Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password `
            -Query "SELECT CAST(N'$safeRaw3' AS xml).value('(/Parameters/AbortOnIntegrityError)[1]','int') AS Val;"

        if ($rVal3.ExitCode -eq 0 -and $rVal3.DataSet -and
            $rVal3.DataSet.Tables.Count -gt 0 -and $rVal3.DataSet.Tables[0].Rows.Count -gt 0) {
            $xmlVal3 = $rVal3.DataSet.Tables[0].Rows[0]["Val"]
            if ($xmlVal3 -ne [DBNull]::Value -and [int]$xmlVal3 -eq 1) {
                $t3Ok     = $true
                $t3Detail = "SP_STATUPDATE_START XML: AbortOnIntegrityError = 1 (default)"
            } else {
                $t3Detail = "SP_STATUPDATE_START XML: AbortOnIntegrityError = '$xmlVal3' (expected 1)"
            }
        } else {
            $t3Detail = "XML value query failed: $($rVal3.Output | Select-Object -First 1)"
        }
    }
} else {
    $t3Detail = "Proc raised exception: $($r3.Output | Select-Object -First 1)"
}
Add-Result "T_AIE3: AbortOnIntegrityError=1 in START XML (default omitted)" $(if ($t3Ok) { "PASS" } else { "FAIL" }) $t3Detail

# Seed again for T_AIE4
Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password -Db $db `
    -Query "IF OBJECT_ID(N'dbo.Orders') IS NOT NULL INSERT INTO dbo.Orders (CustomerName, Amount) SELECT TOP 500 N'AIEseed4', 1.00 FROM sys.all_columns;" | Out-Null

# Clear lock
Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password `
    -Query "IF OBJECT_ID(N'dbo.StatUpdateLock') IS NOT NULL DELETE FROM dbo.StatUpdateLock WHERE LoginTime < DATEADD(MINUTE, -10, SYSDATETIME());" | Out-Null

# ================================================================
# T_AIE4: SP_STATUPDATE_START ExtendedInfo XML contains
#         <AbortOnIntegrityError>0</AbortOnIntegrityError>
#         when called with @AbortOnIntegrityError = 0.
# ================================================================
$r4 = Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password -Timeout 600 `
    -Query @"
EXEC dbo.sp_StatUpdate
    @Databases             = N'$db',
    @ModificationThreshold = 1,
    @LogToTable            = N'Y',
    @Execute               = N'Y',
    @AbortOnIntegrityError = 0;
"@

$t4Ok     = $false
$t4Detail = "SP_STATUPDATE_START row not found in CommandLog"

if ($r4.ExitCode -eq 0) {
    $rOut4    = ($r4.Output -join "`n")
    $runLabel4 = if ($rOut4 -match 'Run:\s*(\S+)\s*\(logged') { $Matches[1] } else { $null }

    $whereClause4 = if ($runLabel4) {
        $safe4 = $runLabel4 -replace "'", "''"
        "WHERE CommandType = N'SP_STATUPDATE_START' AND ExtendedInfo.value('(/Parameters/RunLabel)[1]','nvarchar(100)') = N'$safe4'"
    } else {
        "WHERE CommandType = N'SP_STATUPDATE_START' ORDER BY StartTime DESC"
    }

    $rXml4 = Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password `
        -Query "SELECT TOP 1 ExtendedInfo FROM dbo.CommandLog $whereClause4;"

    if ($rXml4.ExitCode -eq 0 -and $rXml4.DataSet -and
        $rXml4.DataSet.Tables.Count -gt 0 -and $rXml4.DataSet.Tables[0].Rows.Count -gt 0) {

        $raw4     = [string]$rXml4.DataSet.Tables[0].Rows[0]["ExtendedInfo"]
        $safeRaw4 = $raw4 -replace "'", "''"
        $rVal4    = Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password `
            -Query "SELECT CAST(N'$safeRaw4' AS xml).value('(/Parameters/AbortOnIntegrityError)[1]','int') AS Val;"

        if ($rVal4.ExitCode -eq 0 -and $rVal4.DataSet -and
            $rVal4.DataSet.Tables.Count -gt 0 -and $rVal4.DataSet.Tables[0].Rows.Count -gt 0) {
            $xmlVal4 = $rVal4.DataSet.Tables[0].Rows[0]["Val"]
            if ($xmlVal4 -ne [DBNull]::Value -and [int]$xmlVal4 -eq 0) {
                $t4Ok     = $true
                $t4Detail = "SP_STATUPDATE_START XML: AbortOnIntegrityError = 0"
            } else {
                $t4Detail = "SP_STATUPDATE_START XML: AbortOnIntegrityError = '$xmlVal4' (expected 0)"
            }
        } else {
            $t4Detail = "XML value query failed: $($rVal4.Output | Select-Object -First 1)"
        }
    }
} else {
    $t4Detail = "Proc raised exception: $($r4.Output | Select-Object -First 1)"
}
Add-Result "T_AIE4: AbortOnIntegrityError=0 in START XML (explicit 0)" $(if ($t4Ok) { "PASS" } else { "FAIL" }) $t4Detail

# Clear lock
Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password `
    -Query "IF OBJECT_ID(N'dbo.StatUpdateLock') IS NOT NULL DELETE FROM dbo.StatUpdateLock WHERE LoginTime < DATEADD(MINUTE, -10, SYSDATETIME());" | Out-Null

# ================================================================
# T_AIE5: @Help = 1 output includes @AbortOnIntegrityError row
#         in the RS 3 parameter reference table.
# ================================================================
$r5 = Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password -Timeout 60 `
    -Query "EXEC dbo.sp_StatUpdate @Help = N'1';"

$t5Ok     = $false
$t5Detail = "@AbortOnIntegrityError row not found in @Help RS 3 output"

if ($r5.ExitCode -eq 0) {
    # @Help returns multiple result sets.  RS 3 (index 2) is the parameter table.
    # Search all tables in the DataSet for a row where parameter = @AbortOnIntegrityError.
    $found = $false
    if ($r5.DataSet -and $r5.DataSet.Tables.Count -gt 0) {
        foreach ($tbl in $r5.DataSet.Tables) {
            if ($tbl.Columns.Contains("parameter")) {
                foreach ($row in $tbl.Rows) {
                    if ([string]$row["parameter"] -eq "@AbortOnIntegrityError") {
                        $found = $true
                        break
                    }
                }
            }
            if ($found) { break }
        }
    }
    if ($found) {
        $t5Ok     = $true
        $t5Detail = "@AbortOnIntegrityError found in @Help RS 3 parameter table"
    } else {
        $t5Detail = "@AbortOnIntegrityError row absent from @Help RS 3 parameter table (searched $($r5.DataSet.Tables.Count) result sets)"
    }
} else {
    $t5Detail = "Proc raised exception: $($r5.Output | Select-Object -First 1)"
}
Add-Result "T_AIE5: @Help output includes @AbortOnIntegrityError row" $(if ($t5Ok) { "PASS" } else { "FAIL" }) $t5Detail

# =====================================================================
# STEP 4: RESULTS SUMMARY
# =====================================================================
Write-Host ""
Write-Host "[4/4] Results" -ForegroundColor Yellow
Write-Host ""
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host " RESULTS" -ForegroundColor Cyan
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host ""

$passed  = @($results | Where-Object { $_.Result -eq "PASS" }).Count
$failed  = @($results | Where-Object { $_.Result -eq "FAIL" }).Count
$total   = $results.Count

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
Write-Host ("{0,-6} {1,-8} {2,-6}" -f "Tests", "Passed", "Status")
Write-Host ("{0,-6} {1,-8} {2,-6}" -f "-----", "------", "------")
$status = if ($failed -eq 0) { "PASS" } else { "FAIL" }
$sc     = if ($failed -eq 0) { "Green" } else { "Red" }
Write-Host ("{0,-6} {1,-8} {2,-6}" -f $total, $passed, $status) -ForegroundColor $sc
Write-Host ""

if ($ResultsPath) {
    $major = @{ '2019' = 15; '2022' = 16; '2025' = 17 }[$Version]
    $sqlStatus = Get-StructuredSqlStatus -Server $Server -Port $Port -Password $Password -ExpectedMajor $major
    $commit = (& git -C $RepoRoot rev-parse HEAD).Trim()
    if ($LASTEXITCODE -ne 0 -or $commit -notmatch '^[0-9a-fA-F]{40}$') { throw 'Could not resolve the tested source commit.' }
    Write-StructuredLegacyResults -Path $ResultsPath -Commit $commit -SqlVersion $Version -SqlBuild $sqlStatus.Build `
        -ServerAliveAtEnd $sqlStatus.Alive -Suite 'AbortOnIntegrityError' -ExpectedCount 5 `
        -Results @($results) -StartedAt $startedAt -EndedAt (Get-Date)
}

if ($failed -gt 0 -or $skipped -gt 0) {
    Write-Host "RESULT: $failed test(s) FAILED." -ForegroundColor Red
    exit 1
} else {
    Write-Host "RESULT: All $total tests PASSED." -ForegroundColor Green
    exit 0
}
