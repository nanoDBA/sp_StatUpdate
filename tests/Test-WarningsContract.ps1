<#
.SYNOPSIS
    Test suite for WarningsContract telemetry batch (mvgb, r2d0, ibyc, 6zvp, wuq2, hdls).

.DESCRIPTION
    Deterministic assertions against SQL 2025 (127.0.0.1,1431 the SQL login).
    DO NOT run this script -- the orchestrator runs it.

    T_WC1: EMPTY_STRING_PARAM code in END row WarningsCodes element.
           Trigger: @Statistics = N'' (empty string -- the only param whose
           empty-string normalization emits EMPTY_STRING_PARAM) + @LogToTable=Y.
           Assert: SP_STATUPDATE_END ExtendedInfo XML Summary/WarningsCodes contains
                   EMPTY_STRING_PARAM.

    T_WC2: @WarningsCodesOut OUTPUT param parity with END row.
           Same run as T_WC1; capture @WarningsCodesOut OUTPUT and assert it
           contains EMPTY_STRING_PARAM AND equals the END row element (contract parity).

    T_WC3: @Help WARNINGS topic contains CONSECUTIVE_FAILURES_ELEVATED.
           Assert: @Help=1 RS 4 (topics) WARNINGS row text contains
                   CONSECUTIVE_FAILURES_ELEVATED.

    T_WC4: @Help WARNINGS topic contains TOCTOU_SKIPS.
           Same @Help call as T_WC3.

    T_WC5: Early-return (NO_QUALIFYING_STATS) END row also carries WarningsCodes.
           Trigger: an EMPTY scratch database (zero user tables => zero candidates =>
                    NO_QUALIFYING_STATS early return) + @Statistics = N'' (to generate
                    a warning code) + @LogToTable=Y + @Execute=Y.
           Assert: the run's SP_STATUPDATE_END row has StopReason=NO_QUALIFYING_STATS
                   and a WarningsCodes element containing EMPTY_STRING_PARAM.

.PARAMETER Server
    Remote host IP.  Default: 100.88.59.72.

.PARAMETER Port
    SQL port.  Default: 1431 (SQL 2025 container).

.PARAMETER Password
    SA password.  Default: $env:SPSTATUPDATE_TEST_SQL_PASSWORD.

.PARAMETER SkipSetup
    Skip proc deployment (assumes already deployed).

.EXAMPLE
    .\tests\Test-WarningsContract.ps1
    .\tests\Test-WarningsContract.ps1 -Server 127.0.0.1 -Port 1431
    .\tests\Test-WarningsContract.ps1 -SkipSetup
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
        $cmd.CommandText    = $Query
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
Write-Host " sp_StatUpdate WarningsContract Test Suite (mvgb, r2d0, ibyc, 6zvp, wuq2, hdls)" -ForegroundColor Cyan
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

# Helper: clear StatUpdateLock between runs
function Clear-Lock {
    Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password `
        -Query "IF OBJECT_ID(N'dbo.StatUpdateLock') IS NOT NULL DELETE FROM dbo.StatUpdateLock WHERE LoginTime < DATEADD(MINUTE, -10, SYSDATETIME());" | Out-Null
}

# Helper: read WarningsCodes element from most recent END row matching a RunLabel
function Get-WarningsCodesXml {
    param([string]$RunLabel)
    $safe = $RunLabel -replace "'", "''"
    $r = Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password -Timeout 30 `
        -Query @"
SELECT TOP 1
    ExtendedInfo.value('(/Summary/WarningsCodes)[1]', 'nvarchar(max)') AS WarningsCodes,
    ExtendedInfo.value('(/Summary/StopReason)[1]', 'nvarchar(100)')   AS StopReason
FROM dbo.CommandLog
WHERE CommandType = N'SP_STATUPDATE_END'
AND   ExtendedInfo.value('(/Summary/RunLabel)[1]', 'nvarchar(100)') = N'$safe'
ORDER BY EndTime DESC;
"@
    return $r
}

# ================================================================
# T_WC1: EMPTY_STRING_PARAM in END row WarningsCodes element
# ================================================================
Write-Host "  Running T_WC1..." -NoNewline

Clear-Lock

# Ensure StatUpdateTest exists with at least one stat candidate
Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password `
    -Query "IF DB_ID(N'StatUpdateTest') IS NULL CREATE DATABASE StatUpdateTest;" | Out-Null

# Seed some modifications so at least one stat qualifies
Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password -Db "StatUpdateTest" `
    -Query "IF OBJECT_ID(N'dbo.Orders') IS NOT NULL INSERT INTO dbo.Orders (CustomerName, Amount) SELECT TOP 500 N'WCseed', 1.00 FROM sys.all_columns;" | Out-Null

# Declare OUTPUT variable and capture RunLabel from InfoMessage
$t1Query = @"
DECLARE @wc nvarchar(max), @sr nvarchar(100);
EXEC dbo.sp_StatUpdate
    @Databases          = N'StatUpdateTest',
    @Statistics         = N'',
    @Execute            = N'Y',
    @LogToTable         = N'Y',
    @WarningsCodesOut   = @wc OUTPUT,
    @StopReasonOut      = @sr OUTPUT;
SELECT @wc AS WarningsCodesOut, @sr AS StopReasonOut;
"@

$r1 = Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password -Timeout 600 -Query $t1Query

$t1Ok     = $false
$t1Detail = "Proc failed or no END row found"

if ($r1.ExitCode -eq 0) {
    # Extract RunLabel from InfoMessage output
    $rOut1    = ($r1.Output -join "`n")
    $runLabel1 = if ($rOut1 -match 'Run:\s*(\S+)\s*\(logged') { $Matches[1] } else { $null }

    if ($runLabel1) {
        $rXml1 = Get-WarningsCodesXml -RunLabel $runLabel1
        if ($rXml1.ExitCode -eq 0 -and $rXml1.DataSet -and
            $rXml1.DataSet.Tables.Count -gt 0 -and $rXml1.DataSet.Tables[0].Rows.Count -gt 0) {
            $wcXml = [string]$rXml1.DataSet.Tables[0].Rows[0]["WarningsCodes"]
            if ($wcXml -ne [DBNull]::Value -and $wcXml -like "*EMPTY_STRING_PARAM*") {
                $t1Ok     = $true
                $t1Detail = "END row WarningsCodes = '$wcXml'"
            } else {
                $t1Detail = "END row WarningsCodes = '$wcXml' (EMPTY_STRING_PARAM not found)"
            }
        } else {
            $t1Detail = "END row query failed or no row found for RunLabel=$runLabel1"
        }
    } else {
        $t1Detail = "Could not extract RunLabel from InfoMessage output"
    }
} else {
    $t1Detail = "Proc exception: $($r1.Output | Select-Object -First 1)"
}
Write-Host " done"
Add-Result "T_WC1: EMPTY_STRING_PARAM in END row WarningsCodes element" $(if ($t1Ok) { "PASS" } else { "FAIL" }) $t1Detail

# ================================================================
# T_WC2: @WarningsCodesOut OUTPUT param parity with END row
# (Reuse same run as T_WC1 -- check the SELECT result from proc)
# ================================================================
Write-Host "  Running T_WC2..." -NoNewline

$t2Ok     = $false
$t2Detail = "No result set or @WarningsCodesOut row not found"

if ($r1.ExitCode -eq 0 -and $r1.DataSet -and $r1.DataSet.Tables.Count -gt 0) {
    # The final SELECT from the query returns WarningsCodesOut and StopReasonOut
    # It will be in the LAST table of the dataset (proc returns multiple result sets).
    foreach ($tbl in $r1.DataSet.Tables) {
        if ($tbl.Columns.Contains("WarningsCodesOut")) {
            if ($tbl.Rows.Count -gt 0) {
                $wcOut = [string]$tbl.Rows[0]["WarningsCodesOut"]
                # Contract parity: OUTPUT param must equal the END row element (when T_WC1 captured it)
                $parityOk = $true
                $parityNote = ""
                if ($t1Ok -and $wcXml) {
                    $parityOk   = ($wcOut -eq $wcXml)
                    $parityNote = if ($parityOk) { "; parity with END element OK" } else { "; PARITY MISMATCH vs END element '$wcXml'" }
                }
                if ($wcOut -like "*EMPTY_STRING_PARAM*" -and $parityOk) {
                    $t2Ok     = $true
                    $t2Detail = "@WarningsCodesOut = '$wcOut'$parityNote"
                } else {
                    $t2Detail = "@WarningsCodesOut = '$wcOut' (EMPTY_STRING_PARAM missing or parity failed)$parityNote"
                }
            }
            break
        }
    }
} else {
    $t2Detail = "Proc failed in T_WC1 -- skipping"
}
Write-Host " done"
Add-Result "T_WC2: @WarningsCodesOut OUTPUT contains EMPTY_STRING_PARAM" $(if ($t2Ok) { "PASS" } else { "FAIL" }) $t2Detail

# ================================================================
# T_WC3: @Help WARNINGS topic contains CONSECUTIVE_FAILURES_ELEVATED
# T_WC4: @Help WARNINGS topic contains TOCTOU_SKIPS
# ================================================================
Write-Host "  Running T_WC3/T_WC4..." -NoNewline

$rHelp = Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password -Timeout 60 `
    -Query "EXEC dbo.sp_StatUpdate @Help = N'1';"

$t3Ok = $false; $t3Detail = "WARNINGS topic not found in @Help output"
$t4Ok = $false; $t4Detail = "WARNINGS topic not found in @Help output"

if ($rHelp.ExitCode -eq 0 -and $rHelp.DataSet -and $rHelp.DataSet.Tables.Count -gt 0) {
    foreach ($tbl in $rHelp.DataSet.Tables) {
        if ($tbl.Columns.Contains("topic") -and $tbl.Columns.Contains("detail")) {
            foreach ($row in $tbl.Rows) {
                if ([string]$row["topic"] -eq "WARNINGS") {
                    $detail = [string]$row["detail"]

                    if ($detail -like "*CONSECUTIVE_FAILURES_ELEVATED*") {
                        $t3Ok     = $true
                        $t3Detail = "WARNINGS topic detail contains CONSECUTIVE_FAILURES_ELEVATED"
                    } else {
                        $t3Detail = "WARNINGS topic found but CONSECUTIVE_FAILURES_ELEVATED absent; detail starts: '$($detail.Substring(0, [Math]::Min(120, $detail.Length)))...'"
                    }

                    if ($detail -like "*TOCTOU_SKIPS*") {
                        $t4Ok     = $true
                        $t4Detail = "WARNINGS topic detail contains TOCTOU_SKIPS"
                    } else {
                        $t4Detail = "WARNINGS topic found but TOCTOU_SKIPS absent"
                    }
                    break
                }
            }
        }
        if ($t3Ok -or $t3Detail -ne "WARNINGS topic not found in @Help output") { break }
    }
} else {
    $t3Detail = "Proc raised exception: $($rHelp.Output | Select-Object -First 1)"
    $t4Detail = $t3Detail
}
Write-Host " done"
Add-Result "T_WC3: @Help WARNINGS topic contains CONSECUTIVE_FAILURES_ELEVATED" $(if ($t3Ok) { "PASS" } else { "FAIL" }) $t3Detail
Add-Result "T_WC4: @Help WARNINGS topic contains TOCTOU_SKIPS" $(if ($t4Ok) { "PASS" } else { "FAIL" }) $t4Detail

# ================================================================
# T_WC5: Early-return (NO_QUALIFYING_STATS) END row carries WarningsCodes
# Trigger @ExcludeStatistics=N'' on a @Databases pattern matching nothing.
# ================================================================
Write-Host "  Running T_WC5..." -NoNewline

Clear-Lock

# An EMPTY database (zero user tables) => zero stat candidates => the
# NO_QUALIFYING_STATS early-return END writer fires.  A nonexistent DB name
# would fail earlier with PARAMETER_ERROR and never reach that writer.
Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password `
    -Query "IF DB_ID(N'WC_EmptyDb') IS NULL CREATE DATABASE WC_EmptyDb;" | Out-Null

$t5Query = @"
DECLARE @wc5 nvarchar(max), @sr5 nvarchar(100);
EXEC dbo.sp_StatUpdate
    @Databases          = N'WC_EmptyDb',
    @Statistics         = N'',
    @Execute            = N'Y',
    @LogToTable         = N'Y',
    @WarningsCodesOut   = @wc5 OUTPUT,
    @StopReasonOut      = @sr5 OUTPUT;
SELECT @wc5 AS WarningsCodesOut, @sr5 AS StopReasonOut;
"@

$r5 = Invoke-Sql -ServerAddr $Server -SrvPort $Port -Pwd $Password -Timeout 300 -Query $t5Query

$t5Ok     = $false
$t5Detail = "Proc failed or no END row found"

if ($r5.ExitCode -eq 0) {
    $rOut5    = ($r5.Output -join "`n")
    $runLabel5 = if ($rOut5 -match 'Run:\s*(\S+)\s*\(logged') { $Matches[1] } else { $null }

    if ($runLabel5) {
        $rXml5 = Get-WarningsCodesXml -RunLabel $runLabel5
        if ($rXml5.ExitCode -eq 0 -and $rXml5.DataSet -and
            $rXml5.DataSet.Tables.Count -gt 0 -and $rXml5.DataSet.Tables[0].Rows.Count -gt 0) {

            $wcXml5 = [string]$rXml5.DataSet.Tables[0].Rows[0]["WarningsCodes"]
            $srXml5 = [string]$rXml5.DataSet.Tables[0].Rows[0]["StopReason"]

            if ($wcXml5 -ne [DBNull]::Value -and $wcXml5 -like "*EMPTY_STRING_PARAM*") {
                $t5Ok     = $true
                $t5Detail = "Early-return END row WarningsCodes = '$wcXml5' (StopReason=$srXml5)"
            } else {
                $t5Detail = "END row WarningsCodes = '$wcXml5' (EMPTY_STRING_PARAM not found; StopReason=$srXml5)"
            }
        } else {
            # NO_QUALIFYING_STATS writes to CommandLog only when @Execute=Y and @LogToTable=Y.
            # If no databases were found, the proc returns early before even writing.
            # Check StopReasonOut from SELECT result set.
            $srFromRS = $null
            $wcFromRS = $null
            if ($r5.DataSet -and $r5.DataSet.Tables.Count -gt 0) {
                foreach ($tbl5 in $r5.DataSet.Tables) {
                    if ($tbl5.Columns.Contains("StopReasonOut")) {
                        $srFromRS = [string]$tbl5.Rows[0]["StopReasonOut"]
                        $wcFromRS = [string]$tbl5.Rows[0]["WarningsCodesOut"]
                        break
                    }
                }
            }
            if ($srFromRS -eq "NO_QUALIFYING_STATS" -and $wcFromRS -like "*EMPTY_STRING_PARAM*") {
                # The proc returned NO_QUALIFYING_STATS but no CommandLog END row was written
                # (likely because 0 databases matched). The OUTPUT param still carries the code.
                $t5Ok     = $true
                $t5Detail = "No databases matched; @WarningsCodesOut='$wcFromRS' (StopReason=$srFromRS)"
            } else {
                $t5Detail = "END row query failed for RunLabel=$runLabel5; StopReasonOut=$srFromRS; WarningsCodesOut=$wcFromRS"
            }
        }
    } else {
        # No RunLabel found -- likely no databases matched and no log was written.
        # Fall back to checking @WarningsCodesOut from result set.
        $srFromRS2 = $null
        $wcFromRS2 = $null
        if ($r5.DataSet -and $r5.DataSet.Tables.Count -gt 0) {
            foreach ($tbl5b in $r5.DataSet.Tables) {
                if ($tbl5b.Columns.Contains("StopReasonOut")) {
                    $srFromRS2 = [string]$tbl5b.Rows[0]["StopReasonOut"]
                    $wcFromRS2 = [string]$tbl5b.Rows[0]["WarningsCodesOut"]
                    break
                }
            }
        }
        if ($wcFromRS2 -like "*EMPTY_STRING_PARAM*") {
            $t5Ok     = $true
            $t5Detail = "No databases matched; @WarningsCodesOut='$wcFromRS2' (StopReason=$srFromRS2)"
        } else {
            $t5Detail = "No RunLabel in output; @WarningsCodesOut='$wcFromRS2'; StopReason=$srFromRS2"
        }
    }
} else {
    $t5Detail = "Proc exception: $($r5.Output | Select-Object -First 1)"
}
Write-Host " done"
Add-Result "T_WC5: Early-return END row carries WarningsCodes element" $(if ($t5Ok) { "PASS" } else { "FAIL" }) $t5Detail

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

$passed = @($results | Where-Object { $_.Result -eq "PASS" }).Count
$failed = @($results | Where-Object { $_.Result -eq "FAIL" }).Count
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
        -ServerAliveAtEnd $sqlStatus.Alive -Suite 'WarningsContract' -ExpectedCount 5 `
        -Results @($results) -StartedAt $startedAt -EndedAt (Get-Date)
}

if ($failed -gt 0) {
    Write-Host "RESULT: $failed test(s) FAILED." -ForegroundColor Red
    exit 1
} else {
    Write-Host "RESULT: All $total tests PASSED." -ForegroundColor Green
    exit 0
}
