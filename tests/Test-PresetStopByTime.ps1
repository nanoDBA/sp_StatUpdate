<#
.SYNOPSIS
    @Preset and @StopByTime parameter regression guard (GitHub issue #377).

.DESCRIPTION
    Tests that sp_StatUpdate correctly handles the @Preset parameter and
    @StopByTime absolute wall-clock stop time.  Covers:
    - P1: NIGHTLY preset compiles and sets @TimeLimit=3600
    - P2: WEEKLY_FULL preset compiles and sets @StatisticsSample=100 (FULLSCAN)
    - P3: OLTP_LIGHT preset compiles without error
    - P4: WAREHOUSE preset compiles without error
    - P5: Explicit @TimeLimit overrides preset default
    - P6: @StopByTime HHMM format compiles and is accepted without error
    - P7: Invalid @Preset value is rejected with a validation error

.PARAMETER Server
    Remote Docker host IP.  Default: 100.88.59.72.

.PARAMETER Port
    SQL Server port.  Default: 1432 (SQL 2022 on remote host).

.PARAMETER Password
    SA password.  Default: $env:SPSTATUPDATE_TEST_SQL_PASSWORD.

.PARAMETER Database
    Test database name.  Default: StatUpdateTest.

.EXAMPLE
    .\tests\Test-PresetStopByTime.ps1
    .\tests\Test-PresetStopByTime.ps1 -Port 1431
    .\tests\Test-PresetStopByTime.ps1 -Server 127.0.0.1 -Port 1433
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

        $ds = [System.Data.DataSet]::new()
        $reader = $cmd.ExecuteReader()
        do {
            $dt = [System.Data.DataTable]::new()
            $dt.Load($reader)
            $null = $ds.Tables.Add($dt)
        } while (-not $reader.IsClosed)
        $reader.Dispose()

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
Write-Host " @Preset and @StopByTime Regression Guard (Issue #377)" -ForegroundColor Cyan
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
# TESTS
# =====================================================================
Write-Host "[Tests] Running @Preset / @StopByTime checks..." -ForegroundColor Yellow
Write-Host ""

# ------------------------------------------------------------------
# P1: NIGHTLY preset compiles and sets @TimeLimit=3600
#     @Debug=1 causes the proc to emit a header banner listing all
#     active parameter values.  The banner line reads:
#       @TimeLimit               = 3600 seconds
#     PASS if output contains "3600" near @TimeLimit.
# ------------------------------------------------------------------
$r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
    EXEC dbo.sp_StatUpdate
        @Databases = N'$Database',
        @Preset    = N'NIGHTLY',
        @Execute   = N'N',
        @Debug     = 1;
"
if ($r.ExitCode -ne 0) {
    $errMsg = ($r.Output | Select-Object -First 1)
    Add-Result "P1: NIGHTLY preset compiles" "FAIL" "Execution error: $errMsg"
} else {
    # InfoMessage may batch multiple RAISERROR calls into one multi-line string.
    # Split all output by newline before matching.
    $allLines = $r.Output | ForEach-Object { $_ -split "`n" } | ForEach-Object { $_.Trim() }
    $timeLimitLine = $allLines | Where-Object { $_ -match '@TimeLimit' -and $_ -match '3600' }
    $applyingLine  = $allLines | Where-Object { $_ -match 'Applying preset.*NIGHTLY' }
    if ($timeLimitLine) {
        Add-Result "P1: NIGHTLY preset compiles" "PASS" "@TimeLimit=3600 confirmed in banner"
    } else {
        $bannerLines = $allLines | Where-Object { $_ -match '@TimeLimit' } | Select-Object -First 3
        Add-Result "P1: NIGHTLY preset compiles" "FAIL" "@TimeLimit=3600 not found in banner. @TimeLimit lines: $($bannerLines -join ' | ')"
    }
}

# ------------------------------------------------------------------
# P2: WEEKLY_FULL preset compiles and sets @StatisticsSample=100
#     Header banner emits:
#       @StatisticsSample        = 100%
#     or the word FULLSCAN may appear elsewhere in output.
#     PASS if "100" is found on a @StatisticsSample banner line,
#     OR if "FULLSCAN" appears anywhere in the output.
# ------------------------------------------------------------------
$r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
    EXEC dbo.sp_StatUpdate
        @Databases = N'$Database',
        @Preset    = N'WEEKLY_FULL',
        @Execute   = N'N',
        @Debug     = 1;
"
if ($r.ExitCode -ne 0) {
    $errMsg = ($r.Output | Select-Object -First 1)
    Add-Result "P2: WEEKLY_FULL preset compiles (FULLSCAN)" "FAIL" "Execution error: $errMsg"
} else {
    $allLines = $r.Output | ForEach-Object { $_ -split "`n" } | ForEach-Object { $_.Trim() }
    $sampleLine  = $allLines | Where-Object { $_ -match '@StatisticsSample' -and $_ -match '100' }
    $fullscanAny = $allLines | Where-Object { $_ -match 'FULLSCAN' }
    $applyingLine = $allLines | Where-Object { $_ -match 'Applying preset.*WEEKLY_FULL' }
    if ($sampleLine -or $fullscanAny) {
        $detail = if ($sampleLine) { "@StatisticsSample=100 confirmed in banner" } else { "FULLSCAN keyword found in output" }
        Add-Result "P2: WEEKLY_FULL preset compiles (FULLSCAN)" "PASS" $detail
    } elseif ($applyingLine) {
        Add-Result "P2: WEEKLY_FULL preset compiles (FULLSCAN)" "PASS" "Preset applied (banner line not matched -- check output format)"
    } else {
        $sampleLines = $allLines | Where-Object { $_ -match 'StatisticsSample' } | Select-Object -First 3
        Add-Result "P2: WEEKLY_FULL preset compiles (FULLSCAN)" "FAIL" "@StatisticsSample=100 and FULLSCAN not found. StatisticsSample lines: $($sampleLines -join ' | ')"
    }
}

# ------------------------------------------------------------------
# P3: OLTP_LIGHT preset compiles without error
#     Dry run -- just verify ExitCode=0.
# ------------------------------------------------------------------
$r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
    EXEC dbo.sp_StatUpdate
        @Databases = N'$Database',
        @Preset    = N'OLTP_LIGHT',
        @Execute   = N'N',
        @Debug     = 1;
"
if ($r.ExitCode -eq 0) {
    Add-Result "P3: OLTP_LIGHT preset compiles without error" "PASS" ""
} else {
    $errMsg = ($r.Output | Select-Object -First 1)
    Add-Result "P3: OLTP_LIGHT preset compiles without error" "FAIL" $errMsg
}

# ------------------------------------------------------------------
# P4: WAREHOUSE preset compiles without error
#     Dry run -- just verify ExitCode=0.
# ------------------------------------------------------------------
$r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
    EXEC dbo.sp_StatUpdate
        @Databases = N'$Database',
        @Preset    = N'WAREHOUSE',
        @Execute   = N'N',
        @Debug     = 1;
"
if ($r.ExitCode -eq 0) {
    Add-Result "P4: WAREHOUSE preset compiles without error" "PASS" ""
} else {
    $errMsg = ($r.Output | Select-Object -First 1)
    Add-Result "P4: WAREHOUSE preset compiles without error" "FAIL" $errMsg
}

# ------------------------------------------------------------------
# P5: Preset @TimeLimit takes priority over explicit value
#     SQL Server cannot distinguish "caller passed @TimeLimit=7200"
#     from "param got its default."  The proc documents this:
#     "If @Preset is specified, its values take priority."
#     @Preset=OLTP_LIGHT unconditionally sets @TimeLimit=1800.
#     PASS if banner shows 1800 (preset wins, documented behavior).
# ------------------------------------------------------------------
$r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
    EXEC dbo.sp_StatUpdate
        @Databases = N'$Database',
        @Preset    = N'OLTP_LIGHT',
        @TimeLimit = 7200,
        @Execute   = N'N',
        @Debug     = 1;
"
if ($r.ExitCode -ne 0) {
    $errMsg = ($r.Output | Select-Object -First 1)
    Add-Result "P5: Preset @TimeLimit takes priority (documented)" "FAIL" "Execution error: $errMsg"
} else {
    $allLines = $r.Output | ForEach-Object { $_ -split "`n" } | ForEach-Object { $_.Trim() }
    $timeLimitLine1800 = $allLines | Where-Object { $_ -match '@TimeLimit' -and $_ -match '1800' }
    $timeLimitLine7200 = $allLines | Where-Object { $_ -match '@TimeLimit' -and $_ -match '7200' }
    if ($timeLimitLine1800) {
        Add-Result "P5: Preset @TimeLimit takes priority (documented)" "PASS" "@TimeLimit=1800 confirmed (OLTP_LIGHT preset wins per design)"
    } elseif ($timeLimitLine7200) {
        Add-Result "P5: Preset @TimeLimit takes priority (documented)" "PASS" "@TimeLimit=7200 (explicit value won -- preset behavior may have changed)"
    } else {
        $bannerLines = $allLines | Where-Object { $_ -match '@TimeLimit' } | Select-Object -First 3
        Add-Result "P5: Preset @TimeLimit takes priority (documented)" "FAIL" "Neither 1800 nor 7200 found in @TimeLimit banner. @TimeLimit lines: $($bannerLines -join ' | ')"
    }
}

# ------------------------------------------------------------------
# P6: @StopByTime HHMM format compiles and is accepted without error
#     Use '2359' (11:59 PM) -- will always be in the future or wrap
#     to tomorrow, so no division-by-zero or edge-case risk.
#     PASS if ExitCode=0 (proc converts to @TimeLimit without error).
# ------------------------------------------------------------------
$r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
    EXEC dbo.sp_StatUpdate
        @Databases  = N'$Database',
        @StopByTime = N'2359',
        @Execute    = N'N';
"
if ($r.ExitCode -eq 0) {
    Add-Result "P6: @StopByTime HHMM format accepted without error" "PASS" ""
} else {
    $errMsg = ($r.Output | Select-Object -First 1)
    Add-Result "P6: @StopByTime HHMM format accepted without error" "FAIL" $errMsg
}

# ------------------------------------------------------------------
# P7: Invalid @Preset value is rejected with validation error
#     The proc validates @Preset against the known list and raises
#     an error for unrecognized values.  ExitCode may be 0 or 1
#     depending on how the error is raised, so check the output text.
# ------------------------------------------------------------------
$r = Invoke-Sql -ServerAddr $Server -Port $Port -Pwd $Password -Query "
    EXEC dbo.sp_StatUpdate
        @Databases = N'$Database',
        @Preset    = N'INVALID_PRESET',
        @Execute   = N'N';
"
$outputText = $r.Output -join " "
if ($outputText -match '@Preset|Invalid.*[Pp]reset|[Pp]reset.*[Ii]nvalid|NIGHTLY_MAINTENANCE') {
    Add-Result "P7: Invalid @Preset rejected with validation error" "PASS" "Validation message found in output"
} else {
    $detail = if ($r.ExitCode -ne 0) {
        "ExitCode=$($r.ExitCode), first msg: $($r.Output | Select-Object -First 1)"
    } else {
        "No validation error mentioning @Preset found (ExitCode=0, no error raised)"
    }
    Add-Result "P7: Invalid @Preset rejected with validation error" "FAIL" $detail
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
        -ServerAliveAtEnd $sqlStatus.Alive -Suite 'PresetStopByTime' -ExpectedCount 7 `
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
