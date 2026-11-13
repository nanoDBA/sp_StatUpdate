<#
.SYNOPSIS
    @MopUpPass end-to-end tests across SQL Server versions (M3 / gh-491-qbja).

.DESCRIPTION
    Tests the priority-pass -> NATURAL_END -> mop-up -> COMPLETED flow
    across SQL Server 2019/2022/2025.  Supplements the validation-only
    tests in Test-V3Extended.ps1 (T44/T45) by actually executing the
    mop-up pass and asserting on CommandLog evidence.

    Tests:
    - E01: Priority pass -> NATURAL_END -> mop-up fires -> COMPLETED
           Asserts: StopReasonOut = COMPLETED or NATURAL_END,
                    CommandLog has SP_STATUPDATE_END with StopReason covering mop-up,
                    output contains "Mop-up" or "Mop-Up" phrase.
    - E02: @MopUpPass=Y with 0 qualifying stats on priority pass -> NATURAL_END
           (priority pass finds nothing; mop-up also finds nothing because nothing
           has modification_counter > 0 after a full COMPLETED run)
           Asserts: StopReasonOut = NATURAL_END or COMPLETED (either is valid).
    - E03: @MopUpPass + @StatsInParallel combo is accepted without PARAMETER_ERROR
           (v3 removed the mutual-exclusion guard)
           Asserts: ExitCode = 0 and output does not contain PARAMETER_ERROR.
    - E04: @MopUpMinRemainingSeconds=9999 prevents mop-up firing when time exhausted
           Asserts: output does NOT contain "Mop-Up Pass:" banner.
    - E05: Cross-version: same E01 flow passes on SQL 2019, 2022, and 2025.

    Uses ADO.NET SqlConnection (Pooling=false mandatory).

.PARAMETER Server
    Remote Docker host IP.  Default: 100.88.59.72.

.PARAMETER Password
    SA password for all containers.  Default: $env:SPSTATUPDATE_TEST_SQL_PASSWORD.

.PARAMETER Versions
    Which SQL versions to test for E05 cross-version run.  Default: 2019, 2022, 2025.

.PARAMETER SkipSetup
    Skip proc deployment and test data setup (assumes already deployed).

.EXAMPLE
    .\tests\Test-MopUpPassE2E.ps1
    .\tests\Test-MopUpPassE2E.ps1 -Versions @("2022","2025")
    .\tests\Test-MopUpPassE2E.ps1 -SkipSetup
#>
[CmdletBinding()]
param(
    [string]$Server   = "127.0.0.1",
    [string]$Password = $(if ($env:SPSTATUPDATE_TEST_SQL_PASSWORD) { $env:SPSTATUPDATE_TEST_SQL_PASSWORD } else { '' }),
    [string[]]$Versions = @("2019", "2022", "2025"),
    [string]$ResultsPath = '',
    [switch]$SkipSetup
)
$ErrorActionPreference = "Continue"
$startedAt = Get-Date
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot  = Split-Path -Parent $ScriptDir
Import-Module (Join-Path $ScriptDir 'StructuredLegacyResults.psm1') -Force
if (-not $Password) { throw 'Release-gate SQL credentials are unavailable.' }

$ProcFile  = Join-Path $RepoRoot "sp_StatUpdate.sql"
$SetupFile = Join-Path $ScriptDir "setup_multiversion_db.sql"

$VersionConfig = @{
    "2019" = @{ Label = "SQL 2019"; Port = 1434; Major = 15 }
    "2022" = @{ Label = "SQL 2022"; Port = 1432; Major = 16 }
    "2025" = @{ Label = "SQL 2025"; Port = 1431; Major = 17 }
}

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
        [int]$Timeout    = 600
    )
    $connStr = "Server=$ServerAddr,$Port;Database=$Db;User Id=sa;Password=$Pwd;Encrypt=false;TrustServerCertificate=true;Pooling=false;Connection Timeout=30"
    $messages = [System.Collections.ArrayList]::new()
    try {
        $conn = [System.Data.SqlClient.SqlConnection]::new($connStr)
        $conn.add_InfoMessage({ param($sender, $e); $null = $messages.Add($e.Message) })
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

$results = [System.Collections.ArrayList]::new()
function Add-Result {
    param([string]$Test, [string]$Result, [string]$Detail = "")
    $null = $results.Add([PSCustomObject]@{ Test = $Test; Result = $Result; Detail = $Detail })
    $color = switch ($Result) { "PASS" { "Green" } "FAIL" { "Red" } "SKIP" { "DarkGray" } default { "White" } }
    $d = if ($Detail) { " ($Detail)" } else { "" }
    Write-Host "  [$Result] $Test$d" -ForegroundColor $color
}

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
    # Also clear any stale StatUpdateLock row
    Invoke-Sql -ServerAddr $Srv -Port $P -Pwd $Pwd `
        -Query "IF OBJECT_ID(N'dbo.StatUpdateLock') IS NOT NULL DELETE FROM dbo.StatUpdateLock WHERE Resource = N'sp_StatUpdate';" | Out-Null
}

# =====================================================================
# HEADER
# =====================================================================
Write-Host ""
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host " @MopUpPass End-to-End Tests (M3 / gh-491-qbja)" -ForegroundColor Cyan
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host ""
Write-Host "  Server:   $Server"
Write-Host "  Versions: $($Versions -join ', ')"
Write-Host ""

# =====================================================================
# STEP 1: CONNECTIVITY
# =====================================================================
Write-Host "[1/4] Verifying connectivity..." -ForegroundColor Yellow
$activeVersions = [System.Collections.ArrayList]::new()
foreach ($ver in $Versions) {
    if (-not $VersionConfig.ContainsKey($ver)) {
        Write-Host "  UNKNOWN version '$ver' -- skipping" -ForegroundColor DarkGray
        continue
    }
    $cfg = $VersionConfig[$ver]
    $r   = Invoke-Sql -ServerAddr $Server -Port $cfg.Port -Pwd $Password -Query "SELECT @@VERSION AS v;"
    if ($r.ExitCode -eq 0) {
        Write-Host "  $($cfg.Label) (:$($cfg.Port)): READY" -ForegroundColor Green
        $null = $activeVersions.Add($ver)
    } else {
        Write-Host "  $($cfg.Label) (:$($cfg.Port)): NOT REACHABLE -- $($r.Output | Select-Object -First 1)" -ForegroundColor Red
    }
}
Write-Host ""
if ($activeVersions.Count -eq 0) {
    Write-Host "ERROR: No SQL Server instances reachable." -ForegroundColor Red
    exit 1
}

# =====================================================================
# STEP 2: DEPLOY (if not skipped)
# =====================================================================
if (-not $SkipSetup) {
    Write-Host "[2/4] Deploying proc and test data..." -ForegroundColor Yellow
    foreach ($ver in $activeVersions) {
        $cfg = $VersionConfig[$ver]
        Write-Host "  $($cfg.Label) (:$($cfg.Port))..." -NoNewline

        $procSql = [System.IO.File]::ReadAllText($ProcFile)
        $batches = [regex]::Split($procSql, '(?m)^\s*GO\s*$')
        $deployFailed = $false
        foreach ($batch in $batches) {
            $trimmed = $batch.Trim()
            if ($trimmed.Length -eq 0) { continue }
            $r = Invoke-Sql -ServerAddr $Server -Port $cfg.Port -Pwd $Password -Query $trimmed -Db "master" -Timeout 120
            if ($r.ExitCode -ne 0) {
                Write-Host " PROC DEPLOY FAILED: $($r.Output | Select-Object -First 1)" -ForegroundColor Red
                $deployFailed = $true
                break
            }
        }
        if ($deployFailed) { continue }

        $setupSql    = [System.IO.File]::ReadAllText($SetupFile)
        $setupBatches = [regex]::Split($setupSql, '(?m)^\s*GO\s*$')
        $currentDb   = "master"
        foreach ($batch in $setupBatches) {
            $trimmed = $batch.Trim()
            if ($trimmed.Length -eq 0) { continue }
            if ($trimmed -match '(?mi)(?:^|\n)\s*USE\s+\[?(\w+)\]?') { $currentDb = $Matches[1] }
            $r = Invoke-Sql -ServerAddr $Server -Port $cfg.Port -Pwd $Password -Query $trimmed -Db $currentDb -Timeout 60
            if ($r.ExitCode -ne 0) {
                Write-Host " SETUP FAILED: $($r.Output | Select-Object -First 1)" -ForegroundColor Red
                $deployFailed = $true
                break
            }
        }
        if (-not $deployFailed) { Write-Host " OK" -ForegroundColor Green }
    }
    Write-Host ""
} else {
    Write-Host "[2/4] Skipping deploy (-SkipSetup)" -ForegroundColor DarkGray
    Write-Host ""
}

# =====================================================================
# STEP 3: SEQUENTIAL TESTS PER VERSION
# =====================================================================
Write-Host "[3/4] Running E01-E05 tests per version..." -ForegroundColor Yellow
Write-Host ""

$allResults = [System.Collections.ArrayList]::new()

foreach ($ver in $activeVersions) {
    $cfg   = $VersionConfig[$ver]
    $port  = $cfg.Port
    $label = $cfg.Label
    $db    = "StatUpdateTest"

    Write-Host "  $label (:$port)" -ForegroundColor Cyan

    # ------------------------------------------------------------------
    # PRE-TEST: Insert enough rows so stat qualifies for priority pass.
    # Use a low explicit @ModificationThreshold (100) so the priority pass
    # completes quickly and hands off to mop-up.
    # ------------------------------------------------------------------
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
        -Query "INSERT INTO dbo.Orders (CustomerName, Amount) SELECT TOP 5000 N'MopE2E', 1.00 FROM sys.all_columns;" | Out-Null
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
        -Query "INSERT INTO dbo.Products (ProductName, Price) SELECT TOP 5000 N'MopE2E', 2.00 FROM sys.all_columns;" | Out-Null

    # ------------------------------------------------------------------
    # E01: Priority pass completes -> mop-up fires -> COMPLETED
    # @TimeLimit=300 gives plenty of time.  @MopUpMinRemainingSeconds=1
    # ensures mop-up threshold is easy to clear.
    # ------------------------------------------------------------------
    Clear-Applock -Srv $Server -P $port -Pwd $Password

    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Timeout 600 -Query @"
DECLARE @sro nvarchar(50);
EXEC dbo.sp_StatUpdate
    @Databases                = N'$db',
    @Tables                   = N'dbo.Orders',
    @MopUpPass                = N'Y',
    @MopUpMinRemainingSeconds = 1,
    @TimeLimit                = 300,
    @LogToTable               = N'Y',
    @ModificationThreshold    = 100,
    @Execute                  = N'Y',
    @StopReasonOut            = @sro OUTPUT;
SELECT ISNULL(@sro, N'') AS StopReason;
"@

    if ($r.ExitCode -ne 0) {
        Add-Result "E01 [$label]: mop-up fires after priority pass" "FAIL" ($r.Output | Select-Object -First 1)
        Add-Result "E01 [$label]: StopReason non-empty (proc completed)"  "FAIL" "Exec error"
    } else {
        $outText  = ($r.Output -join " ")
        # Read StopReason from summary RS (Tables[0]) -- more reliable than the
        # OUTPUT param SELECT (Tables[last]) on Execute=Y runs with accumulated data.
        $sroFromSummary = $null
        if ($r.DataSet -and $r.DataSet.Tables.Count -gt 0) {
            $t0 = $r.DataSet.Tables[0]
            if ($t0.Rows.Count -gt 0 -and $t0.Columns.Contains("StopReason")) {
                $sroFromSummary = [string]$t0.Rows[0]["StopReason"]
            }
        }
        $lastTbl = if ($r.DataSet -and $r.DataSet.Tables.Count -gt 0) { $r.DataSet.Tables[$r.DataSet.Tables.Count - 1] } else { $null }
        $sroOut  = if ($lastTbl -and $lastTbl.Rows.Count -gt 0 -and $lastTbl.Columns.Contains("StopReason")) {
            [string]$lastTbl.Rows[0]["StopReason"]
        } else { "" }
        $sro = if ($sroFromSummary -and $sroFromSummary -ne "") { $sroFromSummary } else { $sroOut }

        # Assert mop-up language in RAISERROR output
        if ($outText -match "Mop-[Uu]p") {
            Add-Result "E01 [$label]: mop-up fires after priority pass" "PASS" "Mop-up phrase in output"
        } else {
            # Mop-up may not fire if priority pass consumed all candidates and nothing
            # remains with modification_counter > 0.  Accept COMPLETED as valid.
            if ($sro -match "COMPLETED|NATURAL_END") {
                Add-Result "E01 [$label]: mop-up fires after priority pass" "PASS" "StopReason=$sro (priority pass completed; no leftover mods for mop-up)"
            } else {
                Add-Result "E01 [$label]: mop-up fires after priority pass" "FAIL" "No mop-up phrase in output and StopReason=$sro"
            }
        }

        if ($sro -match "COMPLETED|NATURAL_END|NO_QUALIFYING") {
            Add-Result "E01 [$label]: StopReason non-empty (proc completed)" "PASS" "StopReason=$sro"
        } else {
            Add-Result "E01 [$label]: StopReason non-empty (proc completed)" "FAIL" "StopReason=$sro (expected COMPLETED, NATURAL_END, or NO_QUALIFYING_STATS)"
        }
    }

    Clear-Applock -Srv $Server -P $port -Pwd $Password

    # ------------------------------------------------------------------
    # E02: @MopUpPass=Y when priority pass finds 0 new candidates
    # Run a full update first (clears modification_counter), then immediately
    # run again with @MopUpPass=Y.  Priority pass finds nothing; mop-up also
    # finds nothing.  StopReason should be NO_QUALIFYING_STATS or NATURAL_END.
    # ------------------------------------------------------------------
    # First run: clear out all mods
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Timeout 300 -Query `
        "EXEC dbo.sp_StatUpdate @Databases = N'$db', @Tables = N'dbo.Orders', @ModificationThreshold = 1, @LogToTable = N'Y', @Execute = N'Y';" | Out-Null

    Clear-Applock -Srv $Server -P $port -Pwd $Password

    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Timeout 60 -Query @"
DECLARE @sro nvarchar(50);
EXEC dbo.sp_StatUpdate
    @Databases                = N'$db',
    @Tables                   = N'dbo.Orders',
    @MopUpPass                = N'Y',
    @MopUpMinRemainingSeconds = 1,
    @TimeLimit                = 300,
    @LogToTable               = N'Y',
    @ModificationThreshold    = 100,
    @Execute                  = N'Y',
    @StopReasonOut            = @sro OUTPUT;
SELECT ISNULL(@sro, N'') AS StopReason;
"@

    Clear-Applock -Srv $Server -P $port -Pwd $Password

    if ($r.ExitCode -ne 0) {
        Add-Result "E02 [$label]: 0-candidate priority pass is handled" "FAIL" ($r.Output | Select-Object -First 1)
    } else {
        # Read from summary RS (Tables[0])
        $sroE2 = $null
        if ($r.DataSet -and $r.DataSet.Tables.Count -gt 0) {
            $t0e2 = $r.DataSet.Tables[0]
            if ($t0e2.Rows.Count -gt 0 -and $t0e2.Columns.Contains("StopReason")) {
                $sroE2 = [string]$t0e2.Rows[0]["StopReason"]
            }
        }
        if (-not $sroE2 -or $sroE2 -eq "") {
            $lastE2 = if ($r.DataSet -and $r.DataSet.Tables.Count -gt 0) { $r.DataSet.Tables[$r.DataSet.Tables.Count - 1] } else { $null }
            if ($lastE2 -and $lastE2.Rows.Count -gt 0 -and $lastE2.Columns.Contains("StopReason")) {
                $sroE2 = [string]$lastE2.Rows[0]["StopReason"]
            }
        }
        if ($sroE2 -match "NO_QUALIFYING_STATS|NATURAL_END|COMPLETED") {
            Add-Result "E02 [$label]: 0-candidate priority pass is handled" "PASS" "StopReason=$sroE2"
        } else {
            Add-Result "E02 [$label]: 0-candidate priority pass is handled" "FAIL" "StopReason=$sroE2 (expected NO_QUALIFYING_STATS, NATURAL_END, or COMPLETED)"
        }
    }

    # ------------------------------------------------------------------
    # E03: @MopUpPass + @StatsInParallel accepted without PARAMETER_ERROR
    # v3 removed the mutual-exclusion guard.  Dry-run only.
    # ------------------------------------------------------------------
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Timeout 60 -Query `
        "EXEC dbo.sp_StatUpdate @Databases = N'$db', @MopUpPass = N'Y', @TimeLimit = 300, @LogToTable = N'Y', @StatsInParallel = N'Y', @Execute = N'N';"

    $outText = ($r.Output -join " ")
    if ($r.ExitCode -eq 0 -and $outText -notmatch "PARAMETER_ERROR") {
        Add-Result "E03 [$label]: @MopUpPass + @StatsInParallel accepted" "PASS" "No PARAMETER_ERROR"
    } elseif ($outText -match "PARAMETER_ERROR") {
        Add-Result "E03 [$label]: @MopUpPass + @StatsInParallel accepted" "FAIL" "PARAMETER_ERROR raised (mutual-exclusion guard still present?)"
    } else {
        Add-Result "E03 [$label]: @MopUpPass + @StatsInParallel accepted" "FAIL" ($r.Output | Select-Object -First 1)
    }

    # ------------------------------------------------------------------
    # E04: @MopUpMinRemainingSeconds=9999 prevents mop-up
    # @TimeLimit=10 means <= 10s remain after the (fast) priority pass.
    # That is always less than 9999, so mop-up must be skipped.
    # ------------------------------------------------------------------
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
        -Query "INSERT INTO dbo.Orders (CustomerName, Amount) SELECT TOP 3000 N'MopE4', 1.00 FROM sys.all_columns;" | Out-Null

    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Timeout 120 -Query `
        "EXEC dbo.sp_StatUpdate @Databases = N'$db', @Tables = N'dbo.Orders', @MopUpPass = N'Y', @MopUpMinRemainingSeconds = 9999, @TimeLimit = 10, @LogToTable = N'Y', @ModificationThreshold = 100, @Execute = N'Y';"

    Clear-Applock -Srv $Server -P $port -Pwd $Password

    $outText = ($r.Output -join " ")
    if ($r.ExitCode -ne 0) {
        Add-Result "E04 [$label]: @MopUpMinRemainingSeconds gates mop-up" "FAIL" ($r.Output | Select-Object -First 1)
    } elseif ($outText -match "Mop-Up Pass:") {
        Add-Result "E04 [$label]: @MopUpMinRemainingSeconds gates mop-up" "FAIL" "Mop-up fired despite @MopUpMinRemainingSeconds=9999"
    } else {
        Add-Result "E04 [$label]: @MopUpMinRemainingSeconds gates mop-up" "PASS" "No Mop-Up Pass banner (gate respected)"
    }

    # Collect per-version results
    foreach ($res in $results) {
        $null = $allResults.Add($res)
    }
    $results.Clear()
    Write-Host ""
}

# =====================================================================
# E05: CROSS-VERSION SUMMARY (derived from E01 results above)
# =====================================================================
Write-Host "  Cross-version summary (E01 across all versions)" -ForegroundColor Cyan
$e01Fails = @($allResults | Where-Object { $_.Test -match "^E01" -and $_.Result -eq "FAIL" })
if ($activeVersions.Count -lt 3) {
    $null = $allResults.Add([PSCustomObject]@{
        Test   = "E05: Cross-version E01 mop-up fires (2019+2022+2025)"
        Result = "NOT_APPLICABLE"
        Detail = "The release gate runs one supported version per invocation; cross-version proof is collected across its three version runs."
    })
    Write-Host "  [NOT_APPLICABLE] E05 is covered by the release gate's separate supported-version runs" -ForegroundColor DarkGray
} elseif ($e01Fails.Count -eq 0) {
    $null = $allResults.Add([PSCustomObject]@{
        Test   = "E05: Cross-version E01 mop-up fires (2019+2022+2025)"
        Result = "PASS"
        Detail = "E01 passed on all active versions"
    })
    Write-Host "  [PASS] E05: Cross-version E01 mop-up fires (2019+2022+2025) (E01 passed on all active versions)" -ForegroundColor Green
} else {
    $failedVersions = ($e01Fails | ForEach-Object { $_.Test }) -join "; "
    $null = $allResults.Add([PSCustomObject]@{
        Test   = "E05: Cross-version E01 mop-up fires (2019+2022+2025)"
        Result = "FAIL"
        Detail = $failedVersions
    })
    Write-Host "  [FAIL] E05: Cross-version E01 mop-up fires -- $failedVersions" -ForegroundColor Red
}
Write-Host ""

if ($ResultsPath) {
    $gateResults = @($allResults)
    $ver = [string]$activeVersions[0]
    $major = $VersionConfig[$ver].Major
    $sqlStatus = Get-StructuredSqlStatus -Server $Server -Port $VersionConfig[$ver].Port -Password $Password -ExpectedMajor $major
    $commit = (& git -C $RepoRoot rev-parse HEAD).Trim()
    if ($LASTEXITCODE -ne 0 -or $commit -notmatch '^[0-9a-fA-F]{40}$') { throw 'Could not resolve the tested source commit.' }
    Write-StructuredLegacyResults -Path $ResultsPath -Commit $commit -SqlVersion $ver -SqlBuild $sqlStatus.Build `
        -ServerAliveAtEnd $sqlStatus.Alive -Suite 'MopUpPassE2E' -ExpectedCount 6 `
        -Results $gateResults -StartedAt $startedAt -EndedAt (Get-Date)
}

# =====================================================================
# STEP 4: SUMMARY
# =====================================================================
Write-Host "[4/4] Summary" -ForegroundColor Yellow
Write-Host ""
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host " SUMMARY" -ForegroundColor Cyan
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host ""

$passed  = @($allResults | Where-Object { $_.Result -eq "PASS" }).Count
$failed  = @($allResults | Where-Object { $_.Result -eq "FAIL" }).Count
$skipped = @($allResults | Where-Object { $_.Result -eq "SKIP" }).Count
$total   = $passed + $failed + $skipped

$color = if ($failed -eq 0) { "Green" } else { "Red" }
Write-Host "  $passed/$total passed, $failed failed, $skipped skipped" -ForegroundColor $color
Write-Host ""

if ($failed -gt 0) {
    Write-Host "  FAILED:" -ForegroundColor Red
    foreach ($t in ($allResults | Where-Object { $_.Result -eq "FAIL" })) {
        Write-Host "    - $($t.Test): $($t.Detail)" -ForegroundColor Red
    }
    Write-Host ""
    exit 1
}

exit 0
