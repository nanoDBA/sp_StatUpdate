<#
.SYNOPSIS
    Targeted test suite for sp_StatUpdate v3 -- verifying recent bug fixes (T60-T69).

.DESCRIPTION
    Tests sp_StatUpdate v3 across SQL Server 2019/2022/2025 on a remote Docker host.
    All versions are tested concurrently using ForEach-Object -Parallel.

    Covers 10 targeted tests per version verifying specific issue fixes:
    - T60: #402 -- @LongRunningThresholdMinutes override: XML element + adaptive-sampling behavior
    - T61: #403 -- debug header uses friendly labels (no @i_ names exposed)
    - T62: #405 -- SP_STATUPDATE_START Command uses public param names (@TimeLimit/@SortOrder)
    - T63: #404 -- "triggering legacy fallback" phrase absent (v3 removed legacy path)
    - T64: @QueryStoreMinExecutions=50 verified in SP_STATUPDATE_START ExtendedInfo XML
    - T65: @QueryStoreRecentHours=24 verified in SP_STATUPDATE_START ExtendedInfo XML
    - T66: @MaxAGRedoQueueMB=500 verified in SP_STATUPDATE_START ExtendedInfo XML
    - T67: @MinTempdbFreeMB=100 verified in SP_STATUPDATE_START ExtendedInfo XML
    - T68: @MopUpMinRemainingSeconds=120 verified in SP_STATUPDATE_START ExtendedInfo XML
    - T69: All 5 presets (DEFAULT/NIGHTLY/WEEKLY_FULL/OLTP_LIGHT/WAREHOUSE) complete without error

    Note: T60 now emits TWO result lines (T60 XML and T60 behavioral), so
    the suite total is 11 per version instead of 10.

    Uses ADO.NET SqlConnection (not go-sqlcmd) to avoid TLS cert parsing
    issues with SQL 2017's negative serial number certificates.

    Prerequisites:
      - sp_StatUpdate.sql deployed to master on each target instance,
        OR run without -SkipSetup to deploy automatically.
      - tests/setup_multiversion_db.sql deployed to create StatUpdateTest DB
        and test tables.

.PARAMETER Server
    Remote Docker host IP.  Default: 100.88.59.72.

.PARAMETER Password
    SA password for all containers.  Default: $env:SPSTATUPDATE_TEST_SQL_PASSWORD.

.PARAMETER Versions
    Which SQL versions to test.  Default: 2019, 2022, 2025.

.PARAMETER SkipSetup
    Skip proc deployment and test data setup (assumes already deployed).

.EXAMPLE
    .\tests\Test-V3Fixes.ps1
    .\tests\Test-V3Fixes.ps1 -Versions @("2022","2025")
    .\tests\Test-V3Fixes.ps1 -SkipSetup
    .\tests\Test-V3Fixes.ps1 -Server 127.0.0.1 -Versions @("2022")
#>
[CmdletBinding()]
param(
    [string]$Server   = "127.0.0.1",
    [string]$Password = $(if ($env:SPSTATUPDATE_TEST_SQL_PASSWORD) { $env:SPSTATUPDATE_TEST_SQL_PASSWORD } else { "$env:SPSTATUPDATE_TEST_SQL_PASSWORD" }),
    [string[]]$Versions = @("2019", "2022", "2025"),
    [switch]$SkipSetup,
    [string]$ResultsPath = ""
)

$ErrorActionPreference = "Continue"
$script:RunStartedAt = Get-Date
$script:LastCheckStartedAt = $script:RunStartedAt
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot  = Split-Path -Parent $ScriptDir

$ProcFile  = Join-Path $RepoRoot "sp_StatUpdate.sql"
$SetupFile = Join-Path $ScriptDir "setup_multiversion_db.sql"

# Version config: label -> port on remote host
# NOTE: SQL 2022 is on port 1432 (not 1433 -- that port is DOWN on remote host).
$VersionConfig = @{
    "2019" = @{ Label = "SQL 2019"; Port = 1434; Major = 15 }
    "2022" = @{ Label = "SQL 2022"; Port = 1432; Major = 16 }
    "2025" = @{ Label = "SQL 2025"; Port = 1431; Major = 17 }
}

# =====================================================================
# ADO.NET HELPER (outer scope -- for setup phase)
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
    $connStr = "Server=$ServerAddr,$Port;Database=$Db;User Id=sa;Password=$Pwd;Encrypt=false;TrustServerCertificate=true;Connection Timeout=30;Pooling=false"
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
        $cmd.CommandText  = $Query
        $cmd.CommandTimeout = $Timeout
        # Use ExecuteReader instead of SqlDataAdapter.Fill() --
        # Fill() on SQL 2025 swallows InfoMessage events before they fire.
        $ds = [System.Data.DataSet]::new()
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
Write-Host " sp_StatUpdate v3 Bug-Fix Verification Suite (T60-T69)" -ForegroundColor Cyan
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host ""
Write-Host "  Server:   $Server"
Write-Host "  Versions: $($Versions -join ', ')"
Write-Host "  Proc:     $ProcFile"
Write-Host ""

# =====================================================================
# STEP 1: VERIFY CONNECTIVITY
# =====================================================================
Write-Host "[1/4] Verifying connectivity..." -ForegroundColor Yellow

$activeVersions = [System.Collections.ArrayList]::new()
foreach ($ver in $Versions) {
    if (-not $VersionConfig.ContainsKey($ver)) {
        Write-Host "  UNKNOWN version '$ver' -- skipping" -ForegroundColor DarkGray
        continue
    }
    $cfg = $VersionConfig[$ver]
    $r = Invoke-Sql -ServerAddr $Server -Port $cfg.Port -Pwd $Password -Query "SELECT @@VERSION AS v;"
    if ($r.ExitCode -eq 0) {
        $verStr = if ($r.DataSet.Tables.Count -gt 0) {
            ([string]$r.DataSet.Tables[0].Rows[0]["v"]).Substring(0, [Math]::Min(60, ([string]$r.DataSet.Tables[0].Rows[0]["v"]).Length))
        } else { "Connected" }
        Write-Host "  $($cfg.Label) (:$($cfg.Port)): READY - $verStr" -ForegroundColor Green
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

if (-not (Test-Path $ProcFile)) {
    Write-Host "ERROR: Proc file not found: $ProcFile" -ForegroundColor Red
    exit 1
}

# =====================================================================
# STEP 2: DEPLOY PROC + TEST DATA (serial)
# =====================================================================
if (-not $SkipSetup) {
    Write-Host "[2/4] Deploying v3 proc and test data..." -ForegroundColor Yellow

    foreach ($ver in $activeVersions) {
        $cfg = $VersionConfig[$ver]
        Write-Host "  $($cfg.Label) (:$($cfg.Port))..." -NoNewline

        # Deploy proc -- split on GO, execute each batch in master
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

        # Deploy test data -- track USE context across batches
        $setupSql = [System.IO.File]::ReadAllText($SetupFile)
        $setupBatches = [regex]::Split($setupSql, '(?m)^\s*GO\s*$')
        $currentDb = "master"

        foreach ($batch in $setupBatches) {
            $trimmed = $batch.Trim()
            if ($trimmed.Length -eq 0) { continue }
            if ($trimmed -match '(?mi)(?:^|\n)\s*USE\s+\[?(\w+)\]?') {
                $currentDb = $Matches[1]
            }
            $r = Invoke-Sql -ServerAddr $Server -Port $cfg.Port -Pwd $Password -Query $trimmed -Db $currentDb -Timeout 60
            if ($r.ExitCode -ne 0) {
                Write-Host " TEST DATA FAILED (db=$currentDb): $($r.Output | Select-Object -First 1)" -ForegroundColor Red
                $deployFailed = $true
                break
            }
        }
        if ($deployFailed) { continue }

        Write-Host " OK" -ForegroundColor Green
    }
    Write-Host ""
} else {
    Write-Host "[2/4] Skipping deploy (-SkipSetup)" -ForegroundColor DarkGray
    Write-Host ""
}

# =====================================================================
# STEP 3: PARALLEL TEST EXECUTION
# =====================================================================
Write-Host "[3/4] Running 11 tests per version..." -ForegroundColor Yellow
Write-Host ""

$allResults = $activeVersions | ForEach-Object {
    $ver           = $_
    $Server        = $Server
    $Password      = $Password
    $VersionConfig = $VersionConfig

    $cfg   = $VersionConfig[$ver]
    $port  = $cfg.Port
    $label = $cfg.Label
    $db    = "StatUpdateTest"

    # --- ADO.NET helper must be redefined inside parallel block ---
    function Invoke-Sql {
        param([string]$ServerAddr, [int]$Port, [string]$Pwd, [string]$Query, [string]$Db = "master", [int]$Timeout = 300)
        $connStr = "Server=$ServerAddr,$Port;Database=$Db;User Id=sa;Password=$Pwd;Encrypt=false;TrustServerCertificate=true;Connection Timeout=30;Pooling=false"
        $messages = [System.Collections.ArrayList]::new()
        try {
            $conn = [System.Data.SqlClient.SqlConnection]::new($connStr)
            $conn.add_InfoMessage({ param($s, $e); $null = $messages.Add($e.Message) })
            $conn.FireInfoMessageEventOnUserErrors = $true
            $conn.Open()
            $cmd = $conn.CreateCommand()
            $cmd.CommandText  = $Query
            $cmd.CommandTimeout = $Timeout
            # Use ExecuteReader instead of SqlDataAdapter.Fill() --
            # Fill() on SQL 2025 swallows InfoMessage events before they fire.
            $ds = [System.Data.DataSet]::new()
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
    $script:LastCheckStartedAt = Get-Date
    function Add-Result {
        param([string]$Test, [string]$Result, [string]$Detail = "")
        $now = Get-Date
        $durationMs = [long][Math]::Max(0, ($now - $script:LastCheckStartedAt).TotalMilliseconds)
        $null = $results.Add([PSCustomObject]@{ Test = $Test; Result = $Result; Detail = $Detail; DurationMs = $durationMs })
        $script:LastCheckStartedAt = $now
    }

    # Detect server major version
    $vr = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
        -Query "SELECT CONVERT(int, SERVERPROPERTY(N'ProductMajorVersion')) AS MajorVer;"
    $serverMajor = if ($vr.ExitCode -eq 0 -and $vr.DataSet.Tables.Count -gt 0) {
        [int]$vr.DataSet.Tables[0].Rows[0]["MajorVer"]
    } else { 0 }

    # ----------------------------------------------------------------
    # PRE-TEST: ensure modifications exist so discovery has candidates.
    # ----------------------------------------------------------------
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
        -Query "INSERT INTO dbo.Orders (CustomerName, Amount) SELECT TOP 3000 N'V3Fix', 1.00 FROM sys.all_columns;" | Out-Null
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
        -Query "INSERT INTO dbo.Products (ProductName, Price) SELECT TOP 1500 N'V3Fix', 2.00 FROM sys.all_columns;" | Out-Null

    # ================================================================
    # T60: Verify #402 fix -- @LongRunningThresholdMinutes override works
    #
    # Two-part contract check:
    #
    # Part A (XML contract): Run with @Execute=Y @LogToTable=Y and assert
    # the SP_STATUPDATE_START ExtendedInfo XML element LongRunningThresholdMinutes
    # equals 60.  This proves the public param wiring reaches the durable
    # contract that sp_StatUpdate_Diag consumes.
    #
    # Part B (behavioral): Seed a synthetic UPDATE_STATISTICS CommandLog row
    # whose duration >= 60 minutes for a known stat in dbo.Orders, then run
    # the proc with @LongRunningThresholdMinutes=60 @Execute=N and assert the
    # InfoMessage output contains "Adaptive Sampling: Found" (proc detected the
    # slow stat).  Seed row is cleaned up by a distinctive Command marker
    # ("T60_SYNTHETIC_SLOW") regardless of pass/fail.
    #
    # Note: @LogToTable=Y is required for adaptive sampling -- the proc reads
    # CommandLog history to find historically slow stats.
    # ================================================================

    # -- Part A: XML contract --
    # Seed fresh modifications so at least one stat qualifies and the proc
    # does not exit early with NO_QUALIFYING_STATS (which skips the START log).
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
        -Query "INSERT INTO dbo.Orders (CustomerName, Amount) SELECT TOP 500 N'T60seed', 1.00 FROM sys.all_columns;" | Out-Null

    # Obtain the RunLabel so we can target the exact CommandLog row.
    $rT60a = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Timeout 600 `
        -Query @"
DELETE FROM master.dbo.StatUpdateLock WHERE LoginTime < DATEADD(MINUTE, -10, SYSDATETIME());
EXEC dbo.sp_StatUpdate
    @Databases = N'$db',
    @Tables = N'dbo.Orders',
    @ModificationThreshold = 1,
    @LongRunningThresholdMinutes = 60,
    @LongRunningSamplePercent = 10,
    @LogToTable = N'Y',
    @Execute = N'Y';
"@

    $t60XmlOk = $false
    $t60XmlDetail = "SP_STATUPDATE_START row not found in CommandLog"

    if ($rT60a.ExitCode -eq 0) {
        # Extract RunLabel from InfoMessage ("Run: <label> (logged to CommandLog)")
        $rT60aOut = ($rT60a.Output -join "`n")
        $runLabelT60 = if ($rT60aOut -match 'Run:\s*(\S+)\s*\(logged') { $Matches[1] } else { $null }

        $xmlQry = if ($runLabelT60) {
            $safeT60 = $runLabelT60 -replace "'", "''"
            "SELECT TOP 1 ExtendedInfo FROM dbo.CommandLog WHERE CommandType = N'SP_STATUPDATE_START' AND ExtendedInfo.value('(/Parameters/RunLabel)[1]','nvarchar(100)') = N'$safeT60';"
        } else {
            "SELECT TOP 0 ExtendedInfo FROM dbo.CommandLog WHERE CommandType = N'SP_STATUPDATE_START';"
        }
        $rT60xml = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query $xmlQry
        if ($rT60xml.ExitCode -eq 0 -and $rT60xml.DataSet -and
            $rT60xml.DataSet.Tables.Count -gt 0 -and $rT60xml.DataSet.Tables[0].Rows.Count -gt 0) {
            # Read the XML element value via a separate query (avoids PS XML parsing quirks)
            $xmlRaw = [string]$rT60xml.DataSet.Tables[0].Rows[0]["ExtendedInfo"]
            $safeXml = $xmlRaw -replace "'", "''"
            $rT60val = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
                -Query "SELECT CAST(N'$safeXml' AS xml).value('(/Parameters/LongRunningThresholdMinutes)[1]','int') AS Val;"
            if ($rT60val.ExitCode -eq 0 -and $rT60val.DataSet -and
                $rT60val.DataSet.Tables.Count -gt 0 -and $rT60val.DataSet.Tables[0].Rows.Count -gt 0) {
                $xmlVal = $rT60val.DataSet.Tables[0].Rows[0]["Val"]
                if ($xmlVal -ne [DBNull]::Value -and [int]$xmlVal -eq 60) {
                    $t60XmlOk = $true
                    $t60XmlDetail = "LongRunningThresholdMinutes = 60 in SP_STATUPDATE_START ExtendedInfo XML"
                } else {
                    $t60XmlDetail = "LongRunningThresholdMinutes XML value = '$xmlVal' (expected 60)"
                }
            } else {
                $t60XmlDetail = "XML value query failed: $($rT60val.Output | Select-Object -First 1)"
            }
        }
    } else {
        $t60XmlDetail = "Proc raised exception: $($rT60a.Output | Select-Object -First 1)"
    }
    Add-Result "T60: #402 LongRunningThreshold override (XML)" $(if ($t60XmlOk) { "PASS" } else { "FAIL" }) $t60XmlDetail

    # -- Part B: behavioral -- seed a synthetic slow-stat row then dry-run --
    # Discover a real stat name on dbo.Orders so the proc can match it.
    $rStatName = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
        -Query "SELECT TOP 1 s.name AS StatName FROM sys.stats s JOIN sys.objects o ON s.object_id = o.object_id WHERE o.name = N'Orders' AND o.schema_id = SCHEMA_ID(N'dbo') ORDER BY s.stats_id;"
    $ordersStatName = if ($rStatName.ExitCode -eq 0 -and $rStatName.DataSet -and
        $rStatName.DataSet.Tables.Count -gt 0 -and $rStatName.DataSet.Tables[0].Rows.Count -gt 0) {
        [string]$rStatName.DataSet.Tables[0].Rows[0]["StatName"]
    } else { $null }

    $t60BehOk     = $false
    $t60BehDetail = "Could not discover a stat name on dbo.Orders"

    if ($ordersStatName) {
        $safeStatName = $ordersStatName -replace "'", "''"
        $t60SeedMarker = "T60_SYNTHETIC_SLOW_$([guid]::NewGuid().ToString('N'))"
        # Seed a synthetic row: CommandType=UPDATE_STATISTICS, duration >= 60 min,
        # matched by DatabaseName/SchemaName/ObjectName/StatisticsName.
        # Distinctive Command value used as cleanup marker.
        Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
            -Query @"
INSERT INTO dbo.CommandLog
    (DatabaseName, SchemaName, ObjectName, ObjectType, StatisticsName,
     Command, CommandType, StartTime, EndTime, ErrorNumber)
VALUES
    (N'$db', N'dbo', N'Orders', N'U', N'$safeStatName',
      N'$t60SeedMarker',
     N'UPDATE_STATISTICS',
     /* duration must comfortably exceed @LongRunningThresholdMinutes = 60:
        -120 min start to -15 min end = 105 min (the original -61 end gave 59) */
     DATEADD(HOUR, -2, SYSDATETIME()),
     DATEADD(MINUTE, -15, SYSDATETIME()),
     0);
"@ | Out-Null

        # Seed modifications so the stat qualifies in dry-run discovery.
        Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
            -Query "INSERT INTO dbo.Orders (CustomerName, Amount) SELECT TOP 500 N'T60beh', 1.00 FROM sys.all_columns;" | Out-Null

        $rT60b = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Timeout 300 `
            -Query @"
EXEC dbo.sp_StatUpdate
    @Databases = N'$db',
    @Tables = N'dbo.Orders',
    @ModificationThreshold = 1,
    @LongRunningThresholdMinutes = 60,
    @LongRunningSamplePercent = 10,
    @LogToTable = N'Y',
    @Execute = N'N',
    @Debug = 1;
"@

        $outT60b = ($rT60b.Output -join "`n")
        if ($rT60b.ExitCode -eq 0) {
            if ($outT60b -match 'Adaptive Sampling: Found') {
                $t60BehOk = $true
                $t60BehDetail = "Adaptive Sampling: Found message present -- slow stat detected correctly"
            } else {
                $t60BehDetail = "Adaptive Sampling: Found NOT in output (stat='$ordersStatName'). Output snippet: $($outT60b.Substring(0,[Math]::Min(300,$outT60b.Length)))"
            }
        } else {
            $t60BehDetail = "Proc raised exception: $($rT60b.Output | Select-Object -First 1)"
        }

        # Cleanup synthetic row unconditionally.
        Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
            -Query "DELETE FROM dbo.CommandLog WHERE Command = N'$t60SeedMarker';" | Out-Null
    }
    Add-Result "T60: #402 LongRunningThreshold override (behavioral)" $(if ($t60BehOk) { "PASS" } else { "FAIL" }) $t60BehDetail

    # ================================================================
    # T61: Verify #403 fix -- debug header uses friendly labels, not @i_ names
    #
    # Prior bug: debug header printed "@i_tiered_thresholds", "@i_sort_order",
    # "@i_qs_metric" etc. instead of "TieredThresholds", "@SortOrder", etc.
    # Fix: replaced @i_ variable references in RAISERROR calls with resolved values.
    #
    # Run with @Preset=NIGHTLY to exercise QS path (which showed @i_qs_metric).
    # ================================================================
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Timeout 600 `
        -Query "EXEC dbo.sp_StatUpdate @Preset = N'NIGHTLY', @Databases = N'$db', @Tables = N'dbo.Orders', @Execute = N'N', @Debug = 1;"

    $out = ($r.Output -join "`n")
    if ($r.ExitCode -eq 0) {
        $hasIInternal = $out -match '@i_tiered_thresholds' -or
                        $out -match '@i_sort_order' -or
                        $out -match '@i_qs_metric' -or
                        $out -match '@i_time_limit' -or
                        $out -match '@i_modification_threshold'
        if (-not $hasIInternal) {
            Add-Result "T61: #403 No @i_ labels in debug header" "PASS" "No internal @i_ variable names found in debug output"
        } else {
            # Find the first offending label for the detail message
            $offender = if ($out -match '(@i_\w+)') { $Matches[1] } else { "unknown @i_ name" }
            Add-Result "T61: #403 No @i_ labels in debug header" "FAIL" "Internal variable name exposed: $offender"
        }
    } else {
        Add-Result "T61: #403 No @i_ labels in debug header" "FAIL" "Proc raised exception: $($r.Output | Select-Object -First 1)"
    }

    # ================================================================
    # T62: Verify #405 fix -- SP_STATUPDATE_START Command uses public param names
    #
    # The Command column in CommandLog should contain "@TimeLimit" and "@SortOrder"
    # (public parameter names), not "@i_time_limit" or "@i_sort_order".
    #
    # Run with @LogToTable=Y and @Execute=Y -- SP_STATUPDATE_START is only logged when Execute=Y.
    # Check the CommandLog Command field for this specific run.
    # ================================================================

    # Must use @Execute=Y -- SP_STATUPDATE_START is only logged when @LogToTable=Y AND @Execute=Y.
    # Extract the RunLabel from RAISERROR output ("Run: <label> (logged to CommandLog)")
    # and use it to find the specific CommandLog entry for this run.
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Timeout 600 `
        -Query "EXEC dbo.sp_StatUpdate @Databases = N'$db', @LogToTable = N'Y', @Execute = N'Y', @TimeLimit = 120;"

    if ($r.ExitCode -eq 0) {
        # Extract RunLabel from InfoMessage output
        $rOut = ($r.Output -join "`n")
        $runLabel = if ($rOut -match 'Run:\s*(\S+)\s*\(logged') { $Matches[1] } else { $null }

        # Query CommandLog by RunLabel extracted from ExtendedInfo XML for exact match
        if ($runLabel) {
            $safeLabel = $runLabel -replace "'", "''"
            $r2 = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
                -Query "SELECT TOP 1 Command FROM dbo.CommandLog WHERE CommandType = N'SP_STATUPDATE_START' AND ExtendedInfo.value('(/Parameters/RunLabel)[1]', 'nvarchar(100)') = N'$safeLabel' ORDER BY StartTime DESC;"
        } else {
            $r2 = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
                -Query "SELECT TOP 0 Command FROM dbo.CommandLog WHERE CommandType = N'SP_STATUPDATE_START';"
        }

        if ($r2.ExitCode -eq 0 -and $r2.DataSet -and $r2.DataSet.Tables.Count -gt 0 -and $r2.DataSet.Tables[0].Rows.Count -gt 0) {
            $cmdText = [string]$r2.DataSet.Tables[0].Rows[0]["Command"]
            $hasTimeLimit  = $cmdText -match '@TimeLimit'
            $hasSortOrder  = $cmdText -match '@SortOrder'
            $hasITimeLimit = $cmdText -match '@i_time_limit'
            $hasISortOrder = $cmdText -match '@i_sort_order'

            if ($hasTimeLimit -and $hasSortOrder -and -not $hasITimeLimit -and -not $hasISortOrder) {
                Add-Result "T62: #405 START Command uses public param names" "PASS" "Command has @TimeLimit/@SortOrder, no @i_ names"
            } elseif ($hasITimeLimit -or $hasISortOrder) {
                Add-Result "T62: #405 START Command uses public param names" "FAIL" "Command contains internal @i_ name: $cmdText"
            } elseif (-not $hasTimeLimit -or -not $hasSortOrder) {
                Add-Result "T62: #405 START Command uses public param names" "FAIL" "Command missing @TimeLimit or @SortOrder: $cmdText"
            } else {
                Add-Result "T62: #405 START Command uses public param names" "FAIL" "Unexpected Command content: $cmdText"
            }
        } else {
            Add-Result "T62: #405 START Command uses public param names" "FAIL" "No SP_STATUPDATE_START entry found in CommandLog"
        }
    } else {
        Add-Result "T62: #405 START Command uses public param names" "FAIL" "Proc raised exception: $($r.Output | Select-Object -First 1)"
    }

    # ================================================================
    # T63: Verify #404 fix -- no "triggering legacy fallback" in output
    #
    # v3 removed the legacy discovery path entirely.  The phrase
    # "triggering legacy fallback" should never appear in any output.
    # v3 replaced fallback with "continuing with partial data" warnings.
    # ================================================================
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
        -Query "EXEC dbo.sp_StatUpdate @Databases = N'$db', @Execute = N'N', @Debug = 1;"

    $out = ($r.Output -join "`n")
    if ($r.ExitCode -eq 0) {
        if ($out -match 'triggering legacy fallback') {
            Add-Result "T63: #404 No 'triggering legacy fallback' phrase" "FAIL" "Legacy fallback phrase found in output (v3 should not have this)"
        } else {
            Add-Result "T63: #404 No 'triggering legacy fallback' phrase" "PASS" "Phrase absent from output"
        }
    } else {
        Add-Result "T63: #404 No 'triggering legacy fallback' phrase" "FAIL" "Proc raised exception: $($r.Output | Select-Object -First 1)"
    }

    # ================================================================
    # T64-T68: parameter override wiring verified via SP_STATUPDATE_START
    #          ExtendedInfo XML (the durable contract sp_StatUpdate_Diag consumes).
    #
    # Single proc invocation passes all five params; five separate XML assertions
    # follow -- one per original test ID so the suite count is unchanged.
    #
    # Contract: when @LogToTable=Y AND @Execute=Y, the proc writes a
    # CommandLog row CommandType='SP_STATUPDATE_START' whose ExtendedInfo XML
    # (root <Parameters>) contains the EFFECTIVE resolved values coming out of
    # the preset + override resolution block.
    #
    # XML element names verified from sp_StatUpdate.sql @parameters_xml SELECT:
    #   QueryStoreMinExecutions  (@i_qs_min_executions  AS QueryStoreMinExecutions)
    #   QueryStoreRecentHours    (@i_qs_recent_hours    AS QueryStoreRecentHours)
    #   MaxAGRedoQueueMB         (@i_max_ag_redo_queue_mb AS MaxAGRedoQueueMB)
    #   MinTempdbFreeMB          (@i_min_tempdb_free_mb AS MinTempdbFreeMB)
    #   MopUpMinRemainingSeconds (@i_mop_up_min_remaining AS MopUpMinRemainingSeconds)
    #
    # Using @ModificationThreshold=1 to guarantee at least one qualifying stat
    # so the proc doesn't short-circuit before writing the START marker.
    # ================================================================

    # Seed fresh modifications.
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
        -Query "INSERT INTO dbo.Orders (CustomerName, Amount) SELECT TOP 500 N'T64seed', 1.00 FROM sys.all_columns;" | Out-Null

    # Clear any stale applock entry left by a prior test so we don't get ALREADY_RUNNING.
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
        -Query "DELETE FROM master.dbo.StatUpdateLock WHERE LoginTime < DATEADD(MINUTE, -10, SYSDATETIME());" | Out-Null

    # Run the proc once with all five override params.
    $rT64to68 = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Timeout 600 `
        -Query @"
EXEC dbo.sp_StatUpdate
    @Databases = N'$db',
    @Tables = N'dbo.Orders',
    @ModificationThreshold = 1,
    @QueryStore = N'CPU',
    @QueryStoreMinExecutions = 50,
    @QueryStoreRecentHours = 24,
    @MaxAGRedoQueueMB = 500,
    @MinTempdbFreeMB = 100,
    @MopUpPass = N'Y',
    @TimeLimit = 3600,
    @MopUpMinRemainingSeconds = 120,
    @LogToTable = N'Y',
    @Execute = N'Y';
"@

    # Helper: read a single int element from the most recent START XML row.
    # We load the raw XML string and cast it server-side to avoid PS XML namespace issues.
    $t64to68RunLabel = $null
    if ($rT64to68.ExitCode -eq 0) {
        $combinedOut = ($rT64to68.Output -join "`n")
        $t64to68RunLabel = if ($combinedOut -match 'Run:\s*(\S+)\s*\(logged') { $Matches[1] } else { $null }
    }

    function Get-StartXmlInt {
        param([string]$SrvAddr, [int]$SrvPort, [string]$SrvPwd, [string]$RunLabel, [string]$Element)
        $whereClause = if ($RunLabel) {
            $safe = $RunLabel -replace "'", "''"
            "WHERE CommandType = N'SP_STATUPDATE_START' AND ExtendedInfo.value('(/Parameters/RunLabel)[1]','nvarchar(100)') = N'$safe'"
        } else {
            "WHERE 1 = 0 AND CommandType = N'SP_STATUPDATE_START'"
        }
        $xmlQry = "SELECT TOP 1 ExtendedInfo FROM dbo.CommandLog $whereClause;"
        $rXml = Invoke-Sql -ServerAddr $SrvAddr -Port $SrvPort -Pwd $SrvPwd -Query $xmlQry
        if ($rXml.ExitCode -ne 0 -or -not $rXml.DataSet -or
            $rXml.DataSet.Tables.Count -eq 0 -or $rXml.DataSet.Tables[0].Rows.Count -eq 0) {
            return $null
        }
        $raw = [string]$rXml.DataSet.Tables[0].Rows[0]["ExtendedInfo"]
        $safeRaw = $raw -replace "'", "''"
        $rVal = Invoke-Sql -ServerAddr $SrvAddr -Port $SrvPort -Pwd $SrvPwd `
            -Query "SELECT CAST(N'$safeRaw' AS xml).value('(/Parameters/$Element)[1]','int') AS Val;"
        if ($rVal.ExitCode -eq 0 -and $rVal.DataSet -and
            $rVal.DataSet.Tables.Count -gt 0 -and $rVal.DataSet.Tables[0].Rows.Count -gt 0) {
            $v = $rVal.DataSet.Tables[0].Rows[0]["Val"]
            if ($v -eq [DBNull]::Value) { return $null }
            return [int]$v
        }
        return $null
    }

    $t64to68ProcErr = if ($rT64to68.ExitCode -ne 0) { "Proc raised exception: $($rT64to68.Output | Select-Object -First 1)" } else { $null }

    # ----------------------------------------------------------------
    # T64: QueryStoreMinExecutions must equal 50
    # ----------------------------------------------------------------
    if ($t64to68ProcErr) {
        Add-Result "T64: @QueryStoreMinExecutions override" "FAIL" $t64to68ProcErr
    } else {
        $v64 = Get-StartXmlInt -SrvAddr $Server -SrvPort $port -SrvPwd $Password -RunLabel $t64to68RunLabel -Element "QueryStoreMinExecutions"
        if ($v64 -ne $null -and $v64 -eq 50) {
            Add-Result "T64: @QueryStoreMinExecutions override" "PASS" "SP_STATUPDATE_START XML: QueryStoreMinExecutions = 50"
        } elseif ($v64 -ne $null) {
            Add-Result "T64: @QueryStoreMinExecutions override" "FAIL" "SP_STATUPDATE_START XML: QueryStoreMinExecutions = $v64 (expected 50)"
        } else {
            Add-Result "T64: @QueryStoreMinExecutions override" "FAIL" "SP_STATUPDATE_START XML: QueryStoreMinExecutions element null or row not found"
        }
    }

    # ----------------------------------------------------------------
    # T65: QueryStoreRecentHours must equal 24
    # ----------------------------------------------------------------
    if ($t64to68ProcErr) {
        Add-Result "T65: @QueryStoreRecentHours override" "FAIL" $t64to68ProcErr
    } else {
        $v65 = Get-StartXmlInt -SrvAddr $Server -SrvPort $port -SrvPwd $Password -RunLabel $t64to68RunLabel -Element "QueryStoreRecentHours"
        if ($v65 -ne $null -and $v65 -eq 24) {
            Add-Result "T65: @QueryStoreRecentHours override" "PASS" "SP_STATUPDATE_START XML: QueryStoreRecentHours = 24"
        } elseif ($v65 -ne $null) {
            Add-Result "T65: @QueryStoreRecentHours override" "FAIL" "SP_STATUPDATE_START XML: QueryStoreRecentHours = $v65 (expected 24)"
        } else {
            Add-Result "T65: @QueryStoreRecentHours override" "FAIL" "SP_STATUPDATE_START XML: QueryStoreRecentHours element null or row not found"
        }
    }

    # ----------------------------------------------------------------
    # T66: MaxAGRedoQueueMB must equal 500
    # ----------------------------------------------------------------
    if ($t64to68ProcErr) {
        Add-Result "T66: @MaxAGRedoQueueMB accepted" "FAIL" $t64to68ProcErr
    } else {
        $v66 = Get-StartXmlInt -SrvAddr $Server -SrvPort $port -SrvPwd $Password -RunLabel $t64to68RunLabel -Element "MaxAGRedoQueueMB"
        if ($v66 -ne $null -and $v66 -eq 500) {
            Add-Result "T66: @MaxAGRedoQueueMB accepted" "PASS" "SP_STATUPDATE_START XML: MaxAGRedoQueueMB = 500"
        } elseif ($v66 -ne $null) {
            Add-Result "T66: @MaxAGRedoQueueMB accepted" "FAIL" "SP_STATUPDATE_START XML: MaxAGRedoQueueMB = $v66 (expected 500)"
        } else {
            Add-Result "T66: @MaxAGRedoQueueMB accepted" "FAIL" "SP_STATUPDATE_START XML: MaxAGRedoQueueMB element null or row not found"
        }
    }

    # ----------------------------------------------------------------
    # T67: MinTempdbFreeMB must equal 100
    # ----------------------------------------------------------------
    if ($t64to68ProcErr) {
        Add-Result "T67: @MinTempdbFreeMB accepted" "FAIL" $t64to68ProcErr
    } else {
        $v67 = Get-StartXmlInt -SrvAddr $Server -SrvPort $port -SrvPwd $Password -RunLabel $t64to68RunLabel -Element "MinTempdbFreeMB"
        if ($v67 -ne $null -and $v67 -eq 100) {
            Add-Result "T67: @MinTempdbFreeMB accepted" "PASS" "SP_STATUPDATE_START XML: MinTempdbFreeMB = 100"
        } elseif ($v67 -ne $null) {
            Add-Result "T67: @MinTempdbFreeMB accepted" "FAIL" "SP_STATUPDATE_START XML: MinTempdbFreeMB = $v67 (expected 100)"
        } else {
            Add-Result "T67: @MinTempdbFreeMB accepted" "FAIL" "SP_STATUPDATE_START XML: MinTempdbFreeMB element null or row not found"
        }
    }

    # ----------------------------------------------------------------
    # T68: MopUpMinRemainingSeconds must equal 120
    # ----------------------------------------------------------------
    if ($t64to68ProcErr) {
        Add-Result "T68: @MopUpMinRemainingSeconds override" "FAIL" $t64to68ProcErr
    } else {
        $v68 = Get-StartXmlInt -SrvAddr $Server -SrvPort $port -SrvPwd $Password -RunLabel $t64to68RunLabel -Element "MopUpMinRemainingSeconds"
        if ($v68 -ne $null -and $v68 -eq 120) {
            Add-Result "T68: @MopUpMinRemainingSeconds override" "PASS" "SP_STATUPDATE_START XML: MopUpMinRemainingSeconds = 120"
        } elseif ($v68 -ne $null) {
            Add-Result "T68: @MopUpMinRemainingSeconds override" "FAIL" "SP_STATUPDATE_START XML: MopUpMinRemainingSeconds = $v68 (expected 120)"
        } else {
            Add-Result "T68: @MopUpMinRemainingSeconds override" "FAIL" "SP_STATUPDATE_START XML: MopUpMinRemainingSeconds element null or row not found"
        }
    }

    # ================================================================
    # T69: All 5 presets complete without error (smoke test)
    #
    # Run each preset with @Execute=N and @Debug=1.  Verify ExitCode=0
    # (no exception) and no severity-16 error text in InfoMessage output.
    # PASS if all 5 complete cleanly.
    # ================================================================
    $presets     = @("DEFAULT", "NIGHTLY", "WEEKLY_FULL", "OLTP_LIGHT", "WAREHOUSE")
    $presetFails = [System.Collections.ArrayList]::new()

    foreach ($preset in $presets) {
        # Scope with @Tables to avoid SQL 2025 discovery timeout on bloated
        # catalogs (same hygiene pattern as T47/T53/T61/T64-T68).
        $rp = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Timeout 600 `
            -Query "EXEC dbo.sp_StatUpdate @Preset = N'$preset', @Databases = N'$db', @Tables = N'dbo.Orders', @Execute = N'N', @Debug = 1;"

        if ($rp.ExitCode -ne 0) {
            $null = $presetFails.Add("$preset (exception: $($rp.Output | Select-Object -First 1))")
        } else {
            # Check for error-level messages in InfoMessage output
            # (severity 16 errors captured when FireInfoMessageEventOnUserErrors=true)
            $pout = ($rp.Output -join "`n")
            if ($pout -match 'Msg \d+, Level 1[6-9]|Msg \d+, Level [2-9]\d') {
                $null = $presetFails.Add("$preset (severity >= 16 in output)")
            }
        }
    }

    if ($presetFails.Count -eq 0) {
        Add-Result "T69: All 5 presets smoke test" "PASS" "DEFAULT/NIGHTLY/WEEKLY_FULL/OLTP_LIGHT/WAREHOUSE all completed"
    } else {
        $failList = $presetFails -join "; "
        Add-Result "T69: All 5 presets smoke test" "FAIL" "Failed presets: $failList"
    }

    # ================================================================
    # RETURN per-version result object
    # ================================================================
    [PSCustomObject]@{
        Version = $ver
        Label   = $label
        Major   = $serverMajor
        Results = $results
        Passed  = @($results | Where-Object { $_.Result -eq "PASS" }).Count
        Failed  = @($results | Where-Object { $_.Result -eq "FAIL" }).Count
        Skipped = @($results | Where-Object { $_.Result -eq "SKIP" }).Count
    }
}

# =====================================================================
# Emit the release gate's structured result list when invoked by the gate runner.
if ($ResultsPath) {
    if ($Versions.Count -ne 1) { throw "Results output requires exactly one SQL version." }
    $versionResults = @($allResults | Where-Object { $_.Version -eq $Versions[0] })
    if ($versionResults.Count -ne 1 -or $versionResults[0].Results.Count -ne 11) { throw "V3Fixes expected 11 checks for one version." }
    $cfg = $VersionConfig[$Versions[0]]
    $alive = Invoke-Sql -ServerAddr $Server -Port $cfg.Port -Pwd $Password -Query "SELECT CONVERT(nvarchar(64), SERVERPROPERTY(N'ProductVersion')) AS ProductVersion;" -Timeout 10
    $serverAliveAtEnd = $alive.ExitCode -eq 0
    $sqlBuild = if ($serverAliveAtEnd -and $alive.DataSet.Tables.Count -gt 0) { [string]$alive.DataSet.Tables[0].Rows[0]["ProductVersion"] } else { "unavailable" }
    $checks = @($versionResults[0].Results | ForEach-Object {
        $status = switch ($_.Result) { "PASS" { "PASS" } "FAIL" { "FAIL" } "SKIP" { "NOT_APPLICABLE" } default { "FAIL" } }
        $check = [ordered]@{ check = $_.Test; result = $status; duration_ms = [long]$_.DurationMs }
        if ($status -ne "PASS") { $check.reason = [string]$_.Detail }
        $check
    })
    $commit = (& git -C $RepoRoot rev-parse HEAD).Trim()
    if ($LASTEXITCODE -ne 0) { throw "Could not resolve the tested source commit." }
    Import-Module (Join-Path $ScriptDir "ReleaseResults.psm1") -Force
    $endedAt = Get-Date
    $durationMs = [long](($endedAt - $script:RunStartedAt).TotalMilliseconds)
    [void](Write-ReleaseRunList -Path $ResultsPath -Commit $commit -SqlVersion $Versions[0] -SqlBuild $sqlBuild `
        -ServerAliveAtEnd $serverAliveAtEnd -Suite "V3Fixes" -ExpectedCount 11 -Checks $checks `
        -StartedAt $script:RunStartedAt -EndedAt $endedAt -DurationMs $durationMs)
}

# STEP 4: RESULTS SUMMARY
# =====================================================================
Write-Host ""
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host " RESULTS" -ForegroundColor Cyan
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host ""

$totalFailed = 0

foreach ($vr in $allResults | Sort-Object { $_.Major }) {
    $headerColor = if ($vr.Failed -eq 0) { "Green" } else { "Red" }
    $total = $vr.Passed + $vr.Failed + $vr.Skipped
    Write-Host "  $($vr.Label) (major=$($vr.Major)):  $($vr.Passed)/$total passed, $($vr.Failed) failed, $($vr.Skipped) skipped" -ForegroundColor $headerColor

    foreach ($t in $vr.Results) {
        $tc = switch ($t.Result) {
            "PASS" { "DarkGreen" }
            "FAIL" { "Red"       }
            "SKIP" { "DarkGray"  }
            default { "White"    }
        }
        $detail = if ($t.Detail) { " ($($t.Detail))" } else { "" }
        Write-Host "    [$($t.Result)] $($t.Test)$detail" -ForegroundColor $tc
    }
    Write-Host ""
    $totalFailed += $vr.Failed
}

# Tabular summary
Write-Host ("{0,-14} {1,-10} {2,-8} {3,-6}" -f "Version", "Tests", "Passed", "Status")
Write-Host ("{0,-14} {1,-10} {2,-8} {3,-6}" -f "-------", "-----", "------", "------")
foreach ($vr in $allResults | Sort-Object { $_.Major }) {
    $total  = $vr.Passed + $vr.Failed + $vr.Skipped
    $status = if ($vr.Failed -eq 0) { "PASS" } else { "FAIL" }
    $sc     = if ($vr.Failed -eq 0) { "Green" } else { "Red" }
    Write-Host ("{0,-14} {1,-10} {2,-8} {3,-6}" -f $vr.Label, $total, $vr.Passed, $status) -ForegroundColor $sc
}

Write-Host ""
if ($totalFailed -gt 0) {
    Write-Host "RESULT: $totalFailed test(s) FAILED." -ForegroundColor Red
    exit 1
} else {
    $totalTests = ($allResults | ForEach-Object { $_.Passed + $_.Failed + $_.Skipped } | Measure-Object -Sum).Sum
    Write-Host "RESULT: All $totalTests tests passed across $($allResults.Count) version(s)." -ForegroundColor Green
    exit 0
}
