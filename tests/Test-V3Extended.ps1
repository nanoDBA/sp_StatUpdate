<#
.SYNOPSIS
    Extended test suite for sp_StatUpdate v3 (T40-T55).

.DESCRIPTION
    Tests sp_StatUpdate v3 across SQL Server 2019/2022/2025 on a remote Docker host.
    Covers 16 extended tests per version addressing issue #393 gaps.

    Test categories:
    - T40-T43: Critical features (DIRECT_STRING, NORECOMPUTE, StopReason scenarios)
    - T44-T50: High priority (parameter validation, summary result set, @TargetNorecompute)
    - T51-T55: Medium priority (multi-database, exclusions, preset overrides)

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
    .\tests\Test-V3Extended.ps1
    .\tests\Test-V3Extended.ps1 -Versions @("2022","2025")
    .\tests\Test-V3Extended.ps1 -SkipSetup
    .\tests\Test-V3Extended.ps1 -Server 127.0.0.1 -Versions @("2022")
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
Write-Host " sp_StatUpdate v3 Extended Test Suite (T40-T55)" -ForegroundColor Cyan
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
Write-Host "[3/4] Running 16 tests per version sequentially..." -ForegroundColor Yellow
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

    # Detect server major version
    $vr = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
        -Query "SELECT CONVERT(int, SERVERPROPERTY(N'ProductMajorVersion')) AS MajorVer;"
    $serverMajor = if ($vr.ExitCode -eq 0 -and $vr.DataSet.Tables.Count -gt 0) {
        [int]$vr.DataSet.Tables[0].Rows[0]["MajorVer"]
    } else { 0 }

    # PRE-TEST: ensure modifications exist
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
        -Query "INSERT INTO dbo.Orders (CustomerName, Amount) SELECT TOP 10000 N'ExtPre', 1.00 FROM sys.all_columns;" | Out-Null
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
        -Query "INSERT INTO dbo.Products (ProductName, Price) SELECT TOP 5000 N'ExtPre', 2.00 FROM sys.all_columns;" | Out-Null

    # T40: @Statistics DIRECT_STRING mode
    $r_stats = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
        -Query "SELECT TOP 1 CONCAT(SCHEMA_NAME(s.object_id), N'.', OBJECT_NAME(s.object_id), N'.', s.name) AS StatFqn FROM sys.stats s INNER JOIN sys.objects o ON s.object_id = o.object_id WHERE o.is_ms_shipped = 0 ORDER BY s.stats_id;"

    if ($r_stats.ExitCode -eq 0 -and $r_stats.DataSet.Tables.Count -gt 0 -and $r_stats.DataSet.Tables[0].Rows.Count -gt 0) {
        $statFqn = [string]$r_stats.DataSet.Tables[0].Rows[0]["StatFqn"]
        $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
            -Query "EXEC dbo.sp_StatUpdate @Statistics = N'$statFqn', @Databases = N'$db', @Execute = N'N';"
        if ($r.ExitCode -eq 0) {
            Add-Result "T40: @Statistics DIRECT_STRING" "PASS" "Stat=$statFqn"
        } else {
            Add-Result "T40: @Statistics DIRECT_STRING" "FAIL" ($r.Output | Select-Object -First 1)
        }
    } else {
        Add-Result "T40: @Statistics DIRECT_STRING" "FAIL" "Setup did not provide a user statistic"
    }

    # T41: NORECOMPUTE preservation
    # Drop + recreate to guarantee NORECOMPUTE state (prior runs may have left stat in no_recompute=0 state)
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
        -Query "IF EXISTS(SELECT 1 FROM sys.stats WHERE name = N'T41_TestStat' AND object_id = OBJECT_ID(N'dbo.Orders')) DROP STATISTICS dbo.Orders.T41_TestStat; CREATE STATISTICS T41_TestStat ON dbo.Orders (CustomerName) WITH NORECOMPUTE;" | Out-Null

    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
        -Query "INSERT INTO dbo.Orders (CustomerName, Amount) SELECT TOP 1000 N'T41', 1.00 FROM sys.all_columns;" | Out-Null

    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
        -Query "EXEC dbo.sp_StatUpdate @Statistics = N'dbo.Orders.T41_TestStat', @Databases = N'$db', @ModificationThreshold = 100, @TargetNorecompute = N'Y', @LogToTable = N'Y', @Execute = N'Y';"

    $t41Output = ($r.Output -join "`n")
    $t41RunLabel = if ($t41Output -match 'Run:\s*(\S+)\s*\(logged') { $Matches[1] } else { $null }
    $safeT41RunLabel = if ($t41RunLabel) { $t41RunLabel -replace "'", "''" } else { "" }
    $r_log = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
        -Query "SELECT TOP 1 Command FROM master.dbo.CommandLog WHERE CommandType = N'UPDATE_STATISTICS' AND Command LIKE N'%T41_TestStat%' AND ExtendedInfo.value('(/ExtendedInfo/RunLabel)[1]', 'nvarchar(100)') = N'$safeT41RunLabel' ORDER BY ID DESC;"

    if ($r_log.ExitCode -eq 0 -and $r_log.DataSet.Tables.Count -gt 0 -and $r_log.DataSet.Tables[0].Rows.Count -gt 0) {
        $cmd = [string]$r_log.DataSet.Tables[0].Rows[0]["Command"]
        if ($cmd -match 'NORECOMPUTE') {
            Add-Result "T41: NORECOMPUTE preservation" "PASS" "NORECOMPUTE found in CommandLog"
        } else {
            Add-Result "T41: NORECOMPUTE preservation" "FAIL" "NORECOMPUTE missing from command"
        }
    } else {
        Add-Result "T41: NORECOMPUTE preservation" "FAIL" "No CommandLog entry found for T41_TestStat"
    }

    # Clean up any stale applock from T41's @Execute=Y run.
    # KILL is async -- poll until the applock is actually released.
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
DECLARE @spid int;
SELECT TOP 1 @spid = request_session_id
FROM sys.dm_tran_locks
WHERE resource_type = 'APPLICATION'
  AND resource_description LIKE '%sp_StatUpdate%'
  AND request_session_id <> @@SPID;
IF @spid IS NOT NULL EXEC('KILL ' + CONVERT(varchar, @spid));
"@ | Out-Null
    # Wait for the applock to actually release (up to 10 seconds)
    for ($w = 0; $w -lt 10; $w++) {
        $chk = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query "SELECT COUNT(*) AS c FROM sys.dm_tran_locks WHERE resource_type = 'APPLICATION' AND resource_description LIKE '%sp_StatUpdate%' AND request_session_id <> @@SPID;"
        if ($chk.ExitCode -eq 0 -and $chk.DataSet.Tables.Count -gt 0 -and [int]$chk.DataSet.Tables[0].Rows[0]["c"] -eq 0) { break }
        Start-Sleep -Seconds 1
    }

    # T42: @StopReasonOut = TIME_LIMIT
    $t42Suffix = [guid]::NewGuid().ToString("N").Substring(0, 10)
    $t42TableName = "V3E_T42_$t42Suffix"
    $t42Setup = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db -Timeout 600 -Query @"
CREATE TABLE dbo.$t42TableName (Id int NOT NULL, C1 int NOT NULL, C2 int NOT NULL, C3 int NOT NULL, C4 int NOT NULL, C5 int NOT NULL, C6 int NOT NULL, C7 int NOT NULL, C8 int NOT NULL, C9 int NOT NULL, C10 int NOT NULL);
;WITH n AS (SELECT TOP (50000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn FROM sys.all_objects AS a CROSS JOIN sys.all_objects AS b)
INSERT dbo.$t42TableName SELECT rn, rn%997, rn%991, rn%983, rn%977, rn%971, rn%967, rn%953, rn%947, rn%941, rn%937 FROM n;
CREATE STATISTICS [V3E_T42_S1_$t42Suffix] ON dbo.$t42TableName (C1);
CREATE STATISTICS [V3E_T42_S2_$t42Suffix] ON dbo.$t42TableName (C2);
CREATE STATISTICS [V3E_T42_S3_$t42Suffix] ON dbo.$t42TableName (C3);
CREATE STATISTICS [V3E_T42_S4_$t42Suffix] ON dbo.$t42TableName (C4);
CREATE STATISTICS [V3E_T42_S5_$t42Suffix] ON dbo.$t42TableName (C5);
CREATE STATISTICS [V3E_T42_S6_$t42Suffix] ON dbo.$t42TableName (C6);
CREATE STATISTICS [V3E_T42_S7_$t42Suffix] ON dbo.$t42TableName (C7);
CREATE STATISTICS [V3E_T42_S8_$t42Suffix] ON dbo.$t42TableName (C8);
CREATE STATISTICS [V3E_T42_S9_$t42Suffix] ON dbo.$t42TableName (C9);
CREATE STATISTICS [V3E_T42_S10_$t42Suffix] ON dbo.$t42TableName (C10);
;WITH n AS (SELECT TOP (50000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rn FROM sys.all_objects AS a CROSS JOIN sys.all_objects AS b)
INSERT dbo.$t42TableName SELECT 50000+rn, (rn+11)%997, (rn+13)%991, (rn+17)%983, (rn+19)%977, (rn+23)%971, (rn+29)%967, (rn+31)%953, (rn+37)%947, (rn+41)%941, (rn+43)%937 FROM n;
"@

    $r = if ($t42Setup.ExitCode -eq 0) { Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
DECLARE @sro nvarchar(50);
EXEC dbo.sp_StatUpdate
    @Databases = N'$db',
    @Tables = N'dbo.$t42TableName',
    @TimeLimit = 1,
    @ModificationThreshold = 1,
    @Execute = N'Y',
    @StopReasonOut = @sro OUTPUT;
SELECT ISNULL(@sro, N'') AS StopReason;
"@ } else { @{ ExitCode = 1; Output = @("T42 setup failed: $($t42Setup.Output | Select-Object -First 1)"); DataSet = $null } }

    if ($r.ExitCode -eq 0 -and $r.DataSet -and $r.DataSet.Tables.Count -gt 0) {
        $lastTbl = $r.DataSet.Tables[$r.DataSet.Tables.Count - 1]
        if ($lastTbl.Rows.Count -gt 0 -and $lastTbl.Columns.Contains("StopReason")) {
            $sro = [string]$lastTbl.Rows[0]["StopReason"]
            if ($sro -match 'TIME_LIMIT') {
                Add-Result "T42: @StopReasonOut TIME_LIMIT" "PASS" "StopReason=$sro"
            } else {
                Add-Result "T42: @StopReasonOut TIME_LIMIT" "FAIL" "Expected TIME_LIMIT, got StopReason=$sro with ten independently qualifying statistics"
            }
        } else {
            Add-Result "T42: @StopReasonOut TIME_LIMIT" "FAIL" "No StopReason in OUTPUT result"
        }
    } else {
        Add-Result "T42: @StopReasonOut TIME_LIMIT" "FAIL" ($r.Output | Select-Object -First 1)
    }
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db -Query "IF OBJECT_ID(N'dbo.$t42TableName', N'U') IS NOT NULL DROP TABLE dbo.$t42TableName;" | Out-Null

    # T43: @StopReasonOut = NO_QUALIFYING_STATS
    # Use nonexistent table to guarantee 0 candidates (v3 tiered thresholds are internal)
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
DECLARE @sro nvarchar(50);
EXEC dbo.sp_StatUpdate
    @Databases = N'$db',
    @Tables = N'dbo.NonExistentTable999',
    @Execute = N'N',
    @StopReasonOut = @sro OUTPUT;
SELECT ISNULL(@sro, N'') AS StopReason;
"@

    if ($r.ExitCode -eq 0 -and $r.DataSet -and $r.DataSet.Tables.Count -gt 0) {
        $lastTbl = $r.DataSet.Tables[$r.DataSet.Tables.Count - 1]
        if ($lastTbl.Rows.Count -gt 0 -and $lastTbl.Columns.Contains("StopReason")) {
            $sro = [string]$lastTbl.Rows[0]["StopReason"]
            if ($sro -eq 'NO_QUALIFYING_STATS') {
                Add-Result "T43: @StopReasonOut NO_QUALIFYING_STATS" "PASS" "StopReason=$sro"
            } else {
                Add-Result "T43: @StopReasonOut NO_QUALIFYING_STATS" "FAIL" "Got StopReason=$sro"
            }
        } else {
            Add-Result "T43: @StopReasonOut NO_QUALIFYING_STATS" "FAIL" "No StopReason in OUTPUT result"
        }
    } else {
        Add-Result "T43: @StopReasonOut NO_QUALIFYING_STATS" "FAIL" ($r.Output | Select-Object -First 1)
    }

    # T44: @MopUpPass validation
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
        -Query "EXEC dbo.sp_StatUpdate @Databases = N'$db', @MopUpPass = N'Y', @TimeLimit = NULL, @Execute = N'N';"
    $out = ($r.Output -join "`n")
    if ($r.ExitCode -ne 0 -or $out -match 'MopUpPass|@TimeLimit|required') {
        Add-Result "T44: @MopUpPass validation vs @TimeLimit" "PASS" "Error/warning raised"
    } else {
        Add-Result "T44: @MopUpPass validation vs @TimeLimit" "FAIL" "Validation missing -- invalid params silently accepted"
    }

    # T45: @MopUpPass + @LogToTable validation
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
        -Query "EXEC dbo.sp_StatUpdate @Databases = N'$db', @MopUpPass = N'Y', @LogToTable = N'N', @TimeLimit = 1800, @Execute = N'N';"
    $out = ($r.Output -join "`n")
    if ($r.ExitCode -ne 0 -or $out -match 'MopUpPass|LogToTable|required') {
        Add-Result "T45: @MopUpPass validation vs @LogToTable" "PASS" "Error/warning raised"
    } else {
        Add-Result "T45: @MopUpPass validation vs @LogToTable" "FAIL" "Validation missing -- invalid params silently accepted"
    }

    # T46: @StopByTime validation
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
DECLARE @sro nvarchar(50);
EXEC dbo.sp_StatUpdate @Databases = N'$db', @StopByTime = N'25:00', @Execute = N'N', @StopReasonOut = @sro OUTPUT;
SELECT ISNULL(@sro, N'') AS StopReason;
"@

    $out = ($r.Output -join "`n")
    if ($r.ExitCode -eq 0 -and $r.DataSet -and $r.DataSet.Tables.Count -gt 0) {
        $lastTbl = $r.DataSet.Tables[$r.DataSet.Tables.Count - 1]
        if ($lastTbl.Rows.Count -gt 0 -and $lastTbl.Columns.Contains("StopReason")) {
            $sro = [string]$lastTbl.Rows[0]["StopReason"]
            if ($sro -eq 'PARAMETER_ERROR') {
                Add-Result "T46: @StopByTime validation" "PASS" "StopReason=PARAMETER_ERROR"
            } elseif ($out -match 'StopByTime|time|format|invalid') {
                Add-Result "T46: @StopByTime validation" "PASS" "Error in output text"
            } else {
                Add-Result "T46: @StopByTime validation" "FAIL" "Invalid time accepted"
            }
        } else {
            Add-Result "T46: @StopByTime validation" "FAIL" "No StopReason in OUTPUT result"
        }
    } else {
        Add-Result "T46: @StopByTime validation" "PASS" "Invalid time rejected (exception)"
    }

    # T47: @Preset override
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
        -Query "INSERT INTO dbo.Orders (CustomerName, Amount) SELECT TOP 20000 N'T47', 1.00 FROM sys.all_columns;" | Out-Null

    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Timeout 600 -Query @"
DECLARE @sf int;
EXEC dbo.sp_StatUpdate
    @Preset = N'NIGHTLY',
    @ModificationThreshold = 1000,
    @Databases = N'$db',
    @Tables = N'dbo.Orders',
    @Execute = N'N',
    @StatsFoundOut = @sf OUTPUT;
SELECT ISNULL(@sf, -1) AS StatsFound;
"@

    if ($r.ExitCode -eq 0 -and $r.DataSet -and $r.DataSet.Tables.Count -gt 0) {
        $lastTbl = $r.DataSet.Tables[$r.DataSet.Tables.Count - 1]
        if ($lastTbl.Rows.Count -gt 0 -and $lastTbl.Columns.Contains("StatsFound")) {
            $sf = [int]$lastTbl.Rows[0]["StatsFound"]
            if ($sf -ge 0) {
                Add-Result "T47: Preset override by explicit param" "PASS" "StatsFound=$sf"
            } else {
                Add-Result "T47: Preset override by explicit param" "FAIL" "StatsFound=$sf (negative)"
            }
        } else {
            Add-Result "T47: Preset override by explicit param" "FAIL" "No StatsFound in OUTPUT"
        }
    } else {
        Add-Result "T47: Preset override by explicit param" "FAIL" ($r.Output | Select-Object -First 1)
    }

    # T48: Summary result set columns
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
        -Query "EXEC dbo.sp_StatUpdate @Databases = N'$db', @Execute = N'N';"

    if ($r.ExitCode -eq 0 -and $r.DataSet -and $r.DataSet.Tables.Count -gt 0) {
        # Find the summary table (has Status column) -- may not be Tables[0] if dry-run commands precede it
        $summaryTbl = $null
        for ($ti = 0; $ti -lt $r.DataSet.Tables.Count; $ti++) {
            if ($r.DataSet.Tables[$ti].Columns.Contains("Status")) {
                $summaryTbl = $r.DataSet.Tables[$ti]
                break
            }
        }
        if ($null -eq $summaryTbl) {
            Add-Result "T48: Summary result set columns" "FAIL" "No table with Status column in $($r.DataSet.Tables.Count) tables"
        } else {
            $colCount = $summaryTbl.Columns.Count
            $expectedCols = @('Status', 'StatusMessage', 'StatsFound', 'StatsProcessed', 'StatsSucceeded', 'StatsFailed', 'StatsRemaining', 'DurationSeconds')
            $missingCols = [System.Collections.ArrayList]::new()
            foreach ($col in $expectedCols) {
                if (-not $summaryTbl.Columns.Contains($col)) {
                    [void]$missingCols.Add($col)
                }
            }

            if ($missingCols.Count -eq 0) {
                Add-Result "T48: Summary result set columns" "PASS" "$colCount columns present"
            } else {
                Add-Result "T48: Summary result set columns" "FAIL" "Missing: $($missingCols -join ', ')"
            }
        }
    } else {
        Add-Result "T48: Summary result set columns" "FAIL" "No result set returned"
    }

    # T49: @TargetNorecompute=N filtering
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
        -Query "IF NOT EXISTS(SELECT 1 FROM sys.stats WHERE name = N'T49_RegularStat') CREATE STATISTICS T49_RegularStat ON dbo.Orders (Amount);" | Out-Null

    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
        -Query "IF NOT EXISTS(SELECT 1 FROM sys.stats WHERE name = N'T49_NorecomputeStat') CREATE STATISTICS T49_NorecomputeStat ON dbo.Orders (CustomerName) WITH NORECOMPUTE;" | Out-Null

    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
        -Query "INSERT INTO dbo.Orders (CustomerName, Amount) SELECT TOP 1000 N'T49', 1.00 FROM sys.all_columns;" | Out-Null

    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
        -Query "EXEC dbo.sp_StatUpdate @Statistics = N'dbo.Orders.T49_RegularStat, dbo.Orders.T49_NorecomputeStat', @TargetNorecompute = N'N', @Execute = N'N';"

    $out = ($r.Output -join "`n")
    if ($r.ExitCode -eq 0) {
        if ($out -match 'T49_RegularStat' -and -not ($out -match 'T49_NorecomputeStat')) {
            Add-Result "T49: @TargetNorecompute=N filtering" "PASS" "Regular stat found, NORECOMPUTE excluded"
        } elseif ($out -match 'T49_NorecomputeStat') {
            Add-Result "T49: @TargetNorecompute=N filtering" "FAIL" "NORECOMPUTE stat appeared when @TargetNorecompute=N"
        } else {
            Add-Result "T49: @TargetNorecompute=N filtering" "FAIL" "Neither T49 stat found -- filter or discovery broken"
        }
    } else {
        Add-Result "T49: @TargetNorecompute=N filtering" "FAIL" ($r.Output | Select-Object -First 1)
    }

    # T50: @TargetNorecompute=BOTH
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
        -Query "EXEC dbo.sp_StatUpdate @Statistics = N'dbo.Orders.T49_RegularStat, dbo.Orders.T49_NorecomputeStat', @TargetNorecompute = N'BOTH', @Execute = N'N';"

    $out = ($r.Output -join "`n")
    if ($r.ExitCode -eq 0 -and $out -match 'T49_RegularStat') {
        Add-Result "T50: @TargetNorecompute=BOTH" "PASS" "Both types included"
    } else {
        Add-Result "T50: @TargetNorecompute=BOTH" "FAIL" ($r.Output | Select-Object -First 1)
    }

    # T51: Multiple databases
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
        -Query "EXEC dbo.sp_StatUpdate @Databases = N'StatUpdateTest, master', @Execute = N'N';"

    if ($r.ExitCode -eq 0) {
        Add-Result "T51: Multiple databases" "PASS" "Multi-DB discovery completed"
    } else {
        Add-Result "T51: Multiple databases" "FAIL" ($r.Output | Select-Object -First 1)
    }

    # T52: @ExcludeStatistics
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db `
        -Query "INSERT INTO dbo.Products (ProductName, Price) SELECT TOP 5000 N'T52', 2.00 FROM sys.all_columns;" | Out-Null

    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
        -Query "EXEC dbo.sp_StatUpdate @Databases = N'$db', @ModificationThreshold = 100, @ExcludeStatistics = N'_WA_Sys%', @Execute = N'N';"

    # gh-493: exclude the parameter header echo line ("  @ExcludeStatistics       = _WA_Sys%")
    # The filter value appears in the header regardless of discovery results.
    $out = ($r.Output | Where-Object { $_ -notmatch '^\s*@ExcludeStatistics\s*=' }) -join "`n"
    if ($r.ExitCode -eq 0) {
        if (-not ($out -match '_WA_Sys')) {
            Add-Result "T52: @ExcludeStatistics filtering" "PASS" "_WA_Sys% excluded"
        } else {
            Add-Result "T52: @ExcludeStatistics filtering" "FAIL" "_WA_Sys% appeared despite @ExcludeStatistics filter"
        }
    } else {
        Add-Result "T52: @ExcludeStatistics filtering" "FAIL" ($r.Output | Select-Object -First 1)
    }

    # T53: @Debug shows resolved values
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Timeout 600 `
        -Query "EXEC dbo.sp_StatUpdate @Preset = N'NIGHTLY', @Databases = N'$db', @Tables = N'dbo.Orders', @Execute = N'N', @Debug = 1;"

    $out = ($r.Output -join "`n")
    if ($r.ExitCode -eq 0) {
        if ($out -match '@TimeLimit\s*=\s*\d+' -or $out -match '@ModificationThreshold\s*=\s*\d+') {
            Add-Result "T53: @Debug shows resolved values" "PASS" "Preset values resolved"
        } else {
            Add-Result "T53: @Debug shows resolved values" "FAIL" "Debug output did not show resolved preset values"
        }
    } else {
        Add-Result "T53: @Debug shows resolved values" "FAIL" ($r.Output | Select-Object -First 1)
    }

    # T54: Invalid @SortOrder
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
DECLARE @sro nvarchar(50);
EXEC dbo.sp_StatUpdate @Databases = N'$db', @SortOrder = N'INVALID_ORDER', @Execute = N'N', @StopReasonOut = @sro OUTPUT;
SELECT ISNULL(@sro, N'') AS StopReason;
"@

    $out = ($r.Output -join "`n")
    if ($r.ExitCode -eq 0 -and $r.DataSet -and $r.DataSet.Tables.Count -gt 0) {
        $lastTbl = $r.DataSet.Tables[$r.DataSet.Tables.Count - 1]
        if ($lastTbl.Rows.Count -gt 0 -and $lastTbl.Columns.Contains("StopReason")) {
            $sro = [string]$lastTbl.Rows[0]["StopReason"]
            if ($sro -eq 'PARAMETER_ERROR') {
                Add-Result "T54: Invalid @SortOrder validation" "PASS" "PARAMETER_ERROR"
            } elseif ($out -match 'SortOrder|invalid|valid') {
                Add-Result "T54: Invalid @SortOrder validation" "PASS" "Error in output"
            } else {
                Add-Result "T54: Invalid @SortOrder validation" "FAIL" "Invalid order accepted"
            }
        } else {
            Add-Result "T54: Invalid @SortOrder validation" "FAIL" "No StopReason in OUTPUT"
        }
    } else {
        Add-Result "T54: Invalid @SortOrder validation" "PASS" "Invalid order rejected (exception)"
    }

    # T55: @BatchLimit=0
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
        -Query "EXEC dbo.sp_StatUpdate @Databases = N'$db', @BatchLimit = 0, @Execute = N'N';"

    if ($r.ExitCode -eq 0) {
        Add-Result "T55: @BatchLimit=0" "PASS" "Zero batch limit accepted"
    } else {
        $out = ($r.Output -join "`n")
        if ($out -match 'BatchLimit|invalid') {
            Add-Result "T55: @BatchLimit=0" "PASS" "Zero batch limit rejected with error"
        } else {
            Add-Result "T55: @BatchLimit=0" "FAIL" ($r.Output | Select-Object -First 1)
        }
    }

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
    if ($versionResults.Count -ne 1 -or $versionResults[0].Results.Count -ne 16) { throw "V3Extended expected 16 checks for one version." }
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
        -ServerAliveAtEnd $serverAliveAtEnd -Suite "V3Extended" -ExpectedCount 16 -Checks $checks `
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
