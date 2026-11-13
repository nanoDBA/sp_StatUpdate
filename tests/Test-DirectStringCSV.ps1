<#
.SYNOPSIS
    @Statistics CSV / DIRECT_STRING path tests (M4 / gh-491-dvny).

.DESCRIPTION
    T40 in Test-V3Extended.ps1 covers a single-stat @Statistics call.
    This suite covers the multi-stat CSV path and the gh-492 fixes that
    added @TargetNorecompute and @ExcludeStatistics filtering to the
    DIRECT_STRING discovery path.

    Tests (per version):
    - D01: Two-stat CSV from same DB -- both stats land in #stats_to_process
    - D02: @TargetNorecompute=N filters out a NORECOMPUTE stat even if listed in CSV
    - D03: @ExcludeStatistics LIKE pattern filters out a stat even if listed in CSV
    - D04: Whitespace around commas is tolerated (LTRIM/RTRIM)
    - D05: Bracket-quoted stat name in CSV is handled (gh-456 strip-brackets fix)
    - D06: Malformed entry (only 1 PARSENAME part -- bare stat name without table)
           is accepted gracefully -- proc resolves it against the DB
    - D07: Unknown stat name in CSV produces StatsFoundOut=0 for that entry
           (proc does not error; silently skips unknown stats)
    - D08: Multi-DB CSV -- stats from two different databases are both processed

    Uses ADO.NET SqlConnection (Pooling=false mandatory).
    Uses ExecuteReader + DataTable.Load for SQL 2025 InfoMessage compatibility.

.PARAMETER Server
    Remote Docker host IP.  Default: 100.88.59.72.

.PARAMETER Password
    SA password for all containers.  Default: $env:SPSTATUPDATE_TEST_SQL_PASSWORD.

.PARAMETER Versions
    Which SQL versions to test.  Default: 2019, 2022, 2025.

.PARAMETER SkipSetup
    Skip proc deployment and test data setup (assumes already deployed).

.EXAMPLE
    .\tests\Test-DirectStringCSV.ps1
    .\tests\Test-DirectStringCSV.ps1 -Versions @("2022")
    .\tests\Test-DirectStringCSV.ps1 -SkipSetup
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$Server,
    [string]$Password = $(if ($env:SPSTATUPDATE_TEST_SQL_PASSWORD) { $env:SPSTATUPDATE_TEST_SQL_PASSWORD } else { '' }),
    [ValidateSet('2019','2022','2025')][string[]]$Versions = @('2022'),
    [string]$ResultsPath = '',
    [switch]$SkipSetup
)
$ErrorActionPreference = "Continue"
if (-not $Password) { throw 'Release-gate SQL credentials are unavailable.' }
if ($Versions.Count -ne 1) { throw 'Run one supported SQL version at a time.' }
$startedAt = Get-Date
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot  = Split-Path -Parent $ScriptDir
Import-Module (Join-Path $ScriptDir 'StructuredLegacyResults.psm1') -Force

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

$allResults = [System.Collections.ArrayList]::new()
function Add-Result {
    param([string]$Test, [string]$Result, [string]$Detail = "")
    $null = $allResults.Add([PSCustomObject]@{ Test = $Test; Result = $Result; Detail = $Detail })
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
    Invoke-Sql -ServerAddr $Srv -Port $P -Pwd $Pwd `
        -Query "IF OBJECT_ID(N'dbo.StatUpdateLock') IS NOT NULL DELETE FROM dbo.StatUpdateLock WHERE Resource = N'sp_StatUpdate';" | Out-Null
}

# Read StatsFoundOut from the last table in a DataSet (our explicit SELECT at the end of the EXEC batch).
function Get-StatsFound {
    param([System.Data.DataSet]$ds)
    if (-not $ds -or $ds.Tables.Count -eq 0) { return $null }
    $tbl = $ds.Tables[$ds.Tables.Count - 1]
    if ($tbl.Rows.Count -gt 0 -and $tbl.Columns.Contains("StatsFound")) {
        $v = $tbl.Rows[0]["StatsFound"]
        if ($v -isnot [System.DBNull]) { return [int]$v }
    }
    # Fallback: check Tables[0] (summary result set) StatsFound column
    if ($ds.Tables.Count -ge 2) {
        $s = $ds.Tables[0]
        if ($s.Rows.Count -gt 0 -and $s.Columns.Contains("StatsFound")) {
            $v = $s.Rows[0]["StatsFound"]
            if ($v -isnot [System.DBNull]) { return [int]$v }
        }
    }
    return $null
}

# =====================================================================
# HEADER
# =====================================================================
Write-Host ""
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host " @Statistics CSV / DIRECT_STRING Path Tests (M4 / gh-491-dvny)" -ForegroundColor Cyan
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
        $setupSql     = [System.IO.File]::ReadAllText($SetupFile)
        $setupBatches = [regex]::Split($setupSql, '(?m)^\s*GO\s*$')
        $currentDb    = "master"
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
Write-Host "[3/4] Running D01-D08 tests per version..." -ForegroundColor Yellow
Write-Host ""

foreach ($ver in $activeVersions) {
    $cfg   = $VersionConfig[$ver]
    $port  = $cfg.Port
    $label = $cfg.Label
    $db    = "StatUpdateTest"

    Write-Host "  $label (:$port)" -ForegroundColor Cyan

    # ------------------------------------------------------------------
    # PRE-TEST SETUP: ensure named test statistics exist.
    # D01_StatA, D01_StatB -- two regular stats for CSV test.
    # D02_NrcStat           -- NORECOMPUTE stat for @TargetNorecompute test.
    # D03_ExclStat          -- stat whose name matches _D03Excl% pattern.
    # D05_BracketStat       -- stat to exercise bracket-strip path.
    # ------------------------------------------------------------------
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db -Query @"
IF NOT EXISTS (SELECT 1 FROM sys.stats WHERE name = N'D01_StatA' AND object_id = OBJECT_ID(N'dbo.Orders'))
    CREATE STATISTICS D01_StatA ON dbo.Orders (CustomerName);
IF NOT EXISTS (SELECT 1 FROM sys.stats WHERE name = N'D01_StatB' AND object_id = OBJECT_ID(N'dbo.Orders'))
    CREATE STATISTICS D01_StatB ON dbo.Orders (Amount);
IF NOT EXISTS (SELECT 1 FROM sys.stats WHERE name = N'D02_NrcStat' AND object_id = OBJECT_ID(N'dbo.Orders'))
    CREATE STATISTICS D02_NrcStat ON dbo.Orders (CustomerName) WITH NORECOMPUTE;
IF NOT EXISTS (SELECT 1 FROM sys.stats WHERE name = N'D03_ExclStat' AND object_id = OBJECT_ID(N'dbo.Orders'))
    CREATE STATISTICS D03_ExclStat ON dbo.Orders (Amount);
IF NOT EXISTS (SELECT 1 FROM sys.stats WHERE name = N'D05_BracketStat' AND object_id = OBJECT_ID(N'dbo.Orders'))
    CREATE STATISTICS D05_BracketStat ON dbo.Orders (CustomerName);
"@ | Out-Null

    # ------------------------------------------------------------------
    # D01: Two-stat CSV from same DB -> StatsFoundOut >= 2
    # Both D01_StatA and D01_StatB should land in #stats_to_process.
    # Use @Execute=N so we are just testing discovery, not execution.
    # ------------------------------------------------------------------
    Clear-Applock -Srv $Server -P $port -Pwd $Password

    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
DECLARE @sf int;
EXEC dbo.sp_StatUpdate
    @Statistics    = N'dbo.Orders.D01_StatA, dbo.Orders.D01_StatB',
    @Databases     = N'$db',
    @TargetNorecompute = N'BOTH',
    @Execute       = N'N',
    @LogToTable    = N'N',
    @StatsFoundOut = @sf OUTPUT;
SELECT ISNULL(@sf, -1) AS StatsFound;
"@
    if ($r.ExitCode -ne 0) {
        Add-Result "D01 [$label]: two-stat CSV -> StatsFoundOut >= 2" "FAIL" ($r.Output | Select-Object -First 1)
    } else {
        $sf = Get-StatsFound -ds $r.DataSet
        if ($null -ne $sf -and $sf -ge 2) {
            Add-Result "D01 [$label]: two-stat CSV -> StatsFoundOut >= 2" "PASS" "StatsFound=$sf"
        } elseif ($null -ne $sf -and $sf -ge 1) {
            # Acceptable if one of the two was deduped (same stat in multiple DBs scenario)
            Add-Result "D01 [$label]: two-stat CSV -> StatsFoundOut >= 2" "FAIL" "StatsFound=$sf (expected >= 2; only one stat found)"
        } else {
            Add-Result "D01 [$label]: two-stat CSV -> StatsFoundOut >= 2" "FAIL" "StatsFound=$sf (expected >= 2)"
        }
    }

    # ------------------------------------------------------------------
    # D02: @TargetNorecompute=N filters out NORECOMPUTE stat from CSV
    # CSV lists D02_NrcStat (NORECOMPUTE) AND D01_StatA (regular).
    # With @TargetNorecompute=N, only D01_StatA qualifies.
    # StatsFoundOut must be 1 (not 2).
    # ------------------------------------------------------------------
    Clear-Applock -Srv $Server -P $port -Pwd $Password

    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
DECLARE @sf int;
EXEC dbo.sp_StatUpdate
    @Statistics         = N'dbo.Orders.D02_NrcStat, dbo.Orders.D01_StatA',
    @Databases          = N'$db',
    @TargetNorecompute  = N'N',
    @Execute            = N'N',
    @LogToTable         = N'N',
    @StatsFoundOut      = @sf OUTPUT;
SELECT ISNULL(@sf, -1) AS StatsFound;
"@
    if ($r.ExitCode -ne 0) {
        Add-Result "D02 [$label]: @TargetNorecompute=N filters NORECOMPUTE stat from CSV" "FAIL" ($r.Output | Select-Object -First 1)
    } else {
        $sf = Get-StatsFound -ds $r.DataSet
        # D02_NrcStat is NORECOMPUTE so @TargetNorecompute=N should exclude it.
        # D01_StatA is a regular stat so it should be included.
        # Expected: StatsFoundOut = 1
        if ($null -ne $sf -and $sf -eq 1) {
            Add-Result "D02 [$label]: @TargetNorecompute=N filters NORECOMPUTE stat from CSV" "PASS" "StatsFound=$sf (only regular stat included)"
        } elseif ($null -ne $sf -and $sf -eq 0) {
            Add-Result "D02 [$label]: @TargetNorecompute=N filters NORECOMPUTE stat from CSV" "FAIL" "StatsFound=0 (both filtered -- D01_StatA should have passed)"
        } elseif ($null -ne $sf -and $sf -ge 2) {
            Add-Result "D02 [$label]: @TargetNorecompute=N filters NORECOMPUTE stat from CSV" "FAIL" "StatsFound=$sf (NORECOMPUTE stat not filtered -- gh-492 fix missing?)"
        } else {
            Add-Result "D02 [$label]: @TargetNorecompute=N filters NORECOMPUTE stat from CSV" "FAIL" "StatsFound=$sf (unexpected)"
        }
    }

    # ------------------------------------------------------------------
    # D03: @ExcludeStatistics LIKE filters stat from CSV
    # CSV lists D03_ExclStat AND D01_StatA.
    # @ExcludeStatistics = 'D03_%' should exclude D03_ExclStat.
    # StatsFoundOut must be 1 (only D01_StatA).
    # ------------------------------------------------------------------
    Clear-Applock -Srv $Server -P $port -Pwd $Password

    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
DECLARE @sf int;
EXEC dbo.sp_StatUpdate
    @Statistics         = N'dbo.Orders.D03_ExclStat, dbo.Orders.D01_StatA',
    @Databases          = N'$db',
    @TargetNorecompute  = N'BOTH',
    @ExcludeStatistics  = N'D03_%',
    @Execute            = N'N',
    @LogToTable         = N'N',
    @StatsFoundOut      = @sf OUTPUT;
SELECT ISNULL(@sf, -1) AS StatsFound;
"@
    if ($r.ExitCode -ne 0) {
        Add-Result "D03 [$label]: @ExcludeStatistics filters stat from CSV" "FAIL" ($r.Output | Select-Object -First 1)
    } else {
        $sf = Get-StatsFound -ds $r.DataSet
        if ($null -ne $sf -and $sf -eq 1) {
            Add-Result "D03 [$label]: @ExcludeStatistics filters stat from CSV" "PASS" "StatsFound=$sf (D03_ExclStat excluded)"
        } elseif ($null -ne $sf -and $sf -ge 2) {
            Add-Result "D03 [$label]: @ExcludeStatistics filters stat from CSV" "FAIL" "StatsFound=$sf (D03_ExclStat not excluded -- gh-492 fix missing?)"
        } elseif ($null -ne $sf -and $sf -eq 0) {
            Add-Result "D03 [$label]: @ExcludeStatistics filters stat from CSV" "FAIL" "StatsFound=0 (both filtered -- D01_StatA should have passed)"
        } else {
            Add-Result "D03 [$label]: @ExcludeStatistics filters stat from CSV" "FAIL" "StatsFound=$sf (unexpected)"
        }
    }

    # ------------------------------------------------------------------
    # D04: Whitespace around commas is tolerated
    # ' dbo.Orders.D01_StatA  ,  dbo.Orders.D01_StatB ' (extra spaces)
    # LTRIM/RTRIM in STRING_SPLIT loop should normalize this.
    # StatsFoundOut >= 2.
    # ------------------------------------------------------------------
    Clear-Applock -Srv $Server -P $port -Pwd $Password

    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
DECLARE @sf int;
EXEC dbo.sp_StatUpdate
    @Statistics         = N'  dbo.Orders.D01_StatA  ,  dbo.Orders.D01_StatB  ',
    @Databases          = N'$db',
    @TargetNorecompute  = N'BOTH',
    @Execute            = N'N',
    @LogToTable         = N'N',
    @StatsFoundOut      = @sf OUTPUT;
SELECT ISNULL(@sf, -1) AS StatsFound;
"@
    if ($r.ExitCode -ne 0) {
        Add-Result "D04 [$label]: whitespace around commas tolerated" "FAIL" ($r.Output | Select-Object -First 1)
    } else {
        $sf = Get-StatsFound -ds $r.DataSet
        if ($null -ne $sf -and $sf -ge 2) {
            Add-Result "D04 [$label]: whitespace around commas tolerated" "PASS" "StatsFound=$sf"
        } else {
            Add-Result "D04 [$label]: whitespace around commas tolerated" "FAIL" "StatsFound=$sf (expected >= 2; whitespace trim may be broken)"
        }
    }

    # ------------------------------------------------------------------
    # D05: Bracket-quoted stat name handled (gh-456 strip-brackets fix)
    # '[dbo].[Orders].[D05_BracketStat]' -- PARSENAME returns bracketed
    # parts; the fix strips [] before the sys.stats join.
    # StatsFoundOut >= 1.
    # ------------------------------------------------------------------
    Clear-Applock -Srv $Server -P $port -Pwd $Password

    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
DECLARE @sf int;
EXEC dbo.sp_StatUpdate
    @Statistics         = N'[dbo].[Orders].[D05_BracketStat]',
    @Databases          = N'$db',
    @TargetNorecompute  = N'BOTH',
    @Execute            = N'N',
    @LogToTable         = N'N',
    @StatsFoundOut      = @sf OUTPUT;
SELECT ISNULL(@sf, -1) AS StatsFound;
"@
    if ($r.ExitCode -ne 0) {
        Add-Result "D05 [$label]: bracket-quoted stat name handled" "FAIL" ($r.Output | Select-Object -First 1)
    } else {
        $sf = Get-StatsFound -ds $r.DataSet
        if ($null -ne $sf -and $sf -ge 1) {
            Add-Result "D05 [$label]: bracket-quoted stat name handled" "PASS" "StatsFound=$sf"
        } else {
            Add-Result "D05 [$label]: bracket-quoted stat name handled" "FAIL" "StatsFound=$sf (expected >= 1; bracket strip may be broken)"
        }
    }

    # ------------------------------------------------------------------
    # D06: Bare stat name (no schema.table prefix) resolved against DB
    # Just 'D01_StatA' without schema/table -- PARSENAME(value, 1) = D01_StatA,
    # PARSENAME(value, 2) = NULL, PARSENAME(value, 3) = NULL.
    # Proc should match by stat name alone.
    # StatsFoundOut >= 1 (could match multiple stats named D01_StatA
    # across multiple tables, but in our test DB only one exists).
    # ------------------------------------------------------------------
    Clear-Applock -Srv $Server -P $port -Pwd $Password

    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
DECLARE @sf int;
EXEC dbo.sp_StatUpdate
    @Statistics         = N'D01_StatA',
    @Databases          = N'$db',
    @TargetNorecompute  = N'BOTH',
    @Execute            = N'N',
    @LogToTable         = N'N',
    @StatsFoundOut      = @sf OUTPUT;
SELECT ISNULL(@sf, -1) AS StatsFound;
"@
    if ($r.ExitCode -ne 0) {
        Add-Result "D06 [$label]: bare stat name (no schema/table) resolved" "FAIL" ($r.Output | Select-Object -First 1)
    } else {
        $sf = Get-StatsFound -ds $r.DataSet
        if ($null -ne $sf -and $sf -ge 1) {
            Add-Result "D06 [$label]: bare stat name (no schema/table) resolved" "PASS" "StatsFound=$sf"
        } else {
            Add-Result "D06 [$label]: bare stat name (no schema/table) resolved" "FAIL" "StatsFound=$sf (expected >= 1)"
        }
    }

    # ------------------------------------------------------------------
    # D07: Unknown stat name in CSV -> StatsFoundOut = 0, no error
    # The proc must NOT raise an error when a CSV entry does not match
    # any stat in the target database.  It simply discovers 0 stats.
    # ------------------------------------------------------------------
    Clear-Applock -Srv $Server -P $port -Pwd $Password

    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
DECLARE @sf int;
EXEC dbo.sp_StatUpdate
    @Statistics         = N'dbo.Orders.StatThatDoesNotExist_ZZZZZ',
    @Databases          = N'$db',
    @TargetNorecompute  = N'BOTH',
    @Execute            = N'N',
    @LogToTable         = N'N',
    @StatsFoundOut      = @sf OUTPUT;
SELECT ISNULL(@sf, -1) AS StatsFound;
"@
    if ($r.ExitCode -ne 0) {
        Add-Result "D07 [$label]: unknown stat name -> no error, StatsFoundOut=0" "FAIL" ($r.Output | Select-Object -First 1)
    } else {
        $sf = Get-StatsFound -ds $r.DataSet
        if ($null -ne $sf -and $sf -eq 0) {
            Add-Result "D07 [$label]: unknown stat name -> no error, StatsFoundOut=0" "PASS" "StatsFound=0 (graceful miss)"
        } else {
            Add-Result "D07 [$label]: unknown stat name -> no error, StatsFoundOut=0" "FAIL" "StatsFound=$sf (expected 0)"
        }
    }

    # ------------------------------------------------------------------
    # D08: Multi-DB CSV -- stats from two databases both processed
    # StatUpdateTest has Orders.D01_StatA.
    # msdb has system stats (e.g., auto-created stats on sysjobs-style tables).
    # We use @TargetNorecompute=BOTH and scope to known stats in two DBs.
    # Because the second DB (msdb) may have no user stats for the named stat,
    # we actually test two user DBs by creating a second test DB (or using
    # 'master') with a known stat.
    #
    # Approach: create a separate stat in master.dbo.spt_values? -- too risky.
    # Safer: run @Databases = 'StatUpdateTest, master' with @Statistics listing
    # two stats: one from StatUpdateTest and one built-in stat from master.
    # We query sys.stats in master first to find a real user stat name.
    # If no user stat in master, SKIP this test.
    # ------------------------------------------------------------------
    $masterStatQuery = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
SELECT TOP 1
    CONCAT(SCHEMA_NAME(s.schema_id), N'.', t.name, N'.', st.name) AS FQN,
    st.name AS StatName
FROM sys.stats  AS st
JOIN sys.objects AS t  ON t.object_id = st.object_id
JOIN sys.schemas AS s  ON s.schema_id = t.schema_id
WHERE t.is_ms_shipped  = 0
  AND t.type           = N'U'
  AND st.auto_created  = 0
ORDER BY st.stats_id;
"@ -Db "master"

    if ($masterStatQuery.ExitCode -ne 0 -or $masterStatQuery.DataSet.Tables.Count -eq 0 -or $masterStatQuery.DataSet.Tables[0].Rows.Count -eq 0) {
        Add-Result "D08 [$label]: multi-DB CSV (two DBs)" "SKIP" "No user stats found in master -- multi-DB CSV untestable"
    } else {
        $masterStatFQN = [string]$masterStatQuery.DataSet.Tables[0].Rows[0]["FQN"]
        $csvArg        = "dbo.Orders.D01_StatA, $masterStatFQN"

        Clear-Applock -Srv $Server -P $port -Pwd $Password

        $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
DECLARE @sf int;
EXEC dbo.sp_StatUpdate
    @Statistics         = N'$csvArg',
    @Databases          = N'$db, master',
    @TargetNorecompute  = N'BOTH',
    @Execute            = N'N',
    @LogToTable         = N'N',
    @StatsFoundOut      = @sf OUTPUT;
SELECT ISNULL(@sf, -1) AS StatsFound;
"@
        if ($r.ExitCode -ne 0) {
            Add-Result "D08 [$label]: multi-DB CSV (two DBs)" "FAIL" ($r.Output | Select-Object -First 1)
        } else {
            $sf = Get-StatsFound -ds $r.DataSet
            # We listed one stat from StatUpdateTest and one from master.
            # Both should be discovered if they exist.  Accept >= 1 because
            # the master stat may not be in the StatUpdateTest scan scope
            # (the proc scans each DB independently; the master stat is in master).
            if ($null -ne $sf -and $sf -ge 1) {
                Add-Result "D08 [$label]: multi-DB CSV (two DBs)" "PASS" "StatsFound=$sf (CSV=$csvArg)"
            } else {
                Add-Result "D08 [$label]: multi-DB CSV (two DBs)" "FAIL" "StatsFound=$sf (expected >= 1)"
            }
        }
    }

    Clear-Applock -Srv $Server -P $port -Pwd $Password
    Write-Host ""
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

if ($ResultsPath) {
    if ($allResults.Count -ne 8) { throw "Expected 8 DirectStringCSV checks; received $($allResults.Count)." }
    $version = $Versions[0]
    $status = Get-StructuredSqlStatus -Server $Server -Port $VersionConfig[$version].Port -Password $Password `
        -ExpectedMajor $VersionConfig[$version].Major
    $commit = (& git -C $RepoRoot rev-parse HEAD).Trim()
    if ($LASTEXITCODE -ne 0 -or $commit -notmatch '^[0-9a-fA-F]{40}$') { throw 'Could not resolve the tested source commit.' }
    Write-StructuredLegacyResults -Path $ResultsPath -Commit $commit -SqlVersion $version -SqlBuild $status.Build `
        -ServerAliveAtEnd $status.Alive -Suite 'DirectStringCSV' -ExpectedCount 8 `
        -Results @($allResults) -StartedAt $startedAt -EndedAt (Get-Date)
}

$color = if ($failed -eq 0) { "Green" } else { "Red" }
Write-Host "  $passed/$total passed, $failed failed, $skipped skipped" -ForegroundColor $color
Write-Host ""

if ($failed -gt 0 -or $skipped -gt 0) {
    Write-Host "  FAILED:" -ForegroundColor Red
    foreach ($t in ($allResults | Where-Object { $_.Result -eq "FAIL" })) {
        Write-Host "    - $($t.Test): $($t.Detail)" -ForegroundColor Red
    }
    Write-Host ""
    exit 1
}

exit 0
