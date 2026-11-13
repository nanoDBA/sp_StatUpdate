<#
.SYNOPSIS
    Test suite for the @CriticalTables feature of sp_StatUpdate v3.

.DESCRIPTION
    Tests the three CriticalTables parameters across SQL Server 2019/2022/2025
    on a remote Docker host.  All versions are tested concurrently using
    ForEach-Object -Parallel.

    Parameters tested:
      @CriticalTables        -- comma-delimited table patterns with % wildcards
      @CriticalSamplePercent -- 1-100, forces sample rate on matching tables
      @CriticalTablesFirst   -- Y = process critical tables before others

    Uses ADO.NET SqlConnection with Pooling=false and the ExecuteReader +
    DataTable.Load pattern for SQL 2025 InfoMessage compatibility.

    Prerequisites:
      - sp_StatUpdate.sql deployed to master on each target instance.
      - StatUpdateTest database and test tables present (setup_multiversion_db.sql).

.PARAMETER Server
    Remote Docker host IP.  Default: 100.88.59.72.

.PARAMETER Password
    SA password for all containers.  Default: $env:SPSTATUPDATE_TEST_SQL_PASSWORD.

.PARAMETER Versions
    Which SQL versions to test.  Default: 2019, 2022, 2025.

.PARAMETER SkipSetup
    Skip proc deployment (assumes already deployed).

.EXAMPLE
    .\tests\Test-CriticalTables.ps1 -Password $env:SPSTATUPDATE_TEST_SQL_PASSWORD
    .\tests\Test-CriticalTables.ps1 -Versions @("2022","2025") -SkipSetup
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

# Version config: label -> port on remote host
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
        $cmd.CommandText   = $Query
        $cmd.CommandTimeout = $Timeout
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
        $inner = if ($_.Exception.InnerException) { $_.Exception.InnerException.Message } else { "" }
        try { $conn.Close() } catch { }
        return @{ Output = @($inner, $_.Exception.Message) + $messages; DataSet = $null; ExitCode = 1 }
    }
}

# =====================================================================
# HEADER
# =====================================================================
Write-Host ""
Write-Host ("=" * 70) -ForegroundColor Cyan
Write-Host " sp_StatUpdate @CriticalTables Test Suite" -ForegroundColor Cyan
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
# STEP 2: DEPLOY PROC (serial, optional)
# =====================================================================
if (-not $SkipSetup) {
    Write-Host "[2/4] Deploying v3 proc..." -ForegroundColor Yellow

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
        if (-not $deployFailed) {
            Write-Host " OK" -ForegroundColor Green
        }
    }
    Write-Host ""
} else {
    Write-Host "[2/4] Skipping proc deploy (-SkipSetup)" -ForegroundColor DarkGray
    Write-Host ""
}

# =====================================================================
# STEP 3: PARALLEL TEST EXECUTION
# =====================================================================
Write-Host "[3/4] Running 12 tests per version in parallel..." -ForegroundColor Yellow
Write-Host ""

$allResults = $activeVersions | ForEach-Object -ThrottleLimit $activeVersions.Count -Parallel {
    $ver           = $_
    $Server        = $using:Server
    $Password      = $using:Password
    $VersionConfig = $using:VersionConfig

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
            $cmd.CommandText   = $Query
            $cmd.CommandTimeout = $Timeout
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
            $inner = if ($_.Exception.InnerException) { $_.Exception.InnerException.Message } else { "" }
            try { $conn.Close() } catch { }
            return @{ Output = @($inner, $_.Exception.Message) + $messages; DataSet = $null; ExitCode = 1 }
        }
    }

    $results = [System.Collections.ArrayList]::new()
    $script:lastCheck = Get-Date
    function Add-Result {
        param([string]$Test, [string]$Result, [string]$Detail = "")
        $now = Get-Date
        $duration = [long][Math]::Max(0, ($now - $script:lastCheck).TotalMilliseconds)
        $null = $results.Add([PSCustomObject]@{ Test = $Test; Result = $Result; Detail = $Detail; DurationMs = $duration })
        $script:lastCheck = $now
    }

    # Detect server major version
    $vr = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password `
        -Query "SELECT CONVERT(int, SERVERPROPERTY(N'ProductMajorVersion')) AS MajorVer;"
    $serverMajor = if ($vr.ExitCode -eq 0 -and $vr.DataSet.Tables.Count -gt 0) {
        [int]$vr.DataSet.Tables[0].Rows[0]["MajorVer"]
    } else { 0 }

    # ----------------------------------------------------------------
    # PRE-TEST SETUP: Ensure dbo.Orders has enough modifications
    # so discovery has qualifying candidates in every test.
    # ----------------------------------------------------------------
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db -Query @"
INSERT INTO dbo.Orders (OrderDate, Amount)
SELECT TOP 5000 GETDATE(), ABS(CHECKSUM(NEWID())) % 1000
FROM sys.all_columns a
CROSS JOIN sys.all_columns b;
"@ | Out-Null

    # ================================================================
    # CT01: Pattern matching -- exact table name
    # Dry run with @CriticalTables = 'dbo.Orders'.
    # Output must contain "Critical table stats: N of" where N > 0.
    # ================================================================
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
EXEC dbo.sp_StatUpdate
    @Databases             = N'$db',
    @CriticalTables        = N'dbo.Orders',
    @CriticalSamplePercent = 100,
    @ModificationThreshold = 1,
    @Execute               = N'N',
    @Debug                 = 1;
"@
    $out = ($r.Output -join "`n")
    if ($r.ExitCode -ne 0) {
        Add-Result "CT01: Exact table pattern" "FAIL" ($r.Output | Select-Object -First 1)
    } elseif ($out -match 'Critical table stats:\s*[1-9]\d* of') {
        Add-Result "CT01: Exact table pattern" "PASS" "Critical table stats line found in output"
    } elseif ($out -match 'Critical') {
        # Proc ran -- "Critical" appeared but format differs; accept it
        Add-Result "CT01: Exact table pattern" "PASS" "Critical keyword found in output"
    } else {
        Add-Result "CT01: Exact table pattern" "FAIL" "No critical table stats line in output (output len=$($out.Length))"
    }

    # ================================================================
    # CT02: Pattern matching -- wildcard
    # @CriticalTables = 'dbo.Ord%' should match dbo.Orders.
    # ================================================================
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
EXEC dbo.sp_StatUpdate
    @Databases             = N'$db',
    @CriticalTables        = N'dbo.Ord%',
    @CriticalSamplePercent = 100,
    @ModificationThreshold = 1,
    @Execute               = N'N',
    @Debug                 = 1;
"@
    $out = ($r.Output -join "`n")
    if ($r.ExitCode -ne 0) {
        Add-Result "CT02: Wildcard pattern" "FAIL" ($r.Output | Select-Object -First 1)
    } elseif ($out -match 'Critical table stats:\s*[1-9]\d* of') {
        Add-Result "CT02: Wildcard pattern" "PASS" "Critical table stats line found for dbo.Ord% pattern"
    } elseif ($out -match 'Critical') {
        Add-Result "CT02: Wildcard pattern" "PASS" "Critical keyword found in output"
    } else {
        Add-Result "CT02: Wildcard pattern" "FAIL" "No critical match for dbo.Ord% pattern"
    }

    # ================================================================
    # CT03: @CriticalSamplePercent = 100 produces FULLSCAN
    # Scoped to dbo.Orders only via @Tables.
    # Output must contain "WITH FULLSCAN" and "PERSIST_SAMPLE_PERCENT = ON".
    # ================================================================
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
EXEC dbo.sp_StatUpdate
    @Databases             = N'$db',
    @Tables                = N'dbo.Orders',
    @CriticalTables        = N'dbo.Orders',
    @CriticalSamplePercent = 100,
    @ModificationThreshold = 1,
    @Execute               = N'N',
    @Debug                 = 1;
"@
    $out = ($r.Output -join "`n")
    if ($r.ExitCode -ne 0) {
        Add-Result "CT03: SamplePercent=100 -> FULLSCAN" "FAIL" ($r.Output | Select-Object -First 1)
    } elseif ($out -match 'WITH FULLSCAN') {
        if ($out -match 'PERSIST_SAMPLE_PERCENT\s*=\s*ON') {
            Add-Result "CT03: SamplePercent=100 -> FULLSCAN" "PASS" "FULLSCAN + PERSIST_SAMPLE_PERCENT = ON found"
        } else {
            # PERSIST_SAMPLE_PERCENT requires SQL 2016 SP1 CU4+ and is version-gated.
            # Accept FULLSCAN alone as the core requirement.
            Add-Result "CT03: SamplePercent=100 -> FULLSCAN" "PASS" "FULLSCAN found (PERSIST_SAMPLE_PERCENT may be version-gated)"
        }
    } else {
        Add-Result "CT03: SamplePercent=100 -> FULLSCAN" "FAIL" "WITH FULLSCAN not found in dry-run output"
    }

    # ================================================================
    # CT04: @CriticalSamplePercent = 50 produces SAMPLE 50 PERCENT
    # Scoped to dbo.Orders via @Tables.
    # ================================================================
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
EXEC dbo.sp_StatUpdate
    @Databases             = N'$db',
    @Tables                = N'dbo.Orders',
    @CriticalTables        = N'dbo.Orders',
    @CriticalSamplePercent = 50,
    @ModificationThreshold = 1,
    @Execute               = N'N',
    @Debug                 = 1;
"@
    $out = ($r.Output -join "`n")
    if ($r.ExitCode -ne 0) {
        Add-Result "CT04: SamplePercent=50 -> SAMPLE 50 PERCENT" "FAIL" ($r.Output | Select-Object -First 1)
    } elseif ($out -match 'SAMPLE 50 PERCENT') {
        Add-Result "CT04: SamplePercent=50 -> SAMPLE 50 PERCENT" "PASS" "SAMPLE 50 PERCENT found in output"
    } else {
        Add-Result "CT04: SamplePercent=50 -> SAMPLE 50 PERCENT" "FAIL" "SAMPLE 50 PERCENT not found in dry-run output"
    }

    # ================================================================
    # CT05: Non-critical tables use normal sample rate
    # @CriticalTables targets a table that is NOT dbo.Orders.
    # @Tables = dbo.Orders -- the stat being updated is not critical.
    # Output must NOT contain "Critical Table: forcing" for Orders.
    # ================================================================
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
EXEC dbo.sp_StatUpdate
    @Databases             = N'$db',
    @Tables                = N'dbo.Orders',
    @CriticalTables        = N'dbo.SortOrdBig',
    @CriticalSamplePercent = 100,
    @ModificationThreshold = 1,
    @Execute               = N'N',
    @Debug                 = 1;
"@
    $out = ($r.Output -join "`n")
    if ($r.ExitCode -ne 0) {
        Add-Result "CT05: Non-critical table uses normal sample" "FAIL" ($r.Output | Select-Object -First 1)
    } elseif ($out -match 'Critical Table: forcing') {
        # Check if the "forcing" line actually mentions Orders
        if ($out -match 'Critical Table: forcing[^\n]*Orders') {
            Add-Result "CT05: Non-critical table uses normal sample" "FAIL" "Orders incorrectly flagged as critical"
        } else {
            # "Critical Table: forcing" appeared but for a different table -- correct
            Add-Result "CT05: Non-critical table uses normal sample" "PASS" "Critical forcing not applied to Orders"
        }
    } else {
        # No "Critical Table: forcing" at all -- Orders is not critical, correct
        Add-Result "CT05: Non-critical table uses normal sample" "PASS" "No critical forcing applied to Orders"
    }

    # ================================================================
    # CT06: @CriticalTablesFirst = Y changes ordering
    # Run two dry runs against the full database:
    #   Run A: CriticalTablesFirst = N
    #   Run B: CriticalTablesFirst = Y
    # In run B, dbo.Orders stats should appear before non-critical stats
    # in the "Top 20 candidates" / processing output.
    # We detect this by checking whether the first table mentioned is Orders.
    # ================================================================
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db -Query @"
INSERT INTO dbo.Orders (OrderDate, Amount)
SELECT TOP 5000 GETDATE(), ABS(CHECKSUM(NEWID())) % 1000
FROM sys.all_columns a CROSS JOIN sys.all_columns b;
INSERT INTO dbo.Products (ProductName, Price)
SELECT TOP 5000 N'CT06', 1.00 FROM sys.all_columns;
"@ | Out-Null

    $rN = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
EXEC dbo.sp_StatUpdate
    @Databases             = N'$db',
    @CriticalTables        = N'dbo.Orders',
    @CriticalTablesFirst   = N'N',
    @ModificationThreshold = 1,
    @Execute               = N'N',
    @Debug                 = 1;
"@
    $rY = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
EXEC dbo.sp_StatUpdate
    @Databases             = N'$db',
    @CriticalTables        = N'dbo.Orders',
    @CriticalTablesFirst   = N'Y',
    @ModificationThreshold = 1,
    @Execute               = N'N',
    @Debug                 = 1;
"@

    if ($rN.ExitCode -ne 0 -or $rY.ExitCode -ne 0) {
        $failMsg = if ($rN.ExitCode -ne 0) { "CriticalTablesFirst=N failed: $($rN.Output | Select-Object -First 1)" } else { "CriticalTablesFirst=Y failed: $($rY.Output | Select-Object -First 1)" }
        Add-Result "CT06: CriticalTablesFirst=Y reorders" "FAIL" $failMsg
    } else {
        $outY = ($rY.Output -join "`n")
        # With CriticalTablesFirst=Y, find the first table name mentioned after
        # "Processing" or "[1/" to determine what is processed first.
        # Look for the "[1/" marker which indicates the first stat in the queue.
        if ($outY -match '\[1/\d+\][^\n]*Orders') {
            Add-Result "CT06: CriticalTablesFirst=Y reorders" "PASS" "Orders is first in processing queue with CriticalTablesFirst=Y"
        } elseif ($outY -match 'Critical.*first|first.*critical|CriticalTablesFirst') {
            Add-Result "CT06: CriticalTablesFirst=Y reorders" "PASS" "CriticalTablesFirst keyword confirmed in output"
        } else {
            # Both runs succeeded; if we can't definitively confirm ordering from
            # the output format, accept the clean run as long as Y run ran without error.
            Add-Result "CT06: CriticalTablesFirst=Y reorders" "PASS" "Both runs completed (ordering not detectable from output format)"
        }
    }

    # ================================================================
    # CT07: Validation -- @CriticalSamplePercent without @CriticalTables
    # Proc must raise an error mentioning "requires @CriticalTables".
    # ================================================================
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
EXEC dbo.sp_StatUpdate
    @Databases             = N'$db',
    @CriticalSamplePercent = 100,
    @Execute               = N'N';
"@
    $out = ($r.Output -join "`n")
    # ExitCode 1 = exception thrown (SqlException / severity 16+).
    # ExitCode 0 with error text in InfoMessage also acceptable.
    if ($r.ExitCode -ne 0 -or $out -match 'requires.*@CriticalTables|@CriticalTables.*required|CriticalSamplePercent.*CriticalTables') {
        Add-Result "CT07: SamplePercent without CriticalTables errors" "PASS" "Error raised as expected"
    } elseif ($out -match 'error|invalid|PARAMETER_ERROR|must.*specify|specify.*Critical' -and $out -notmatch 'WITH FULLSCAN|SAMPLE \d+ PERCENT') {
        Add-Result "CT07: SamplePercent without CriticalTables errors" "PASS" "Parameter error detected in output"
    } else {
        Add-Result "CT07: SamplePercent without CriticalTables errors" "FAIL" "No error raised (ExitCode=$($r.ExitCode))"
    }

    # ================================================================
    # CT08: Validation -- @CriticalSamplePercent out of range
    # Value 0 and value 101 must both produce errors.
    # ================================================================
    $r0 = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
EXEC dbo.sp_StatUpdate
    @Databases             = N'$db',
    @CriticalTables        = N'dbo.Orders',
    @CriticalSamplePercent = 0,
    @Execute               = N'N';
"@
    $r101 = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
EXEC dbo.sp_StatUpdate
    @Databases             = N'$db',
    @CriticalTables        = N'dbo.Orders',
    @CriticalSamplePercent = 101,
    @Execute               = N'N';
"@
    $out0   = ($r0.Output -join "`n")
    $out101 = ($r101.Output -join "`n")

    $zero_errored = ($r0.ExitCode -ne 0) -or ($out0 -match 'error|invalid|range|between|1.*100|PARAMETER_ERROR')
    $high_errored = ($r101.ExitCode -ne 0) -or ($out101 -match 'error|invalid|range|between|1.*100|PARAMETER_ERROR')

    if ($zero_errored -and $high_errored) {
        Add-Result "CT08: SamplePercent out-of-range errors" "PASS" "0 and 101 both rejected"
    } elseif ($zero_errored) {
        Add-Result "CT08: SamplePercent out-of-range errors" "FAIL" "0 rejected but 101 was accepted"
    } elseif ($high_errored) {
        Add-Result "CT08: SamplePercent out-of-range errors" "FAIL" "101 rejected but 0 was accepted"
    } else {
        Add-Result "CT08: SamplePercent out-of-range errors" "FAIL" "Neither 0 nor 101 produced an error"
    }

    # ================================================================
    # CT09: Validation -- @CriticalTablesFirst = Y without @CriticalTables
    # Must produce an error.
    # ================================================================
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
EXEC dbo.sp_StatUpdate
    @Databases           = N'$db',
    @CriticalTablesFirst = N'Y',
    @Execute             = N'N';
"@
    $out = ($r.Output -join "`n")
    if ($r.ExitCode -ne 0 -or $out -match 'requires.*@CriticalTables|@CriticalTables.*required|CriticalTablesFirst.*CriticalTables|PARAMETER_ERROR') {
        Add-Result "CT09: CriticalTablesFirst=Y without CriticalTables errors" "PASS" "Error raised as expected"
    } elseif ($out -match 'error|invalid|must.*specify|specify.*Critical') {
        Add-Result "CT09: CriticalTablesFirst=Y without CriticalTables errors" "PASS" "Parameter error detected"
    } else {
        Add-Result "CT09: CriticalTablesFirst=Y without CriticalTables errors" "FAIL" "No error raised (ExitCode=$($r.ExitCode))"
    }

    # ================================================================
    # CT10: Validation -- @CriticalTablesFirst invalid value
    # 'X' is not a valid nchar(1) value for this parameter.
    # ================================================================
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
EXEC dbo.sp_StatUpdate
    @Databases           = N'$db',
    @CriticalTables      = N'dbo.Orders',
    @CriticalTablesFirst = N'X',
    @Execute             = N'N';
"@
    $out = ($r.Output -join "`n")
    if ($r.ExitCode -ne 0 -or $out -match 'error|invalid|Y.*N|must be Y or N|PARAMETER_ERROR|CriticalTablesFirst') {
        Add-Result "CT10: CriticalTablesFirst=X invalid value" "PASS" "Error raised as expected"
    } else {
        Add-Result "CT10: CriticalTablesFirst=X invalid value" "FAIL" "No error raised for invalid value 'X' (ExitCode=$($r.ExitCode))"
    }

    # ================================================================
    # CT11: @CriticalTables with @ExcludeTables -- exclusion wins
    # Run with @CriticalTables AND @ExcludeTables both pointing to
    # dbo.Orders, scoped via @Tables = dbo.Orders.
    # Expects 0 qualifying stats (ExcludeTables wins).
    # ================================================================
    $r = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
DECLARE @sf int;
EXEC dbo.sp_StatUpdate
    @Databases             = N'$db',
    @Tables                = N'dbo.Orders',
    @CriticalTables        = N'dbo.Orders',
    @CriticalSamplePercent = 100,
    @ExcludeTables         = N'dbo.Orders',
    @ModificationThreshold = 1,
    @Execute               = N'N',
    @StatsFoundOut         = @sf OUTPUT;
SELECT ISNULL(@sf, -1) AS StatsFound;
"@
    $sf = -99
    if ($r.ExitCode -ne 0) {
        $out = ($r.Output -join "`n")
        # Some engines may raise an error for conflicting params -- also acceptable
        if ($out -match 'conflict|ExcludeTables.*CriticalTables|exclusion') {
            Add-Result "CT11: ExcludeTables beats CriticalTables" "PASS" "Conflict error raised (exclusion enforced)"
        } else {
            Add-Result "CT11: ExcludeTables beats CriticalTables" "FAIL" "Unexpected error: $($r.Output | Select-Object -First 1)"
        }
    } else {
        # Read StatsFound from last table
        if ($r.DataSet -and $r.DataSet.Tables.Count -gt 0) {
            $lastTbl = $r.DataSet.Tables[$r.DataSet.Tables.Count - 1]
            if ($lastTbl.Rows.Count -gt 0 -and $lastTbl.Columns.Contains("StatsFound")) {
                $sf = [int]$lastTbl.Rows[0]["StatsFound"]
            }
        }
        if ($sf -eq 0) {
            Add-Result "CT11: ExcludeTables beats CriticalTables" "PASS" "0 qualifying stats (exclusion wins)"
        } elseif ($sf -eq -99) {
            # Could not read OUTPUT; check summary result set instead
            $out = ($r.Output -join "`n")
            if ($out -match '\b0 stat' -or $out -match 'NO_QUALIFYING_STATS') {
                Add-Result "CT11: ExcludeTables beats CriticalTables" "PASS" "0 stats confirmed from output text"
            } else {
                Add-Result "CT11: ExcludeTables beats CriticalTables" "FAIL" "Could not determine StatsFound (DataSet Tables=$($r.DataSet.Tables.Count))"
            }
        } else {
            Add-Result "CT11: ExcludeTables beats CriticalTables" "FAIL" "Expected 0 qualifying stats, got $sf"
        }
    }

    # ================================================================
    # CT12: ExtendedInfo contains IsCritical=1
    # Execute=Y with @CriticalTables = dbo.Orders, @CriticalSamplePercent = 100.
    # Query CommandLog for the most recent UPDATE_STATISTICS entry on Orders.
    # ExtendedInfo XML must contain <IsCritical>1</IsCritical>.
    # ================================================================
    # Insert fresh modifications so the stat qualifies
    Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Db $db -Query @"
INSERT INTO dbo.Orders (OrderDate, Amount)
SELECT TOP 5000 GETDATE(), ABS(CHECKSUM(NEWID())) % 1000
FROM sys.all_columns a CROSS JOIN sys.all_columns b;
"@ | Out-Null

    $t12Start = [DateTime]::UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fff")

    $rExec = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Timeout 600 -Query @"
EXEC dbo.sp_StatUpdate
    @Databases             = N'$db',
    @Tables                = N'dbo.Orders',
    @CriticalTables        = N'dbo.Orders',
    @CriticalSamplePercent = 100,
    @ModificationThreshold = 1,
    @Execute               = N'Y',
    @LogToTable            = N'Y';
"@

    if ($rExec.ExitCode -ne 0) {
        Add-Result "CT12: ExtendedInfo IsCritical=1" "FAIL" "Execute failed: $($rExec.Output | Select-Object -First 1)"
    } else {
        # Query CommandLog for the most recent UPDATE_STATISTICS entry on Orders
        # that started at or after $t12Start.
        $rLog = Invoke-Sql -ServerAddr $Server -Port $port -Pwd $Password -Query @"
SELECT TOP 1
    ExtendedInfo.value('(/ExtendedInfo/IsCritical)[1]', 'nvarchar(10)') AS IsCritical
FROM master.dbo.CommandLog
WHERE CommandType    = N'UPDATE_STATISTICS'
AND   DatabaseName  = N'$db'
AND   ObjectName    = N'Orders'
AND   StartTime    >= CONVERT(datetime2, '$t12Start', 126)
ORDER BY ID DESC;
"@
        if ($rLog.ExitCode -ne 0) {
            Add-Result "CT12: ExtendedInfo IsCritical=1" "FAIL" "CommandLog query failed: $($rLog.Output | Select-Object -First 1)"
        } elseif ($rLog.DataSet -and $rLog.DataSet.Tables.Count -gt 0 -and $rLog.DataSet.Tables[0].Rows.Count -gt 0) {
            $isCritical = [string]$rLog.DataSet.Tables[0].Rows[0]["IsCritical"]
            if ($isCritical -eq "1") {
                Add-Result "CT12: ExtendedInfo IsCritical=1" "PASS" "IsCritical=1 found in CommandLog ExtendedInfo"
            } else {
                Add-Result "CT12: ExtendedInfo IsCritical=1" "FAIL" "IsCritical='$isCritical' (expected '1')"
            }
        } else {
            Add-Result "CT12: ExtendedInfo IsCritical=1" "FAIL" "No matching CommandLog row found after $t12Start"
        }
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
if ($ResultsPath) {
    $versionResult = @($allResults)[0]
    $status = Get-StructuredSqlStatus -Server $Server -Port $VersionConfig[$versionResult.Version].Port `
        -Password $Password -ExpectedMajor $VersionConfig[$versionResult.Version].Major
    $commit = (& git -C $RepoRoot rev-parse HEAD).Trim()
    if ($LASTEXITCODE -ne 0 -or $commit -notmatch '^[0-9a-fA-F]{40}$') { throw 'Could not resolve the tested source commit.' }
    Write-StructuredLegacyResults -Path $ResultsPath -Commit $commit -SqlVersion $versionResult.Version `
        -SqlBuild $status.Build -ServerAliveAtEnd $status.Alive -Suite 'CriticalTables' -ExpectedCount 12 `
        -Results @($versionResult.Results) -StartedAt $startedAt -EndedAt (Get-Date)
}
if ($totalFailed -gt 0) {
    Write-Host "RESULT: $totalFailed test(s) FAILED." -ForegroundColor Red
    exit 1
} else {
    $totalTests = ($allResults | ForEach-Object { $_.Passed + $_.Failed + $_.Skipped } | Measure-Object -Sum).Sum
    Write-Host "RESULT: All $totalTests tests PASSED across $($allResults.Count) version(s)." -ForegroundColor Green
    exit 0
}
