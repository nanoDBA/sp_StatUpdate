<#
.SYNOPSIS
    Smoke test: verifies sp_StatUpdate.sql compiles without errors.

.DESCRIPTION
    Addresses GitHub issue #395 (build pipeline fragility).  Syntax errors in the
    proc have slipped through multiple times because there was no automated compile
    check.  This script splits the file on GO batch separators and executes each
    batch against master on every requested SQL Server version.  A compile error
    in any batch fails the version immediately with the error message.

    This is a pure compile check -- it does NOT execute the procedure or require
    any test database.  It is intentionally minimal.

    Uses ADO.NET SqlConnection (not go-sqlcmd) to avoid TLS certificate parsing
    issues with SQL 2017's self-signed certificates.

.PARAMETER Server
    Server address. Use a loopback address when testing through an SSH tunnel.

.PARAMETER Password
    SA password for all containers.  Default: $env:SPSTATUPDATE_TEST_SQL_PASSWORD.

.PARAMETER Versions
    Which SQL versions to test.  Default: 2019, 2022, 2025.

.EXAMPLE
    .\tests\Test-ProcCompiles.ps1
    .\tests\Test-ProcCompiles.ps1 -Versions @("2022")
    .\tests\Test-ProcCompiles.ps1 -Server 127.0.0.1 -Password $env:SPSTATUPDATE_TEST_SQL_PASSWORD -Versions @("2022","2025")

.NOTES
    Exit code: 0 if all versions pass, 1 if any version fails.
#>
[CmdletBinding()]
param(
    [string]$Server   = "127.0.0.1",
    [string]$Password = $(if ($env:SPSTATUPDATE_TEST_SQL_PASSWORD) { $env:SPSTATUPDATE_TEST_SQL_PASSWORD } else { "$env:SPSTATUPDATE_TEST_SQL_PASSWORD" }),
    [string[]]$Versions = @("2019", "2022", "2025"),

    [string]$ResultsPath = "",
    [int]$PortOverride = 0
)

$ErrorActionPreference = "Continue"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot  = Split-Path -Parent $ScriptDir
$ProcFile  = Join-Path $RepoRoot "sp_StatUpdate.sql"
$runStartedAt = Get-Date

# Version config: label -> port on remote host.
# Ports are the existing test wrapper ports; use PortOverride for a tunnel.
$VersionConfig = @{
    "2019" = @{ Label = "SQL 2019"; Port = 1434 }
    "2022" = @{ Label = "SQL 2022"; Port = 1432 }
    "2025" = @{ Label = "SQL 2025"; Port = 1431 }
}

# =====================================================================
# ADO.NET HELPER
# Executes a single SQL batch.  Returns @{ ExitCode; ErrorMessage }.
# Uses ExecuteReader + DataTable.Load instead of SqlDataAdapter.Fill
# so InfoMessage events fire synchronously (SQL 2025 compatibility).
# =====================================================================
function Invoke-SqlBatch {
    param(
        [string]$ServerAddr,
        [int]$Port,
        [string]$Pwd,
        [string]$Sql,
        [int]$Timeout = 120
    )
    $connStr = "Server=$ServerAddr,$Port;Database=master;User Id=sa;" +
               "Password=$Pwd;Encrypt=false;TrustServerCertificate=true;" +
               "Connection Timeout=30;Pooling=false"
    try {
        $conn = [System.Data.SqlClient.SqlConnection]::new($connStr)
        $conn.Open()
        $cmd = $conn.CreateCommand()
        $cmd.CommandText    = $Sql
        $cmd.CommandTimeout = $Timeout
        $reader = $cmd.ExecuteReader()
        do {
            $dt = [System.Data.DataTable]::new()
            $dt.Load($reader)
        } while (-not $reader.IsClosed)
        $conn.Close()
        return @{ ExitCode = 0; ErrorMessage = "" }
    } catch {
        $msg = if ($_.Exception.InnerException) {
            $_.Exception.InnerException.Message
        } else {
            $_.Exception.Message
        }
        try { $conn.Close() } catch { }
        return @{ ExitCode = 1; ErrorMessage = $msg }
    }
}

# =====================================================================
# HEADER
# =====================================================================
Write-Host ""
Write-Host ("=" * 60) -ForegroundColor Cyan
Write-Host " sp_StatUpdate v3 -- Compile Smoke Test (#395)" -ForegroundColor Cyan
Write-Host ("=" * 60) -ForegroundColor Cyan
Write-Host ""
Write-Host "  Server:   $Server"
Write-Host "  Versions: $($Versions -join ', ')"
Write-Host "  File:     $ProcFile"
Write-Host ""

# =====================================================================
# PRE-FLIGHT: proc file must exist
# =====================================================================
if (-not (Test-Path $ProcFile)) {
    Write-Host "FAIL: proc file not found: $ProcFile" -ForegroundColor Red
    exit 1
}

# Split on GO batch separators.  The regex matches a line that is
# exclusively "GO" (case-insensitive, optional surrounding whitespace).
$procSql = [System.IO.File]::ReadAllText($ProcFile)
$batches  = [regex]::Split($procSql, '(?mi)^\s*GO\s*$')

# Collect non-empty batches into an ArrayList.
$execBatches = [System.Collections.ArrayList]::new()
foreach ($b in $batches) {
    $trimmed = $b.Trim()
    if ($trimmed.Length -gt 0) {
        $null = $execBatches.Add($trimmed)
    }
}

Write-Host "  Batches:  $($execBatches.Count) (after stripping empty)"
Write-Host ""

if ($execBatches.Count -eq 0) {
    Write-Host "FAIL: proc file produced 0 executable batches -- file may be empty." -ForegroundColor Red
    exit 1
}

# =====================================================================
# COMPILE CHECK -- one version at a time (serial; simple + clear output)
# =====================================================================
$results = [System.Collections.ArrayList]::new()
$sqlBuild = "unavailable"
$testedPort = 0

foreach ($ver in $Versions) {
    if (-not $VersionConfig.ContainsKey($ver)) {
        Write-Host "  SKIP  [SQL $ver] -- unknown version" -ForegroundColor DarkGray
        $null = $results.Add(@{ Version = $ver; Passed = $false; Error = "Unknown version '$ver'" })
        continue
    }

    $cfg   = $VersionConfig[$ver]
    $label = $cfg.Label
    $port  = $cfg.Port
    if ($PortOverride -gt 0) { $port = $PortOverride }
    $testedPort = $port

    # Verify connectivity first with a trivial query.
    $ping = Invoke-SqlBatch -ServerAddr $Server -Port $port -Pwd $Password -Sql "SELECT 1;" -Timeout 10
    if ($ping.ExitCode -ne 0) {
        Write-Host ("  SKIP  [{0,-9}] -- not reachable ({1})" -f $label, $ping.ErrorMessage) -ForegroundColor DarkGray
        $null = $results.Add(@{ Version = $ver; Passed = $false; Error = "Not reachable: $($ping.ErrorMessage)" })
        continue
    }

    # Execute all batches on a SINGLE connection so SET options persist.
    # ALTER PROCEDURE requires SET ANSI_NULLS ON / SET QUOTED_IDENTIFIER ON
    # from the same session -- separate connections lose these settings.
    $failed      = $false
    $failMessage = ""
    $batchNum    = 0

    $connStr = "Server=$Server,$port;Database=master;User Id=sa;" +
               "Password=$Password;Encrypt=false;TrustServerCertificate=true;" +
               "Connection Timeout=30;Pooling=false"
    try {
        $conn = [System.Data.SqlClient.SqlConnection]::new($connStr)
        $conn.Open()

        $buildCommand = $conn.CreateCommand()
        $buildCommand.CommandText = "SELECT CONVERT(nvarchar(128), SERVERPROPERTY('ProductVersion'))"
        $sqlBuild = [string]$buildCommand.ExecuteScalar()

        foreach ($batch in $execBatches) {
            $batchNum++
            try {
                $cmd = $conn.CreateCommand()
                $cmd.CommandText    = $batch
                $cmd.CommandTimeout = 120
                $reader = $cmd.ExecuteReader()
                do {
                    $dt = [System.Data.DataTable]::new()
                    $dt.Load($reader)
                } while (-not $reader.IsClosed)
            } catch {
                $failed = $true
                $msg = if ($_.Exception.InnerException) {
                    $_.Exception.InnerException.Message
                } else {
                    $_.Exception.Message
                }
                $failMessage = "Batch $batchNum of $($execBatches.Count): $msg"
                break
            }
        }

        $conn.Close()
    } catch {
        if (-not $failed) {
            $failed = $true
            $msg = if ($_.Exception.InnerException) {
                $_.Exception.InnerException.Message
            } else {
                $_.Exception.Message
            }
            $failMessage = "Connection failed: $msg"
        }
        try { $conn.Close() } catch { }
    }

    if ($failed) {
        Write-Host ("  FAIL  [{0,-9}] -- {1}" -f $label, $failMessage) -ForegroundColor Red
        $null = $results.Add(@{ Version = $ver; Passed = $false; Error = $failMessage })
    } else {
        Write-Host ("  PASS  [{0,-9}] -- $($execBatches.Count) batches compiled successfully" -f $label) -ForegroundColor Green
        $null = $results.Add(@{ Version = $ver; Passed = $true; Error = "" })
    }
}

# =====================================================================
# SUMMARY
# =====================================================================
# tracking issue: @() is required -- a single-element pipeline returns the bare hashtable,
# and .Count on a hashtable is its KEY count (3), not 1.  That produced "3/1 versions
# compiled successfully" whenever exactly one version was tested.
$passCount = @($results | Where-Object { $_.Passed }).Count
$failCount = @($results | Where-Object { -not $_.Passed }).Count
$total     = $results.Count

if ($ResultsPath) {
    if ($results.Count -ne 1) { throw "Results output requires exactly one SQL version; received $($results.Count)." }
    $serverAliveAtEnd = (Invoke-SqlBatch -ServerAddr $Server -Port $testedPort -Pwd $Password -Sql "SELECT 1;" -Timeout 10).ExitCode -eq 0
    Import-Module (Join-Path $PSScriptRoot "ReleaseResults.psm1") -Force
    $item = $results[0]
    $endedAt = Get-Date
    $durationMs = [long](($endedAt - $runStartedAt).TotalMilliseconds)
    $check = [ordered]@{ check = "procedure_compiles"; result = $(if ($item.Passed) { "PASS" } else { "FAIL" }); duration_ms = $durationMs }
    if (-not $item.Passed) { $check.reason = [string]$item.Error }
    $commit = (& git -C $RepoRoot rev-parse HEAD).Trim()
    if ($LASTEXITCODE -ne 0) { throw "Could not resolve the tested source commit." }
    [void](Write-ReleaseRunList -Path $ResultsPath -Commit $commit -SqlVersion $Versions[0] -SqlBuild $sqlBuild `
        -ServerAliveAtEnd $serverAliveAtEnd -Suite "Compile" -ExpectedCount 1 -Checks @($check) `
        -StartedAt $runStartedAt -EndedAt $endedAt -DurationMs $durationMs)
}

Write-Host ""
Write-Host ("=" * 60) -ForegroundColor Cyan
Write-Host ("  Result: {0}/{1} versions compiled successfully" -f $passCount, $total)
Write-Host ("=" * 60) -ForegroundColor Cyan
Write-Host ""

if ($failCount -gt 0) {
    exit 1
} else {
    exit 0
}
