<#
.SYNOPSIS
    Multi-server diagnostic analysis for sp_StatUpdate using CommandLog data.

.DESCRIPTION
    Orchestrates sp_StatUpdate_Diag across one or more SQL Servers in parallel,
    merges results, detects cross-server patterns, and generates a consolidated
    Markdown or HTML report with severity-categorized recommendations.

    When -Obfuscate is specified, produces three output files:
    - _SAFE_TO_SHARE: obfuscated data safe for vendors/consultants
    - _CONFIDENTIAL: real names for internal use only
    - _CONFIDENTIAL_DECODE.sql: T-SQL script to decode obfuscated tokens

    Prerequisites:
    - PowerShell 7+
    - sp_StatUpdate_Diag procedure deployed on target servers

.PARAMETER Servers
    Array of SQL Server instance names to analyze.

.PARAMETER CommandLogDatabase
    Database containing dbo.CommandLog table. Defaults to 'master'.

.PARAMETER OutputPath
    Directory for report output. Defaults to current directory.

.PARAMETER OutputFormat
    Report format: Markdown, HTML, or JSON. Defaults to Markdown.

.PARAMETER DaysBack
    Number of days of history to analyze. Defaults to 30.

.PARAMETER MaxParallel
    Maximum parallel threads. Defaults to 10.

.PARAMETER Obfuscate
    When specified, produces dual output: obfuscated (safe to share) and
    confidential (real names). Also generates a decode SQL script.

.PARAMETER ObfuscationSeed
    Salt for HASHBYTES obfuscation. Makes tokens deterministic across runs/servers
    with the same seed but unpredictable without it. Requires -Obfuscate.

.PARAMETER ObfuscationMapTable
    Persist the obfuscation map to this table on each server (auto-created if missing).
    The map stays on prod for decoding. Requires -Obfuscate.

.PARAMETER ExpertMode
    0 = management view (dashboard + recommendations only), 1 = DBA deep-dive (all 13 RS).
    Defaults to 0.

.PARAMETER LongRunningMinutes
    Threshold for long-running stat detection. Defaults to 10.

.PARAMETER FailureThreshold
    Number of failures before triggering C2 CRITICAL. Defaults to 3.

.PARAMETER ThroughputWindowDays
    Window for throughput trend analysis (C4). Defaults to 7.

.PARAMETER TopN
    Limit for detail result sets. Defaults to 20.

.PARAMETER EfficacyDaysBack
    Broad trending window for QS efficacy analysis (RS 9). Defaults to @DaysBack.

.PARAMETER EfficacyDetailDays
    Close-up run-over-run window for QS efficacy detail (RS 10). Defaults to 14.

.PARAMETER TrustServerCertificate
    Trust the SQL Server certificate without validation. Defaults to $true.
    Set to $false when connecting to servers with properly configured TLS certificates.

.PARAMETER Credential
    Optional PSCredential for SQL authentication. If not provided, uses Windows auth.

.EXAMPLE
    # Single server, Windows auth
    .\Invoke-StatUpdateDiag.ps1 -Servers @('PROD-SQL01')

.EXAMPLE
    # Multi-server, obfuscated for sharing (produces 3 files)
    .\Invoke-StatUpdateDiag.ps1 -Servers @('PROD-SQL01','PROD-SQL02') -Obfuscate -OutputFormat JSON

.EXAMPLE
    # SQL auth, custom CommandLog location
    $cred = Get-Credential
    .\Invoke-StatUpdateDiag.ps1 -Servers (Get-Content servers.txt) -Credential $cred -CommandLogDatabase 'DBATools'

.NOTES
    Requires: PowerShell 7+ (uses ADO.NET directly, no SqlServer module needed)
    See also: sp_StatUpdate_Diag.sql (the T-SQL diagnostic procedure)
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string[]]$Servers,

    [string]$CommandLogDatabase = "master",

    [string]$OutputPath = ".",

    [ValidateSet("Markdown", "HTML", "JSON")]
    [string]$OutputFormat = "Markdown",

    [int]$DaysBack = 30,

    [int]$MaxParallel = 10,

    [switch]$Obfuscate,

    [string]$ObfuscationSeed,

    [string]$ObfuscationMapTable,

    [int]$ExpertMode = 0,

    [int]$LongRunningMinutes = 10,

    [int]$FailureThreshold = 3,

    [int]$ThroughputWindowDays = 7,

    [int]$TopN = 20,

    [Nullable[int]]$EfficacyDaysBack,

    [Nullable[int]]$EfficacyDetailDays,

    [bool]$TrustServerCertificate = $true,

    [PSCredential]$Credential
)

$ErrorActionPreference = "Stop"

# =============================================================================
# Prerequisites
# =============================================================================

if ($PSVersionTable.PSVersion.Major -lt 7) {
    throw "This script requires PowerShell 7 or higher. Current version: $($PSVersionTable.PSVersion)"
}

if (-not (Test-Path $OutputPath)) {
    New-Item -ItemType Directory -Path $OutputPath -Force | Out-Null
}
$OutputPath = (Resolve-Path $OutputPath).Path
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"

Write-Host "===============================================================================" -ForegroundColor Cyan
Write-Host " sp_StatUpdate Diagnostic Analysis" -ForegroundColor Cyan
Write-Host "===============================================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Servers:         $($Servers.Count)"
Write-Host "CommandLog DB:   $CommandLogDatabase"
Write-Host "Days back:       $DaysBack"
Write-Host "Obfuscate:       $Obfuscate$(if ($ObfuscationSeed) { ' (seeded)' })$(if ($Obfuscate) { ' (dual output)' })"
Write-Host "ExpertMode:      $ExpertMode"
Write-Host "Output:          $OutputPath"
Write-Host "Format:          $OutputFormat"
Write-Host ""

# =============================================================================
# Execute sp_StatUpdate_Diag on each server
# =============================================================================

$procParams = @{
    DaysBack             = $DaysBack
    ObfuscationSeed      = $ObfuscationSeed
    ObfuscationMapTable  = $ObfuscationMapTable
    ExpertMode           = $ExpertMode
    LongRunningMinutes   = $LongRunningMinutes
    FailureThreshold     = $FailureThreshold
    ThroughputWindowDays = $ThroughputWindowDays
    TopN                 = $TopN
    EfficacyDaysBack     = $EfficacyDaysBack
    EfficacyDetailDays   = $EfficacyDetailDays
    CommandLogDatabase   = $CommandLogDatabase
    IsObfuscateMode      = [bool]$Obfuscate
}

# Thread-safe collections for parallel execution
$allResults = [System.Collections.Concurrent.ConcurrentDictionary[string, object]]::new()
$allErrors = [System.Collections.Concurrent.ConcurrentBag[PSObject]]::new()
$progress = [System.Collections.Concurrent.ConcurrentDictionary[string, string]]::new()

Write-Host "Querying $($Servers.Count) server(s)..." -ForegroundColor Yellow
Write-Host ""

$Servers | ForEach-Object -ThrottleLimit $MaxParallel -Parallel {
    $server = $_
    $paramsLocal = $using:procParams
    $resultsLocal = $using:allResults
    $errorsLocal = $using:allErrors
    $progressLocal = $using:progress
    $credLocal = $using:Credential
    $dbLocal = $using:CommandLogDatabase
    $trustCert = $using:TrustServerCertificate

    $progressLocal[$server] = "Running"

    # Maps DataSet tables to named result sets by unique column signatures.
    # Defined inside the parallel block because scriptblocks can't cross the $using: boundary.
    function Map-ResultSets {
        param([System.Data.DataSet]$DataSet)
        $map = @{}
        foreach ($table in $DataSet.Tables) {
            $cols = $table.Columns | ForEach-Object { $_.ColumnName }
            if     ($cols -contains "Grade" -and $cols -contains "Score" -and $cols -contains "Headline") { $map["Dashboard"] = $table }
            elseif ($cols -contains "Finding" -and $cols -contains "Recommendation" -and $cols -contains "Severity") { $map["Recommendations"] = $table }
            elseif ($cols -contains "TotalRuns") { $map["RunHealth"] = $table }
            elseif ($cols -contains "RunLabel" -and $cols -contains "StopReason" -and $cols -contains "IsKilled") { $map["RunDetail"] = $table }
            elseif ($cols -contains "TotalDurationSec") { $map["TopTables"] = $table }
            elseif ($cols -contains "FailureCount") { $map["FailingStats"] = $table }
            elseif ($cols -contains "AvgDurationSec" -and -not ($cols -contains "FailureCount")) { $map["LongRunning"] = $table }
            elseif ($cols -contains "TieredThresholds") { $map["ParamHistory"] = $table }
            elseif ($cols -contains "OriginalName" -and $cols -contains "ObfuscatedName") { $map["ObfuscationMap"] = $table }
            elseif ($cols -contains "WeekLabel" -and $cols -contains "TrendDirection") { $map["EfficacyTrend"] = $table }
            elseif ($cols -contains "DeltaVsPrior") { $map["EfficacyDetail"] = $table }
            elseif ($cols -contains "ProcessingPosition" -and $cols -contains "WorkloadRank") { $map["HighCpuPositions"] = $table }
            elseif ($cols -contains "CpuTrend" -and $cols -contains "CpuChangePct") { $map["QSCorrelation"] = $table }
        }
        return $map
    }

    try {
        # Build connection string
        $connStr = "Server=$server;Database=$dbLocal;TrustServerCertificate=$trustCert;Connection Timeout=30;"
        if ($credLocal) {
            $connStr += "User ID=$($credLocal.UserName);Password=$($credLocal.GetNetworkCredential().Password);"
        }
        else {
            $connStr += "Trusted_Connection=True;"
        }

        # Helper: build EXEC statement for sp_StatUpdate_Diag
        function Build-ExecStatement {
            param([int]$Obfuscate, [int]$ExpertMode, [hashtable]$Params)

            $paramList = @(
                "@DaysBack = $($Params.DaysBack)",
                "@Obfuscate = $Obfuscate",
                "@ExpertMode = $ExpertMode",
                "@LongRunningMinutes = $($Params.LongRunningMinutes)",
                "@FailureThreshold = $($Params.FailureThreshold)",
                "@ThroughputWindowDays = $($Params.ThroughputWindowDays)",
                "@TopN = $($Params.TopN)"
            )
            if ($Params.ObfuscationSeed -and $Obfuscate -eq 1) {
                $paramList += "@ObfuscationSeed = N'$($Params.ObfuscationSeed)'"
            }
            if ($Params.ObfuscationMapTable -and $Obfuscate -eq 1) {
                $paramList += "@ObfuscationMapTable = N'$($Params.ObfuscationMapTable)'"
            }
            if ($null -ne $Params.EfficacyDaysBack) {
                $paramList += "@EfficacyDaysBack = $($Params.EfficacyDaysBack)"
            }
            if ($null -ne $Params.EfficacyDetailDays) {
                $paramList += "@EfficacyDetailDays = $($Params.EfficacyDetailDays)"
            }
            if ($Params.CommandLogDatabase -and $Params.CommandLogDatabase -ne $dbLocal) {
                $paramList += "@CommandLogDatabase = N'$($Params.CommandLogDatabase)'"
            }
            return "EXECUTE dbo.sp_StatUpdate_Diag $($paramList -join ', ');"
        }

        # Helper: execute a query and return DataSet
        function Invoke-DiagCall {
            param([string]$ConnStr, [string]$Sql)

            $c = New-Object System.Data.SqlClient.SqlConnection($ConnStr)
            $cm = $c.CreateCommand()
            $cm.CommandTimeout = 600
            $cm.CommandText = $Sql
            $a = New-Object System.Data.SqlClient.SqlDataAdapter($cm)
            $d = New-Object System.Data.DataSet
            try {
                $c.Open()
                $a.Fill($d) | Out-Null
            }
            finally {
                $c.Close()
                $c.Dispose()
            }
            return $d
        }

        if ($paramsLocal.IsObfuscateMode) {
            # --- Two-call architecture for obfuscation mode ---
            # Call 1: Unobfuscated (Confidential) — always ExpertMode=1 for full data
            $sql1 = Build-ExecStatement -Obfuscate 0 -ExpertMode 1 -Params $paramsLocal
            $ds1 = Invoke-DiagCall -ConnStr $connStr -Sql $sql1
            $confidentialMap = Map-ResultSets $ds1

            # Call 2: Obfuscated (SafeToShare) — always ExpertMode=1 for map RS
            $sql2 = Build-ExecStatement -Obfuscate 1 -ExpertMode 1 -Params $paramsLocal
            $ds2 = Invoke-DiagCall -ConnStr $connStr -Sql $sql2
            $safeToShareMap = Map-ResultSets $ds2

            $result = @{
                # Cross-server analysis and report generation use Confidential data
                Dashboard         = $confidentialMap["Dashboard"]
                Recommendations   = $confidentialMap["Recommendations"]
                RunHealth         = $confidentialMap["RunHealth"]
                RunDetail         = $confidentialMap["RunDetail"]
                TopTables         = $confidentialMap["TopTables"]
                FailingStats      = $confidentialMap["FailingStats"]
                LongRunning       = $confidentialMap["LongRunning"]
                ParamHistory      = $confidentialMap["ParamHistory"]
                EfficacyTrend     = $confidentialMap["EfficacyTrend"]
                EfficacyDetail    = $confidentialMap["EfficacyDetail"]
                HighCpuPositions  = $confidentialMap["HighCpuPositions"]
                QSCorrelation     = $confidentialMap["QSCorrelation"]
                # Obfuscation-specific data
                ObfuscationMap    = $safeToShareMap["ObfuscationMap"]
                ConfidentialDS    = $ds1
                SafeToShareDS     = $ds2
                SafeToShareMap    = $safeToShareMap
            }
        }
        else {
            # --- Single call (no obfuscation) ---
            $sql = Build-ExecStatement -Obfuscate 0 -ExpertMode $paramsLocal.ExpertMode -Params $paramsLocal
            $ds = Invoke-DiagCall -ConnStr $connStr -Sql $sql
            $rsMap = Map-ResultSets $ds

            $result = @{
                Dashboard         = $rsMap["Dashboard"]
                Recommendations   = $rsMap["Recommendations"]
                RunHealth         = $rsMap["RunHealth"]
                RunDetail         = $rsMap["RunDetail"]
                TopTables         = $rsMap["TopTables"]
                FailingStats      = $rsMap["FailingStats"]
                LongRunning       = $rsMap["LongRunning"]
                ParamHistory      = $rsMap["ParamHistory"]
                EfficacyTrend     = $rsMap["EfficacyTrend"]
                EfficacyDetail    = $rsMap["EfficacyDetail"]
                HighCpuPositions  = $rsMap["HighCpuPositions"]
                QSCorrelation     = $rsMap["QSCorrelation"]
                ObfuscationMap    = $null
                ConfidentialDS    = $null
                SafeToShareDS     = $null
                SafeToShareMap    = $null
            }
        }

        $resultsLocal[$server] = $result
        $progressLocal[$server] = "Complete"
    }
    catch {
        $progressLocal[$server] = "Failed"
        $errorsLocal.Add([PSCustomObject]@{
            Server    = $server
            Error     = $_.Exception.Message
            Timestamp = Get-Date
        })
    }
}

# Show progress summary
$completed = ($progress.Values | Where-Object { $_ -eq "Complete" }).Count
$failed = ($progress.Values | Where-Object { $_ -eq "Failed" }).Count

Write-Host "  Completed: $completed" -ForegroundColor Green
if ($failed -gt 0) {
    Write-Host "  Failed:    $failed" -ForegroundColor Red
    foreach ($err in $allErrors) {
        Write-Host "    $($err.Server): $($err.Error)" -ForegroundColor Red
    }
}
Write-Host ""

if ($completed -eq 0) {
    Write-Host "No servers returned data. Exiting." -ForegroundColor Red
    exit 1
}

function Get-DisplayName {
    param([string]$ServerName)
    if ($Obfuscate) { "SRV_" + ($ServerName.GetHashCode().ToString("X8")).Substring(0, 4) }
    else { $ServerName }
}

# =============================================================================
# Cross-Server Analysis (always uses Confidential/real data)
# =============================================================================

Write-Host "Running cross-server analysis..." -ForegroundColor Yellow

$crossServerFindings = [System.Collections.Generic.List[PSObject]]::new()

# Version skew detection
$versions = @{}
foreach ($server in $allResults.Keys) {
    $data = $allResults[$server]
    if ($data.RunHealth -and $data.RunHealth.Rows.Count -gt 0) {
        if ($data.RunDetail -and $data.RunDetail.Rows.Count -gt 0) {
            $ver = $data.RunDetail.Rows[0]["Version"]
            if ($ver -and $ver -ne [DBNull]::Value) {
                $versions[$server] = $ver.ToString()
            }
        }
    }
}

$distinctVersions = $versions.Values | Sort-Object -Unique
if ($distinctVersions.Count -gt 1) {
    $versionDetail = ($versions.GetEnumerator() | ForEach-Object { "$(Get-DisplayName $_.Key): $($_.Value)" }) -join ", "
    $crossServerFindings.Add([PSCustomObject]@{
        Severity       = "WARNING"
        Category       = "VERSION_SKEW"
        Finding        = "sp_StatUpdate version varies across $($versions.Count) servers ($($distinctVersions.Count) distinct versions)"
        Evidence       = $versionDetail
        Recommendation = "Standardize sp_StatUpdate version across all servers to ensure consistent behavior."
    })
}

# Parameter inconsistency detection
$timeLimits = @{}
foreach ($server in $allResults.Keys) {
    $data = $allResults[$server]
    if ($data.RunDetail -and $data.RunDetail.Rows.Count -gt 0) {
        $tl = $data.RunDetail.Rows[0]["TimeLimit"]
        if ($tl -and $tl -ne [DBNull]::Value) {
            $timeLimits[$server] = [int]$tl
        }
    }
}

$distinctTimeLimits = $timeLimits.Values | Sort-Object -Unique
if ($distinctTimeLimits.Count -gt 1) {
    $tlDetail = ($timeLimits.GetEnumerator() | ForEach-Object { "$(Get-DisplayName $_.Key): $($_.Value)s" }) -join ", "
    $crossServerFindings.Add([PSCustomObject]@{
        Severity       = "INFO"
        Category       = "PARAM_INCONSISTENCY"
        Finding        = "TimeLimit varies across servers ($($distinctTimeLimits -join ', ') seconds)"
        Evidence       = $tlDetail
        Recommendation = "Review whether different time limits are intentional (different maintenance windows) or accidental."
    })
}

Write-Host "  Cross-server findings: $($crossServerFindings.Count)" -ForegroundColor $(if ($crossServerFindings.Count -gt 0) { "Yellow" } else { "Green" })
Write-Host ""

# =============================================================================
# Aggregate Recommendations
# =============================================================================

Write-Host "Aggregating recommendations..." -ForegroundColor Yellow

$allRecommendations = [System.Collections.Generic.List[PSObject]]::new()

foreach ($server in $allResults.Keys) {
    $data = $allResults[$server]
    if ($data.Recommendations -and $data.Recommendations.Rows.Count -gt 0) {
        foreach ($row in $data.Recommendations.Rows) {
            $allRecommendations.Add([PSCustomObject]@{
                Server         = (Get-DisplayName $server)
                Severity       = $row["Severity"].ToString()
                Category       = $row["Category"].ToString()
                Finding        = $row["Finding"].ToString()
                Evidence       = if ($row["Evidence"] -ne [DBNull]::Value) { $row["Evidence"].ToString() } else { "" }
                Recommendation = if ($row["Recommendation"] -ne [DBNull]::Value) { $row["Recommendation"].ToString() } else { "" }
                ExampleCall    = if ($row["ExampleCall"] -ne [DBNull]::Value) { $row["ExampleCall"].ToString() } else { "" }
            })
        }
    }
}

# Add cross-server findings
foreach ($finding in $crossServerFindings) {
    $allRecommendations.Add([PSCustomObject]@{
        Server         = "CROSS-SERVER"
        Severity       = $finding.Severity
        Category       = $finding.Category
        Finding        = $finding.Finding
        Evidence       = $finding.Evidence
        Recommendation = $finding.Recommendation
        ExampleCall    = ""
    })
}

$criticalCount = ($allRecommendations | Where-Object { $_.Severity -eq "CRITICAL" }).Count
$warningCount = ($allRecommendations | Where-Object { $_.Severity -eq "WARNING" }).Count
$infoCount = ($allRecommendations | Where-Object { $_.Severity -eq "INFO" }).Count

Write-Host "  CRITICAL: $criticalCount" -ForegroundColor $(if ($criticalCount -gt 0) { "Red" } else { "Green" })
Write-Host "  WARNING:  $warningCount" -ForegroundColor $(if ($warningCount -gt 0) { "Yellow" } else { "Green" })
Write-Host "  INFO:     $infoCount" -ForegroundColor Cyan
Write-Host ""

# =============================================================================
# Report Generation
# =============================================================================

Write-Host "Generating $OutputFormat report..." -ForegroundColor Yellow

function ConvertTo-MarkdownTable {
    param([System.Data.DataTable]$Table, [int]$MaxRows = 50)

    if (-not $Table -or $Table.Rows.Count -eq 0) { return "*No data*`n" }

    $cols = $Table.Columns | ForEach-Object { $_.ColumnName }
    $header = "| " + ($cols -join " | ") + " |"
    $separator = "| " + (($cols | ForEach-Object { "---" }) -join " | ") + " |"

    $rows = @($header, $separator)
    $count = 0
    foreach ($row in $Table.Rows) {
        if ($count -ge $MaxRows) {
            $rows += "| *... $($Table.Rows.Count - $MaxRows) more rows* |" + (" |" * ($cols.Count - 1))
            break
        }
        $values = $cols | ForEach-Object {
            $val = $row[$_]
            if ($val -eq [DBNull]::Value) { "" }
            else { $val.ToString().Replace("|", "\|").Replace("`n", " ") }
        }
        $rows += "| " + ($values -join " | ") + " |"
        $count++
    }

    return ($rows -join "`n") + "`n"
}

function Build-MarkdownReport {
    param([hashtable]$AllResults, [System.Collections.Generic.List[PSObject]]$AllRecommendations, [bool]$IsObfuscated)

    $report = [System.Text.StringBuilder]::new()

    [void]$report.AppendLine("# sp_StatUpdate Diagnostic Report")
    [void]$report.AppendLine("")
    [void]$report.AppendLine("Generated: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')")
    [void]$report.AppendLine("Servers analyzed: $($Servers.Count) (completed: $completed, failed: $failed)")
    [void]$report.AppendLine("Analysis window: $DaysBack days")
    if ($IsObfuscated) { [void]$report.AppendLine("**Mode: OBFUSCATED** (names hashed for safe sharing)") }
    [void]$report.AppendLine("")

    # Executive Summary
    [void]$report.AppendLine("## Executive Summary")
    [void]$report.AppendLine("")
    [void]$report.AppendLine("| Severity | Count |")
    [void]$report.AppendLine("| --- | --- |")
    [void]$report.AppendLine("| CRITICAL | $criticalCount |")
    [void]$report.AppendLine("| WARNING | $warningCount |")
    [void]$report.AppendLine("| INFO | $infoCount |")
    [void]$report.AppendLine("")

    # Recommendations by severity
    foreach ($severity in @("CRITICAL", "WARNING", "INFO")) {
        $findings = $AllRecommendations | Where-Object { $_.Severity -eq $severity }
        if ($findings.Count -eq 0) { continue }

        [void]$report.AppendLine("## $severity Findings")
        [void]$report.AppendLine("")

        foreach ($finding in $findings) {
            [void]$report.AppendLine("### [$($finding.Category)] $($finding.Finding)")
            [void]$report.AppendLine("")
            [void]$report.AppendLine("**Server:** $($finding.Server)")
            [void]$report.AppendLine("")
            if ($finding.Evidence) {
                [void]$report.AppendLine("**Evidence:** $($finding.Evidence)")
                [void]$report.AppendLine("")
            }
            if ($finding.Recommendation) {
                [void]$report.AppendLine("**Recommendation:** $($finding.Recommendation)")
                [void]$report.AppendLine("")
            }
            if ($finding.ExampleCall) {
                [void]$report.AppendLine('```sql')
                [void]$report.AppendLine($finding.ExampleCall)
                [void]$report.AppendLine('```')
                [void]$report.AppendLine("")
            }
        }
    }

    # Per-server details
    [void]$report.AppendLine("## Server Details")
    [void]$report.AppendLine("")

    foreach ($server in ($AllResults.Keys | Sort-Object)) {
        $data = $AllResults[$server]

        [void]$report.AppendLine("### Server: $(Get-DisplayName $server)")
        [void]$report.AppendLine("")

        # Run Health
        if ($data.RunHealth -and $data.RunHealth.Rows.Count -gt 0) {
            [void]$report.AppendLine("#### Run Health Summary")
            [void]$report.AppendLine("")
            $rh = $data.RunHealth.Rows[0]
            [void]$report.AppendLine("| Metric | Value |")
            [void]$report.AppendLine("| --- | --- |")
            foreach ($col in $data.RunHealth.Columns) {
                $val = $rh[$col.ColumnName]
                $displayVal = if ($val -eq [DBNull]::Value) { "N/A" } else { $val.ToString() }
                [void]$report.AppendLine("| $($col.ColumnName) | $displayVal |")
            }
            [void]$report.AppendLine("")
        }

        # Top Tables
        if ($data.TopTables -and $data.TopTables.Rows.Count -gt 0) {
            [void]$report.AppendLine("#### Top Tables by Maintenance Cost")
            [void]$report.AppendLine("")
            [void]$report.AppendLine((ConvertTo-MarkdownTable -Table $data.TopTables -MaxRows 10))
        }

        # Failing Stats
        if ($data.FailingStats -and $data.FailingStats.Rows.Count -gt 0) {
            [void]$report.AppendLine("#### Failing Statistics")
            [void]$report.AppendLine("")
            [void]$report.AppendLine((ConvertTo-MarkdownTable -Table $data.FailingStats -MaxRows 10))
        }

        # Long-Running Stats
        if ($data.LongRunning -and $data.LongRunning.Rows.Count -gt 0) {
            [void]$report.AppendLine("#### Long-Running Statistics")
            [void]$report.AppendLine("")
            [void]$report.AppendLine((ConvertTo-MarkdownTable -Table $data.LongRunning -MaxRows 10))
        }
    }

    # Connection failures
    if ($allErrors.Count -gt 0) {
        [void]$report.AppendLine("## Connection Failures")
        [void]$report.AppendLine("")
        foreach ($err in $allErrors) {
            $errServer = Get-DisplayName $err.Server
            [void]$report.AppendLine("- **$errServer**: $($err.Error)")
        }
        [void]$report.AppendLine("")
    }

    [void]$report.AppendLine("---")
    [void]$report.AppendLine("*Generated by sp_StatUpdate_Diag / Invoke-StatUpdateDiag.ps1*")

    return $report.ToString()
}

function Build-JsonOutput {
    param([hashtable]$AllResults, [System.Collections.Generic.List[PSObject]]$AllRecommendations, [bool]$StripObfuscationMap)

    $serverDetails = @{}
    foreach ($server in $AllResults.Keys) {
        $data = $AllResults[$server]
        $detail = @{}

        foreach ($rsName in @("Dashboard", "Recommendations", "RunHealth", "RunDetail", "TopTables", "FailingStats", "LongRunning", "ParamHistory", "EfficacyTrend", "EfficacyDetail", "HighCpuPositions", "QSCorrelation")) {
            $table = $data[$rsName]
            if ($table -and $table.Rows.Count -gt 0) {
                $rows = [System.Collections.Generic.List[hashtable]]::new()
                foreach ($row in $table.Rows) {
                    $rowHash = @{}
                    foreach ($col in $table.Columns) {
                        $val = $row[$col.ColumnName]
                        $rowHash[$col.ColumnName] = if ($val -eq [DBNull]::Value) { $null } else { $val }
                    }
                    $rows.Add($rowHash)
                }
                $detail[$rsName] = $rows
            }
        }

        $displayName = Get-DisplayName $server
        $serverDetails[$displayName] = $detail
    }

    $jsonOutput = @{
        GeneratedAt         = (Get-Date -Format "yyyy-MM-ddTHH:mm:ss")
        ServersAnalyzed     = $Servers.Count
        Completed           = $completed
        Failed              = $failed
        DaysBack            = $DaysBack
        Obfuscated          = $StripObfuscationMap
        Summary             = @{
            Critical = $criticalCount
            Warning  = $warningCount
            Info     = $infoCount
        }
        Recommendations     = @($AllRecommendations | ForEach-Object {
            @{
                Server         = $_.Server
                Severity       = $_.Severity
                Category       = $_.Category
                Finding        = $_.Finding
                Evidence       = $_.Evidence
                Recommendation = $_.Recommendation
                ExampleCall    = $_.ExampleCall
            }
        })
        CrossServerFindings = @($crossServerFindings | ForEach-Object {
            @{
                Severity       = $_.Severity
                Category       = $_.Category
                Finding        = $_.Finding
                Evidence       = $_.Evidence
                Recommendation = $_.Recommendation
            }
        })
        ServerDetails       = $serverDetails
    }

    return ($jsonOutput | ConvertTo-Json -Depth 10)
}

function New-DecodeSqlScript {
    param([hashtable]$AllResults)

    $sb = [System.Text.StringBuilder]::new()

    [void]$sb.AppendLine("/* WARNING: CONFIDENTIAL - contains real server/database/table names. Do NOT share externally. */")
    [void]$sb.AppendLine("/* Generated by Invoke-StatUpdateDiag.ps1 on $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') */")
    [void]$sb.AppendLine("")
    [void]$sb.AppendLine("CREATE TABLE #ObfuscationMap (")
    [void]$sb.AppendLine("    ServerName     nvarchar(128)  NOT NULL,")
    [void]$sb.AppendLine("    ObjectType     nvarchar(20)   NOT NULL,")
    [void]$sb.AppendLine("    OriginalName   nvarchar(256)  NOT NULL,")
    [void]$sb.AppendLine("    ObfuscatedName nvarchar(50)   NOT NULL")
    [void]$sb.AppendLine(");")
    [void]$sb.AppendLine("")

    # Collect all map rows across servers
    $allRows = [System.Collections.Generic.List[PSObject]]::new()
    foreach ($server in $AllResults.Keys) {
        $data = $AllResults[$server]
        if ($data.ObfuscationMap -and $data.ObfuscationMap.Rows.Count -gt 0) {
            foreach ($row in $data.ObfuscationMap.Rows) {
                $allRows.Add([PSCustomObject]@{
                    ServerName     = $server
                    ObjectType     = $row["ObjectType"].ToString()
                    OriginalName   = $row["OriginalName"].ToString()
                    ObfuscatedName = $row["ObfuscatedName"].ToString()
                })
            }
        }
    }

    if ($allRows.Count -eq 0) {
        [void]$sb.AppendLine("/* No obfuscation map data collected. */")
    }
    else {
        # Chunk INSERTs at 1000 rows (SQL Server VALUES list limit)
        $chunkSize = 1000
        for ($i = 0; $i -lt $allRows.Count; $i += $chunkSize) {
            $end = [Math]::Min($i + $chunkSize, $allRows.Count)
            [void]$sb.AppendLine("INSERT INTO #ObfuscationMap (ServerName, ObjectType, OriginalName, ObfuscatedName) VALUES")

            for ($j = $i; $j -lt $end; $j++) {
                $r = $allRows[$j]
                $escapedServer = $r.ServerName.Replace("'", "''")
                $escapedType = $r.ObjectType.Replace("'", "''")
                $escapedOrig = $r.OriginalName.Replace("'", "''")
                $escapedObf = $r.ObfuscatedName.Replace("'", "''")
                $comma = if ($j -lt ($end - 1)) { "," } else { ";" }
                [void]$sb.AppendLine("    (N'$escapedServer', N'$escapedType', N'$escapedOrig', N'$escapedObf')$comma")
            }
            [void]$sb.AppendLine("")
        }
    }

    [void]$sb.AppendLine("/* === Decode a single token === */")
    [void]$sb.AppendLine("SELECT ServerName, ObjectType, OriginalName, ObfuscatedName")
    [void]$sb.AppendLine("FROM #ObfuscationMap")
    [void]$sb.AppendLine("WHERE ObfuscatedName = N'<paste_token_here>';")
    [void]$sb.AppendLine("")
    [void]$sb.AppendLine("/* === Full map === */")
    [void]$sb.AppendLine("SELECT ServerName, ObjectType, OriginalName, ObfuscatedName")
    [void]$sb.AppendLine("FROM #ObfuscationMap")
    [void]$sb.AppendLine("ORDER BY ServerName, ObjectType, OriginalName;")
    [void]$sb.AppendLine("")
    [void]$sb.AppendLine("/* Cleanup */")
    [void]$sb.AppendLine("DROP TABLE #ObfuscationMap;")

    return $sb.ToString()
}

# =============================================================================
# Build report content (Markdown used for Markdown and HTML formats)
# =============================================================================

$reportContent = Build-MarkdownReport -AllResults $allResults -AllRecommendations $allRecommendations -IsObfuscated $false

# =============================================================================
# Output
# =============================================================================

if ($Obfuscate) {
    # --- Dual output mode: SAFE_TO_SHARE + CONFIDENTIAL + DECODE ---

    $baseFileName = "sp_StatUpdate_Diag_${timestamp}"

    # Build obfuscated recommendations from SafeToShare data
    $obfRecommendations = [System.Collections.Generic.List[PSObject]]::new()
    foreach ($server in $allResults.Keys) {
        $data = $allResults[$server]
        $safeRecs = $data.SafeToShareMap["Recommendations"]
        if ($safeRecs -and $safeRecs.Rows.Count -gt 0) {
            foreach ($row in $safeRecs.Rows) {
                $obfRecommendations.Add([PSCustomObject]@{
                    Server         = (Get-DisplayName $server)
                    Severity       = $row["Severity"].ToString()
                    Category       = $row["Category"].ToString()
                    Finding        = $row["Finding"].ToString()
                    Evidence       = if ($row["Evidence"] -ne [DBNull]::Value) { $row["Evidence"].ToString() } else { "" }
                    Recommendation = if ($row["Recommendation"] -ne [DBNull]::Value) { $row["Recommendation"].ToString() } else { "" }
                    ExampleCall    = if ($row["ExampleCall"] -ne [DBNull]::Value) { $row["ExampleCall"].ToString() } else { "" }
                })
            }
        }
    }

    # Build obfuscated view of allResults using SafeToShare data
    $safeResults = @{}
    foreach ($server in $allResults.Keys) {
        $data = $allResults[$server]
        if ($data.SafeToShareMap) {
            $safeResults[$server] = @{
                Dashboard        = $data.SafeToShareMap["Dashboard"]
                Recommendations  = $data.SafeToShareMap["Recommendations"]
                RunHealth        = $data.SafeToShareMap["RunHealth"]
                RunDetail        = $data.SafeToShareMap["RunDetail"]
                TopTables        = $data.SafeToShareMap["TopTables"]
                FailingStats     = $data.SafeToShareMap["FailingStats"]
                LongRunning      = $data.SafeToShareMap["LongRunning"]
                ParamHistory     = $data.SafeToShareMap["ParamHistory"]
                EfficacyTrend    = $data.SafeToShareMap["EfficacyTrend"]
                EfficacyDetail   = $data.SafeToShareMap["EfficacyDetail"]
                HighCpuPositions = $data.SafeToShareMap["HighCpuPositions"]
                QSCorrelation    = $data.SafeToShareMap["QSCorrelation"]
            }
        }
    }

    switch ($OutputFormat) {
        "Markdown" {
            # CONFIDENTIAL
            $confPath = Join-Path $OutputPath "${baseFileName}_CONFIDENTIAL.md"
            $reportContent | Out-File -FilePath $confPath -Encoding UTF8
            Write-Host "  CONFIDENTIAL: $confPath" -ForegroundColor Yellow

            # SAFE_TO_SHARE (obfuscated)
            $safePath = Join-Path $OutputPath "${baseFileName}_SAFE_TO_SHARE.md"
            $safeReport = Build-MarkdownReport -AllResults $safeResults -AllRecommendations $obfRecommendations -IsObfuscated $true
            $safeReport | Out-File -FilePath $safePath -Encoding UTF8
            Write-Host "  SAFE_TO_SHARE: $safePath" -ForegroundColor Green
        }
        "HTML" {
            # CONFIDENTIAL
            $confPath = Join-Path $OutputPath "${baseFileName}_CONFIDENTIAL.html"
            $html = @"
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>sp_StatUpdate Diagnostic Report (CONFIDENTIAL)</title>
<style>
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; max-width: 1200px; margin: 0 auto; padding: 20px; background: #f5f5f5; }
    h1 { color: #1a1a2e; border-bottom: 3px solid #16213e; padding-bottom: 10px; }
    h2 { color: #16213e; margin-top: 30px; }
    h3 { color: #0f3460; }
    table { border-collapse: collapse; width: 100%; margin: 10px 0; background: white; }
    th { background: #16213e; color: white; padding: 8px 12px; text-align: left; }
    td { padding: 6px 12px; border-bottom: 1px solid #eee; }
    tr:hover td { background: #f0f0f0; }
    code, pre { background: #e8e8e8; padding: 2px 6px; border-radius: 3px; font-family: 'Cascadia Code', Consolas, monospace; }
    pre { padding: 12px; overflow-x: auto; }
    .critical { color: #d32f2f; font-weight: bold; }
    .warning { color: #f57c00; font-weight: bold; }
    .info { color: #1976d2; }
</style>
</head>
<body>
$($reportContent -replace '```sql\r?\n(.*?)\r?\n```', '<pre><code>$1</code></pre>' -replace '\*\*([^*]+)\*\*', '<strong>$1</strong>' -replace '# (.+)', '<h1>$1</h1>' -replace '## (.+)', '<h2>$1</h2>' -replace '### (.+)', '<h3>$1</h3>')
</body>
</html>
"@
            $html | Out-File -FilePath $confPath -Encoding UTF8
            Write-Host "  CONFIDENTIAL: $confPath" -ForegroundColor Yellow

            # SAFE_TO_SHARE
            $safePath = Join-Path $OutputPath "${baseFileName}_SAFE_TO_SHARE.html"
            $safeReport = Build-MarkdownReport -AllResults $safeResults -AllRecommendations $obfRecommendations -IsObfuscated $true
            $safeHtml = @"
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>sp_StatUpdate Diagnostic Report (Safe to Share)</title>
<style>
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; max-width: 1200px; margin: 0 auto; padding: 20px; background: #f5f5f5; }
    h1 { color: #1a1a2e; border-bottom: 3px solid #16213e; padding-bottom: 10px; }
    h2 { color: #16213e; margin-top: 30px; }
    h3 { color: #0f3460; }
    table { border-collapse: collapse; width: 100%; margin: 10px 0; background: white; }
    th { background: #16213e; color: white; padding: 8px 12px; text-align: left; }
    td { padding: 6px 12px; border-bottom: 1px solid #eee; }
    tr:hover td { background: #f0f0f0; }
    code, pre { background: #e8e8e8; padding: 2px 6px; border-radius: 3px; font-family: 'Cascadia Code', Consolas, monospace; }
    pre { padding: 12px; overflow-x: auto; }
    .critical { color: #d32f2f; font-weight: bold; }
    .warning { color: #f57c00; font-weight: bold; }
    .info { color: #1976d2; }
</style>
</head>
<body>
$($safeReport -replace '```sql\r?\n(.*?)\r?\n```', '<pre><code>$1</code></pre>' -replace '\*\*([^*]+)\*\*', '<strong>$1</strong>' -replace '# (.+)', '<h1>$1</h1>' -replace '## (.+)', '<h2>$1</h2>' -replace '### (.+)', '<h3>$1</h3>')
</body>
</html>
"@
            $safeHtml | Out-File -FilePath $safePath -Encoding UTF8
            Write-Host "  SAFE_TO_SHARE: $safePath" -ForegroundColor Green
        }
        "JSON" {
            # CONFIDENTIAL
            $confPath = Join-Path $OutputPath "${baseFileName}_CONFIDENTIAL.json"
            $confJson = Build-JsonOutput -AllResults $allResults -AllRecommendations $allRecommendations -StripObfuscationMap $false
            $confJson | Out-File -FilePath $confPath -Encoding UTF8
            Write-Host "  CONFIDENTIAL: $confPath" -ForegroundColor Yellow

            # SAFE_TO_SHARE (obfuscated, no map)
            $safePath = Join-Path $OutputPath "${baseFileName}_SAFE_TO_SHARE.json"
            $safeJson = Build-JsonOutput -AllResults $safeResults -AllRecommendations $obfRecommendations -StripObfuscationMap $true
            $safeJson | Out-File -FilePath $safePath -Encoding UTF8
            Write-Host "  SAFE_TO_SHARE: $safePath" -ForegroundColor Green
        }
    }

    # DECODE SQL (always generated in obfuscate mode)
    $decodePath = Join-Path $OutputPath "${baseFileName}_CONFIDENTIAL_DECODE.sql"
    $decodeSql = New-DecodeSqlScript -AllResults $allResults
    $decodeSql | Out-File -FilePath $decodePath -Encoding UTF8
    Write-Host "  DECODE:        $decodePath" -ForegroundColor Yellow

    # Also export recommendations CSV (confidential)
    if ($allRecommendations.Count -gt 0) {
        $csvPath = Join-Path $OutputPath "sp_StatUpdate_Diag_Recommendations_${timestamp}_CONFIDENTIAL.csv"
        $allRecommendations | Export-Csv -Path $csvPath -NoTypeInformation -Encoding UTF8
        Write-Host "  CSV:           $csvPath" -ForegroundColor Yellow
    }

    # Confidentiality warning
    Write-Host ""
    Write-Host "  *** CONFIDENTIAL files contain real server/database/table names ***" -ForegroundColor Red
    Write-Host "  *** Only share the _SAFE_TO_SHARE file with external parties    ***" -ForegroundColor Red
}
else {
    # --- Single output mode (no obfuscation) ---

    switch ($OutputFormat) {
        "Markdown" {
            $filePath = Join-Path $OutputPath "sp_StatUpdate_Diag_${timestamp}.md"
            $reportContent | Out-File -FilePath $filePath -Encoding UTF8
            Write-Host "  Report: $filePath" -ForegroundColor Green
        }
        "HTML" {
            $filePath = Join-Path $OutputPath "sp_StatUpdate_Diag_${timestamp}.html"

            $html = @"
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>sp_StatUpdate Diagnostic Report</title>
<style>
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; max-width: 1200px; margin: 0 auto; padding: 20px; background: #f5f5f5; }
    h1 { color: #1a1a2e; border-bottom: 3px solid #16213e; padding-bottom: 10px; }
    h2 { color: #16213e; margin-top: 30px; }
    h3 { color: #0f3460; }
    table { border-collapse: collapse; width: 100%; margin: 10px 0; background: white; }
    th { background: #16213e; color: white; padding: 8px 12px; text-align: left; }
    td { padding: 6px 12px; border-bottom: 1px solid #eee; }
    tr:hover td { background: #f0f0f0; }
    code, pre { background: #e8e8e8; padding: 2px 6px; border-radius: 3px; font-family: 'Cascadia Code', Consolas, monospace; }
    pre { padding: 12px; overflow-x: auto; }
    .critical { color: #d32f2f; font-weight: bold; }
    .warning { color: #f57c00; font-weight: bold; }
    .info { color: #1976d2; }
</style>
</head>
<body>
$($reportContent -replace '```sql\r?\n(.*?)\r?\n```', '<pre><code>$1</code></pre>' -replace '\*\*([^*]+)\*\*', '<strong>$1</strong>' -replace '# (.+)', '<h1>$1</h1>' -replace '## (.+)', '<h2>$1</h2>' -replace '### (.+)', '<h3>$1</h3>')
</body>
</html>
"@
            $html | Out-File -FilePath $filePath -Encoding UTF8
            Write-Host "  Report: $filePath" -ForegroundColor Green
        }
        "JSON" {
            $filePath = Join-Path $OutputPath "sp_StatUpdate_Diag_${timestamp}.json"
            $jsonContent = Build-JsonOutput -AllResults $allResults -AllRecommendations $allRecommendations -StripObfuscationMap $false
            $jsonContent | Out-File -FilePath $filePath -Encoding UTF8
            Write-Host "  Report: $filePath" -ForegroundColor Green
        }
    }

    # Also export raw recommendations as CSV
    if ($allRecommendations.Count -gt 0) {
        $csvPath = Join-Path $OutputPath "sp_StatUpdate_Diag_Recommendations_${timestamp}.csv"
        $allRecommendations | Export-Csv -Path $csvPath -NoTypeInformation -Encoding UTF8
        Write-Host "  CSV:    $csvPath" -ForegroundColor Green
    }
}

# =============================================================================
# Summary
# =============================================================================

Write-Host ""
Write-Host "===============================================================================" -ForegroundColor Cyan
Write-Host " COMPLETE" -ForegroundColor Cyan
Write-Host "===============================================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Servers:        $($Servers.Count) ($completed OK, $failed failed)"
Write-Host "Findings:       $criticalCount CRITICAL, $warningCount WARNING, $infoCount INFO" -ForegroundColor $(
    if ($criticalCount -gt 0) { "Red" }
    elseif ($warningCount -gt 0) { "Yellow" }
    else { "Green" }
)
Write-Host ""
Write-Host "Output files:"
Get-ChildItem -Path $OutputPath -Filter "sp_StatUpdate_Diag*${timestamp}*" | ForEach-Object {
    Write-Host "  $($_.Name) ($([math]::Round($_.Length/1KB, 1)) KB)" -ForegroundColor Cyan
}
Write-Host ""
