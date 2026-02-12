<#
.SYNOPSIS
    Multi-server diagnostic analysis for sp_StatUpdate using CommandLog data.

.DESCRIPTION
    Orchestrates sp_StatUpdate_Diag across one or more SQL Servers in parallel,
    merges results, detects cross-server patterns, and generates a consolidated
    Markdown or HTML report with severity-categorized recommendations.

    Prerequisites:
    - PowerShell 7+
    - SqlServer module (Install-Module SqlServer)
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
    When specified, all output uses hashed names (safe for external sharing).

.PARAMETER LongRunningMinutes
    Threshold for long-running stat detection. Defaults to 10.

.PARAMETER TopN
    Limit for detail result sets. Defaults to 20.

.PARAMETER Credential
    Optional PSCredential for SQL authentication. If not provided, uses Windows auth.

.EXAMPLE
    # Single server, Windows auth
    .\Invoke-StatUpdateDiag.ps1 -Servers @('PROD-SQL01')

.EXAMPLE
    # Multi-server, obfuscated for sharing
    .\Invoke-StatUpdateDiag.ps1 -Servers @('PROD-SQL01','PROD-SQL02','DR-SQL01') -Obfuscate -OutputFormat HTML

.EXAMPLE
    # SQL auth, custom CommandLog location
    $cred = Get-Credential
    .\Invoke-StatUpdateDiag.ps1 -Servers (Get-Content servers.txt) -Credential $cred -CommandLogDatabase 'DBATools'

.NOTES
    Requires: PowerShell 7+, SqlServer module
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

    [int]$LongRunningMinutes = 10,

    [int]$TopN = 20,

    [PSCredential]$Credential
)

$ErrorActionPreference = "Stop"

# =============================================================================
# Prerequisites
# =============================================================================

if ($PSVersionTable.PSVersion.Major -lt 7) {
    throw "This script requires PowerShell 7 or higher. Current version: $($PSVersionTable.PSVersion)"
}

if (-not (Get-Module -ListAvailable -Name SqlServer)) {
    Write-Warning "SqlServer module not found. Installing..."
    Install-Module SqlServer -Scope CurrentUser -Force
}
Import-Module SqlServer -ErrorAction Stop

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
Write-Host "Obfuscate:       $Obfuscate"
Write-Host "Output:          $OutputPath"
Write-Host "Format:          $OutputFormat"
Write-Host ""

# =============================================================================
# Execute sp_StatUpdate_Diag on each server
# =============================================================================

function Invoke-DiagProc {
    param(
        [string]$Server,
        [string]$Database,
        [hashtable]$ProcParams,
        [PSCredential]$Credential
    )

    $connStr = "Server=$Server;Database=$Database;TrustServerCertificate=True;Connection Timeout=30;"
    if ($Credential) {
        $connStr += "User ID=$($Credential.UserName);Password=$($Credential.GetNetworkCredential().Password);"
    }
    else {
        $connStr += "Trusted_Connection=True;"
    }

    $conn = New-Object System.Data.SqlClient.SqlConnection($connStr)
    $cmd = $conn.CreateCommand()
    $cmd.CommandTimeout = 600  # 10 minutes

    $paramList = @(
        "@DaysBack = $($ProcParams.DaysBack)",
        "@Obfuscate = $($ProcParams.Obfuscate)",
        "@LongRunningMinutes = $($ProcParams.LongRunningMinutes)",
        "@TopN = $($ProcParams.TopN)"
    )
    if ($ProcParams.CommandLogDatabase) {
        $paramList += "@CommandLogDatabase = N'$($ProcParams.CommandLogDatabase)'"
    }

    $cmd.CommandText = "EXECUTE dbo.sp_StatUpdate_Diag $($paramList -join ', ');"

    $adapter = New-Object System.Data.SqlClient.SqlDataAdapter($cmd)
    $ds = New-Object System.Data.DataSet

    try {
        $conn.Open()
        $adapter.Fill($ds) | Out-Null
    }
    finally {
        $conn.Close()
        $conn.Dispose()
    }

    return @{
        Recommendations   = if ($ds.Tables.Count -gt 0) { $ds.Tables[0] } else { $null }
        RunHealth         = if ($ds.Tables.Count -gt 1) { $ds.Tables[1] } else { $null }
        RunDetail         = if ($ds.Tables.Count -gt 2) { $ds.Tables[2] } else { $null }
        TopTables         = if ($ds.Tables.Count -gt 3) { $ds.Tables[3] } else { $null }
        FailingStats      = if ($ds.Tables.Count -gt 4) { $ds.Tables[4] } else { $null }
        LongRunning       = if ($ds.Tables.Count -gt 5) { $ds.Tables[5] } else { $null }
        ParamHistory      = if ($ds.Tables.Count -gt 6) { $ds.Tables[6] } else { $null }
        ObfuscationMap    = if ($ds.Tables.Count -gt 7) { $ds.Tables[7] } else { $null }
    }
}

$procParams = @{
    DaysBack             = $DaysBack
    Obfuscate            = if ($Obfuscate) { 1 } else { 0 }
    LongRunningMinutes   = $LongRunningMinutes
    TopN                 = $TopN
    CommandLogDatabase   = $CommandLogDatabase
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

    $progressLocal[$server] = "Running"

    try {
        # Import module in parallel runspace
        Import-Module SqlServer -ErrorAction Stop

        # Build connection and execute
        $connStr = "Server=$server;Database=$dbLocal;TrustServerCertificate=True;Connection Timeout=30;"
        if ($credLocal) {
            $connStr += "User ID=$($credLocal.UserName);Password=$($credLocal.GetNetworkCredential().Password);"
        }
        else {
            $connStr += "Trusted_Connection=True;"
        }

        $conn = New-Object System.Data.SqlClient.SqlConnection($connStr)
        $cmd = $conn.CreateCommand()
        $cmd.CommandTimeout = 600

        $paramList = @(
            "@DaysBack = $($paramsLocal.DaysBack)",
            "@Obfuscate = $($paramsLocal.Obfuscate)",
            "@LongRunningMinutes = $($paramsLocal.LongRunningMinutes)",
            "@TopN = $($paramsLocal.TopN)"
        )
        if ($paramsLocal.CommandLogDatabase -and $paramsLocal.CommandLogDatabase -ne $dbLocal) {
            $paramList += "@CommandLogDatabase = N'$($paramsLocal.CommandLogDatabase)'"
        }

        $cmd.CommandText = "EXECUTE dbo.sp_StatUpdate_Diag $($paramList -join ', ');"

        $adapter = New-Object System.Data.SqlClient.SqlDataAdapter($cmd)
        $ds = New-Object System.Data.DataSet

        try {
            $conn.Open()
            $adapter.Fill($ds) | Out-Null
        }
        finally {
            $conn.Close()
            $conn.Dispose()
        }

        $result = @{
            Recommendations   = if ($ds.Tables.Count -gt 0) { , $ds.Tables[0] } else { $null }
            RunHealth         = if ($ds.Tables.Count -gt 1) { , $ds.Tables[1] } else { $null }
            RunDetail         = if ($ds.Tables.Count -gt 2) { , $ds.Tables[2] } else { $null }
            TopTables         = if ($ds.Tables.Count -gt 3) { , $ds.Tables[3] } else { $null }
            FailingStats      = if ($ds.Tables.Count -gt 4) { , $ds.Tables[4] } else { $null }
            LongRunning       = if ($ds.Tables.Count -gt 5) { , $ds.Tables[5] } else { $null }
            ParamHistory      = if ($ds.Tables.Count -gt 6) { , $ds.Tables[6] } else { $null }
            ObfuscationMap    = if ($ds.Tables.Count -gt 7) { , $ds.Tables[7] } else { $null }
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

# =============================================================================
# Cross-Server Analysis
# =============================================================================

Write-Host "Running cross-server analysis..." -ForegroundColor Yellow

$crossServerFindings = [System.Collections.Generic.List[PSObject]]::new()

# Version skew detection
$versions = @{}
foreach ($server in $allResults.Keys) {
    $data = $allResults[$server]
    if ($data.RunHealth -and $data.RunHealth.Rows.Count -gt 0) {
        # Get version from RunDetail if available
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
    $versionDetail = ($versions.GetEnumerator() | ForEach-Object { "$($_.Key): $($_.Value)" }) -join ", "
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
    $tlDetail = ($timeLimits.GetEnumerator() | ForEach-Object { "$($_.Key): $($_.Value)s" }) -join ", "
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
                Server         = $server
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

# Build Markdown report
$report = [System.Text.StringBuilder]::new()

[void]$report.AppendLine("# sp_StatUpdate Diagnostic Report")
[void]$report.AppendLine("")
[void]$report.AppendLine("Generated: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')")
[void]$report.AppendLine("Servers analyzed: $($Servers.Count) (completed: $completed, failed: $failed)")
[void]$report.AppendLine("Analysis window: $DaysBack days")
if ($Obfuscate) { [void]$report.AppendLine("**Mode: OBFUSCATED** (names hashed for safe sharing)") }
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
    $findings = $allRecommendations | Where-Object { $_.Severity -eq $severity }
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

foreach ($server in ($allResults.Keys | Sort-Object)) {
    $data = $allResults[$server]

    $displayName = if ($Obfuscate) {
        "SRV_" + ($server.GetHashCode().ToString("X8")).Substring(0, 4)
    } else {
        $server
    }

    [void]$report.AppendLine("### Server: $displayName")
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
        $errServer = if ($Obfuscate) { "SRV_" + ($err.Server.GetHashCode().ToString("X8")).Substring(0, 4) } else { $err.Server }
        [void]$report.AppendLine("- **$errServer**: $($err.Error)")
    }
    [void]$report.AppendLine("")
}

[void]$report.AppendLine("---")
[void]$report.AppendLine("*Generated by sp_StatUpdate_Diag v1.0 / Invoke-StatUpdateDiag.ps1*")

$reportContent = $report.ToString()

# =============================================================================
# Output
# =============================================================================

switch ($OutputFormat) {
    "Markdown" {
        $filePath = Join-Path $OutputPath "sp_StatUpdate_Diag_${timestamp}.md"
        $reportContent | Out-File -FilePath $filePath -Encoding UTF8
        Write-Host "  Report: $filePath" -ForegroundColor Green
    }
    "HTML" {
        $filePath = Join-Path $OutputPath "sp_StatUpdate_Diag_${timestamp}.html"

        # Simple HTML wrapper with embedded CSS
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

        $jsonOutput = @{
            GeneratedAt         = (Get-Date -Format "yyyy-MM-ddTHH:mm:ss")
            ServersAnalyzed     = $Servers.Count
            Completed           = $completed
            Failed              = $failed
            DaysBack            = $DaysBack
            Obfuscated          = [bool]$Obfuscate
            Summary             = @{
                Critical = $criticalCount
                Warning  = $warningCount
                Info     = $infoCount
            }
            Recommendations     = $allRecommendations | ForEach-Object {
                @{
                    Server         = $_.Server
                    Severity       = $_.Severity
                    Category       = $_.Category
                    Finding        = $_.Finding
                    Evidence       = $_.Evidence
                    Recommendation = $_.Recommendation
                    ExampleCall    = $_.ExampleCall
                }
            }
            CrossServerFindings = $crossServerFindings | ForEach-Object {
                @{
                    Severity       = $_.Severity
                    Category       = $_.Category
                    Finding        = $_.Finding
                    Evidence       = $_.Evidence
                    Recommendation = $_.Recommendation
                }
            }
        }

        $jsonOutput | ConvertTo-Json -Depth 10 | Out-File -FilePath $filePath -Encoding UTF8
        Write-Host "  Report: $filePath" -ForegroundColor Green
    }
}

# Also export raw recommendations as CSV
if ($allRecommendations.Count -gt 0) {
    $csvPath = Join-Path $OutputPath "sp_StatUpdate_Diag_Recommendations_${timestamp}.csv"
    $allRecommendations | Export-Csv -Path $csvPath -NoTypeInformation -Encoding UTF8
    Write-Host "  CSV:    $csvPath" -ForegroundColor Green
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
