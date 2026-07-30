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
    Narrative report format: Markdown, HTML, or JSON. Defaults to Markdown.

    A per-result-set CSV export is written on every run regardless of this
    setting: one CSV per result set, unioned across the whole fleet with a
    leading Server column, in a <basename>_csv subfolder. That is the shape
    you want for Excel/Power BI. Suppress it with -NoCsv.
    -OutputFormat CSV writes only that export and skips the narrative report.

.PARAMETER NoCsv
    Suppress the per-result-set CSV export. Ignored when -OutputFormat is CSV
    (that would leave the run with no output at all).

.PARAMETER DaysBack
    Number of days of history to analyze. Defaults to 30.

.PARAMETER MaxParallel
    Maximum parallel threads. Defaults to 10.

.PARAMETER Obfuscate
    When specified, produces dual output: obfuscated (safe to share) and
    confidential (real names). Also generates a decode SQL script.

.PARAMETER ObfuscationSeed
    Salt for the obfuscation hashes. MANDATORY with -Obfuscate.

    Unseeded, the tokens are a bare MD5/SHA of the real name, so a recipient
    can recover 'dbo', 'Orders', 'Production', and your instance names from a
    short dictionary in seconds -- the _SAFE_TO_SHARE file would not be safe
    to share. Seeded, the same name maps to the same token across runs and
    across the fleet (so reports stay comparable over time) but is opaque to
    anyone without the seed. Treat the seed as a secret; keep it stable.

    The seed travels to each instance as a literal in the EXECUTE batch, so it
    is visible in sys.dm_exec_sql_text, Query Store and any SQL audit on those
    servers. That is fine against the threat this defends -- the report
    recipient has no DMV access, and anyone who does already has the real
    names -- but do not reuse a password as the seed.

.PARAMETER ObfuscationMapTable
    Persist the obfuscation map to this table on each server (auto-created if missing).
    The map stays on prod for decoding. Requires -Obfuscate.

.PARAMETER SkipHistory
    Pass @SkipHistory = 1, so sp_StatUpdate_Diag neither creates nor writes
    dbo.StatUpdateDiagHistory on the target instances. Use when you do not want
    the diagnostic to leave a permanent table behind on every box in the fleet.
    Note this also disables the "trend vs prior assessment" line in the dashboard.

.PARAMETER TimeLimitExhaustionPct
    Percentage of runs that must hit TIME_LIMIT before C3 fires. Defaults to 80.

.PARAMETER GradeOverrides
    Passed through to @GradeOverrides, e.g. 'RELIABILITY=A, SPEED=IGNORE'.
    Applies to every server in the fleet run.

.PARAMETER GradeWeights
    Passed through to @GradeWeights, e.g. 'COMPLETION=40, WORKLOAD=40'.
    Applies to every server in the fleet run.

.PARAMETER NoServerDetail
    Omit the per-server "Server Details" sections from the narrative report and
    keep only the Fleet Scoreboard and grouped findings. Past roughly 25
    instances the detail sections are hundreds of tables nobody reads; the CSV
    export carries the same data in a form you can actually query.

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

.PARAMETER ConnectTimeout
    Seconds to wait for a connection to each server. Defaults to 30.

.PARAMETER QueryTimeout
    Seconds to wait for sp_StatUpdate_Diag to return. Defaults to 600.
    Raise this for servers with deep CommandLog retention or a large -DaysBack.

.PARAMETER RetryCount
    Additional attempts per server after a failure. Defaults to 1 (two tries total).
    Set to 0 to fail fast.

.PARAMETER PassThru
    Emit a result object (per-server status, recommendations, output file paths)
    to the pipeline so the run can be consumed by automation.

.EXAMPLE
    # Single server, Windows auth
    .\Invoke-StatUpdateDiag.ps1 -Servers @('PROD-SQL01')

.EXAMPLE
    # Multi-server, obfuscated for sharing. -ObfuscationSeed is mandatory here.
    .\Invoke-StatUpdateDiag.ps1 -Servers @('PROD-SQL01','PROD-SQL02') -Obfuscate -ObfuscationSeed 'acme-2026' -OutputFormat JSON

.EXAMPLE
    # SQL auth, custom CommandLog location
    $cred = Get-Credential
    .\Invoke-StatUpdateDiag.ps1 -Servers (Get-Content servers.txt) -Credential $cred -CommandLogDatabase 'DBATools'

.EXAMPLE
    # Fleet run (15-75 instances), capturing the result object for automation.
    # Note the call operator (&) -- do NOT dot-source (see .NOTES).
    $splat = @{
        Servers            = Get-Content .\instances.txt
        CommandLogDatabase = 'DBATools'
        Obfuscate          = $true
        ObfuscationSeed    = 'fleet-2026-Q3'   # keep this constant across runs
        OutputPath         = 'D:\#DBA\reports'
        Credential         = $cred
        MaxParallel        = 16
        NoServerDetail     = $true             # scoreboard + findings; detail lives in the CSVs
        SkipHistory        = $true             # leave no permanent table on prod
        PassThru           = $true
    }
    $run = & .\Invoke-StatUpdateDiag.ps1 @splat
    $run.Failures | Format-Table
    $run.Files

.EXAMPLE
    # Fleet data straight into Excel/Power BI, no narrative report
    & .\Invoke-StatUpdateDiag.ps1 -Servers $instances -OutputFormat CSV -ExpertMode 1 -OutputPath 'D:\#DBA\csv'
    # -> D:\#DBA\csv\sp_StatUpdate_Diag_<timestamp>_csv\{Dashboard,RunDetail,TopTables,...}.csv

.NOTES
    Requires: PowerShell 7+ (uses ADO.NET directly, no SqlServer module needed)
    Invoke with the call operator (&) or by path -- do NOT dot-source it.
    Dot-sourcing leaks every script variable into the caller's session and
    leaves $ErrorActionPreference = 'Stop' behind.
    See also: sp_StatUpdate_Diag.sql (the T-SQL diagnostic procedure)
#>

[CmdletBinding()]
param(
    # AllowEmptyString: a mandatory [string[]] rejects empty elements in the
    # binder, so -Servers (Get-Content servers.txt) died on a trailing blank
    # line with "Cannot bind argument ... empty string" and no hint as to which
    # entry. The cleanup below strips blanks and says what it dropped instead.
    [Parameter(Mandatory = $true)]
    [AllowEmptyString()]
    [string[]]$Servers,

    [string]$CommandLogDatabase = "master",

    [string]$OutputPath = ".",

    [ValidateSet("Markdown", "HTML", "JSON", "CSV")]
    [string]$OutputFormat = "Markdown",

    [switch]$NoCsv,

    [int]$DaysBack = 30,

    [int]$MaxParallel = 10,

    [switch]$Obfuscate,

    [string]$ObfuscationSeed,

    [string]$ObfuscationMapTable,

    [int]$ExpertMode = 0,

    [int]$LongRunningMinutes = 10,

    [int]$FailureThreshold = 3,

    [ValidateRange(1, 100)]
    [int]$TimeLimitExhaustionPct = 80,

    [int]$ThroughputWindowDays = 7,

    [int]$TopN = 20,

    [switch]$SkipHistory,

    [string]$GradeOverrides,

    [string]$GradeWeights,

    [switch]$NoServerDetail,

    [Nullable[int]]$EfficacyDaysBack,

    [Nullable[int]]$EfficacyDetailDays,

    [bool]$TrustServerCertificate = $true,

    [PSCredential]$Credential,

    [ValidateRange(5, 600)]
    [int]$ConnectTimeout = 30,

    [ValidateRange(30, 21600)]
    [int]$QueryTimeout = 600,

    [ValidateRange(0, 5)]
    [int]$RetryCount = 1,

    [switch]$PassThru
)

$ErrorActionPreference = "Stop"

# =============================================================================
# Prerequisites
# =============================================================================

if ($PSVersionTable.PSVersion.Major -lt 7) {
    throw "This script requires PowerShell 7 or higher. Current version: $($PSVersionTable.PSVersion)"
}

if (-not (Test-Path -LiteralPath $OutputPath)) {
    New-Item -ItemType Directory -Path $OutputPath -Force | Out-Null
}
# -LiteralPath: fleet report directories routinely contain characters PowerShell
# treats as wildcards (D:\#DBA\csv, paths with [brackets]).
$OutputPath = (Resolve-Path -LiteralPath $OutputPath).Path
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"

# De-duplicate and trim the instance list. Fleet inventories arrive from CMS
# queries and text files, and a repeated instance would be queried twice and
# then silently collapse to one key in $allResults.
$rawServerCount = $Servers.Count
$Servers = @(
    $Servers |
        ForEach-Object { if ($null -ne $_) { $_.Trim() } } |
        Where-Object { $_ } |
        Sort-Object -Unique
)
if ($Servers.Count -eq 0) {
    throw "No usable instance names in -Servers (all entries were empty or whitespace)."
}
if ($Servers.Count -ne $rawServerCount) {
    Write-Warning "-Servers reduced from $rawServerCount to $($Servers.Count) entries (blanks/duplicates removed)."
}

# Unseeded obfuscation is not obfuscation. Both hashing layers -- HASHBYTES('MD5')
# in the proc and SHA256 for the SRV_ tokens here -- reduce to a bare hash of the
# real name when the seed is empty, so anyone holding a _SAFE_TO_SHARE file can
# recover 'dbo', 'Orders', 'Production' and your instance names from a short
# dictionary. Refusing is the only honest option for a file named SAFE_TO_SHARE.
if ($Obfuscate -and [string]::IsNullOrWhiteSpace($ObfuscationSeed)) {
    throw @"
-Obfuscate requires -ObfuscationSeed.

Without a seed the tokens are unsalted hashes of the real names and are
reversible by dictionary attack, so the _SAFE_TO_SHARE file would not be
safe to share. Pass a secret you keep constant across runs, e.g.:

    -Obfuscate -ObfuscationSeed 'fleet-2026-Q3'

Keeping the seed stable is what makes two reports comparable over time;
changing it renumbers every token.
"@
}
if ($ObfuscationSeed -and -not $Obfuscate) {
    Write-Warning "-ObfuscationSeed is ignored without -Obfuscate; this run will emit real names."
}
if ($ObfuscationMapTable -and -not $Obfuscate) {
    Write-Warning "-ObfuscationMapTable is ignored without -Obfuscate; no map will be written."
}
if ($NoCsv -and $OutputFormat -eq "CSV") {
    Write-Warning "-NoCsv ignored with -OutputFormat CSV (it would leave the run with no output)."
    $NoCsv = $false
}

# In -Obfuscate mode the script issues two proc calls per server and both are
# pinned to ExpertMode=1: the confidential pass feeds cross-server analysis
# (which needs RS4) and the obfuscated pass must return the obfuscation map RS.
if ($Obfuscate -and $ExpertMode -ne 1) {
    Write-Warning "-Obfuscate forces ExpertMode=1 on both proc calls; the -ExpertMode $ExpertMode you passed is ignored."
}
# Without RS4 there is no Version or TimeLimit data, so cross-server analysis
# cannot run. Say so up front rather than reporting zero findings.
$crossServerAnalysisEnabled = ($Obfuscate -or $ExpertMode -eq 1)
if (-not $crossServerAnalysisEnabled) {
    Write-Warning "Cross-server analysis (version skew, parameter drift) needs -ExpertMode 1; it will be skipped."
}

Write-Host "===============================================================================" -ForegroundColor Cyan
Write-Host " sp_StatUpdate Diagnostic Analysis" -ForegroundColor Cyan
Write-Host "===============================================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Servers:         $($Servers.Count)"
Write-Host "CommandLog DB:   $CommandLogDatabase"
Write-Host "Days back:       $DaysBack"
Write-Host "Obfuscate:       $Obfuscate$(if ($ObfuscationSeed) { ' (seeded)' })$(if ($Obfuscate) { ' (dual output)' })"
Write-Host "ExpertMode:      $(if ($Obfuscate) { '1 (forced by -Obfuscate)' } else { $ExpertMode })"
Write-Host "Parallelism:     $MaxParallel"
Write-Host "Timeouts:        connect ${ConnectTimeout}s / query ${QueryTimeout}s"
Write-Host "Output:          $OutputPath"
Write-Host "Format:          $OutputFormat$(if (-not $NoCsv -and $OutputFormat -ne 'CSV') { ' + per-result-set CSV' })"
Write-Host "Server detail:   $(if ($NoServerDetail) { 'omitted (-NoServerDetail)' } else { 'included' })"
Write-Host "Diag history:    $(if ($SkipHistory) { 'skipped (@SkipHistory = 1)' } else { 'written to dbo.StatUpdateDiagHistory on each instance' })"
Write-Host ""

# The per-server sections are ~10 tables each. Past a couple of dozen instances
# nobody reads them, and the CSV export carries the same rows in queryable form.
if (-not $NoServerDetail -and $OutputFormat -ne "CSV" -and $Servers.Count -gt 25) {
    Write-Host "  Hint: $($Servers.Count) instances will produce ~$($Servers.Count * 10) per-server tables in one file." -ForegroundColor DarkYellow
    Write-Host "        Consider -NoServerDetail and read the detail from the CSV export instead." -ForegroundColor DarkYellow
    Write-Host ""
}

# =============================================================================
# Execute sp_StatUpdate_Diag on each server
# =============================================================================

$procParams = @{
    DaysBack               = $DaysBack
    ObfuscationSeed        = $ObfuscationSeed
    ObfuscationMapTable    = $ObfuscationMapTable
    ExpertMode             = $ExpertMode
    LongRunningMinutes     = $LongRunningMinutes
    FailureThreshold       = $FailureThreshold
    TimeLimitExhaustionPct = $TimeLimitExhaustionPct
    ThroughputWindowDays   = $ThroughputWindowDays
    TopN                   = $TopN
    EfficacyDaysBack       = $EfficacyDaysBack
    EfficacyDetailDays     = $EfficacyDetailDays
    CommandLogDatabase     = $CommandLogDatabase
    SkipHistory            = [int][bool]$SkipHistory
    GradeOverrides         = $GradeOverrides
    GradeWeights           = $GradeWeights
    IsObfuscateMode        = [bool]$Obfuscate
}

# Thread-safe collections for parallel execution
$allResults = [System.Collections.Concurrent.ConcurrentDictionary[string, object]]::new()
$allErrors = [System.Collections.Concurrent.ConcurrentBag[PSObject]]::new()
$progress = [System.Collections.Concurrent.ConcurrentDictionary[string, string]]::new()

Write-Host "Querying $($Servers.Count) server(s)..." -ForegroundColor Yellow
Write-Host ""

$fleetSw = [System.Diagnostics.Stopwatch]::StartNew()

# Each instance reports as it lands. A fleet run is otherwise silent for minutes,
# which is indistinguishable from a hang. The downstream ForEach-Object must stay
# attached to the pipeline -- assigning the parallel output to a variable first
# would buffer everything until the last server finished.
$script:serverStatus = [System.Collections.Generic.List[PSObject]]::new()
$script:serversDone = 0
$serverTotal = $Servers.Count

$Servers | ForEach-Object -ThrottleLimit $MaxParallel -Parallel {
    $server = $_
    $paramsLocal = $using:procParams
    $resultsLocal = $using:allResults
    $errorsLocal = $using:allErrors
    $progressLocal = $using:progress
    $credLocal = $using:Credential
    $dbLocal = $using:CommandLogDatabase
    $trustCert = $using:TrustServerCertificate
    $connTimeout = $using:ConnectTimeout
    $queryTimeout = $using:QueryTimeout
    $retries = $using:RetryCount

    $progressLocal[$server] = "Running"
    $sw = [System.Diagnostics.Stopwatch]::StartNew()

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
        # Connection string via the builder, not string concatenation: instance
        # and database names are escaped correctly, and a password containing
        # ; " or = can neither break the string nor inject a keyword.
        $csb = [System.Data.SqlClient.SqlConnectionStringBuilder]::new()
        $csb['Data Source'] = $server
        $csb['Initial Catalog'] = $dbLocal
        $csb['TrustServerCertificate'] = $trustCert
        $csb['Connect Timeout'] = $connTimeout
        # Identifies the session in sp_whoisactive / dm_exec_sessions on prod.
        $csb['Application Name'] = 'Invoke-StatUpdateDiag'

        $sqlCredential = $null
        if ($credLocal) {
            # SqlCredential carries the password as a read-only SecureString
            # instead of materializing it in the connection string.
            $securePw = $credLocal.Password.Copy()
            $securePw.MakeReadOnly()
            $sqlCredential = [System.Data.SqlClient.SqlCredential]::new($credLocal.UserName, $securePw)
        }
        else {
            $csb['Integrated Security'] = $true
        }
        $connStr = $csb.ConnectionString

        # Helper: build EXEC statement for sp_StatUpdate_Diag.
        # Every string value goes through ConvertTo-SqlLiteral. These are all
        # operator-supplied rather than attacker-supplied, but an -ObfuscationSeed
        # or a database name containing an apostrophe would otherwise produce a
        # syntactically broken batch and fail the whole instance for no reason.
        function ConvertTo-SqlLiteral {
            param([string]$Value)
            return "N'" + $Value.Replace("'", "''") + "'"
        }

        function Build-ExecStatement {
            param([int]$Obfuscate, [int]$ExpertMode, [hashtable]$Params)

            $paramList = @(
                "@DaysBack = $($Params.DaysBack)",
                "@Obfuscate = $Obfuscate",
                "@ExpertMode = $ExpertMode",
                "@LongRunningMinutes = $($Params.LongRunningMinutes)",
                "@FailureThreshold = $($Params.FailureThreshold)",
                "@TimeLimitExhaustionPct = $($Params.TimeLimitExhaustionPct)",
                "@ThroughputWindowDays = $($Params.ThroughputWindowDays)",
                "@TopN = $($Params.TopN)",
                "@SkipHistory = $($Params.SkipHistory)"
            )
            if ($Params.ObfuscationSeed -and $Obfuscate -eq 1) {
                $paramList += "@ObfuscationSeed = $(ConvertTo-SqlLiteral $Params.ObfuscationSeed)"
            }
            if ($Params.ObfuscationMapTable -and $Obfuscate -eq 1) {
                $paramList += "@ObfuscationMapTable = $(ConvertTo-SqlLiteral $Params.ObfuscationMapTable)"
            }
            if ($null -ne $Params.EfficacyDaysBack) {
                $paramList += "@EfficacyDaysBack = $($Params.EfficacyDaysBack)"
            }
            if ($null -ne $Params.EfficacyDetailDays) {
                $paramList += "@EfficacyDetailDays = $($Params.EfficacyDetailDays)"
            }
            if ($Params.GradeOverrides) {
                $paramList += "@GradeOverrides = $(ConvertTo-SqlLiteral $Params.GradeOverrides)"
            }
            if ($Params.GradeWeights) {
                $paramList += "@GradeWeights = $(ConvertTo-SqlLiteral $Params.GradeWeights)"
            }
            # Always passed, never inferred. The old code only sent this when it
            # differed from the connection's Initial Catalog -- which, since both
            # come from -CommandLogDatabase, was never. The proc then fell back to
            # DB_NAME() and happened to be right, but only because the connection
            # was pointed at the CommandLog database and sp_-prefixed procs resolve
            # out of master. Stating it explicitly removes that coincidence.
            if ($Params.CommandLogDatabase) {
                $paramList += "@CommandLogDatabase = $(ConvertTo-SqlLiteral $Params.CommandLogDatabase)"
            }
            return "EXECUTE dbo.sp_StatUpdate_Diag $($paramList -join ', ');"
        }

        # Helper: execute a query and return DataSet.
        # Retries on failure -- across a large fleet, a handful of instances will
        # always drop a login or hit a transient network blip, and losing a whole
        # server's report to one flaky connect is not worth it.
        function Invoke-DiagCall {
            param([string]$ConnStr, [string]$Sql, [int]$Timeout, $SqlCredential, [int]$Attempts)

            $lastError = $null
            for ($attempt = 0; $attempt -le $Attempts; $attempt++) {
                $c = New-Object System.Data.SqlClient.SqlConnection($ConnStr)
                if ($SqlCredential) { $c.Credential = $SqlCredential }
                $cm = $c.CreateCommand()
                $cm.CommandTimeout = $Timeout
                $cm.CommandText = $Sql
                $a = New-Object System.Data.SqlClient.SqlDataAdapter($cm)
                $d = New-Object System.Data.DataSet
                try {
                    $c.Open()
                    $a.Fill($d) | Out-Null
                    return $d
                }
                catch {
                    $lastError = $_
                    # A query timeout means the proc really is that slow on this
                    # instance; retrying just burns another $Timeout seconds.
                    if ($_.Exception.Message -match 'Execution Timeout Expired') { throw }
                }
                finally {
                    $c.Close()
                    $c.Dispose()
                }
                if ($attempt -lt $Attempts) { Start-Sleep -Seconds (3 * ($attempt + 1)) }
            }
            throw $lastError
        }

        if ($paramsLocal.IsObfuscateMode) {
            # --- Two-call architecture for obfuscation mode ---
            # Call 1: Unobfuscated (Confidential) — always ExpertMode=1 for full data
            $sql1 = Build-ExecStatement -Obfuscate 0 -ExpertMode 1 -Params $paramsLocal
            $ds1 = Invoke-DiagCall -ConnStr $connStr -Sql $sql1 -Timeout $queryTimeout -SqlCredential $sqlCredential -Attempts $retries
            $confidentialMap = Map-ResultSets $ds1

            # Call 2: Obfuscated (SafeToShare) — always ExpertMode=1 for map RS
            $sql2 = Build-ExecStatement -Obfuscate 1 -ExpertMode 1 -Params $paramsLocal
            $ds2 = Invoke-DiagCall -ConnStr $connStr -Sql $sql2 -Timeout $queryTimeout -SqlCredential $sqlCredential -Attempts $retries
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
            $ds = Invoke-DiagCall -ConnStr $connStr -Sql $sql -Timeout $queryTimeout -SqlCredential $sqlCredential -Attempts $retries
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
        $sw.Stop()
        [PSCustomObject]@{ Server = $server; Status = "Complete"; Seconds = [math]::Round($sw.Elapsed.TotalSeconds, 1); Error = $null }
    }
    catch {
        $progressLocal[$server] = "Failed"
        $sw.Stop()
        $errorsLocal.Add([PSCustomObject]@{
            Server    = $server
            Error     = $_.Exception.Message
            Timestamp = Get-Date
        })
        [PSCustomObject]@{ Server = $server; Status = "Failed"; Seconds = [math]::Round($sw.Elapsed.TotalSeconds, 1); Error = $_.Exception.Message }
    }
} | ForEach-Object {
    $script:serverStatus.Add($_)
    $script:serversDone++
    $pct = [int](100 * $script:serversDone / $serverTotal)
    if ($_.Status -eq "Complete") {
        Write-Host ("  [{0,3}%] {1,-40} OK    {2,7}s" -f $pct, $_.Server, $_.Seconds) -ForegroundColor Green
    }
    else {
        Write-Host ("  [{0,3}%] {1,-40} FAIL  {2,7}s  {3}" -f $pct, $_.Server, $_.Seconds, $_.Error) -ForegroundColor Red
    }
}

$fleetSw.Stop()

$completed = @($progress.Values | Where-Object { $_ -eq "Complete" }).Count
$failed = @($progress.Values | Where-Object { $_ -eq "Failed" }).Count

Write-Host ""
Write-Host "  Completed: $completed of $($Servers.Count) in $([math]::Round($fleetSw.Elapsed.TotalSeconds, 1))s" -ForegroundColor Green
if ($failed -gt 0) {
    Write-Host "  Failed:    $failed" -ForegroundColor Red
}
Write-Host ""

if ($completed -eq 0) {
    # throw, not exit: 'exit' would kill the caller's session if this script is
    # dot-sourced, and returns no diagnostic detail to automation.
    throw "No servers returned data ($failed failed). First error: $(@($allErrors)[0].Error)"
}

# Deterministic per-server token. [string]::GetHashCode() is randomized per
# process in .NET Core, so the previous implementation produced a different
# SRV_xxxx token for the same instance on every run -- unusable for comparing
# two SAFE_TO_SHARE reports over time. SHA256 over seed+name is stable across
# runs and machines, and unpredictable to anyone without the seed.
$script:displayNameCache = @{}
function Get-DisplayName {
    param([string]$ServerName)

    if (-not $Obfuscate) { return $ServerName }
    if ($script:displayNameCache.ContainsKey($ServerName)) { return $script:displayNameCache[$ServerName] }

    $sha = [System.Security.Cryptography.SHA256]::Create()
    try {
        $bytes = [System.Text.Encoding]::UTF8.GetBytes("$ObfuscationSeed|$($ServerName.ToUpperInvariant())")
        $hash = [System.BitConverter]::ToString($sha.ComputeHash($bytes)).Replace("-", "")
    }
    finally { $sha.Dispose() }

    $token = "SRV_" + $hash.Substring(0, 8)
    $script:displayNameCache[$ServerName] = $token
    return $token
}

# =============================================================================
# Cross-Server Analysis (always uses Confidential/real data)
# =============================================================================

Write-Host "Running cross-server analysis..." -ForegroundColor Yellow

$crossServerFindings = [System.Collections.Generic.List[PSObject]]::new()

# Reads a column from a DataRow without assuming the column exists. Indexing a
# DataRow by a missing column name throws, and with $ErrorActionPreference =
# 'Stop' that would abort the whole fleet report over one schema difference
# (an older sp_StatUpdate_Diag on a single instance).
function Get-CellValue {
    param([System.Data.DataRow]$Row, [string]$Column)

    if (-not $Row) { return $null }
    if (-not $Row.Table.Columns.Contains($Column)) { return $null }
    $val = $Row[$Column]
    if ($null -eq $val -or $val -eq [DBNull]::Value) { return $null }
    return $val
}

# Version skew detection
$versions = @{}
if ($crossServerAnalysisEnabled) {
    foreach ($server in $allResults.Keys) {
        $data = $allResults[$server]
        if ($data.RunDetail -and $data.RunDetail.Rows.Count -gt 0) {
            $ver = Get-CellValue -Row $data.RunDetail.Rows[0] -Column "Version"
            if ($ver) { $versions[$server] = $ver.ToString() }
        }
    }
}

$distinctVersions = @($versions.Values | Sort-Object -Unique)
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
if ($crossServerAnalysisEnabled) {
    foreach ($server in $allResults.Keys) {
        $data = $allResults[$server]
        if ($data.RunDetail -and $data.RunDetail.Rows.Count -gt 0) {
            $tl = Get-CellValue -Row $data.RunDetail.Rows[0] -Column "TimeLimit"
            if ($null -ne $tl) { $timeLimits[$server] = [int]$tl }
        }
    }
}

$distinctTimeLimits = @($timeLimits.Values | Sort-Object -Unique)
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

if ($crossServerAnalysisEnabled) {
    Write-Host "  Cross-server findings: $($crossServerFindings.Count)" -ForegroundColor $(if ($crossServerFindings.Count -gt 0) { "Yellow" } else { "Green" })
}
else {
    Write-Host "  Cross-server findings: skipped (requires -ExpertMode 1 or -Obfuscate)" -ForegroundColor DarkGray
}
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
    param(
        [hashtable]$AllResults,
        [System.Collections.Generic.List[PSObject]]$AllRecommendations,
        [bool]$IsObfuscated,
        [PSObject[]]$ConnectionErrors = @()
    )

    $report = [System.Text.StringBuilder]::new()

    # Counts MUST come from the recommendation list actually being rendered.
    # (Previously these read script-scope $criticalCount/$warningCount/$infoCount,
    #  which are computed from the non-obfuscated pass, so the SAFE_TO_SHARE
    #  report's summary disagreed with its own body.)
    $critCount = @($AllRecommendations | Where-Object { $_.Severity -eq "CRITICAL" }).Count
    $warnCount = @($AllRecommendations | Where-Object { $_.Severity -eq "WARNING" }).Count
    $nfoCount  = @($AllRecommendations | Where-Object { $_.Severity -eq "INFO" }).Count

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
    [void]$report.AppendLine("| CRITICAL | $critCount |")
    [void]$report.AppendLine("| WARNING | $warnCount |")
    [void]$report.AppendLine("| INFO | $nfoCount |")
    [void]$report.AppendLine("")

    # Fleet scoreboard.
    # Per-server detail sections are unreadable past a handful of instances --
    # this is the "which boxes do I look at first" table, worst grade first.
    $scoreboard = [System.Collections.Generic.List[PSObject]]::new()
    foreach ($server in $AllResults.Keys) {
        $data = $AllResults[$server]
        $display = Get-DisplayName $server

        $overall = $null
        if ($data.Dashboard -and $data.Dashboard.Rows.Count -gt 0) {
            $overall = @($data.Dashboard.Rows | Where-Object {
                (Get-CellValue -Row $_ -Column "Category") -eq "OVERALL"
            })[0]
            # Older Diag builds may not tag an OVERALL row; fall back to the first.
            if (-not $overall) { $overall = $data.Dashboard.Rows[0] }
        }

        $serverRecs = @($AllRecommendations | Where-Object { $_.Server -eq $display })
        $score = Get-CellValue -Row $overall -Column "Score"

        $scoreboard.Add([PSCustomObject]@{
            Server   = $display
            Grade    = [string](Get-CellValue -Row $overall -Column "Grade")
            Score    = $score
            SortKey  = if ($null -ne $score) { [int]$score } else { 999 }
            Critical = @($serverRecs | Where-Object { $_.Severity -eq "CRITICAL" }).Count
            Warning  = @($serverRecs | Where-Object { $_.Severity -eq "WARNING" }).Count
            Headline = [string](Get-CellValue -Row $overall -Column "Headline")
        })
    }

    if ($scoreboard.Count -gt 0) {
        [void]$report.AppendLine("## Fleet Scoreboard")
        [void]$report.AppendLine("")
        [void]$report.AppendLine("| Server | Grade | Score | CRITICAL | WARNING | Headline |")
        [void]$report.AppendLine("| --- | --- | --- | --- | --- | --- |")

        $ranked = $scoreboard | Sort-Object -Property @{ Expression = "SortKey" },
                                                      @{ Expression = "Critical"; Descending = $true },
                                                      @{ Expression = "Server" }
        foreach ($row in $ranked) {
            $headline = $row.Headline -replace '\|', '\|' -replace "`n", " "
            $scoreText = if ($null -ne $row.Score) { $row.Score } else { "N/A" }
            $gradeText = if ($row.Grade) { $row.Grade } else { "?" }
            [void]$report.AppendLine("| $($row.Server) | $gradeText | $scoreText | $($row.Critical) | $($row.Warning) | $headline |")
        }
        [void]$report.AppendLine("")
    }

    # Recommendations by severity.
    # Findings that are identical apart from the server they came from are
    # emitted once with a server list instead of repeated verbatim per server.
    foreach ($severity in @("CRITICAL", "WARNING", "INFO")) {
        $findings = @($AllRecommendations | Where-Object { $_.Severity -eq $severity })
        if ($findings.Count -eq 0) { continue }

        $groups = @($findings | Group-Object -Property { "$($_.Category)`u{241F}$($_.Finding)`u{241F}$($_.Recommendation)`u{241F}$($_.ExampleCall)" })

        [void]$report.AppendLine("## $severity Findings")
        [void]$report.AppendLine("")
        if ($groups.Count -lt $findings.Count) {
            [void]$report.AppendLine("*$($findings.Count) finding(s) across all servers, grouped into $($groups.Count) distinct issue(s).*")
            [void]$report.AppendLine("")
        }

        $ordered = $groups | Sort-Object -Property @{ Expression = { $_.Count }; Descending = $true }, Name
        foreach ($group in $ordered) {
            $first = $group.Group[0]
            $groupServers = @($group.Group | ForEach-Object { $_.Server } | Sort-Object -Unique)

            [void]$report.AppendLine("### [$($first.Category)] $($first.Finding)")
            [void]$report.AppendLine("")

            if ($groupServers.Count -eq 1) {
                [void]$report.AppendLine("**Server:** $($groupServers[0])")
            }
            else {
                [void]$report.AppendLine("**Servers ($($groupServers.Count)):** $($groupServers -join ', ')")
            }
            [void]$report.AppendLine("")

            $evidence = @($group.Group | Where-Object { $_.Evidence } | Select-Object -Property Server, Evidence)
            $distinctEvidence = @($evidence | ForEach-Object { $_.Evidence } | Sort-Object -Unique)
            if ($distinctEvidence.Count -eq 1) {
                [void]$report.AppendLine("**Evidence:** $($distinctEvidence[0])")
                [void]$report.AppendLine("")
            }
            elseif ($distinctEvidence.Count -gt 1) {
                [void]$report.AppendLine("**Evidence (per server):**")
                [void]$report.AppendLine("")
                $shown = 0
                foreach ($e in ($evidence | Sort-Object Server)) {
                    if ($shown -ge 20) {
                        [void]$report.AppendLine("- *... $($evidence.Count - 20) more server(s)*")
                        break
                    }
                    [void]$report.AppendLine("- **$($e.Server)**: $($e.Evidence)")
                    $shown++
                }
                [void]$report.AppendLine("")
            }

            if ($first.Recommendation) {
                [void]$report.AppendLine("**Recommendation:** $($first.Recommendation)")
                [void]$report.AppendLine("")
            }
            if ($first.ExampleCall) {
                [void]$report.AppendLine('```sql')
                [void]$report.AppendLine($first.ExampleCall)
                [void]$report.AppendLine('```')
                [void]$report.AppendLine("")
            }
        }
    }

    # Per-server details
    [void]$report.AppendLine("## Server Details")
    [void]$report.AppendLine("")

    $detailServers = @()
    if ($NoServerDetail) {
        [void]$report.AppendLine("*Omitted (-NoServerDetail) for $($AllResults.Keys.Count) instance(s). The same rows are in the CSV export, one file per result set.*")
        [void]$report.AppendLine("")
    }
    else {
        $detailServers = @($AllResults.Keys | Sort-Object)
    }

    foreach ($server in $detailServers) {
        $data = $AllResults[$server]

        [void]$report.AppendLine("### Server: $(Get-DisplayName $server)")
        [void]$report.AppendLine("")

        # Executive Dashboard (RS 1) - letter grades and health score
        if ($data.Dashboard -and $data.Dashboard.Rows.Count -gt 0) {
            [void]$report.AppendLine("#### Executive Dashboard")
            [void]$report.AppendLine("")
            [void]$report.AppendLine((ConvertTo-MarkdownTable -Table $data.Dashboard -MaxRows 20))
        }

        # Run Health
        if ($data.RunHealth -and $data.RunHealth.Rows.Count -gt 0) {
            [void]$report.AppendLine("#### Run Health Summary")
            [void]$report.AppendLine("")
            $rh = $data.RunHealth.Rows[0]
            [void]$report.AppendLine("| Metric | Value |")
            [void]$report.AppendLine("| --- | --- |")
            foreach ($col in $data.RunHealth.Columns) {
                $val = $rh[$col.ColumnName]
                $displayVal = if ($val -eq [DBNull]::Value) { "N/A" } else { $val.ToString().Replace("|", "\|").Replace("`n", " ") }
                [void]$report.AppendLine("| $($col.ColumnName) | $displayVal |")
            }
            [void]$report.AppendLine("")
        }

        # Remaining collected result sets. Row caps keep a 40-server report readable.
        $sections = @(
            @{ Key = "RunDetail";        Title = "Recent Runs";                     MaxRows = 5 },
            @{ Key = "TopTables";        Title = "Top Tables by Maintenance Cost";  MaxRows = 5 },
            @{ Key = "FailingStats";     Title = "Failing Statistics";              MaxRows = 5 },
            @{ Key = "LongRunning";      Title = "Long-Running Statistics";         MaxRows = 5 },
            @{ Key = "ParamHistory";     Title = "Parameter Change History";        MaxRows = 5 },
            @{ Key = "EfficacyTrend";    Title = "Query Store Efficacy Trend";      MaxRows = 8 },
            @{ Key = "EfficacyDetail";   Title = "Query Store Efficacy Detail";     MaxRows = 5 },
            @{ Key = "HighCpuPositions"; Title = "High-CPU Statistic Positions";    MaxRows = 5 },
            @{ Key = "QSCorrelation";    Title = "Query Store Performance Correlation"; MaxRows = 5 }
        )

        foreach ($section in $sections) {
            $table = $data[$section.Key]
            if ($table -and $table.Rows.Count -gt 0) {
                [void]$report.AppendLine("#### $($section.Title)")
                [void]$report.AppendLine("")
                [void]$report.AppendLine((ConvertTo-MarkdownTable -Table $table -MaxRows $section.MaxRows))
            }
        }
    }

    # Connection failures
    if ($ConnectionErrors -and $ConnectionErrors.Count -gt 0) {
        [void]$report.AppendLine("## Connection Failures")
        [void]$report.AppendLine("")
        foreach ($err in $ConnectionErrors) {
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
            # Counted from the list being serialized, not from script scope --
            # the obfuscated pass carries a different recommendation list.
            Critical = @($AllRecommendations | Where-Object { $_.Severity -eq "CRITICAL" }).Count
            Warning  = @($AllRecommendations | Where-Object { $_.Severity -eq "WARNING" }).Count
            Info     = @($AllRecommendations | Where-Object { $_.Severity -eq "INFO" }).Count
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

        # The SRV_xxxx tokens are minted here in PowerShell, not by the proc, so
        # they never appeared in the map the proc returns. Without these rows the
        # decode script could resolve every database and table in a finding but
        # not the instance it came from -- which is the first thing you need when
        # a consultant reports "SRV_1A2B3C4D looks bad".
        $allRows.Add([PSCustomObject]@{
            ServerName     = $server
            ObjectType     = "SERVER"
            OriginalName   = $server
            ObfuscatedName = (Get-DisplayName $server)
        })

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
# Per-result-set CSV export
# =============================================================================

# One CSV per result set, unioned across the fleet with a leading Server column.
# This is the shape a 15-75 instance fleet actually needs: the narrative report
# answers "which boxes do I look at first", but "show me every failing statistic
# on every instance, sorted by failure count" is a pivot table, not prose.
#
# Columns are unioned rather than taken from the first server -- a fleet running
# mixed sp_StatUpdate_Diag builds returns different column sets, and Export-Csv
# would otherwise silently drop every column the first server happened not to have.
function Export-ResultSetCsv {
    param(
        [hashtable]$AllResults,
        [System.Collections.Generic.List[PSObject]]$AllRecommendations,
        [string]$Directory
    )

    if (-not (Test-Path -LiteralPath $Directory)) {
        New-Item -ItemType Directory -Path $Directory -Force | Out-Null
    }

    $written = [System.Collections.Generic.List[string]]::new()

    # Recommendations come from the aggregated list, not the per-server DataTable,
    # because only the aggregated list carries the CROSS-SERVER findings.
    if ($AllRecommendations.Count -gt 0) {
        $recPath = Join-Path $Directory "Recommendations.csv"
        $AllRecommendations | Export-Csv -LiteralPath $recPath -NoTypeInformation -Encoding UTF8
        $written.Add($recPath)
    }

    $rsNames = @(
        "Dashboard", "RunHealth", "RunDetail", "TopTables", "FailingStats",
        "LongRunning", "ParamHistory", "EfficacyTrend", "EfficacyDetail",
        "HighCpuPositions", "QSCorrelation"
    )

    $sortedServers = @($AllResults.Keys | Sort-Object)

    foreach ($rsName in $rsNames) {
        $cols = [System.Collections.Generic.List[string]]::new()
        $seen = [System.Collections.Generic.HashSet[string]]::new()

        foreach ($server in $sortedServers) {
            $table = $AllResults[$server][$rsName]
            if (-not $table -or $table.Rows.Count -eq 0) { continue }
            foreach ($col in $table.Columns) {
                if ($seen.Add($col.ColumnName)) { $cols.Add($col.ColumnName) }
            }
        }
        if ($cols.Count -eq 0) { continue }

        $rows = [System.Collections.Generic.List[PSObject]]::new()
        foreach ($server in $sortedServers) {
            $table = $AllResults[$server][$rsName]
            if (-not $table -or $table.Rows.Count -eq 0) { continue }
            $display = Get-DisplayName $server

            foreach ($row in $table.Rows) {
                $ordered = [ordered]@{ Server = $display }
                foreach ($col in $cols) {
                    # A result set of its own with a Server column would silently
                    # overwrite the instance token; keep both.
                    $key = if ($col -eq "Server") { "Server_RS" } else { $col }
                    $val = $null
                    if ($table.Columns.Contains($col)) {
                        $val = $row[$col]
                        if ($val -eq [DBNull]::Value) { $val = $null }
                    }
                    $ordered[$key] = $val
                }
                $rows.Add([PSCustomObject]$ordered)
            }
        }

        $path = Join-Path $Directory "$rsName.csv"
        $rows | Export-Csv -LiteralPath $path -NoTypeInformation -Encoding UTF8
        $written.Add($path)
    }

    return $written
}

# =============================================================================
# Markdown -> HTML rendering
# =============================================================================

function Format-HtmlInline {
    param([string]$Text)

    $t = $Text -replace '&', '&amp;' -replace '<', '&lt;' -replace '>', '&gt;'
    $t = $t -replace '`([^`]+)`', '<code>$1</code>'
    $t = $t -replace '\*\*([^*]+)\*\*', '<strong>$1</strong>'
    $t = $t -replace '(?<!\*)\*([^*]+)\*(?!\*)', '<em>$1</em>'
    return $t
}

function ConvertTo-HtmlTable {
    param([string[]]$Lines)

    $rows = [System.Collections.Generic.List[string[]]]::new()
    foreach ($line in $Lines) {
        $trim = $line.Trim()
        if ($trim.StartsWith("|")) { $trim = $trim.Substring(1) }
        if ($trim.EndsWith("|")) { $trim = $trim.Substring(0, [Math]::Max(0, $trim.Length - 1)) }
        # Split on pipes that were not escaped by ConvertTo-MarkdownTable
        $cells = @([regex]::Split($trim, '(?<!\\)\|') | ForEach-Object { $_.Trim().Replace('\|', '|') })
        $rows.Add($cells)
    }
    if ($rows.Count -eq 0) { return "" }

    $sb = [System.Text.StringBuilder]::new()
    [void]$sb.AppendLine("<table>")
    [void]$sb.Append("<thead><tr>")
    foreach ($cell in $rows[0]) { [void]$sb.Append("<th>$(Format-HtmlInline $cell)</th>") }
    [void]$sb.AppendLine("</tr></thead>")

    # Row 1 is the markdown separator (---) when present
    $start = 1
    if ($rows.Count -gt 1 -and @($rows[1] | Where-Object { $_ -notmatch '^:?-{2,}:?$' }).Count -eq 0) { $start = 2 }

    if ($rows.Count -gt $start) {
        [void]$sb.AppendLine("<tbody>")
        for ($r = $start; $r -lt $rows.Count; $r++) {
            [void]$sb.Append("<tr>")
            foreach ($cell in $rows[$r]) { [void]$sb.Append("<td>$(Format-HtmlInline $cell)</td>") }
            [void]$sb.AppendLine("</tr>")
        }
        [void]$sb.AppendLine("</tbody>")
    }
    [void]$sb.AppendLine("</table>")
    return $sb.ToString()
}

function ConvertTo-HtmlBody {
    param([string]$Markdown)

    $out = [System.Text.StringBuilder]::new()
    $lines = @($Markdown -split "`r?`n")
    $i = 0

    while ($i -lt $lines.Count) {
        $line = $lines[$i]

        # Fenced code block
        if ($line -match '^\s*```') {
            $i++
            $code = [System.Collections.Generic.List[string]]::new()
            while ($i -lt $lines.Count -and $lines[$i] -notmatch '^\s*```') {
                $code.Add($lines[$i])
                $i++
            }
            $i++  # closing fence
            $escaped = ($code -join "`n") -replace '&', '&amp;' -replace '<', '&lt;' -replace '>', '&gt;'
            [void]$out.AppendLine("<pre><code>$escaped</code></pre>")
            continue
        }

        # Table (consume the whole contiguous block)
        if ($line -match '^\s*\|.*\|\s*$') {
            $tbl = [System.Collections.Generic.List[string]]::new()
            while ($i -lt $lines.Count -and $lines[$i] -match '^\s*\|.*\|\s*$') {
                $tbl.Add($lines[$i])
                $i++
            }
            [void]$out.AppendLine((ConvertTo-HtmlTable -Lines $tbl.ToArray()))
            continue
        }

        # Headings - most specific first, otherwise '### x' matches the h1 pattern
        if ($line -match '^######\s+(.*)$') { [void]$out.AppendLine("<h6>$(Format-HtmlInline $Matches[1])</h6>"); $i++; continue }
        if ($line -match '^#####\s+(.*)$')  { [void]$out.AppendLine("<h5>$(Format-HtmlInline $Matches[1])</h5>"); $i++; continue }
        if ($line -match '^####\s+(.*)$')   { [void]$out.AppendLine("<h4>$(Format-HtmlInline $Matches[1])</h4>"); $i++; continue }
        if ($line -match '^###\s+(.*)$')    { [void]$out.AppendLine("<h3>$(Format-HtmlInline $Matches[1])</h3>"); $i++; continue }
        if ($line -match '^##\s+(.*)$')     { [void]$out.AppendLine("<h2>$(Format-HtmlInline $Matches[1])</h2>"); $i++; continue }
        if ($line -match '^#\s+(.*)$')      { [void]$out.AppendLine("<h1>$(Format-HtmlInline $Matches[1])</h1>"); $i++; continue }

        # Horizontal rule
        if ($line -match '^\s*-{3,}\s*$') { [void]$out.AppendLine("<hr />"); $i++; continue }

        # Unordered list
        if ($line -match '^\s*-\s+(.*)$') {
            [void]$out.AppendLine("<ul>")
            while ($i -lt $lines.Count -and $lines[$i] -match '^\s*-\s+(.*)$') {
                [void]$out.AppendLine("<li>$(Format-HtmlInline $Matches[1])</li>")
                $i++
            }
            [void]$out.AppendLine("</ul>")
            continue
        }

        if ([string]::IsNullOrWhiteSpace($line)) { $i++; continue }

        [void]$out.AppendLine("<p>$(Format-HtmlInline $line)</p>")
        $i++
    }

    return $out.ToString()
}

function New-HtmlDocument {
    param([string]$Markdown, [string]$Title)

    $body = ConvertTo-HtmlBody -Markdown $Markdown

    return @"
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>$Title</title>
<style>
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; max-width: 1200px; margin: 0 auto; padding: 20px; background: #f5f5f5; }
    h1 { color: #1a1a2e; border-bottom: 3px solid #16213e; padding-bottom: 10px; }
    h2 { color: #16213e; margin-top: 30px; }
    h3 { color: #0f3460; }
    h4 { color: #0f3460; }
    table { border-collapse: collapse; width: 100%; margin: 10px 0; background: white; display: block; overflow-x: auto; }
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
$body
</body>
</html>
"@
}

# =============================================================================
# Build report content (Markdown used for Markdown and HTML formats)
# =============================================================================

# Skipped for -OutputFormat CSV: on a 75-instance fleet this builds a
# multi-megabyte string that nothing would consume.
$reportContent = if ($OutputFormat -eq "CSV") { $null }
                 else { Build-MarkdownReport -AllResults $allResults -AllRecommendations $allRecommendations -IsObfuscated $false -ConnectionErrors @($allErrors) }

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
            $safeReport = Build-MarkdownReport -AllResults $safeResults -AllRecommendations $obfRecommendations -IsObfuscated $true -ConnectionErrors @($allErrors)
            $safeReport | Out-File -FilePath $safePath -Encoding UTF8
            Write-Host "  SAFE_TO_SHARE: $safePath" -ForegroundColor Green
        }
        "HTML" {
            # CONFIDENTIAL
            $confPath = Join-Path $OutputPath "${baseFileName}_CONFIDENTIAL.html"
            $html = New-HtmlDocument -Markdown $reportContent -Title "sp_StatUpdate Diagnostic Report (CONFIDENTIAL)"
            $html | Out-File -FilePath $confPath -Encoding UTF8
            Write-Host "  CONFIDENTIAL: $confPath" -ForegroundColor Yellow

            # SAFE_TO_SHARE
            $safePath = Join-Path $OutputPath "${baseFileName}_SAFE_TO_SHARE.html"
            $safeReport = Build-MarkdownReport -AllResults $safeResults -AllRecommendations $obfRecommendations -IsObfuscated $true -ConnectionErrors @($allErrors)
            $safeHtml = New-HtmlDocument -Markdown $safeReport -Title "sp_StatUpdate Diagnostic Report (Safe to Share)"
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
        "CSV" {
            # No narrative report; the CSV export below is the whole output.
        }
    }

    # DECODE SQL (always generated in obfuscate mode)
    $decodePath = Join-Path $OutputPath "${baseFileName}_CONFIDENTIAL_DECODE.sql"
    $decodeSql = New-DecodeSqlScript -AllResults $allResults
    $decodeSql | Out-File -FilePath $decodePath -Encoding UTF8
    Write-Host "  DECODE:        $decodePath" -ForegroundColor Yellow

    # Per-result-set CSV export, both sides of the confidential/shareable split.
    if (-not $NoCsv) {
        $confCsvDir = Join-Path $OutputPath "${baseFileName}_CONFIDENTIAL_csv"
        $confCsvFiles = Export-ResultSetCsv -AllResults $allResults -AllRecommendations $allRecommendations -Directory $confCsvDir
        Write-Host "  CSV (conf):    $confCsvDir ($($confCsvFiles.Count) files)" -ForegroundColor Yellow

        $safeCsvDir = Join-Path $OutputPath "${baseFileName}_SAFE_TO_SHARE_csv"
        $safeCsvFiles = Export-ResultSetCsv -AllResults $safeResults -AllRecommendations $obfRecommendations -Directory $safeCsvDir
        Write-Host "  CSV (safe):    $safeCsvDir ($($safeCsvFiles.Count) files)" -ForegroundColor Green
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

            $html = New-HtmlDocument -Markdown $reportContent -Title "sp_StatUpdate Diagnostic Report"
            $html | Out-File -FilePath $filePath -Encoding UTF8
            Write-Host "  Report: $filePath" -ForegroundColor Green
        }
        "JSON" {
            $filePath = Join-Path $OutputPath "sp_StatUpdate_Diag_${timestamp}.json"
            $jsonContent = Build-JsonOutput -AllResults $allResults -AllRecommendations $allRecommendations -StripObfuscationMap $false
            $jsonContent | Out-File -FilePath $filePath -Encoding UTF8
            Write-Host "  Report: $filePath" -ForegroundColor Green
        }
        "CSV" {
            # No narrative report; the CSV export below is the whole output.
        }
    }

    # Per-result-set CSV export
    if (-not $NoCsv) {
        $csvDir = Join-Path $OutputPath "sp_StatUpdate_Diag_${timestamp}_csv"
        $csvFiles = Export-ResultSetCsv -AllResults $allResults -AllRecommendations $allRecommendations -Directory $csvDir
        Write-Host "  CSV:    $csvDir ($($csvFiles.Count) files)" -ForegroundColor Green
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
# The CSV export writes into a subfolder, so this walks one level down rather
# than calling .Length on a DirectoryInfo (which has no such property and would
# silently report every folder as 0 KB).
$outputFiles = [System.Collections.Generic.List[string]]::new()
foreach ($item in @(Get-ChildItem -LiteralPath $OutputPath -Filter "sp_StatUpdate_Diag*${timestamp}*")) {
    if ($item.PSIsContainer) {
        $inner = @(Get-ChildItem -LiteralPath $item.FullName -File)
        $innerKb = [math]::Round((($inner | Measure-Object -Property Length -Sum).Sum) / 1KB, 1)
        Write-Host "  $($item.Name)$([IO.Path]::DirectorySeparatorChar) ($($inner.Count) files, $innerKb KB)" -ForegroundColor Cyan
        foreach ($f in $inner) { $outputFiles.Add($f.FullName) }
    }
    else {
        Write-Host "  $($item.Name) ($([math]::Round($item.Length/1KB, 1)) KB)" -ForegroundColor Cyan
        $outputFiles.Add($item.FullName)
    }
}
Write-Host ""

if ($PassThru) {
    [PSCustomObject]@{
        GeneratedAt     = Get-Date
        DurationSeconds = [math]::Round($fleetSw.Elapsed.TotalSeconds, 1)
        Servers         = $Servers
        Completed       = $completed
        Failed          = $failed
        ServerStatus    = $serverStatus.ToArray()
        Failures        = @($allErrors)
        Critical        = $criticalCount
        Warning         = $warningCount
        Info            = $infoCount
        Recommendations = $allRecommendations.ToArray()
        CrossServer     = $crossServerFindings.ToArray()
        Files           = $outputFiles.ToArray()
    }
}
