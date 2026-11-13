<#
.SYNOPSIS
    Live end-to-end test for Invoke-StatUpdateDiag.ps1 against a real SQL Server.

.DESCRIPTION
    The wrapper's reporting half is covered by a synthetic-DataTable harness, but
    the SQL-facing half -- the generated EXECUTE statement, the two-call
    obfuscation architecture, and the shape of the DataSet that comes back -- can
    only be proven against a real sp_StatUpdate_Diag.

    Covers the 2026-07-29 wrapper changes:
      - -Obfuscate now hard-fails without -ObfuscationSeed
      - @CommandLogDatabase is actually sent (the old guard made it dead code)
      - decode script carries SERVER rows so SRV_ tokens resolve
      - per-result-set CSV export
      - -NoServerDetail
      - single quotes in seed / database name no longer break the batch

    Assumes the target already has sp_StatUpdate_Diag deployed ; test containers persist state; pass -Deploy to (re)deploy it first.

.PARAMETER Server
    Instance to test, e.g. "127.0.0.1,1432" (2022), 1434 (2019), 1431 (2025).

.EXAMPLE
    .\tests\Test-DiagWrapperLive.ps1 -Server "127.0.0.1,1432"

.EXAMPLE
    .\tests\Test-DiagWrapperLive.ps1 -Server "127.0.0.1,1434" -Deploy -KeepOutput
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [string]$Server,

    [string]$SqlUser  = "sa",
    [string]$Password = $env:SPSTATUPDATE_TEST_SQL_PASSWORD,

    [string]$CommandLogDatabase = "master",

    # Deploy sp_StatUpdate_Diag.sql before testing
    [switch]$Deploy,

    # Leave the generated reports on disk for inspection
    [switch]$KeepOutput
)

$ErrorActionPreference = "Stop"
if (-not $Password) { throw 'Set SPSTATUPDATE_TEST_SQL_PASSWORD or pass -Password.' }

$repoRoot  = Split-Path -Parent $PSScriptRoot
$wrapper   = Join-Path $repoRoot "Invoke-StatUpdateDiag.ps1"
$diagSql   = Join-Path $repoRoot "sp_StatUpdate_Diag.sql"
$outRoot   = Join-Path ([IO.Path]::GetTempPath()) "diagwrapper_$(Get-Date -Format 'yyyyMMdd_HHmmss')"

if (-not (Test-Path -LiteralPath $wrapper)) { throw "wrapper not found: $wrapper" }
New-Item -ItemType Directory -Path $outRoot -Force | Out-Null

$securePw = ConvertTo-SecureString $Password -AsPlainText -Force
$cred     = [PSCredential]::new($SqlUser, $securePw)

$connStr = "Server=$Server;Database=$CommandLogDatabase;User Id=$SqlUser;Password=$Password;" +
           "Encrypt=false;TrustServerCertificate=true;Connection Timeout=15"

function Invoke-Sql {
    param([string]$Sql, [int]$Timeout = 300)
    $c = [System.Data.SqlClient.SqlConnection]::new($connStr)
    $c.Open()
    try {
        $cmd = $c.CreateCommand()
        $cmd.CommandTimeout = $Timeout
        $cmd.CommandText = $Sql
        $a = [System.Data.SqlClient.SqlDataAdapter]::new($cmd)
        $d = [System.Data.DataSet]::new()
        [void]$a.Fill($d)
        return $d
    }
    finally { $c.Close(); $c.Dispose() }
}

$script:pass = 0
$script:fail = 0
$script:failNames = [System.Collections.Generic.List[string]]::new()

function Check {
    param([string]$Name, [scriptblock]$Test)
    try {
        if (& $Test) { Write-Host "  PASS  $Name" -ForegroundColor Green; $script:pass++ }
        else { Write-Host "  FAIL  $Name" -ForegroundColor Red; $script:fail++; $script:failNames.Add($Name) }
    }
    catch {
        Write-Host "  FAIL  $Name -- $($_.Exception.Message)" -ForegroundColor Red
        $script:fail++; $script:failNames.Add($Name)
    }
}

Write-Host "===============================================================" -ForegroundColor Cyan
Write-Host " Invoke-StatUpdateDiag.ps1 -- live test" -ForegroundColor Cyan
Write-Host "===============================================================" -ForegroundColor Cyan
Write-Host " Server:  $Server"
Write-Host " Output:  $outRoot"
Write-Host ""

# ---------------------------------------------------------------- preflight --
Write-Host "--- Preflight ---" -ForegroundColor Cyan

$ver = (Invoke-Sql "SELECT v = @@VERSION;").Tables[0].Rows[0][0]
Write-Host "  $(($ver -split "`n")[0].Trim())"

if ($Deploy) {
    Write-Host "  Deploying sp_StatUpdate_Diag.sql ..."
    $batches = (Get-Content -LiteralPath $diagSql -Raw) -split '(?im)^\s*GO\s*$'
    $n = 0
    foreach ($b in $batches) {
        if ([string]::IsNullOrWhiteSpace($b)) { continue }
        Invoke-Sql $b | Out-Null
        $n++
    }
    Write-Host "  Deployed ($n batches)."
}

$procExists = (Invoke-Sql "SELECT c = COUNT(*) FROM sys.procedures WHERE name = 'sp_StatUpdate_Diag';").Tables[0].Rows[0][0]
Check "sp_StatUpdate_Diag is deployed" { $procExists -gt 0 }
if ($procExists -eq 0) { throw "sp_StatUpdate_Diag not found -- rerun with -Deploy" }

$logRows = (Invoke-Sql "
    SELECT c = CASE WHEN OBJECT_ID('dbo.CommandLog') IS NULL THEN -1
                    ELSE (SELECT COUNT(*) FROM dbo.CommandLog WHERE CommandType LIKE 'SP_STATUPDATE%') END;").Tables[0].Rows[0][0]
if ($logRows -lt 0) { Write-Warning "dbo.CommandLog missing in [$CommandLogDatabase] -- the proc will return empty result sets." }
elseif ($logRows -eq 0) { Write-Warning "CommandLog has no sp_StatUpdate rows -- run tests/Seed-DiagTestData.ps1 first." }
else { Write-Host "  CommandLog: $logRows sp_StatUpdate rows" }

# A few assertions need the proc to have found something to report on. With an
# empty CommandLog the proc correctly writes no history rows and no obfuscation
# map, and asserting on those would report a data gap as a wrapper failure.
$hasData = $logRows -gt 0
if (-not $hasData) { Write-Warning "  data-dependent assertions will be skipped" }

# ------------------------------------------------------- 1: plain + CSV run --
Write-Host "`n--- 1: plain run (ExpertMode 1, Markdown + CSV) ---" -ForegroundColor Cyan

$out1 = Join-Path $outRoot "plain"
$run1 = & $wrapper -Servers $Server -Credential $cred -CommandLogDatabase $CommandLogDatabase `
                   -OutputPath $out1 -ExpertMode 1 -DaysBack 3650 -PassThru -RetryCount 0

Check "run completed"                 { $run1.Completed -eq 1 -and $run1.Failed -eq 0 }
Check "markdown report written"       { @(Get-ChildItem -LiteralPath $out1 -Filter '*.md').Count -eq 1 }
Check "csv folder written"            { @(Get-ChildItem -LiteralPath $out1 -Directory -Filter '*_csv').Count -eq 1 }

$csvDir1 = @(Get-ChildItem -LiteralPath $out1 -Directory -Filter '*_csv')[0].FullName
$csvFiles = @(Get-ChildItem -LiteralPath $csvDir1 -Filter '*.csv')
Check "csv export produced files"     { $csvFiles.Count -ge 1 }
Check "PassThru.Files lists csv files" { @($run1.Files | Where-Object { $_ -like '*_csv*' }).Count -eq $csvFiles.Count }

$dashCsv = Join-Path $csvDir1 'Dashboard.csv'
Check "Dashboard.csv exists"          { Test-Path -LiteralPath $dashCsv }
if (Test-Path -LiteralPath $dashCsv) {
    $dash = @(Import-Csv -LiteralPath $dashCsv)
    Check "Dashboard.csv has Server first" { $dash[0].PSObject.Properties.Name[0] -eq 'Server' }
    Check "Dashboard.csv Server = real name (no -Obfuscate)" { $dash[0].Server -eq $Server }
    Check "Dashboard.csv has OVERALL row"  { @($dash | Where-Object { $_.Category -eq 'OVERALL' }).Count -eq 1 }
    Check "Dashboard.csv has a grade"      { $dash[0].Grade -match '^[A-F-]$' }
}

# -------------------------------------------- 1b: parameters actually arrive --
# Proving a parameter reached the server by reading the batch text back out of
# sys.dm_exec_cached_plans does not work: a batch whose only statement is an EXEC
# of a stored procedure gets no cached plan of its own, so the lookup returns
# either nothing or (worse) the procedure body, which contains the same strings.
# Test the observable side effect instead -- that proves transmission AND that
# the proc honoured it, which the text never did.
Write-Host "`n--- 1b: @SkipHistory reaches the server (two-sided) ---" -ForegroundColor Cyan

# Destructive but self-healing: dbo.StatUpdateDiagHistory is a diagnostic
# byproduct the proc recreates on the next non-skipped run.
Invoke-Sql "IF OBJECT_ID('dbo.StatUpdateDiagHistory') IS NOT NULL DROP TABLE dbo.StatUpdateDiagHistory;" | Out-Null

# Two statements, not one: SQL Server binds the whole batch at compile time, so
# a COUNT(*) subquery over a missing table fails to compile no matter what CASE
# or ISNULL guards it.
function Get-TableState {
    param([string]$Table)
    $present = [int](Invoke-Sql "SELECT p = CASE WHEN OBJECT_ID('$Table') IS NULL THEN 0 ELSE 1 END;").Tables[0].Rows[0]['p']
    $rows = 0
    if ($present -eq 1) {
        $rows = [int](Invoke-Sql "SELECT c = COUNT(*) FROM $Table;").Tables[0].Rows[0]['c']
    }
    return [PSCustomObject]@{ Present = $present; Rows = $rows }
}

function Get-HistoryState { Get-TableState -Table 'dbo.StatUpdateDiagHistory' }

Check "history absent after drop" { (Get-HistoryState).Present -eq 0 }

$outSkip = Join-Path $outRoot "skiphistory"
& $wrapper -Servers $Server -Credential $cred -CommandLogDatabase $CommandLogDatabase `
           -OutputPath $outSkip -ExpertMode 1 -DaysBack 3650 -SkipHistory -NoCsv -RetryCount 0 | Out-Null

Check "-SkipHistory left no table behind" { (Get-HistoryState).Present -eq 0 }

$outHist = Join-Path $outRoot "withhistory"
& $wrapper -Servers $Server -Credential $cred -CommandLogDatabase $CommandLogDatabase `
           -OutputPath $outHist -ExpertMode 1 -DaysBack 3650 -NoCsv -RetryCount 0 | Out-Null

$histAfter = Get-HistoryState
Check "without -SkipHistory the table is created" { $histAfter.Present -eq 1 }
if ($hasData) { Check "without -SkipHistory rows are persisted" { $histAfter.Rows -gt 0 } }
else { Write-Host "  SKIP  without -SkipHistory rows are persisted (empty CommandLog)" -ForegroundColor DarkGray }

# ------------------------------------- 1c: @CommandLogDatabase reaches the proc --
Write-Host "`n--- 1c: @CommandLogDatabase reaches the server ---" -ForegroundColor Cyan

# Pointed at a database that exists but has no CommandLog, the proc raises
# "CommandLog table not found at [<db>]..." -- the database name in that message
# is the proc's own resolution of @CommandLogDatabase.
$noLogDb = (Invoke-Sql "
    SELECT TOP (1) name
    FROM sys.databases
    WHERE state = 0
      AND HAS_DBACCESS(name) = 1
      AND name NOT IN ('tempdb','model')
      AND OBJECT_ID(QUOTENAME(name) + '.dbo.CommandLog') IS NULL
    ORDER BY name;").Tables[0]

if ($noLogDb.Rows.Count -eq 0) {
    Write-Warning "  every accessible database has a CommandLog -- skipped"
}
else {
    $db = $noLogDb.Rows[0][0].ToString()
    $err = $null
    try {
        & $wrapper -Servers $Server -Credential $cred -CommandLogDatabase $db `
                   -OutputPath (Join-Path $outRoot "nolog") -NoCsv -RetryCount 0 | Out-Null
    }
    catch { $err = $_.Exception.Message }

    Check "missing CommandLog surfaces as a failure"   { $err -and $err -match 'No servers returned data' }
    Check "error names the requested database ($db)"   { $err -match [regex]::Escape($db) }
    Check "error is the proc's own CommandLog message" { $err -match 'CommandLog table not found' }
}

# ------------------------------------------------- 2: unseeded obfuscate = X --
Write-Host "`n--- 2: -Obfuscate without a seed must refuse ---" -ForegroundColor Cyan

$threw = $null
try {
    & $wrapper -Servers $Server -Credential $cred -CommandLogDatabase $CommandLogDatabase `
               -OutputPath (Join-Path $outRoot "unseeded") -Obfuscate -RetryCount 0 | Out-Null
}
catch { $threw = $_.Exception.Message }

Check "throws without -ObfuscationSeed" { $threw -and $threw -match 'requires -ObfuscationSeed' }
Check "refuses before touching the server" { -not (Test-Path -LiteralPath (Join-Path $outRoot "unseeded/*SAFE_TO_SHARE*")) }

# ---------------------------------------------------- 3: seeded obfuscation --
Write-Host "`n--- 3: seeded obfuscation (dual output + decode) ---" -ForegroundColor Cyan

# An apostrophe in the seed used to produce a syntactically broken batch.
$seed    = "live-test's-seed"
$mapTable = "dbo.DiagObfMapTest"
$out3    = Join-Path $outRoot "obf"

Invoke-Sql "IF OBJECT_ID('$mapTable') IS NOT NULL DROP TABLE $mapTable;" | Out-Null

$run3 = & $wrapper -Servers $Server -Credential $cred -CommandLogDatabase $CommandLogDatabase `
                   -OutputPath $out3 -Obfuscate -ObfuscationSeed $seed -ObfuscationMapTable $mapTable `
                   -DaysBack 3650 -PassThru -RetryCount 0

Check "obfuscated run completed"      { $run3.Completed -eq 1 -and $run3.Failed -eq 0 }
Check "apostrophe in seed survived"   { $run3.Failed -eq 0 }

# Second observable proof of parameter transmission: the proc only creates this
# table when it is told to, so its existence means @ObfuscationMapTable arrived.
if ($hasData) {
    $mapState = Get-TableState -Table $mapTable
    Check "@ObfuscationMapTable created on the server" { $mapState.Present -eq 1 }
    Check "@ObfuscationMapTable was populated"         { $mapState.Rows -gt 0 }
}
else {
    Write-Host "  SKIP  @ObfuscationMapTable checks (empty CommandLog -- nothing to map)" -ForegroundColor DarkGray
}

$confMd   = @(Get-ChildItem -LiteralPath $out3 -Filter '*_CONFIDENTIAL.md')
$safeMd   = @(Get-ChildItem -LiteralPath $out3 -Filter '*_SAFE_TO_SHARE.md')
$decode   = @(Get-ChildItem -LiteralPath $out3 -Filter '*_DECODE.sql')
$confCsvD = @(Get-ChildItem -LiteralPath $out3 -Directory -Filter '*_CONFIDENTIAL_csv')
$safeCsvD = @(Get-ChildItem -LiteralPath $out3 -Directory -Filter '*_SAFE_TO_SHARE_csv')

Check "CONFIDENTIAL md written"       { $confMd.Count -eq 1 }
Check "SAFE_TO_SHARE md written"      { $safeMd.Count -eq 1 }
Check "decode sql written"            { $decode.Count -eq 1 }
Check "CONFIDENTIAL csv folder"       { $confCsvD.Count -eq 1 }
Check "SAFE_TO_SHARE csv folder"      { $safeCsvD.Count -eq 1 }

$safeText   = Get-Content -LiteralPath $safeMd[0].FullName -Raw
$decodeText = Get-Content -LiteralPath $decode[0].FullName -Raw

Check "safe report has no real instance name" { $safeText -notmatch [regex]::Escape($Server) }
Check "safe report uses SRV_ tokens"          { $safeText -match 'SRV_[0-9A-F]{8}' }

# The gap this run is meant to close: SRV_ tokens were minted in PowerShell and
# never appeared in the decode map, so a finding could not be traced to a box.
Check "decode has a SERVER row"               { $decodeText -match "N'SERVER'" }
Check "decode maps the real instance name"    { $decodeText -match "N'SERVER', N'$([regex]::Escape($Server))'" }

$tokensInReport = @([regex]::Matches($safeText, 'SRV_[0-9A-F]{8}') | ForEach-Object { $_.Value } | Sort-Object -Unique)
Check "every SRV_ token in the report decodes" {
    $tokensInReport.Count -gt 0 -and
    @($tokensInReport | Where-Object { $decodeText -notmatch [regex]::Escape($_) }).Count -eq 0
}

if ($safeCsvD.Count -eq 1) {
    $safeDash = Join-Path $safeCsvD[0].FullName 'Dashboard.csv'
    if (Test-Path -LiteralPath $safeDash) {
        $sd = @(Import-Csv -LiteralPath $safeDash)
        Check "safe CSV Server column is tokenised" { @($sd | Where-Object { $_.Server -notmatch '^SRV_' }).Count -eq 0 }
    }
}

# Seed stability: same seed -> same tokens on a second run.
$out3b = Join-Path $outRoot "obf2"
& $wrapper -Servers $Server -Credential $cred -CommandLogDatabase $CommandLogDatabase `
           -OutputPath $out3b -Obfuscate -ObfuscationSeed $seed -DaysBack 3650 -RetryCount 0 -NoCsv | Out-Null
$safeMd2 = @(Get-ChildItem -LiteralPath $out3b -Filter '*_SAFE_TO_SHARE.md')
Check "same seed -> same SRV_ token across runs" {
    $t2 = @([regex]::Matches((Get-Content -LiteralPath $safeMd2[0].FullName -Raw), 'SRV_[0-9A-F]{8}') |
            ForEach-Object { $_.Value } | Sort-Object -Unique)
    ($t2 -join ',') -eq ($tokensInReport -join ',')
}

# ------------------------------------------------------- 4: -NoServerDetail --
Write-Host "`n--- 4: -NoServerDetail ---" -ForegroundColor Cyan

$out4 = Join-Path $outRoot "nodetail"
& $wrapper -Servers $Server -Credential $cred -CommandLogDatabase $CommandLogDatabase `
           -OutputPath $out4 -ExpertMode 1 -DaysBack 3650 -NoServerDetail -RetryCount 0 | Out-Null

$md4 = Get-Content -LiteralPath (@(Get-ChildItem -LiteralPath $out4 -Filter '*.md')[0].FullName) -Raw
$md1 = Get-Content -LiteralPath (@(Get-ChildItem -LiteralPath $out1 -Filter '*.md')[0].FullName) -Raw

Check "per-server tables omitted"     { $md4 -notmatch '#### Executive Dashboard' }
Check "omission is stated"            { $md4 -match 'Omitted \(-NoServerDetail\)' }
Check "scoreboard retained"           { $md4 -match '## Fleet Scoreboard' }
Check "report is smaller than full"   { $md4.Length -lt $md1.Length }
Check "CSV still written"             { @(Get-ChildItem -LiteralPath $out4 -Directory -Filter '*_csv').Count -eq 1 }

# ------------------------------------------------------ 5: -OutputFormat CSV --
Write-Host "`n--- 5: -OutputFormat CSV ---" -ForegroundColor Cyan

$out5 = Join-Path $outRoot "csvonly"
& $wrapper -Servers $Server -Credential $cred -CommandLogDatabase $CommandLogDatabase `
           -OutputPath $out5 -ExpertMode 1 -DaysBack 3650 -OutputFormat CSV -RetryCount 0 | Out-Null

Check "no narrative report"           { @(Get-ChildItem -LiteralPath $out5 -File -Filter '*.md').Count -eq 0 }
Check "csv folder present"            { @(Get-ChildItem -LiteralPath $out5 -Directory -Filter '*_csv').Count -eq 1 }
Check "csv folder is non-empty" {
    @(Get-ChildItem -LiteralPath (@(Get-ChildItem -LiteralPath $out5 -Directory -Filter '*_csv')[0].FullName) -Filter '*.csv').Count -ge 1
}

# --------------------------------------------------------------- 6: cleanup --
Write-Host "`n===============================================================" -ForegroundColor Cyan
Write-Host " PASS: $script:pass   FAIL: $script:fail" -ForegroundColor $(if ($script:fail -eq 0) { 'Green' } else { 'Red' })
if ($script:fail -gt 0) {
    Write-Host " Failed:" -ForegroundColor Red
    $script:failNames | ForEach-Object { Write-Host "   - $_" -ForegroundColor Red }
}
Write-Host "===============================================================" -ForegroundColor Cyan

if ($KeepOutput) { Write-Host "`nOutput kept at: $outRoot" }
else { Remove-Item -LiteralPath $outRoot -Recurse -Force; Write-Host "`nOutput removed (-KeepOutput to keep)." }

if ($script:fail -gt 0) { exit 1 }
