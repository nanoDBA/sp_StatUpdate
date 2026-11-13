Set-StrictMode -Version Latest

function Write-ReleaseRunList {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$Path,
        [Parameter(Mandatory)][string]$Commit,
        [Parameter(Mandatory)][ValidateSet('2019','2022','2025')][string]$SqlVersion,
        [Parameter(Mandatory)][string]$SqlBuild,
        [Parameter(Mandatory)][bool]$ServerAliveAtEnd,
        [Parameter(Mandatory)][string]$Suite,
        [Parameter(Mandatory)][int]$ExpectedCount,
        [Parameter(Mandatory)][object[]]$Checks,
        [Parameter(Mandatory)][datetime]$StartedAt,
        [Parameter(Mandatory)][datetime]$EndedAt,
        [Parameter(Mandatory)][long]$DurationMs
    )
    if ($Commit -notmatch '^[0-9a-fA-F]{40}$') { throw 'Commit must be a full 40-character hash.' }
    if ([string]::IsNullOrWhiteSpace($SqlBuild)) { throw 'SQL build is required.' }
    if ($ExpectedCount -lt 0 -or $Checks.Count -ne $ExpectedCount) {
        throw "Expected $ExpectedCount checks; received $($Checks.Count)."
    }
    foreach ($check in $Checks) {
        if ($check.result -notin @('PASS','FAIL','NOT_APPLICABLE','SKIPPED')) {
            throw "Invalid result for check $($check.check)."
        }
        if ($check.result -ne 'PASS' -and [string]::IsNullOrWhiteSpace([string]$check.reason)) {
            throw "Non-pass check $($check.check) needs a reason."
        }
        if ($check.result -eq 'SKIPPED' -and [string]::IsNullOrWhiteSpace([string]$check.issue)) {
            throw "Skipped check $($check.check) needs an issue id."
        }
        if ($check.duration_ms -lt 0) { throw "Check $($check.check) has a negative duration." }
    }
    $suiteStarted = $StartedAt.ToUniversalTime().ToString('o')
    $run = [ordered]@{
        run_id = [guid]::NewGuid().ToString()
        commit = $Commit.ToLowerInvariant()
        sql_version = $SqlVersion
        sql_build = $SqlBuild
        started_at = $suiteStarted
        ended_at = $EndedAt.ToUniversalTime().ToString('o')
        duration_ms = $DurationMs
        server_alive_at_end = $ServerAliveAtEnd
        suites = @([ordered]@{
            suite = $Suite
            expected_count = $ExpectedCount
            started_at = $suiteStarted
            duration_ms = $DurationMs
            checks = @($Checks)
        })
    }
    $parent = Split-Path -Parent $Path
    [void][IO.Directory]::CreateDirectory($parent)
    $temporary = "$Path.$([guid]::NewGuid().ToString('N')).tmp"
    $json = $run | ConvertTo-Json -Depth 12
    [IO.File]::WriteAllText($temporary, $json + "`n", [Text.UTF8Encoding]::new($false))
    [IO.File]::Move($temporary, $Path, $true)
    return $run.run_id
}

Export-ModuleMember -Function Write-ReleaseRunList
