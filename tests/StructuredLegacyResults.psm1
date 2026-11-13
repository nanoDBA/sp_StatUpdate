Set-StrictMode -Version Latest

function Get-StructuredSqlStatus {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$Server,
        [Parameter(Mandatory)][int]$Port,
        [Parameter(Mandatory)][string]$Password,
        [Parameter(Mandatory)][int]$ExpectedMajor
    )
    $connection = [System.Data.SqlClient.SqlConnection]::new(
        "Server=$Server,$Port;Database=master;User Id=sa;Password=$Password;Encrypt=false;TrustServerCertificate=true;Pooling=false;Connection Timeout=10")
    try {
        $connection.Open()
        $command = $connection.CreateCommand()
        $command.CommandText = "SELECT CONVERT(int, SERVERPROPERTY(N'ProductMajorVersion')) AS MajorVer, CONVERT(nvarchar(128), SERVERPROPERTY(N'ProductVersion')) AS ProductVersion;"
        $reader = $command.ExecuteReader()
        if (-not $reader.Read()) { throw 'SQL Server status query returned no row.' }
        $major = [int]$reader['MajorVer']
        $build = [string]$reader['ProductVersion']
        $reader.Dispose()
        return [pscustomobject]@{ Alive = ($major -eq $ExpectedMajor); Major = $major; Build = $build }
    } catch {
        return [pscustomobject]@{ Alive = $false; Major = 0; Build = '' }
    } finally {
        $connection.Dispose()
    }
}

function Write-StructuredLegacyResults {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$Path,
        [Parameter(Mandatory)][string]$Commit,
        [Parameter(Mandatory)][ValidateSet('2019','2022','2025')][string]$SqlVersion,
        [Parameter(Mandatory)][string]$SqlBuild,
        [Parameter(Mandatory)][bool]$ServerAliveAtEnd,
        [Parameter(Mandatory)][string]$Suite,
        [Parameter(Mandatory)][int]$ExpectedCount,
        [Parameter(Mandatory)][object[]]$Results,
        [Parameter(Mandatory)][datetime]$StartedAt,
        [Parameter(Mandatory)][datetime]$EndedAt
    )
    if ($Results.Count -ne $ExpectedCount) {
        throw "$Suite expected $ExpectedCount checks; received $($Results.Count)."
    }
    $checks = foreach ($result in $Results) {
        $name = if ($result.PSObject.Properties['Test']) { [string]$result.Test } else { [string]$result.Check }
        $status = ([string]$result.Result).ToUpperInvariant()
        $detail = if ($result.PSObject.Properties['Detail']) { [string]$result.Detail } else { '' }
        if ($status -eq 'SKIP') {
            $status = 'FAIL'
            $detail = "Legacy skip means the check did not prove behavior. $detail".Trim()
        }
        if ($status -eq 'INFO') {
            $status = 'FAIL'
            $detail = "Informational output is not a behavioral proof. $detail".Trim()
        }
        if ($status -notin @('PASS','FAIL','NOT_APPLICABLE','SKIPPED')) {
            throw "$Suite emitted unsupported result '$status' for '$name'."
        }
        if ($status -eq 'SKIPPED' -and -not $result.PSObject.Properties['Issue']) {
            throw "$Suite skipped '$name' without a linked issue."
        }
        if ($status -ne 'PASS' -and [string]::IsNullOrWhiteSpace($detail)) {
            $detail = "No proof detail was provided for $status."
        }
        $duration = if ($result.PSObject.Properties['DurationMs']) { [long]$result.DurationMs } else { 0L }
        $check = [ordered]@{ check = $name; result = $status; duration_ms = [Math]::Max(0L, $duration) }
        if ($status -ne 'PASS') { $check.reason = $detail }
        if ($status -eq 'SKIPPED') { $check.issue = [string]$result.Issue }
        [pscustomobject]$check
    }
    Import-Module (Join-Path $PSScriptRoot 'ReleaseResults.psm1') -Force
    $durationMs = [long]([Math]::Max(0, ($EndedAt - $StartedAt).TotalMilliseconds))
    [void](Write-ReleaseRunList -Path $Path -Commit $Commit -SqlVersion $SqlVersion -SqlBuild $SqlBuild `
        -ServerAliveAtEnd $ServerAliveAtEnd -Suite $Suite -ExpectedCount $ExpectedCount `
        -Checks @($checks) -StartedAt $StartedAt -EndedAt $EndedAt -DurationMs $durationMs)
}

Export-ModuleMember -Function Get-StructuredSqlStatus, Write-StructuredLegacyResults
