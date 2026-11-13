# Run the tests

The release gate checks `sp_StatUpdate` against disposable SQL Server databases. It runs the same declared suites on SQL Server 2019, 2022, and 2025. Each suite gets a clean base database. The Docker provider binds SQL Server to this computer's loopback address and removes only the container it created.

## What must pass for a release?

Every declared suite and check must be present and pass on all three SQL Server versions. The gate also checks that the procedure's declared check counts have not silently changed. Coverage gaps are reported, but they do not fail the gate.

The release process has one additional check that needs the maintainer's Windows machine: SSMS, Microsoft's SQL Server Management Studio, must reject a known-bad script and accept the current diagnostic script. Public CI does not run that desktop-application check. If SSMS is unavailable, the release check fails; it is not recorded as “not applicable.”

## Run the full gate

You need Git, PowerShell 7, Python 3.12 or later, and Docker running with at least 3 GB of memory and two CPU cores available. The SQL Server process is limited to 2 GB inside a 3 GB container. The first run downloads the SQL Server image.

From a PowerShell prompt in the folder where you want the clone and results, run:

```powershell
git clone https://github.com/nanoDBA/sp_StatUpdate.git
pwsh -NoProfile -File .\sp_StatUpdate\tests\Invoke-ReleaseGate.ps1 -Version 2022 -ResultsDirectory .\results\2022
```

Replace `2022` with `2019` or `2025` to choose a version. This command runs every declared suite for that version, in sequence. To run the complete release matrix yourself, repeat it for each version. Reserve up to 90 minutes per version; the first run also downloads the SQL Server image. GitHub Actions runs the three versions in parallel, gives each job a 90-minute limit, and reports the measured time, check totals, and any failures in its summary.

The gate creates a random disposable SQL password if `SPSTATUPDATE_TEST_SQL_PASSWORD` is not set. It writes `run-results.json` and one result file per suite into the results folder. Without a results-database connection configured, the verdict is calculated from these local files. The public workflow uses a throwaway password and does not require repository secrets.

## Run one suite against your own test server

For a SQL Server listening on port 1433 on this computer, use a disposable `sa` password and run the compile suite:

```powershell
$secure = Read-Host 'Disposable SQL password' -AsSecureString
$env:SPSTATUPDATE_TEST_SQL_PASSWORD = [Net.NetworkCredential]::new('', $secure).Password
& .\sp_StatUpdate\tests\Test-ProcCompiles.ps1 -Server 127.0.0.1 -Versions @('2022') -PortOverride 1433
Remove-Item Env:\SPSTATUPDATE_TEST_SQL_PASSWORD
Remove-Variable secure
```

`-Server` is the SQL Server address. `-PortOverride` is its port; 1433 is the default SQL port. The compile suite installs each batch from `sp_StatUpdate.sql` in `master` and checks that SQL Server accepts it. It does not execute the procedure or need the release-gate test database. For a remote test server, provide a secure connection to it and use its address; do not point tests at production.

## What one check does

The Help Parameter Contract suite sends `EXEC dbo.sp_StatUpdate @Help = 1` to SQL Server and reads each returned result set. One real check, `HC0: Returns 4 result sets`, passes only when the call returns exactly four sets. If it returns a different number, the result records a failure and the observed count. The same suite then checks that the returned version, parameter list, presets, and help topics match the procedure.

## Read a failure

Open `run-results.json` and find the suite's `checks` array. Each row has a check name and `PASS` or `FAIL`; a failure includes a reason with the observed error or value. A setup error stops the run before a release verdict is written. In GitHub Actions, open the failed SQL-version job for the summary, then download its results artifact for the individual JSON files.

## Add a regression check

Add a small synthetic case to the suite for the behavior. Seed the data, call the procedure, and assert an observable result with a clear check name. First run it against the unfixed procedure and confirm it fails; then apply the fix and confirm that the same check passes. Update the suite's expected count in `Invoke-ReleaseGate.ps1` and add its proof to the coverage inventory. Run the Python tests, SQL-comment checks, and PowerShell parser check before opening a pull request. Keep existing checks intact.

## Maintainer-only checks

The SSMS parser check runs on the maintainer's Windows machine because it tests the parser shipped inside SSMS. Public CI runs the SQL suites on Linux. Before and after maintainer runs, a fingerprint confirms that the preserved test database files did not change. A shared-host lock prevents overlapping test work from interfering with another project. These safeguards protect maintainer infrastructure; they are not needed for your own disposable Docker run.

## What is not covered

The release verdict covers only the scripts declared in `Invoke-ReleaseGate.ps1`. Other scripts in `tests/` are published for focused or development checks; their results do not affect the release verdict. The coverage report lists inventoried behaviors that do not yet have a passing proof in the declared suites. Those gaps are not tested by this gate, and the report does not claim complete branch coverage. The suites use synthetic fixtures; they do not validate behavior against a production database or workload. The coverage report is uploaded with each run and does not change the pass/fail verdict.
