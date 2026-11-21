# Test loop plan: faster, cheaper test loops for sp_HeapDoctor and sp_StatUpdate

An ordered task list for the agent doing this work.  Identical copies are committed
in both repos at `docs/agents/test-loop-plan.md`; change both together.

Read `docs/agents/shared-notes.md` first, then each repo's own testing doc
(`docs/testing.md` in sp_HeapDoctor, `TESTING.md` in sp_StatUpdate).

## Ground rules for every task

- One task per branch, one commit per task, `see #<issue>` in the message once an
  issue exists.  Open the issue or bead before you start.
- These tasks change test tooling, not the procedures.  Do not edit
  `sp_HeapDoctor.sql`, `sp_StatUpdate.sql` or `sp_StatUpdate_Diag.sql`.  If a task
  seems to need that, stop and report.
- Never weaken an assertion, delete a check, or lower an expected count to make a
  task pass.  A faster suite that checks less is a regression.
- Every speed claim needs a before and after number from the same rig, the same
  version, and the same commit of the procedure.  Record both in the task's bead.
- Prove each tooling change with a full gate on all three versions at the task's
  final commit.  The PASS total must be unchanged unless the task says otherwise.
- Use the rig only through `tools/rig-use.sh` (sp_HeapDoctor) or the maintainer
  provider (sp_StatUpdate).  Never stop or release another harness's lease.
- Time limit per task: 3 hours of wall clock.  At the limit, stop, write down where
  you are, and check in.  Do not continue past it.

Tasks are in order.  Phase 0 is measurement: do it first, because its numbers
decide whether Phase 2 or Phase 4 pays off more.

---

## Phase 0: measure before changing anything

### T0.1  Per-file timing in sp_HeapDoctor's runner
**Why:** Nothing records how long each test file takes, so nobody knows whether the
fixture rebuild, the test body, or environment creation dominates a 50-minute run.
**Do:** In `tests/run_suite.sh`, record elapsed seconds for setup and for the test
body of each file, separately.  Write them to a `timings.tsv` next to the logs, and
add `setup_s=` and `body_s=` totals to the `=== totals:` line.  `tools/gate.sh`
writes its logs straight into the result directory, so have it pass that directory
to the runner (for example `HEAPDOCTOR_TIMINGS=$OUT/timings_<year>.tsv`).
**Done when:** a full gate produces `timings.tsv` for each version, the PASS total is
unchanged, and the totals line still parses in `gate.sh`'s `total()`.

### T0.2  Mine sp_StatUpdate's existing timings
**Why:** sp_StatUpdate already writes `setup-<suite>.json` with `base_rebuild_ms`,
`install_proc_ms` and `install_diag_ms` for every suite.  The data exists; nobody
has added it up.
**Do:** Write `tests/summarize_timings.py` that reads a results directory and
prints, per suite: base rebuild, proc install, suite runtime (from `run-results.json`
or the file timestamps), and totals.  Run it on the last three full runs per version.
**Done when:** the bead has a table showing what share of the ~90 minutes per
version goes to base rebuilds and proc installs.

### T0.3  Environment creation cost
**Why:** `rig-use.sh run-create` allows 7,500 seconds.  If creation is a large share
of a 10-minute `redgreen`, reusing environments (T4.1) beats everything else.
**Do:** From the last 20 gate result directories in sp_HeapDoctor, take the time
from the start to the end of each `create_<year>.log` and each `destroy_<year>.log`.
Report median and max per version.
**Done when:** the numbers are in the bead.

### T0.4  Rig capacity
**Why:** Splitting suites across environments (T4.2) needs more than one
environment per version, and today the control script allows one slot per version.
**Do:** Report the rig host's RAM, CPU cores and free disk, and the memory each
running SQL Server container uses during a full gate.  Do not change the control
script in this task.
**Done when:** the bead says how many concurrent environments the host can run
without swapping.

### T0.5  Probe the fast reset options on each version
**Why:** Database snapshots on Linux depend on the data volume supporting sparse
files.  Find out before building on them.
**Do:** On one fresh environment per version: build `HeapDoctorTest`, then time
(a) `CREATE DATABASE ... AS SNAPSHOT OF` plus `RESTORE DATABASE ... FROM
DATABASE_SNAPSHOT` after a test file has run, (b) `BACKUP DATABASE ... WITH
COMPRESSION, COPY_ONLY` plus `RESTORE ... WITH REPLACE`, and (c) the current
`01_setup_test_data.sql`.  After (a) and (b), confirm forwarded record counts, page
counts and Query Store plan counts match the freshly built database exactly.
**Done when:** the bead has a table of the three timings per version and a yes/no
on whether each reset reproduced the fixture exactly.

---

## Phase 1: make the gate trustworthy and asynchronous

### T1.1  Gate a commit, not the working tree (sp_HeapDoctor)
**Why:** `tools/gate.sh` runs whatever is in the checkout, including uncommitted
edits, and the agent has to stay idle while it runs.
**Do:** Add `--commit <rev>` (default HEAD).  The gate makes a temporary
`git worktree` at that commit, runs from it, and removes it afterwards.  Refuse to
start if `<rev>` does not resolve.  `result.json` names the resolved commit, and
`dirty` is always false for these runs.
**Done when:** you can edit files in the main tree during a gate without changing
its result, and a full gate at the task's commit passes.

### T1.2  Same for sp_StatUpdate
**Why:** `Invoke-ReleaseGate.ps1` records `git rev-parse HEAD` but runs the working
tree, and it has no dirty check at all.
**Do:** Add a `-Commit` parameter with the same worktree behavior, and refuse
to record a release verdict for a dirty tree.
**Done when:** a gate run on a dirty tree without `-Commit` fails before starting
SQL Server.

### T1.3  A gate queue
**Why:** sp_HeapDoctor keeps one slot record per version per machine, so a second
gate started early exits with code 2 instead of waiting.  With T1.1 in place, the
agent can submit work and keep going, but only if submissions wait their turn.
**Do:** `tools/gate-queue.sh submit <commit> <mode> [args]` appends a job;
one worker takes jobs in order under `flock`; `status` lists jobs; each job's
result goes to its own directory with `result.json`.  Submitting returns
immediately.
**Done when:** three jobs submitted back to back all complete in order with correct
results, and none fails because a slot was busy.

### T1.4  Release guard (both repos)
**Why:** P2 was tagged at `e2067e3` and P2c at `53b6fbf`, but the only full gate on
record was at `fa14bbf`.
**Do:** `tools/release.sh <version>` refuses to bump, tag or push unless a full-gate
`result.json` (sp_HeapDoctor) or `run-results.json` for all three versions
(sp_StatUpdate) names the exact commit being tagged, with `passed: true` and not
dirty.
**Done when:** the script refuses a commit with no matching result and accepts one
that has it.

### T1.5  `redgreen` stops after a failed red
**Why:** `gate.sh redgreen` runs the green half even when red already failed, so
the attempt is wasted either way.
**Do:** If the red run is NOT RED, skip green for that test and say so in the
verdict line.
**Done when:** a deliberately non-failing test shows `NOT RED (green skipped)` and
the run takes about half as long as before.

### T1.6  Failure summary
**Why:** Reading full logs on a failure is the most expensive thing an agent does in
this loop.
**Do:** `tools/gate-failures.sh <result dir>` prints at most 40 lines: each failing
file, its FAIL lines, and its `Msg` lines with one line of context.
**Done when:** on a seeded failure, the summary is enough to find the problem
without opening a log.

---

## Phase 2: build the fixture once, reset it fast

Use T0.5's numbers to pick snapshot revert or backup/restore.  If both reproduce the
fixture exactly, prefer snapshot revert and keep backup/restore as the fallback.

### T2.1  Split sp_HeapDoctor's setup
**Why:** Every one of the 65 test files runs `01_setup_test_data.sql`, which drops
and rebuilds `HeapDoctorTest` and `HeapDoctorCollationTest`, loads about ten heaps,
forces forwarded records, and runs 20 rounds of scans to fill Query Store.
**Do:** Split it into `01a_build_fixture.sql` (everything that does not depend on
the deployed procedure) and `01b_reset.sql` (restore or revert both databases, then
fill `dbo.ExpectedVersion` from the deployed procedure).  `run_suite.sh` builds once
per environment and resets before each file.  `ExpectedVersion` must never come
from the cached fixture: red and green deploy different procedures.
**Done when:** a full gate passes with the same PASS total, `timings.tsv` shows the
setup share dropping, and a test that deliberately damages a heap does not affect
the next file.

### T2.2  Cache the fixture between runs
**Why:** `01_setup_test_data.sql` changes rarely (eight commits since July), so the
build can usually be skipped entirely.
**Do:** Key the backup on a hash of the fixture script plus the version.  Store it
with the rig's staged data, one per version.  Never restore a backup taken on a
newer version, and never restore one taken on an older version either: the upgrade
on restore costs time and changes internal state.
**Done when:** a second full gate on an unchanged fixture skips the build, and
changing one byte of the fixture script forces a rebuild.

### T2.3  Same for sp_StatUpdate's base database
**Why:** `Invoke-ReleaseGate.ps1` runs `Initialize-ReleaseGateBase.ps1` before each
of the 35 suites.  That rebuilds `StatUpdateTest` from `setup_multiversion_db.sql`
and reinstalls about 21,800 lines of T-SQL (`sp_StatUpdate.sql` and
`sp_StatUpdate_Diag.sql`) 35 times per version.
**Do:** Build the base once and reset it per suite with the method from T0.5.
Install both procedures once per environment.  Before each suite, check that
`master.sys.sql_modules` still holds the same definitions (compare a hash), and
reinstall only if a suite changed them.  Keep the shuffled suite order.
**Done when:** the per-suite setup JSON shows the rebuild and install times
dropping, and all three versions still pass with the same totals.

### T2.4  Stop rebuilding large fixtures inside suites
**Why:** `Test-SizeBoundaries.ps1` builds a 1,000,000-row table on every run, and
`Test-StopReasonsCoverage.ps1` creates and drops databases many times.
**Do:** Move fixed-shape data like the 1M-row table into the cached base fixture.
Leave anything a suite must build itself to prove a behavior.  For statistics-only
cases, `UPDATE STATISTICS ... WITH ROWCOUNT = n, PAGECOUNT = n` can fake table size
without loading rows, but it is unsupported and test-only, and it does not change
`modification_counter`.  Use it only where the test reads row or page counts from
statistics metadata, and say so in a comment.
**Done when:** the slowest suites from T0.2 are faster, and no check count changed.

---

## Phase 3: isolation, so environments can be reused safely

### T3.1  Server-state leak check (both repos)
**Why:** 35 of sp_HeapDoctor's 65 test files touch master, msdb, logins or server
settings, or create extra databases.  A database reset does not undo any of that,
and today a leak only shows up as an order-dependent failure.
**Do:** Before and after each test file or suite, snapshot: database names,
server principals, application locks held by any session, XE sessions,
`sys.configurations` values, and SQL Agent jobs.  Fail the file on any difference,
with the difference in the message.
**Done when:** a seeded test that leaves a login behind fails with that login named,
and a full gate passes on all three versions.

### T3.2  Shuffle sp_HeapDoctor's test order
**Why:** sp_StatUpdate already shuffles its suite order to flush out order
dependencies.  sp_HeapDoctor runs in name order, which hides them.
**Do:** Add a seeded shuffle to `run_suite.sh` (01 first, 99 last, the rest
shuffled), print the seed, and accept `--seed` to replay an order.
**Done when:** three full gates with different seeds pass, or the failures are
written up as beads.

---

## Phase 4: parallelism

### T4.1  Warm environments for the inner loop
**Why:** With T2 and T3 in place, a reset environment is as good as a fresh one for
`only` and `redgreen`.
**Do:** Add `--reuse` to `gate.sh only` and `redgreen`: keep the environment between
calls, reset the fixture, and run T3.1's leak check.  Full gates always use fresh
environments.
**Done when:** back-to-back `redgreen` runs skip creation, and T0.3's creation time
disappears from the second run.

### T4.2  Split the full suite across environments
**Why:** This is the only change that shortens the full gate itself.  It depends on
T0.4's capacity numbers.
**Do:** If the host can run two or more environments per version, propose (do not
apply) the control-script change.  In `gate.sh full`, split test files across N
environments per version using T0.1's timings, longest first, and merge the totals.
**Done when:** the proposal is in a bead with the expected time per version, based
on the timing data.

### T4.3  Run sp_StatUpdate's versions side by side locally
**Why:** CI already runs the three versions in parallel.  The maintainer flow runs
one version per command.
**Do:** Add `-Versions 2019,2022,2025` to `Invoke-ReleaseGate.ps1`, running each
version in its own job and writing one combined verdict.  Respect the shared-host
lock.
**Done when:** one command produces all three versions' results in roughly the
time of the slowest one.

---

## Phase 5: the agent loop

### T5.1  Read regions, not whole files
**Why:** `sp_HeapDoctor.sql` is 9,162 lines and `sp_StatUpdate.sql` is 12,319.
Reading either one whole is the biggest token cost in a fix.
**Do:** `tools/region.sh <name|list>` prints one `/*#region ... */` block, with line
numbers.  Both procedures already use these markers (sp_HeapDoctor has 23).  Add a rule to each
repo's agent instructions: grep first, then read a region or at most 150 lines.
**Done when:** `region.sh list` prints every region with its line range.

### T5.2  Mechanical checks before review
**Why:** Test 48 lost two checks in `97aa381` and nothing flagged it.
**Do:** A pre-commit or pre-gate script that fails when a commit removes lines
from a test's assertion columns or literal lists, deletes a `PASS <id>`/`FAIL <id>`
pair, or lowers an expected count in `Invoke-ReleaseGate.ps1`, unless the commit
message has a line starting with `Test change:` that explains why.
**Done when:** replaying `97aa381` through the script fails it.

### T5.3  Review runs alongside the next fix
**Why:** In P2, six follow-up commits and a second gate came from problems found
after the full gate.
**Do:** Change the goal workflow: when a fix commit is green on `redgreen`, start a
separate reviewer agent on its diff and the linked issue, while the next fix starts.
The reviewer writes findings to the bead.  The final full gate waits until every
review is resolved.
**Done when:** the workflow doc describes it, and the next goal's bead notes show
reviews finishing before the final gate.

### T5.4  Cheaper models for bookkeeping
**Do:** Route goal records, bead notes, release text and timing tables to a cheaper
model.  Keep fixes and reviews on the strong model.
**Done when:** the workflow doc says which steps go where.

### T5.5  Enforce the goal time limit
**Why:** The P2 goal ran 11h 50m against a 6-hour limit and nothing acted on it.
**Do:** Make the goal runner stop, or require a check-in, when wall-clock time
reaches the limit.  Record elapsed time per fix in the bead notes automatically.
**Done when:** a goal with a 5-minute test limit stops at 5 minutes.

---

## Phase 6: take the best of each harness

### T6.1  Derive sp_StatUpdate's expected counts from the sources
**Why:** `Invoke-ReleaseGate.ps1` keeps `$requiredCounts` by hand, and commits like
"Align HelpContent check count" and "Correct SortOrderCoverage check count" show it
drifting.  sp_HeapDoctor derives its counts from the `PASS`/`FAIL` literals in each
test file.
**Do:** Derive each suite's expected count from its check calls and compare it with
the declared count.  Only 7 of the 39 `Test-*.ps1` files use `Add-Check`; find the
helper each other suite uses before writing the parser.  Fail on a mismatch, with
the difference in the message.
**Done when:** removing one check from a suite fails the gate with the suite named.

### T6.2  Add a `redgreen` mode to sp_StatUpdate
**Why:** `TESTING.md` asks for a manual run against the unfixed procedure.
sp_HeapDoctor automates that and records the proof.
**Do:** `Invoke-ReleaseGate.ps1 -RedGreen <base> -Suite <name>` installs the
procedure from `<base>`, runs the suite and requires at least one FAIL, then
installs the working copy and requires a pass.
**Done when:** a known fix proves red and green on one version.

### T6.3  Fingerprint sp_HeapDoctor's test tooling
**Why:** sp_StatUpdate records a SHA-256 of its test tooling in every result, so a
result can't be confused with one from older tooling.
**Do:** Add the same to `gate.sh`'s `result.json`.
**Done when:** changing `run_suite.sh` changes the fingerprint in the next result.

---

## Report back after each phase

The before and after numbers, the bead IDs, the PASS totals per version, and
anything that needs a decision from the maintainer.
