# Shared notes: sp_HeapDoctor and sp_StatUpdate

Cross-project memory for people and agents working on either procedure.  Read it
before starting work in either repo.  Each repo's own docs still win for that repo:
`docs/testing.md` and `CONTRACTS.md` in sp_HeapDoctor, `TESTING.md` in
sp_StatUpdate.

## Where this file lives

Identical copies are committed in both repos at `docs/agents/shared-notes.md`,
next to `docs/agents/test-loop-plan.md`.  When you change either file, make the same
change in the other repo in the same session, so a `diff` of the two copies is
always empty.

Both repos use an allowlist `.gitignore`, so a local `CLAUDE.md` or `AGENTS.md` is
never committed.  To load these notes automatically, add this line to the local
`CLAUDE.md` in each clone:

```
@docs/agents/shared-notes.md
```

For agents that read `AGENTS.md` instead, add: "Before any work, read
`docs/agents/shared-notes.md`."

Both repos are public.  Keep this file free of host names, addresses, local paths,
credentials, tracker IDs and people's names.  sp_StatUpdate's CI check
(`.github/scripts/check_no_private_details.py`) enforces most of that there.

## How to update it

- Add a lesson only when it applies to both projects, or when one project learned
  something the other will hit.  Project-only facts go in that project's docs.
- Each lesson gets a date and a short "so what".  Edit an existing line rather than
  adding a near-duplicate.
- Keep the file under about 300 lines.  When it grows past that, fold old lessons
  into the rules above them and delete the history.

---

## The two projects at a glance

| | sp_HeapDoctor | sp_StatUpdate |
|---|---|---|
| What it does | Finds heaps with forwarded records and rebuilds them (or swaps in a clustered index) | Priority-based statistics maintenance: most important statistics first, stops at a time limit |
| Procedure size | ~9,200 lines | ~12,300 lines, plus ~9,500 for `sp_StatUpdate_Diag` |
| Tests | 65 T-SQL files in `tests/`, run by `tests/run_suite.sh` | 35 declared PowerShell suites, run by `tests/Invoke-ReleaseGate.ps1` |
| Gate | `tools/gate.sh full / only / redgreen`, three versions side by side | One version per command locally; CI runs all three in parallel |
| Full run | About 50 minutes | Up to 90 minutes per version |
| Fixture | `tests/01_setup_test_data.sql`, rebuilt before every test file | `tests/setup_multiversion_db.sql`, rebuilt before every suite, plus both procs reinstalled |
| Expected counts | Derived from `PASS <id>` / `FAIL <id>` literals | Kept by hand in `$requiredCounts` |
| Test order | Name order | Shuffled every run |
| Result file | `result.json` outside the repo, names the commit | `run-results.json` plus one file per suite, with a tooling fingerprint |
| Versions | SQL Server 2019, 2022, 2025 | SQL Server 2019, 2022, 2025 |

## Shared rig rules

- Both projects share one test rig.  Leases decide who may use it.  A harness that
  cannot get a lease touches nothing and waits or exits.
- In sp_HeapDoctor, `tools/rig-use.sh` is the only way to start, stop, or connect.
  Never call docker yourself and never run `sqlcmd` around the lease.
- Fresh environments (`run-create`) allow one slot per version.  A second gate on
  the same machine fails with exit code 2; it does not queue.
- Never stop or release another harness's lease, even if it looks stale.  Report it.
- Why this matters (2026-08-02): a harness that ignored a refused lease stopped the
  other project's containers mid-run, and the victim's remaining tests emitted
  nothing, which read as zero failures.

## Gate rules both projects follow

- A result counts only if the gate produced it.  Hand-run `sqlcmd` output is not
  evidence.
- A fix needs a test that fails without it (red) and passes with it (green).
- One commit per issue, with `see #<number>`.  Never a closing keyword: GitHub
  closes issues on words like "fixes" even after "not".
- **A full gate at the exact commit being released, on all three versions.**  Check
  that the result file names that commit before you tag.
- A silent test is a failed test.  Both runners fail on missing or short output.

## Lessons learned

- **2026-11-21: Gate evidence must name the shipped commit.**  The sp_HeapDoctor P2
  goal cited a full gate at `fa14bbf`, but six later commits reworked four of its
  twelve fixes, and the tags went on `e2067e3` and `53b6fbf`.  So what: compare the
  result file's commit with the tag target before releasing.  A release script
  should refuse otherwise.
- **2026-11-21: Watch for tests weakened in the same commit as a fix.**  `97aa381`
  added the real database name to test 48's forbidden patterns, then dropped the two
  columns that would have caught it.  So what: any commit that removes assertion
  columns, literal lists, or expected counts needs a written reason, and a reviewer
  who did not write the fix.
- **2026-11-21: The agent that wrote a fix should not be the only reviewer.**  So
  what: run a separate reviewer on each fix commit as soon as it is green, while the
  next fix starts.
- **2026-11-21: Goal time limits must stop the run.**  The P2 goal ran 11h 50m
  against a 6-hour limit.  So what: a limit that only gets recorded is not a limit.
- **2026-11-21: Fixture rebuilds are the biggest repeated cost in both suites.**
  sp_HeapDoctor rebuilds its fixture 65 times per version per full run;
  sp_StatUpdate rebuilds its base and reinstalls about 21,800 lines of T-SQL 35
  times per version.  So what: build once, reset by snapshot revert or
  backup/restore, and install procedures once per environment.
- **2026-11-21: Clones and fake statistics are not a fixture for sp_HeapDoctor.**
  `DBCC CLONEDATABASE` and scripted statistics carry no data pages, so there are no
  forwarded records and no targets.  They can help sp_StatUpdate, where the test
  reads statistics metadata.  `UPDATE STATISTICS ... WITH ROWCOUNT, PAGECOUNT` is
  unsupported and test-only, and it does not change `modification_counter`.
- **2026-11-21: Backups don't move between versions.**  A backup won't restore to an
  older version, and restoring to a newer one runs an upgrade.  Cache one fixture
  per version.
- **2026-11-21: Database snapshots on Linux need sparse-file support** on the data
  volume.  Probe each version's container before relying on them.

## SQL Server behavior that has bitten us

- `sp_getapplock` with `@LockOwner = 'Session'` lives until the session ends or
  releases it.  An idle SSMS window that once ran the procedure still holds it.
  sp_HeapDoctor's re-entrancy lock is server-wide, so two non-parallel runs can't
  share one instance, even in different databases.
- A severity-16 `RAISERROR` inside a caller's `TRY` jumps straight to the caller's
  `CATCH`.  Code after it, including cleanup, never runs.  Wrapping a whole
  procedure body in `TRY` to release a lock can turn deliberate skip-and-continue
  paths into aborts (sp_HeapDoctor #237).
- Low-privilege paths must stay low-privilege.  `sys.dm_hadr_database_replica_states`
  needs server state permission.  `DATABASEPROPERTYEX(name, 'Updateability')` gives
  the same read-only answer without it.
- An `ID` in a local work table is not a `database_id`.  Look up per-database
  properties by name.
- Collation differs between databases.  Compare names with an explicit `COLLATE`
  taken from the target database.
- `ALL_DATABASES` in sp_HeapDoctor includes master, model and msdb since
  2026.11.19.1 (#228).  Existing jobs will start touching those databases.

## Where to find things

- Fast-loop improvement plan: `docs/agents/test-loop-plan.md`.
- sp_HeapDoctor: `docs/testing.md`, `CONTRACTS.md`, `STYLE_GUIDE.md`,
  `docs/obfuscation.md`, `docs/agents/`.
- sp_StatUpdate: `TESTING.md`, `tests/Invoke-ReleaseGate.ps1` (the declared suites
  and counts), `.github/workflows/sql-tests.yml`.
