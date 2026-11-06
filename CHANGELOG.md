# Changelog

Per-release detail for both procedures, newest first. The short summary lives in the
README's [Version History](README.md#version-history-).

This history used to sit in each procedure's header comment. Keep it here: SSMS 22 stops
splitting batches on `GO` after a block comment larger than 64 KiB, so the header has to
stay small. `.github/scripts/check_sql_comment_size.py` enforces that in CI.

## sp_StatUpdate

```text
3.11.2.2026.08.02 - 0859.9 (DarlingData-audit finding): the gh-505
                TablePriority estimate read historical durations
                from CommandLog with the int-returning
                DATEDIFF(MILLISECOND) -- a row spanning more than
                ~24.8 days (orphan-cleanup END markers) raised
                error 535 during discovery.  Now DATEDIFF_BIG,
                plus a 24-hour span sanity filter so cleanup
                artifacts cannot skew the per-table average
                either.  The end-of-run @duration_ms_total
                computation gets the same DATEDIFF_BIG hardening.

3.11.1.2026.07.31 - o2md.54 (found by the SQL 2025 primary-source
                compatibility audit): the #369 plan-feedback
                awareness filter used state_desc IN (ACTIVE,
                PENDING_VALIDATION) -- but sys.query_store_
                plan_feedback has NO documented ACTIVE state, so
                the count silently degraded to
                PENDING_VALIDATION-only on every version
                (magnified on SQL 2025 where DOP feedback is on
                by default).  Now filters the four states a stats
                update could invalidate: PENDING_VALIDATION,
                IN_VALIDATION, VERIFICATION_PASSED,
                FEEDBACK_VALID.  Also from the audit: the
                plan_forcing_type "SQL 2022+" comments corrected
                to 2017+ (docs; behavior was already
                metadata-detected), and #qs_forced_baseline.
                plan_forcing_type int-typed to match the source
                column.  Audit verified all other hardcoded QS
                value maps (readonly_reason bitmask, wait
                categories, capture modes, version gates) match
                current documentation; no other mismatches --
                see research/sql2025-compat-audit-2026-07-31.md.

3.11.0.2026.07.30 - o2md.18 (maintainer decision): discovery-
                candidate log.  When qualifying work remains
                unprocessed at run end, ONE compact
                SP_STATUPDATE_REMAINING CommandLog row lists the
                remaining qualifying stats (db/schema/table/stat,
                priority, modification counter; capped at 500
                with Truncated flag + TotalRemaining).  In
                parallel skip-discovery mode the per-stat detail
                is structurally unavailable, so the row lists
                remaining TABLES from the queue (Kind=TABLE).
                Previously the qualifying-but-unprocessed
                population was unobservable from CommandLog --
                Diag''s W13 PERPETUALLY_SKIPPED could only infer;
                it now consumes this log as the authoritative
                source (diag 2026.07.30.11).  Non-critical
                TRY/CATCH write; one row per run.

3.10.1.2026.07.30 - o2md.53: the zks1/gh-533 forced-plan baseline
                and post-run delta filtered on plan_forcing_type =
                N'MANUAL', but plan_forcing_type is an INT column
                (1 = MANUAL) -- the comparison raised a conversion
                error on every SQL 2022+/2025 run and was
                TRY/CATCH-swallowed, so the MANUAL-only filter
                never actually applied anywhere it existed (2019
                lacks the column and skipped the filter, which is
                why gates stayed green).  All four sites now
                compare against the integer value.  Live-verified
                on SQL 2025: baseline captures instead of
                skipping.

3.10.0.2026.07.30 - MOP-UP QS + two silent pre-existing bugs
                (o2md.5 + fixes found verifying it).

                o2md.5 (maintainer option a) -- Phase 6 QS
                enrichment extracted into @qs_phase6_sql, a
                fragment variable the staged path concatenates
                back at the exact original position (assembly is
                byte-identical, so the two paths cannot drift)
                and the mop-up paths (parallel-leader + serial)
                execute against a mop-up-built #stat_candidates
                per database.  Mop-up rows now carry real qs_*
                values in per-stat ExtendedInfo (the o2md.16/W5
                "QS not applicable to this pass" dependency) and
                @SortOrder = QUERY_STORE is honored by a
                post-enrichment re-rank; the parallel-leader
                TablePriority fan-out now inherits the per-stat
                priority ranking instead of re-deriving from
                modification counter.  Verified live: FromMopUp
                rows log QSPlanCount from the shared fragment.

                PRE-EXISTING BUG (silent, since sw82): the
                zero-candidate/zero-qualify early-exit shape
                (@empty_disc_select) was missing is_critical,
                so every empty-discovery database failed the
                INSERT...EXECUTE column-count check and was
                silently skipped (DB_SKIPPED swallow).

                PRE-EXISTING BUG (silent, since gh-462 /
                36cea09, 2026-04-18): @mop_up_where_sql carried
                QUADRUPLED quotes, correct only for a
                doubly-nested dynamic string -- both consumers
                concatenate it directly, so serial and
                parallel-leader mop-up discovery died with
                'Incorrect syntax near IsUserTable' on every
                run, the per-database CATCH swallowed it, and
                'no additional stats found' masqueraded as
                normal.  Mop-up discovery had been broken for
                ~3 months of green gates; a regression test now
                asserts discovery actually finds seeded rows.

3.9.0.2026.07.30 - BEHAVIOR CHANGE (maintainer-authorized, o2md.7):
                adaptive sampling on by default in the NIGHTLY and
                WEEKLY_FULL presets.  A single large stat with a
                100% sample could burn 20+ minutes of NIGHTLY's
                60-minute window; historically-slow stats (from
                CommandLog history) now fall back to the sampled
                rate automatically -- NIGHTLY threshold 10 min,
                WEEKLY_FULL 30 min, sample percent stays at the
                internal default (10%).  DEFAULT / OLTP_LIGHT /
                WAREHOUSE presets unchanged.  Explicit
                @LongRunningThresholdMinutes /
                @LongRunningSamplePercent still override (applied
                after presets).  Also in 3.9.0 (o2md.4, found by
                the kill-recovery repro): a parallel run that
                joins a carried-over queue, verifies every
                carried stat was already updated (CommandLog
                freshness check), and drains the queue no longer
                classifies as SILENT_NOOP/ERROR -- it reports
                the new QUEUE_ALREADY_SATISFIED warning code and
                grades normally.  A genuine zombie abandons a
                non-empty queue (remaining > 0) and still
                classifies.  Minor version bump because
                existing NIGHTLY/WEEKLY_FULL callers see changed
                sampling behavior on historically-slow stats.
                To restore pre-3.9.0 behavior on those presets,
                pass a threshold no run can reach (validation
                floor is 1; e.g. @LongRunningThresholdMinutes =
                100000) -- NULL cannot disable it, NULL means
                "preset decides".

3.8.5.2026.07.30 - OBSERVABILITY: self-idle-time buckets + mop-up
                sort-order/tagging (o2md.46, o2md.49).

                o2md.46 - the proc's own wait time was invisible:
                no field captured time spent in the inter-stat
                delay, the AG-redo recheck loop, deadlock/lock-
                timeout retry backoff, or parallel claim-cycle
                overhead (dead-worker sweep + claim + lazy
                discovery), so Diag's W6 EXCESSIVE_OVERHEAD could
                only infer "discovery cost" from wall-clock minus
                per-stat time.  Four accumulators (SYSDATETIME()
                bracketing, DATEDIFF_BIG ms) now emit in the END
                Summary XML as DelayWaitMs / AgRedoWaitMs /
                RetryBackoffMs / QueueClaimMs -- NULLIF omits each
                when zero (matches ForcedPlanFailureDelta; no
                XSINIL in that block, so absent == 0).  The
                claim-cycle bracket stays open across the
                lazy-discovery zero-rows/CATCH CONTINUE loops
                (o2md.1) so failed claim cycles are counted, and
                is never opened in serial mode.

                o2md.49 (split from o2md.5) - all three mop-up
                discovery sites (lazy per-table, parallel-leader,
                serial) hard-coded modification-counter ordering,
                silently ignoring @SortOrder.  A shared
                @mop_up_order_expr now honors ROWS, PAGE_COUNT,
                DAYS_STALE, AUTO_CREATED, and RANDOM (expressions
                mirror the main pass's staged-discovery CASE
                minus the qs_priority_boost term -- mop-up qs_*
                is structurally NULL).  QUERY_STORE and other
                enrichment-dependent sorts keep the
                modification-counter fallback (pre-fix behavior)
                pending the o2md.5 design decision.  Also new:
                #stats_to_process.from_mop_up per-row flag,
                emitted as FromMopUp in per-stat ExtendedInfo --
                unlike QualifyReason's MOP_UP branch (driven by
                the session-scoped @in_mop_up flag) it cannot
                mislabel priority-pass leftovers processed after
                the mop-up flag flips, giving Diag a reliable way
                to tell "no QS data" from "QS not applicable to
                this pass".

3.8.4.2026.07.30 - o2md.8: bounded retry extended to lock timeouts.
                The existing #163 deadlock-retry loop (up to 3
                attempts, exponential backoff) retried error 1205
                only.  A lock timeout (1222) is just as transient
                as a deadlock -- the blocker's own transaction is
                expected to release -- but previously burned the
                stat as a hard failure on the first hit and
                counted against @MaxConsecutiveFailures.  Found on
                a production server where 1222 accounted for 5/5
                (100%) of all recorded failures.  Same backoff now
                applies to both error numbers; the existing
                DEADLOCK_RETRY_OK/FAIL warning text is unchanged
                for 1205, with new LOCK_TIMEOUT_RETRY_OK/FAIL for
                1222.  Caught during testing: RAISERROR only
                accepts variables or literals as substitution
                arguments, not arbitrary expressions -- an inline
                CASE in the retry RAISERROR call failed to deploy
                ("Incorrect syntax near the keyword 'CASE'");
                fixed by assigning the CASE result to a local
                variable first.

3.8.3.2026.07.30 - Query Store diagnostics + priority-pass/mop-up
                inversion warning (o2md.6, QuickieStore comparison).

                Four Query Store observability gaps closed after
                comparing Phase 6 enrichment against Erik Darling's
                sp_QuickieStore (see research/quickiestore-comparison-
                2026-07-30.md for the full analysis): (1)
                wait_stats_capture_mode is now checked before the
                WAITS/WAIT_CPU enrichment queries sys.query_store_
                wait_stats -- capture-mode OFF previously produced
                silent zero enrichment indistinguishable from "no
                matching workload".  (2) READ_ONLY Query Store state
                now decodes readonly_reason (storage limit with
                current/max MB, AG secondary, single-user/emergency
                mode, internal memory limits) instead of a single
                generic "may be stale" message.  (3) query_capture_
                mode is surfaced (NONE/CUSTOM) when it may be
                silently biasing which queries QS-based prioritization
                even sees -- informational only, no behavior change.
                (4) The post-run forced-plan-failure-delta warning
                now samples last_force_failure_reason_desc so the
                message is self-contained instead of "go investigate".
                All four are additive diagnostics (debug/warning text
                only) -- no change to qualification, ordering, or
                enrichment logic.

                o2md.6 -- the priority pass qualifying 0 stats while
                mop-up (raw modification_counter > 0, no threshold
                filtering) finds N > 0 is never legitimate; it means
                priority-pass qualification is broken (the exact
                failure mode behind the icjq effective_counter
                incident, gh-502), not that the fleet has nothing to
                do.  Now detected and surfaced via @WarningsOut /
                @WarningsCodesOut (PRIORITY_PASS_ZERO_MOPUP_FOUND)
                instead of mop-up silently absorbing the gap.

3.8.2.2026.07.28 - SILENT NO-OP: fleet analysis batch (o2md).
                Found by the 40-server production diagnostic sweep
                of 2026-07-28, where ~50 nightly maintenance windows
                reported SUCCESS while processing zero statistics.

                o2md.1 (P0) -- the lazy mop-up branches that discover
                0 stats for a claimed table, and their CATCH, released
                the claim and NULLed @claimed_table_* but had no
                CONTINUE.  Control fell through to the work-claim
                block, whose parallel predicate compares against those
                now-NULL variables, matched 0 rows, and exited the run
                with @stop_reason = COMPLETED -- abandoning every
                remaining queued table.  Signature in the wild:
                StatsFound decaying by roughly one table per night
                (1779 -> 1746 -> 1710 -> 1704) with StatsProcessed = 0.
                Distinct from sp_StatUpdate-v31y, which covers the
                PARALLEL_COMPLETE variant.

                o2md.2 (P1) -- @remaining_stats counted
                #stats_to_process, which is structurally empty when
                @skip_discovery = 1 (worker joined an existing queue),
                so StatsRemaining reported 0 no matter how much work
                was outstanding.  Now derived from dbo.QueueStatistic
                in parallel mode, split under its own IF so the
                optional table is not name-resolved on non-parallel
                runs (cf. sp_StatUpdate-isa2).

                o2md.3 (P1) -- StatsFound > 0 with StatsProcessed = 0
                now grades Status = ERROR, emits the SILENT_NOOP
                warning code, and returns 2 so SQL Agent fails the
                job.  Detection previously existed only in
                sp_StatUpdate_Diag, so no-ops went unnoticed for weeks.

                o2md.9 (P3) -- the lazy mop-up zero-stats debug message
                interpolated @claimed_table_* after they were NULLed
                and always rendered NULL.NULL.

3.8.1.2026.07.25 - MONITORING: per-stat CONTEXT_INFO refresh
                (sp_StatUpdate-03i3 / gh-538).  The gh-423
                CONTEXT_INFO set at proc entry was static for the
                whole run (DB/Job/Preset only) -- a DBA using
                sp_WhoIsActive mid-run saw no run label, no stat
                index/total, no current stat name; only the inner
                dynamic UPDATE STATISTICS SQL was visible.
                CONTEXT_INFO is now refreshed every loop iteration
                with a compact 'SU|<n>/<total>|<schema.table.stat>
                |Run:<label>' payload (varchar, not nvarchar, to
                use the full 128-BYTE budget instead of 64 chars).
                Restored to the caller's original value on every
                exit path (unchanged).  The v2-era
                ##sp_StatUpdate_Progress global temp table this
                issue also proposed extending no longer exists --
                removed in the v3 API simplification -- so that
                part of the original ask is moot; CONTEXT_INFO
                plus the existing RAISERROR job-log progress
                already cover both the DMV/sp_WhoIsActive and
                job-log monitoring surfaces.

3.8.0.2026.07.25 - COLOR: opt-in @Ansi param (sp_StatUpdate-w0xp).
                @Ansi = N'Y' colorizes console (RAISERROR) output
                with ANSI SGR escape codes -- banners, discovery
                line, "Found N qualifying", the per-stat progress
                line, and the summary Succeeded/Failed/Remaining
                counts.  Default N'N' is byte-identical to prior
                output (all color variables are empty strings), so
                existing behavior, tests, CommandLog, OUTPUT params,
                and result sets are unchanged -- codes are injected
                into console RAISERROR text ONLY.  SSMS ignores the
                codes; use a color-aware terminal (Windows Terminal,
                sqlcmd piped to a color-aware pager) to see them.

3.7.4.2026.07.25 - SPILL PROOF: preselection-vs-proof @Help topic
                (sp_StatUpdate-ggnq / gh-552).  Query Store metrics
                (TEMPDB_SPILLS/WAITS/WAIT_CPU) preselect candidates
                before a run; they do NOT prove a sort spill in the
                UPDATE STATISTICS statement itself.  @Help now says
                so and points to sp_StatUpdate_XE_Session.sql, whose
                new sort_warning (primary) + hash_warning (secondary)
                events are the truthful proof surface.  No behavioral
                change to QS enrichment, ranking, or sort logic.

3.7.3.2026.07.23 - Publish early-rejection outcomes before severity-16
                signaling (sp_StatUpdate-2zla / gh-555):  the
                open-transaction guard and the ALREADY_RUNNING
                branch raised their severity-16 error BEFORE
                setting OUTPUT params, emitting the structured
                summary row, and (ALREADY_RUNNING) writing the
                gh-551 SP_STATUPDATE_DENIED artifact.  A caller
                wrapping EXEC in TRY/CATCH transfers control at
                the RAISERROR, so it received the exception
                WITHOUT the summary row or denial telemetry.
                Both paths now publish first and signal last
                (message/severity/state and return codes are
                unchanged; the lock transaction still commits
                before any signal).  Known T-SQL limit: OUTPUT
                parameters are not copied back when the caller
                catches the error, so TRY/CATCH callers should
                key off the streamed summary row, the denial
                CommandLog row, or ERROR_MESSAGE().  Callers
                without TRY/CATCH see identical behavior except
                the error message now follows the result set.

3.7.2.2026.07.23 - Phase 3B delta-enrichment cache key omits SchemaName
                (sp_StatUpdate-mtt4):  the gh-502 cl_lookup CTE
                joined CommandLog on DatabaseName + ObjectName +
                StatisticsName -- the same incomplete key fixed for
                Phase 5B in v3.7.1.  Same-named table+stat pairs in
                different schemas could read the other schema's last
                ModificationCounter, producing wrong
                effective_counter deltas (a stat could be skipped
                as already-current based on the wrong counter)
                and wrong modification_velocity.  SchemaName is now
                part of the join key.

3.7.1.2026.07.23 - QS score cache value + identity fix (sp_StatUpdate-rcng
                / gh-556):  Phase 5B cache hits were zeroed by the
                unconditional Phase 6 qs_priority_boost
                initialization while the live enrichment still
                skipped them, so every hit ranked as 0.  Zero-init
                now applies to cache misses only; the WAITS/WAIT_CPU
                wait-stats UPDATEs gain the same qs_cache_hit
                exclusion.  The cache key now also matches
                SchemaName (same-named table+stat in different
                schemas no longer share scores) and requires the
                cached QSMetric to equal the current run metric
                (the gh-503 invalidation contract); legacy rows
                without QSMetric are cache misses.

3.7.0.2026.07.03 - QS attribution + guardrail batch (sp_StatUpdate-zks1,
                -6aa2, -sss6, -o7gf, -u6n7, -l4ui, -lfbv):
                (1) gh-533 (zks1): pre-run QS forced-plan baseline --
                #qs_forced_baseline snapshots force_failure_count per
                database before any stat is touched (MANUAL-only
                filter on SQL 2017+ via plan_forcing_type; APC noise
                excluded).  Post-run delta: rising failure counts
                emit a QS_FORCED_PLAN_FAILURE_DELTA warning naming
                per-database deltas, and the END Summary XML gains a
                ForcedPlanFailureDelta element (omitted when zero).
                Databases whose baseline capture failed transiently
                are excluded from the delta (#qs_forced_baseline_dbs
                success markers) so pre-existing failures are never
                misattributed to the run.  (2) gh-534 (6aa2):
                @QueryStore = WAIT_CPU -- prioritizes by CPU-category
                waits only (sys.query_store_wait_stats
                wait_category = 1); complements WAITS (all
                categories).  (3) gh-539 (sss6): P2c budget-guard
                overshoot prevention -- before starting each stat,
                estimates duration from CommandLog history (MAX of
                last 10 successful updates, not AVG -- one 4-hour
                outlier must count) and defers stats that cannot fit
                the remaining @TimeLimit/@StopByTime budget.
                (4) gh-551 (o7gf): denied runs (ALREADY_RUNNING) now
                write a durable SP_STATUPDATE_DENIED CommandLog row
                with denial XML (denied session, holder session,
                holder login time / lock acquire time); suppressed
                under @LogToTable = N'N'.  (5) gh-554 (u6n7):
                filtered-drift proof surface -- FILTERED_DRIFT now
                outranks QUERY_STORE_PRIORITY in QualifyReason, and
                @FilteredStatsMode / @FilteredStatsStaleFactor are
                re-promoted to public parameters (the v3.0 API
                collapse had absorbed them with no preset exposing
                EXCLUDE/ONLY/PRIORITY, leaving those modes and the
                FILTERED_DRIFT sort unreachable).  Public input
                params: 42.  (6) gh-553 (l4ui): @Help preset topic
                rewritten as a bounded override contract -- presets
                are packaged configurations; the documented override
                set (@TimeLimit, @ModificationThreshold, @SortOrder,
                @StatisticsSample, @MopUpPass, @QueryStore,
                @LockTimeout) is the supported surface.  (7) lfbv:
                IO-corruption warning (severity 823/824 pattern)
                deduped via flag instead of LIKE-scan; fresh-stat
                physical_row_count aggregated once per object
                (GROUP BY derived table) instead of per stat.
3.6.0.2026.07.02 - Telemetry contract batch (sp_StatUpdate-mvgb, -r2d0,
                -ibyc, -6zvp, -wuq2, -hdls):
                (1) mvgb: END Summary XML gains a WarningsCodes
                element (pipe-delimited, same tokens as
                @WarningsCodesOut) on all five live END writers --
                the four early-return paths and the normal finalize.
                The orphan-cleanup fabricated END keeps no element
                (no live warnings context).  TOCTOU_SKIPS append
                moved above the Summary build so it lands in the
                element.  (2) r2d0: COMPLETED_WITH_SKIPPED_DBS is
                now applied to @stop_reason BEFORE the END row is
                written -- CommandLog previously always recorded
                plain COMPLETED and only the OUTPUT param showed
                the upgraded reason.  (3) gh-546: new
                @max_consecutive_failures_seen peak tracker +
                CONSECUTIVE_FAILURES_ELEVATED warning code (peak
                >= 3 without hitting the bailout); @Help WARNINGS
                topic expanded into the full format contract (all
                code tokens enumerated; codes may be added, never
                renamed/removed without a major version bump).
                (4) gh-518/545: EngineEdition 9 now emits
                AZURE_SQL_EDGE (DB/MI split shipped earlier).
                (5) gh-516: per-stat progress line gains wall-clock
                + ETA suffix (after 5 completed stats); startup
                advisory for the dangerous combo of a 0-1s lock
                timeout with @MaxConsecutiveFailures = 1.
3.5.9.2026.07.02 - Three fixes (sp_StatUpdate-7pqu, -f14h, -lh23):
                (1) gh-550: statistics that have NEVER been computed
                (dm_db_stats_properties returns last_updated NULL /
                modification_counter NULL) over POPULATED tables were
                silently never updated -- the Phase 4 outer gate
                effective_counter > 0 excluded them from every
                qualification branch including @StaleHours.  Fresh
                candidates now get a targeted sys.partitions row
                count (physical_row_count, fresh stats only) and a
                NEVER_UPDATED rescue branch in Phase 4; QualifyReason
                reports NEVER_UPDATED.  Fresh stats on EMPTY tables
                still do not qualify.  Same root cause as Ola
                IndexOptimize #990.  Mop-up pass intentionally
                unchanged (still requires modification_counter > 0).
                (2) gh-549: new @AbortOnIntegrityError bit = 1
                parameter -- default 1 preserves the existing
                abort-on-823/824/825 behavior; 0 emits the CHECKDB
                advisory + IO_CORRUPTION warning code (deduped),
                counts the stat as failed, and continues.  @FailFast
                = 1 still aborts regardless.  TOCTOU CATCH skips
                (208/15009/2767) now close their pre-inserted
                CommandLog row (EndTime + TOCTOU_SKIP message;
                ErrorNumber intentionally NULL so diag failure
                aggregations ignore them) instead of leaving it
                dangling with NULL EndTime like a killed stat.
                (3) gh-544: @QueryStore = WAITS filtered
                wait_category IN (3, 12, 15) = Lock, Preemptive,
                Network IO -- classes stats updates cannot influence.
                Corrected to 5, 6, 17, 21 (Buffer Latch, Buffer IO,
                Memory, Other Disk IO) and centralized into
                @qs_wait_categories ({{WAITS_CATEGORIES}} token) so
                the three consumer sites can no longer drift.
3.5.8.2026.06.12 - Telemetry bug fix (sp_StatUpdate-rse2):
                Four mid-run early returns (NO_QUALIFYING_STATS,
                FINGERPRINT_CONFLICT, MAX_WORKERS,
                QUEUE_INIT_ERROR) exited after the
                SP_STATUPDATE_START insert without writing an
                SP_STATUPDATE_END row.  Healthy runs were orphaned:
                diag C1 raised false KILLED_RUNS criticals, orphan
                cleanup later fabricated END rows with
                StopReason=KILLED, and the RELIABILITY grade was
                poisoned.  All four paths now write a real END row
                with full Summary XML and the true StopReason.
                Also fixed: the FINGERPRINT_CONFLICT branch was
                dead code -- its severity-16 RAISERROR fired inside
                a TRY whose CATCH was empty, so the RETURN 50001 /
                result-set block never ran and conflicting workers
                silently joined the queue anyway.  The conflict now
                sets a flag inside the TRY and exits via a label
                outside it, so the error surfaces to the caller AND
                the documented refuse-the-join semantics actually
                execute.
3.5.7.2026.06.12 - Parallel bug fix (sp_StatUpdate-v31y):
                Queue leadership re-claim no longer blocked by
                recycled SPIDs or idle pooled leader sessions.  The
                claim UPDATE judged liveness by bare session_id, so
                once a drained queue's old SessionID was reused by
                any live session (guaranteed sooner or later), every
                new worker with the same @parameters_string joined
                the completed queue, claimed nothing, and exited
                PARALLEL_COMPLETE with StatsFound > 0 but
                StatsProcessed = 0 -- a silent no-op maintenance
                run.  Liveness is now login_time-qualified
                (genuine claimer logged in before claiming; a
                recycled SPID logged in after), same-SPID rows are
                unconditionally stale (i7by semantics), a fully
                drained queue is re-claimable regardless of leader
                session state, and the active-worker guard is
                scoped to open claims (TableEndTime IS NULL).
                Found by ConcurrentParallel P3 on a fresh gate
                container.
3.5.6.2026.06.10 - Code-review batch (sp_StatUpdate-r77t, -orec, -mc03):
                r77t (P1 bug): fingerprint conflict check hoisted out
                  of the queue-CREATOR branch so it also runs for
                  queue JOINERS.  Previously any worker whose first
                  non-locking Queue SELECT found an existing queue
                  bypassed the @stored_fingerprint comparison
                  entirely, so fingerprint-only params (gh-513
                  critical-table params, mop-up params) could differ
                  silently between members of one queue.
                  Intentionally NOT fixed by adding those params to
                  @parameters_string: differing strings would create
                  separate queues that each process the full stat
                  set (duplicate work).  FINGERPRINT_CONFLICT
                  refusal is the designed semantic.
                orec (P2 bug): CONTEXT_INFO is now restored on all
                  7 early-return paths after it is set (gh-423):
                  PARAMETER_ERROR, NO_DATABASES, AG_SECONDARY,
                  NO_QUALIFYING_STATS, FINGERPRINT_CONFLICT,
                  MAX_WORKERS, QUEUE_INIT_ERROR.  Previously only
                  FINALIZE restored it, so pooled sessions kept a
                  stale sp_StatUpdate|... tag after early exits.
                mc03 (P3 batch): (a) removed the dead
                  /StatInfo/StatisticName XPath limb from the three
                  CommandLog readers (adaptive lookup + known-stats
                  prefetch x2) -- no writer ever produced that XML
                  shape; the StatisticsName column is authoritative.
                  (b) @FirstTimeFullScanCapRows now also intercepts
                  the RESAMPLE-of-persisted-100 path: a stat with
                  PERSIST_SAMPLE_PERCENT = 100 under
                  @StatisticsSample NULL previously re-ran an
                  effective FULLSCAN via RESAMPLE that the
                  =100 cap branch never saw.  (c) reworded the
                  re-entrancy guard branch-A comment (a running call
                  always has an active request; the branch guards
                  the post-call idle window, not "between stats").
                  (d) two em dashes in comments replaced with --.
3.5.5.2026.06.08 - Bug fix (sp_StatUpdate-i7by): false ALREADY_RUNNING
                  on consecutive calls after a validation-error exit.
                  Three-part fix in the re-entrancy guard:
                  (S) When the existing lock holder is the caller's own
                  session (SessionID = @@SPID), the row is unconditionally
                  stale -- the prior call has by definition ended (we are
                  now executing in that same session) so reclaim.
                  Required because callers that wrap EXEC in TRY/CATCH
                  catch the validation-error RAISERROR before the proc
                  reaches its DELETE cleanup, leaving the row.
                  (A) For DIFFERENT sessions: dead-holder reclaim treats
                  a session as dead when it has no active request AND its
                  last request finished > 60 seconds ago (catches async
                  TCP teardown where the SPID lingers briefly in
                  sys.dm_exec_sessions after client disconnect).
                  (D) TTL guard: any lock row with AcquiredAt > 8 hours
                  old reclaims unconditionally regardless of session state.
3.5.4.2026.06.08 - Two fixes (sp_StatUpdate-mhje, sp_StatUpdate-mknv):
                mhje: parallel TablePriority ordering inverted gh-505
                  LPT vs the user's explicit @SortOrder.  LPT was
                  PRIMARY (est_total_seconds DESC) and min_priority
                  (which embeds @SortOrder=QUERY_STORE intent) was
                  fallback.  On mature fleets with CommandLog
                  history, LPT silently overrode QS prioritization
                  -- QS-top stats landed at ProcessingPosition
                  285-296 instead of 1-10.  Now: min_priority is
                  PRIMARY, LPT is tiebreaker.  Preserves gh-505's
                  wall-clock optimization within priority bands
                  while honoring user @SortOrder.
                mknv: new @FirstTimeFullScanCapRows public param
                  caps unbounded FULLSCAN on very large stats
                  that have no CommandLog history yet.  Adaptive
                  sampling (@LongRunningThresholdMinutes) only
                  helps after a prior slow run is logged; first
                  encounters on 770M-15.9B-row tables ran 3000+s
                  FULLSCANs.  When @FirstTimeFullScanCapRows is
                  set and @current_row_count exceeds it AND the
                  stat has no completed CommandLog entry, the
                  cap formula (CEILING(10M / row_count * 100),
                  min 1%) overrides the FULLSCAN.  Default NULL
                  = disabled (non-breaking).  Critical-table
                  override (@CriticalSamplePercent) still wins.
3.5.3.2026.05.29 - Bug fix (sp_StatUpdate-isa2): @total_stats was
                  initialized by a CASE inside a multi-variable
                  DECLARE whose parallel branch reads the optional
                  dbo.QueueStatistic table.  A CASE in a DECLARE is a
                  single always-compiled statement, so SQL Server
                  name-resolved the QueueStatistic branch even on
                  non-parallel runs and failed with Msg 208 when the
                  table was absent (it is auto-created only when
                  @StatsInParallel = Y).  @total_stats is now assigned
                  via IF/ELSE so the QueueStatistic reference is only
                  resolved in parallel mode where the table exists.
3.5.2.2026.05.28 - Bug fix (sp_StatUpdate-trla): orphaned-run
                  cleanup added a lower bound to the orphan START
                  candidate hunt so it only considers STARTs within
                  the same window as #orphan_end_labels (now minus
                  (@i_orphaned_run_threshold_hours + 24h)).  A START
                  older than that window whose real END also
                  predated the window was not found in
                  #orphan_end_labels and received a fabricated
                  KILLED end, falsely flagging long-completed
                  parallel runs as killed.
3.5.1.2026.05.07 - Bug fix (gh-515): failed UPDATE STATISTICS rows
                  now write ExtendedInfo with RunLabel + full
                  discovery context to CommandLog. Previously the
                  per-stat CATCH branch UPDATE'd EndTime / ErrorNumber
                  / ErrorMessage but left ExtendedInfo NULL, so
                  sp_StatUpdate_Diag's C2 (REPEATED_FAILURES) check
                  -- which extracts RunLabel from XML -- silently
                  missed every failure.  TOCTOU rows (errors 208,
                  15009, 2767) intentionally keep NULL ExtendedInfo
                  so they don't aggregate as real failures.
3.5.0.2026.04.23 - @CriticalTables feature: per-table sample rate
                  override with optional priority boost (gh-508).
                  Three new params: @CriticalTables (comma-delimited
                  table patterns with % wildcards), @CriticalSamplePercent
                  (1-100, forces sample rate on critical tables only),
                  @CriticalTablesFirst (Y/N, processes critical tables
                  before everything else regardless of @SortOrder).
                  PERSIST_SAMPLE_PERCENT auto-enabled for critical tables.
                  ExtendedInfo includes IsCritical + CriticalSampleOverride.
3.4.1.2026.04.23 - @LockTimeout promoted to public parameter.
                  Overrides preset @i_lock_timeout.  Values:
                  -1 = wait forever, 0 = no wait, N > 0 = seconds.
                  Addresses P3 Error 1222 lock-timeout failures
                  observed in 2026-04-22 diagnostic run.
3.4.0.2026.04.22 - CommandLog intelligence: Phase 3B uses CommandLog
                  ModificationCounter delta for threshold qualification
                  instead of raw counter; stats with delta=0 skip
                  qualification entirely (gh-502).  New sort order
                  MODIFICATION_VELOCITY ranks by mods/hour (gh-507).
                  Phase 5B caches QS scores from CommandLog; Phase 6
                  skips expensive QS DMV joins for stats with fresh
                  cached scores (gh-503).
3.3.5.2026.04.22 - QS forced plan check: replace CHARINDEX scan with
                  sql_expression_dependencies integer join for compiled
                  objects (gh-500); ad-hoc fallback retained.  Parallel
                  mode: lead-worker gate prevents N-worker redundancy
                  (gh-498).  Progress entries: parallel mode now shows
                  global QueueStatistic total as denominator + GlobalStatsDone
                  field in ExtendedInfo XML (gh-499).
3.3.4.2026.04.20 - Bug fix (gh-428 follow-up): removed server-level
                  AG-secondary hard-error from parallel pre-flight.
                  Previously @StatsInParallel=Y failed with severity-16
                  ERROR whenever the server hosted any AG-secondary
                  replicas, even when non-AG USER_DATABASES were
                  also present -- regressing v2 behavior of silently
                  skipping unreadable AG databases and proceeding
                  against the remainder.  Region 04-DB-PARSE
                  already filters AG-secondary databases out of
                  @tmpDatabases and short-circuits when no
                  eligible database remains, so the pre-flight
                  guard was both redundant and incorrect.  Kept
                  gh-428 Check 2 (orphan QueueStatistic row
                  backlog warning).
3.3.1.2026.04.20 - Deploy fix: pre-ALTER-PROCEDURE migration for
                  dbo.QueueStatistic.  ALTER PROCEDURE body has
                  static references to ClaimLoginTime (bd -h9a) and
                  LastStatCompletedAt (v2.3); upgrades from v2.26 or
                  earlier failed compilation with "Invalid column
                  name".  In-proc runtime migrations fire only when
                  @StatsInParallel = Y (too late for compile).  New
                  batches between stub CREATE and ALTER PROCEDURE
                  add both columns if the table exists without
                  them.  Idempotent; no-op on fresh installs.
3.3.0.2026.04.18 - Additive enhancements (5 issues):
                gh-423 @JobName input param -- optional context tag written
                  to CONTEXT_INFO at entry and restored at exit.  Surfaced
                  via Extended Events sessions and sys.dm_exec_requests.
                gh-425 @DeadWorkerTimeoutMinutes NULL coercion (floor 30s)
                  + cross-QueueID stale-row sweep on parallel leader path.
                  Old rows without matching sys.dm_exec_sessions entries are
                  removed to prevent phantom worker inflation.
                gh-426 @WarningsCodesOut OUTPUT param -- pipe-delimited
                  stable code tokens paired with existing @WarningsOut.
                  Every @warnings site now also emits a code token so
                  callers can parse warnings without string matching.
                gh-427 @SkipTablesWithNCCI / @SkipTablesWithCCI params --
                  replaces the single @i_skip_tables_with_columnstore
                  internal flag with per-type controls (type 6 NCCI,
                  type 5 CCI).  Default: skip NCCI (Y), update CCI (N).
                gh-428 Parallel pre-flight validation -- AG-secondary guard
                  and orphan-row backlog check added before queue init when
                  @StatsInParallel = Y.
3.2.2.2026.04.17 - Internal refactors (3 issues, behavior-preserving):
                gh-461 @parameters_string built unconditionally in region 07B;
                  removed duplicate construction in region 08.
                gh-462 mop-up discovery WHERE filter extracted into
                  @mop_up_where_sql -- both parallel-leader and serial
                  paths reference the shared variable.
                gh-465 empty-schema SELECT (WHERE 1=0 sentinel, ~35 cols)
                  extracted into @empty_disc_select -- all 6 staged
                  discovery bailout paths reference the shared variable.
3.2.1.2026.04.17 - Phase 5 bug fix (1 issue):
                gh-492 DIRECT_STRING discovery path (@Statistics param)
                  now honors @TargetNorecompute and @ExcludeStatistics.
                  Previously both filters were only applied in the
                  staged discovery path.
3.2.2026.04.17 - Phase 4 quality/perf batch (9 issues):
                gh-470 per-database warning block (6 checks) now
                  short-circuits when the database has zero stats
                  in #stats_to_process -- saves up to 6 sp_executesql
                  compilations + DMV scans per skipped db.
                gh-460 Phase 6 plan feedback query now bounded by
                  @i_qs_recent_hours + @i_qs_top_plans -- prevents
                  unbounded XML parsing on deep QS retention.
                gh-463 five COUNT_BIG(*) full-scans of #stats_to_process
                  collapsed to one SUM(CASE) pass (NORECOMPUTE,
                  incremental, heap, memory-opt, persisted sample).
                gh-464 orphan CommandLog cleanup materializes END
                  RunLabels into a temp table -- replaces correlated
                  per-row XML NOT EXISTS.
                gh-466 MAX_GRANT_PERCENT hint now token-replaced
                  ({{MAX_GRANT_HINT}}) instead of sentinel-replacing
                  the literal '= 25)' (fragile).
                gh-467 sys.partitions count for incremental stats
                  cached per (database, object_id) across consecutive
                  stats on the same table.
                gh-468 six per-database warning checks (RLS, wide stats,
                  filter mismatch, columnstore, compressed, computed
                  cols) now log failures to @warnings instead of
                  silently swallowing in empty CATCH blocks.
                gh-469 threshold-logic explanation block now gated
                  on @Debug = 1 (was firing on every run).
3.1.2026.04.17 - Phase 1 correctness batch (9 issues):
                gh-451 parallel early-return paths (FINGERPRINT_CONFLICT,
                  MAX_WORKERS, QUEUE_INIT_ERROR) now set all OUTPUT params
                  and return the summary result set.
                gh-452 second LOCK_TIMEOUT restore after the forced-plan
                  check (sp_executesql SET LOCK_TIMEOUT persists).
                gh-453 @QueryStore = AVG_CPU now sorts by average CPU
                  (divides SUM by SUM(count_executions)), not total CPU.
                gh-454 @parameter_fingerprint now includes
                  @QueryStoreMinExecutions and @QueryStoreRecentHours so
                  workers with different QS criteria cannot share a queue.
                gh-455 @MopUpPass + @StopByTime (without @TimeLimit) no
                  longer rejected at validation.
                gh-456 bracket-quoted names in @Statistics
                  (e.g. 'dbo.[My Table].[My Stat]') are now matched --
                  brackets stripped from PARSENAME output.
                gh-457 @log_used_pct reset per-iteration so a failed
                  sp_executesql cannot carry a stale value across dbs.
                gh-458 @SortOrder = RANDOM + @QueryStore enabled now
                  raises a startup warning (QS boost is ignored).
                gh-459 parallel mop-up follower now reports correct
                  MopUpFound (uses @mop_up_queued, not local
                  #stats_to_process which has 0 unprocessed rows).
3.0.2026.04.07 - v3 architecture: preset-first API, 33 input params (was 58),
                + 10 OUTPUT params.  Absorbed 25 params into @i_ internal
                variables controlled by presets.  Table-driven validation,
                6-phase staged discovery only (no fallback path), removed join
                pattern grouping, removed global temp progress table, unified
                mop-up filters.  Full behavioral parity with v2.37.
```

## sp_StatUpdate_Diag

```text
2026.08.02.1 - 0859.9 + 0859.10 (DarlingData-audit findings).
               0859.9: #stat_updates extraction computed
               DATEDIFF(MILLISECOND, StartTime, EndTime) with the
               int-returning DATEDIFF -- a CommandLog row spanning
               more than ~24.8 days (orphaned START whose END was
               written by orphan cleanup weeks later) raised error
               535 and hard-failed the ENTIRE diag run.  Both
               extraction paths (cache + direct) now use
               DATEDIFF_BIG and store NULL for such spans: a
               duration measured to a cleanup marker is fiction,
               and NULL also keeps it out of W2/throughput math.
               0859.10: the four RS 13 ChangePct columns
               (Cpu/CpuPerExec/MemoryGrant/Tempdb) had no
               denominator floor -- a 2 ms baseline reported
               +249,900%, and past ~1e9% the CONVERT to
               decimal(10, 1) overflowed (8115) and killed the
               run.  Both emission paths now floor the baseline
               (100 ms CPU / 0.1 ms-per-exec / 1 MB grant /
               128 tempdb pages -- aligned with the o2md.22 I8
               headline constants, NULL below floor) and clamp at
               +1000%.  The raw First/LastCpuPerExec display
               columns widened decimal(10, 2) -> decimal(18, 2):
               the regression fixture proved the row itself
               overflowed (8115) before any pct math.  Trend
               label columns are unchanged (bounded).
2026.07.30.11 - o2md.18 (maintainer decision): W13
               PERPETUALLY_SKIPPED consumes the proc v3.11.0
               SP_STATUPDATE_REMAINING discovery-candidate log as
               its authoritative source.  When every one of the
               last N completed runs carries the log, the finding
               counts stats present in ALL of their remaining
               lists -- a real qualifies-but-never-reached
               intersection -- with deepest-queued examples and a
               truncation caveat when any run capped its list at
               500.  The prior inference (with its documented
               over-counting) remains as fallback for runs from
               older proc versions.
2026.07.30.10 - o2md.52: @CriticalTables parameter values leaked
               real table names into the obfuscated SAFE_TO_SHARE
               report.  The obfuscation pass rewrites
               #stat_updates and #runs.Databases in place before
               any checks run, but #runs.CriticalTables (sw82
               ingestion, added after BUG-09) was never included
               -- so W16 CRITICAL_TABLES_NO_MATCH, I17
               CRITICAL_TABLES_COVERAGE, and RS8 Parameter Change
               History echoed the configured patterns verbatim.
               Fixed at the source: per-element PAT_ tokens
               (comma-split, seed-prefixed MD5, own namespace --
               deliberately not joinable to TBL_ tokens, same
               acknowledged limitation as BUG-09 multi-database
               lists), map entries under ObjectType
               CriticalTablePattern.  The I4d UNUSED_FEATURES
               hardcoded example was also changed to obviously-
               fake placeholder names so leakage greps on SAFE
               reports cannot false-trip on it.  Found by the
               standard leakage grep while verifying the o2md.51
               report styling; regression test added to the
               obfuscation suite.
2026.07.30.9 - o2md.24: I10 RECOMMENDED_CONFIG severity-precedence
               rewrite (maintainer option a).  I10 -- the "just run
               this" output -- previously consumed only 4 finding
               categories and contradicted higher-severity findings
               on the same server (recommended @StatsInParallel=Y
               where CRITICAL SILENT_NOOP_RUN implicated parallel
               mode; echoed thresholds W12 said to change; never
               emitted @MopUpPass on parallel fleets).

               Precedence policy: CRITICAL > WARNING > baseline
               echo; conflicts render as explicit /* CONFLICT */
               and /* CAVEAT */ comment lines appended to the
               generated call instead of silently picking a side.

               - SILENT_NOOP_RUN (CRITICAL): blocks W3's newly-
                 enable-parallel suggestion (conflict comment);
                 baseline parallel=Y is kept (the remedy is the
                 version fix, not disabling parallel) with a
                 caveat gated on max observed VersionKey vs
                 3008002 (3.8.2 mop-up fall-through fix).
               - PRIORITY_PASS_EMPTY (W12): version-gated per
                 the o2md.17 convention -- below 3.7.0 emit an
                 upgrade-first caveat (retuning changes nothing
                 on the effective_counter defect); 3.7.0+ halve
                 @ModificationThreshold (floor 100).
               - QS_PERFORMANCE_TREND (I8): with QS off and real
                 trend data (UNAVAILABLE sentinel rows ignored),
                 enable @QueryStore=CPU + QUERY_STORE sort --
                 unless QS_NOT_EFFECTIVE also fired (WARNING
                 beats INFO; conflict comment).
               - Stale v2.24-era gate removed: @MopUpPass now
                 emits on parallel configurations too (v3
                 validation only requires @TimeLimit/@StopByTime,
                 always present in the generated call).  This
                 gate was why 31/40 parallel fleet servers never
                 saw the @MopUpPass their own W12 recommended.

2026.07.30.8 - Maintainer-decision batch 1 (o2md.19, o2md.14,
               o2md.33, o2md.35).  All four implement explicit
               maintainer choices recorded on the bd issues.

               o2md.19 - W3 STALE_BACKLOG zero-processed guard:
               when EVERY non-killed run in the window confirmed
               StatsProcessed = 0, W3 defers to C6 (throughput
               advice is nonsense for pure no-op windows).  Mixed
               windows keep current behavior including the kzx0
               un-suppression (untouched on @w3_parallel_complete,
               so the COMPLETION fast-path effect is unchanged).
               NULL StatsProcessed blocks the deferral (o2md.12:
               NULL is unconfirmed, not zero).

               o2md.14 - C4 thin-window cap: either window under
               5 runs caps severity at WARNING (2-run floor still
               fires).  Evidence gains thin-baseline + explicit
               survivorship wording (windows contain only runs
               that completed and processed work).

               o2md.33 - AG-secondary by-design stops (StopReason
               = AG_SECONDARY with StatsFound = 0) excluded from
               grading aggregates (completion average +
               kill/time-limit percentage denominators) and
               surfaced as new INFO check I19
               AG_SECONDARY_BY_DESIGN.  The genuine secondary-
               host defect signature (StatsFound > 0, processed
               0) still fires C6/W3 -- their StatsFound > 0 gates
               already excluded the by-design state.  Verified
               proc-side: non-AG user databases on secondary
               hosts are already maintained (per-database
               deselection, hard stop only when nothing remains).

               o2md.35 - RS5/RS7 blank-column reason tokens,
               additive only (existing column types/positions
               untouched): RS5 Top Tables gains ImpactDataStatus
               (OK / NO_QS_DATA) explaining
               WorkloadImpactPct/ImpactScore NULLs; RS7 Long-
               Running gains SampleDataStatus (OK /
               RESAMPLE_PERSIST) explaining AvgSamplePct NULLs.
               Applied in both output modes (direct SELECT and
               FOR JSON).
2026.07.30.7 - o2md.50: consume the proc v3.8.5 observability fields
               (follow-up to proc-side o2md.46/o2md.49).

               W6 EXCESSIVE_OVERHEAD now subtracts the four
               measured self-idle-time buckets (Summary XML
               DelayWaitMs / AgRedoWaitMs / RetryBackoffMs /
               QueueClaimMs, ingested into #runs) from the
               overhead percentage in both CTEs, so the figure is
               genuinely-unattributed time rather than
               "everything that isn't an UPDATE STATISTICS".
               Absent elements read as 0 (NULLIF-omitted on the
               proc side; arithmetically correct for both
               zero-on-3.8.5+ and unmeasured-on-older) and the
               evidence line states explicitly whether the worst
               run's figure had measured idle excluded (VersionKey
               >= 3008005) or predates measurement.

               W5 QS_NOT_EFFECTIVE family: per-stat
               ExtendedInfo/FromMopUp (reliable per-row flag)
               ingested through the cache into #stat_updates and
               preferred over the QualifyReason = MOP_UP
               inference on 3.8.5+ runs -- the session-scoped
               @in_mop_up behind QualifyReason mislabels
               priority-pass leftovers processed after the
               mop-up flag flips, so those rows now correctly
               count as priority-pass evidence.  Pre-3.8.5 runs
               and unparseable versions keep the QualifyReason
               inference (this was the exact upgrade path the
               o2md.16 comment anticipated).  Cache table gains
               the FromMopUp column via the existing
               auto-upgrade ALTER path; cache rows written
               before this upgrade read NULL even if the source
               XML had the element (accepted limitation, same
               as sw82).
2026.07.30.6 - Stale v3 parameter references in user-facing
               recommendation/@Help text (live user report:
               @CleanupOrphanedRuns was appearing in Diag alert
               text as something to set, even though v3 runs
               orphan cleanup automatically and removed the
               parameter).  Auditing the pattern found five more
               instances of the same defect class -- v2 params
               the v3 migration absorbed into internal variables
               or removed outright, still referenced as if they
               were live public parameters:

               - @Help "Killed Run Detection" topic told the
                 reader orphans are "cleaned up by
                 @CleanupOrphanedRuns"; now explains cleanup is
                 automatic on every v3 call and the parameter
                 was removed in the v2->v3 migration.
               - W15's PARALLEL_ORPHAN_BACKLOG and
                 STALE_QUEUE_SWEEP recurring-code recommendations
                 both told the reader to "run with
                 @CleanupOrphanedRuns=Y"; replaced with what
                 recurring large sweeps actually indicate (job
                 kills/timeouts) since the sweep itself is no
                 longer optional.
               - W15's AZURE_SQL_MI recommendation told the
                 reader to "verify @IncludeSystemObjects
                 behavior on MI"; the parameter was removed in
                 v3 (system objects are always excluded from
                 discovery) so there is nothing to verify.
               - W6 EXCESSIVE_OVERHEAD recommendation and code
                 comment suggested "@DelayBetweenStats (if set)"
                 and "reduce @CommandLogRetentionDays" -- both
                 v2 params folded into preset-controlled internal
                 variables (@i_delay_between_stats,
                 @i_command_log_retention_days) with no public
                 v3 knob.  Reworded to describe the underlying
                 concept instead of a settable parameter.
               - o2md.18(c)'s code comment referenced
                 @LogSkippedToCommandLog, the v2 parameter name
                 for what is now the internal @i_log_skipped
                 variable (off by default, no public toggle).

               Two of the six were self-inflicted: the
               immediately-prior 2026.07.30.4 batch (o2md.27)
               introduced the @DelayBetweenStats and
               @CommandLogRetentionDays references while fixing
               W6 -- caught only because the user's live report
               prompted auditing for the same defect class rather
               than trusting the single reported instance.

2026.07.30.5 - Redundant/tautological findings + obfuscation
               hardening (o2md.43, o2md.44, o2md.28, o2md.36).

               o2md.43 - W7 HIGH_IMPACT_STATS_DEPRIORITIZED
               compared each top-10 CPU stat's average processing
               position against the overall median -- true, by
               construction, for roughly half of any top-10 list
               regardless of whether prioritization is working,
               and it unconditionally recommended enabling
               @QueryStore=CPU even on servers already running
               it. Gated on the most recent run actually using
               QUERY_STORE sort order: that is the only case
               where "still processed late" is a real signal
               (QS enrichment/scoring not working), and the
               recommendation now points there instead of at a
               sort order that is already active.

               o2md.44 - I9 WORKLOAD_CONCENTRATION fired when the
               top stats reached 80% of workload CPU -- the
               Pareto principle, true of nearly every workload
               distribution, not a defect signal. Reframed as
               neutral descriptive text; no longer suggests
               enabling @QueryStore=CPU when the most recent run
               already has it on (reuses the o2md.43 sort-order
               check).

               o2md.28 - W5 QS_NOT_EFFECTIVE (WARNING), I6
               QS_EFFICACY, and I8 QS_PERFORMANCE_TREND all
               independently reported "no QS data" for the same
               15 servers -- three findings, one fact (~45
               redundant findings out of 446 in the fleet sweep).
               I6 and I8's no-data branches now check for an
               existing W5 QS_NOT_EFFECTIVE row first and skip
               the duplicate. Implemented as nested IF/ELSE
               (matching each block's original outer condition)
               rather than an ELSE IF chain, which would have let
               the suppressed case fall through into the
               has-real-data branch when @Debug = 0.

               o2md.36 - obfuscation tokens were 6-hex (Schema
               tokens were actually 4-hex, the confirmed cause of
               a fleet-wide SCH_ collision), a trivial dictionary-
               attack target with no seed. All 20 HASHBYTES call
               sites now produce 8-hex tokens uniformly (matching
               the width already used for RunLabel tokens).
               @ObfuscationSeed NULL now auto-generates and
               RAISERROR's a random per-run seed instead of
               hashing unsalted -- defense-in-depth for direct
               EXEC callers (the wrapper already requires a seed
               for -Obfuscate). Not returned as an OUTPUT
               parameter, to avoid a breaking signature change
               for existing callers passing @ObfuscationSeed by
               literal value.

2026.07.30.4 - Diag correctness batch (o2md.15, o2md.23, o2md.25,
               o2md.27).

               o2md.15 - C4 DEGRADING_THROUGHPUT recommendation
               and ExampleCall were constants that always
               suggested enabling @StatsInParallel, even when the
               evidence line already prints StatsInParallel=Y
               (5/5 findings in the fleet sweep). Now conditional
               on recent_w.parallel_mode.

               o2md.23 - I8 headline required BOTH an
               improving-count majority AND a median magnitude
               below -5% to report "improving" -- when they
               disagreed (336 improving / 301 degrading, median
               only -3%), it fell through to "stable" regardless
               of the clear count majority. New intermediate
               "trending down (modest)" branch reports the
               count-majority honestly, placed after the more
               specific regressed/degrading branches so it
               doesn't preempt them.

               o2md.25 - I10 TimeLimit jitter normalization only
               rounded when MAX-MIN spread across the whole
               @DaysBack window was under 300s; a single outlier
               (rollout run, stray non-killed-but-atypical run)
               blew the spread and the raw jittery value (21598,
               31499, etc.) was emitted verbatim in 40/40 fleet
               recommendations. Replaced with MODE (most common
               rounded-to-minute value), applied unconditionally
               -- robust to a handful of outliers by construction.

               o2md.27 - W6 EXCESSIVE_OVERHEAD's guard
               (DurationSeconds > 10 AND StatsProcessed >= 5) was
               too weak: a 34-stat run still showed 93.4%
               overhead because fixed startup cost dominates when
               measured stat-update time is tiny, regardless of
               stat count. Tightened to DurationSeconds > 60 AND
               measured stat-time >= 10s (both CTEs kept in
               sync). Recommendation text now states the
               overhead figure is wall-clock minus measured
               stat-update time -- inferred, not measured -- and
               lists @DelayBetweenStats, AG-redo waits, lock
               waits, and parallel-worker queue-idle as untested
               alternative causes before attributing to
               discovery/environment cost.

2026.07.30.3 - o2md.12: C6 SILENT_NOOP_RUN evidence + NULL-vs-0
               split.  Evidence previously carried only
               StartTime/StopReason/StatsFound -- the
               recommendation text says "upgrade to 3.5.7+"
               without ever showing the installed version.  Now
               includes Version, StatsInParallel, DurationSeconds,
               StatsRemaining (all already tracked on #runs).

               Also fixed a false-positive vector:
               ISNULL(r.StatsProcessed, 0) = 0 treated a
               genuinely NULL StatsProcessed (older proc build,
               or a truncated/atypical END writer) the same as a
               confirmed 0.  CRITICAL C6 now requires
               StatsProcessed = 0 explicitly; the NULL case gets
               its own new INFO-severity check (I18
               NOOP_UNCONFIRMED) instead of being silently
               mis-classified as either a confirmed no-op or
               dropped entirely.

2026.07.30.2 - ExampleCall audit sweep, round 1 (o2md.20, o2md.21).

               o2md.20 - I15 HEAP_TIME_BUDGET referenced
               @CollectHeapForwarding, a parameter removed in v3
               -- the emitted example failed with "is not a
               parameter for procedure sp_StatUpdate" on every
               server where it fired (7/40 in the fleet sweep).
               Recommendation text and ExampleCall now point at
               sys.dm_db_index_physical_stats(...).forwarded_
               record_count directly, which is what the removed
               parameter used to surface anyway.

               o2md.21 - W2 LONG_RUNNING_STATS ExampleCall omitted
               @Databases, so copy-pasting it silently targeted
               only the current database instead of the flagged
               server.  Now built from the finding's own
               su.DatabaseName.  Also notes the ~10M-row sample
               cap (v1.8) when the flagged stat's average row
               count exceeds it, since the requested
               @LongRunningSamplePercent overstates what will
               actually be sampled on very large tables.

2026.07.30.1 - o2md.42: named PRIMARY KEY constraints on 5 temp
               tables (#stat_updates, #recommendations,
               #run_version_keys, #forced_plans, #qs_efficacy)
               unnamed.  Temp table NAMES get a per-session
               uniquifier; named CONSTRAINTS on those tables do
               not, so two concurrent Diag sessions on the same
               instance collided on the constraint name and the
               second session's CREATE TABLE failed outright
               ("There is already an object named 'PK_stat_
               updates'").  Mechanical: constraints still exist,
               SQL Server now generates the names.

2026.07.28.1 - Fleet analysis batch (sp_StatUpdate-o2md).  Findings from the
               40-server production diagnostic sweep of 2026-07-28, all of
               them cases where a check asserted more than its evidence
               supported.

               o2md.10 - W16 CRITICAL_TABLES_NO_MATCH was a 100% false
               positive: sp_StatUpdate serializes parameters with
               FOR XML ... ELEMENTS XSINIL, so an unset @CriticalTables
               arrives as '' rather than NULL and the duplicated
               IS NOT NULL guard was always true.  Now a real emptiness
               test.  Same XSINIL blast radius fixed in the C4 parallel_mode
               bucketing, where '' formed a third mode distinct from 'N'.

               o2md.11 - C6 SILENT_NOOP_RUN asserted one root cause (the
               3.5.7 parallel queue-leadership fix) even for COMPLETED /
               NATURAL_END runs, a different code path that fix does not
               touch.  The recommendation now branches on the observed
               StopReason mix and on the parsed run version, and refuses to
               advise an upgrade when the runs already report 3.5.7+.
               Flagged runs are materialized once (#c6_noop_runs) instead of
               duplicating the predicate between count and evidence.

               o2md.13 - C4 DEGRADING_THROUGHPUT graded parallel windows on
               sec/stat, which in parallel mode measures this worker's slice
               against the whole worker lifetime (~10x disagreement with
               GB/min observed on the fleet).  A parallel window is now only
               CRITICAL when the slice-independent GB/min metric agrees;
               otherwise WARNING, with the disagreement stated.

               o2md.16 - W5 QS_NOT_EFFECTIVE blamed customer Query Store
               configuration for a proc-side gap: the mop-up pass carries no
               QS fields, so a mop-up-only run is indistinguishable from a
               run with QS disabled.  Mop-up-only runs are excluded from both
               the numerator and the denominator, and an all-mop-up window
               now reports QS_NOT_EVALUABLE (INFO) instead.

               o2md.17 - W12 PRIORITY_PASS_EMPTY recommended lowering
               @ModificationThreshold even when the priority pass qualified
               EXACTLY zero stats while mop-up found real work -- a state no
               threshold value produces.  That case is now CRITICAL and
               version-gated against the 3.7.0 effective_counter fix.

               o2md.18 - W13 PERPETUALLY_SKIPPED printed "0 of 0 discovered"
               when the position aggregates were unmeasurable (ISNULL(...,0)
               on missing ProcessingPosition data), and asserted the time
               limit as the cause even when no run in the window was
               time-limited.  Both aggregates are now nullable, the finding
               is suppressed when degenerate, and the cause/recommendation
               branch on whether any run actually ran out of time.  The
               over-counting sub-issue is documented in place as NOT FIXED --
               it is not derivable from CommandLog.

               o2md.22 - I8 QS_PERFORMANCE_TREND emitted impossible
               percentages ("avg 9014150.4% change", "1681.8% reduction")
               because it averaged unbounded ratios with no denominator
               floor -- one 1 KB -> 20 MB stat dominated the result, and a
               slightly larger outlier would have overflowed decimal(10,1)
               and killed the whole run.  Now a MEDIAN of per-stat deltas
               with a baseline floor and a clamp, sample size disclosed, and
               the reduction/increase label taken from the SIGN of the median
               rather than from the improving/degrading counts (which is what
               let a degrading trend be reported as a "reduction").

2026.07.25.1 - @Ansi opt-in: also render Executive Dashboard + Recommendations
               as colorized text on the Messages stream (RAISERROR), in
               addition to the normal result sets.  Reads the populated
               #executive_dashboard / #recommendations temp tables (no
               recomputation), block glyphs via NCHAR (deployment-safe),
               dynamic lines %-escaped.  Console-only; result sets, history,
               and OUTPUT params unchanged.  Default N = result sets only.

2026.07.23.2 - Empty-CommandLog single-result envelope (0pgq / gh-557):
               With @SingleResultSet = 1 and no usable CommandLog
               data, the early-return branch previously emitted only
               ResultSetIDs 1-2, silently violating the advertised
               stable-ID contract (IDs 1-13).  IDs 3-13 now each
               carry one marker row whose RowData JSON contains
               DataStatus = UNAVAILABLE, Reason = NO_COMMANDLOG_DATA,
               and the ResultSetName, so automation can distinguish
               unavailable source data from zero findings without
               message parsing.  IDs 1-2 content unchanged; the
               multi-result-set empty path (gh-304) unchanged.
               Also fixed: the empty-path ID 2 RowData subquery was
               uncorrelated, concatenating all recommendation rows
               into one invalid JSON string per row -- now correlated
               on FindingID with sequential RowNum, matching the
               normal-path RS2 shape.

2026.07.23.1 - C4 adjacent-window fix (ehwn / gh-558):
               C4 DEGRADING_THROUGHPUT now compares the recent
               @ThroughputWindowDays against the immediately
               preceding window of equal length.  PRIOR previously
               had no lower bound, so all loaded history outside
               the recent window (up to @DaysBack) entered the
               baseline and @DaysBack silently changed C4 results.
               Runs older than 2 x @ThroughputWindowDays are now
               excluded, and @ThroughputWindowDays is validated to
               be at most @DaysBack / 2 (was: at most @DaysBack).

2026.07.03.1 - I8/RS13 DataStatus surface (w5p2 / gh-532):
               I8 and RS13 no longer go silently empty when Query Store
               CPU history is sparse or absent.  New @i8_data_status
               (AVAILABLE / INSUFFICIENT_HISTORY / UNAVAILABLE) computed
               from QS-CPU-bearing runs and tracked-stat counts.  Sparse
               I8 findings carry a machine-readable suffix:
               [DataStatus: X; QSCpuRuns: N; TrackedStats: N] (AVAILABLE
               findings unchanged -- no suffix).  RS13 returns a single
               [NO DATA] sentinel row (full production column list, reason
               text in StatisticsName) instead of zero rows whenever
               status <> AVAILABLE, on both the multi-result-set and
               @SingleResultSet paths.  The empty-CommandLog early return
               emits the same I8 UNAVAILABLE finding and RS13 sentinel so
               first-run users get the explanation too.  @Help I8 catalog
               entry updated.
2026.07.02.2 - Phase-C diagnostic batch (sw82, 9ufm, atk1, 9axc):
               (sw82) @CriticalTables coverage: ingest CriticalTables /
               CriticalSamplePercent / CriticalTablesFirst into #runs; ingest
               IsCritical / CriticalSampleOverride into #stat_updates (both cache
               and direct paths).  New W16 CRITICAL_TABLES_NO_MATCH: fires when
               CriticalTables configured but zero IsCritical=1 stat rows in that
               run.  New I17 CRITICAL_TABLES_COVERAGE: leadership-friendly summary
               when critical stats exist (count updated, avg ProcessingPosition,
               override count).  Both checks registered in the @Help catalog.
               Cache auto-upgrade adds IsCritical / CriticalSampleOverride columns.
               (9ufm) Version-aware param gating: #runs gains HasFullParamsLog
               (1 when the run's sp_StatUpdate version >= 2.15, the full-parameter-
               logging era).  W1a / W1b / I4a / I4c / I4d only reason about absent
               parameter elements on such runs -- there, FOR XML omitting NULL
               params means absence IS the signal (TimeLimit absent = genuinely
               unlimited).  Pre-2.15 runs are excluded from param reasoning, so they
               no longer produce false UNUSED_FEATURES / SUBOPTIMAL_PARAMS findings.
               (Per-element .exist() gating was rejected: it gates each check on
               the very element whose absence the check detects.)
               (atk1) Executive Dashboard TIME_LIMIT annotation: COMPLETION Headline
               gains a ' [!] Time limit exhausted in N% of runs' banner when the
               C3 threshold is met; COMPLETION Detail gains a CompletionReason clause;
               OVERALL Detail gains a short flag.  Score/grade math unchanged;
               12 existing structure tests remain valid.
               (9axc) History watermark robustness: StatUpdateDiagHistory gains
               MaxStartTime datetime2(3) NULL via ALTER-if-missing pattern.
               Watermark WRITE now records both MaxCommandLogID and MaxStartTime.
               Watermark READ selects MAX(MaxStartTime) into @history_max_run_start.
               NOT EXISTS on RunLabel remains the final duplicate guard.
               (hardening) All three #stat_updates ingestion builds now start with
               a CONVERT(nvarchar(max), N'') operand: sub-4000-char literal pieces
               previously made intermediate concats nvarchar(4000), silently
               truncating the assembled batch at 4000 chars -- the direct-path
               batch lost its WHERE tail and every ingest failed with
               'Incorrect syntax near AND' while the proc kept running with an
               empty #stat_updates.  Cache CREATE path brought into lockstep with
               the upgrade ALTERs (IsCritical / CriticalSampleOverride).
2026.07.02.1 - Phase-B diagnostic batch (70hh, 1h2l, 8gws, xmxs, 4g9s, t8lj):
               (70hh) Skip-row ingestion fix: SKIPPED: Command and TOCTOU_SKIP
               ErrorMessage rows excluded at cache and direct-path ingestion
               layers.  They no longer pollute duration/appearance/top-table
               aggregates.  StatsToctou ingested from END Summary XML into
               #runs and surfaced in RS4 Run Detail.
               (1h2l) QSEnrichmentSkipped ingested from END Summary XML into
               #runs.  W5 QS_NOT_EFFECTIVE now uses it as the primary signal
               (direct element read); inference via QSPlanCount/CPU fallback
               retained for older rows where element is absent.
               (8gws) I4 UNUSED_FEATURES catalog refresh: added I4c
               (@FirstTimeFullScanCapRows, v3.5.4), I4d (@CriticalTables,
               v3.5.0), I4e (@AbortOnIntegrityError, v3.5.9 -- only
               surfaces when IO_CORRUPTION stop reason is seen in history).
               (xmxs) Removed dead v2 leftover: GroupByJoinPattern XPath
               extraction, @latest_group_join variable, and GroupByJoinPattern
               column from #runs and RS8 Parameter History.  Element is absent
               in v3 START XML and variable had zero consumers.
               (4g9s) New W14 RECURRING_SAFETY_STOP: fires when the same
               safety StopReason (TEMPDB_PRESSURE, AG_REDO_QUEUE,
               LOG_SPACE_HIGH, IO_CORRUPTION, FAIL_FAST,
               CONSECUTIVE_FAILURES, BATCH_LIMIT) appears in >= 2 runs.
               One finding row per recurring reason with reason-specific
               remediation.  Evidence caps at 10 most-recent StartTimes.
               (t8lj) New WarningsCodes column in #runs, ingested from END
               Summary XML (pipe-delimited tokens, v3.5.9+).  New W15
               RECURRING_RUNTIME_WARNING (environmental/actionable codes)
               and I16 RECURRING_RUNTIME_WARNING (informational codes) fire
               when a code appears in >= 3 runs or >= 50%% of non-killed
               runs.  Old runs without element produce zero findings.
2026.06.12.1 - Silent no-op parallel run detection (sp_StatUpdate-kzx0,
               validated against the -v31y zombie-queue incident).
               (1) New CRITICAL check C6 SILENT_NOOP_RUN: flags
               non-killed runs that report a normal-completion
               StopReason (PARALLEL_COMPLETE/COMPLETED/NATURAL_END)
               with StatsFound > 0 but StatsProcessed = 0, when no
               other worker in the same time window processed
               anything and no sp_StatUpdate UPDATE_STATISTICS
               activity overlaps the run window.  Catches zombied
               queue leadership and any future claim-nothing
               failure that self-reports success.
               (2) W3 STALE_BACKLOG un-suppressed when the C6
               signature is present: @w3_parallel_complete is
               forced 0 so the backstop check and the COMPLETION
               grade-A fast path no longer trust PARALLEL_COMPLETE
               blindly (also closes the grade-A hole for zombie
               joiners that report StatsRemaining = 0 from an
               empty skip-discovery temp table).
               (3) #qs_efficacy completion math: PARALLEL_COMPLETE
               with StatsRemaining > 0 is self-contradictory
               telemetry -- CompletionPct now cross-checks the
               worker group (overlapping parallel runs) and uses
               SUM(StatsProcessed) / MAX(StatsFound) instead of a
               constant 100.0 when the group did not drain the
               queue.  WorkloadCoveragePct same treatment.
               Genuinely-drained runs (group SUM(processed) >=
               MAX(found), or remaining = 0) keep the 3j5l
               (2026.05.28.1) behavior -- no false-F regression.
2026.06.10.1 - Catalog completion (sp_StatUpdate-8gzw follow-on to
               g4c0): added the 10 emitted-but-undocumented checks
               to the @Help checks catalog -- W7
               HIGH_IMPACT_STATS_DEPRIORITIZED, W8 MOPUP_INEFFECTIVE,
               W9 LOCK_TIMEOUT_INEFFECTIVE, W10 PARAMETER_CHURN,
               C5 SAMPLE_RATE_DEGRADATION, I9 WORKLOAD_CONCENTRATION,
               I11 FAILURE_CLUSTERING, I12 QS_COVERAGE_DRIFT,
               I13 PARALLEL_OPPORTUNITY, I14 MOPUP_MISSING_PAGECOUNT.
               All ten existed as real checks (v2.29 generation +
               Phase 3) but never appeared in @Help.  No behavior
               change to the checks themselves.
2026.06.08.2 - Cosmetic: added QS_PARALLEL_UNRELIABLE to diagnostic
               checks catalog (sp_StatUpdate-g4c0).
2026.06.08.1 - sp_StatUpdate-6x80: W5/I10 QS-parallel refinements.
               (1) W5 parallel-aware: when all QS-priority runs use
               @StatsInParallel='Y' and QS CPU data IS present in
               stat updates (QSTotalCpuMs > 0), W5's zero-data-runs
               branch is suppressed -- per-worker ProcessingPosition
               counters make the "QS data not captured" signal
               unreliable in parallel mode.  A softer INFO note is
               inserted instead of a WARNING.
               (2) I10 W5-conditional QS drop: I10 now only reverts
               @SortOrder to MODIFICATION_COUNTER and clears
               @QueryStore when W5 fired due to genuine absence of
               QS data (not a parallel-ordering artifact).  When W5
               fired but runs are parallel AND QS CPU data was
               observed, I10 keeps @SortOrder=QUERY_STORE and
               @QueryStore=CPU.
               (3) TimeLimit jitter normalization: when MAX-MIN of
               TimeLimit across recent non-killed runs is < 300 s,
               I10 rounds to the nearest minute
               (ROUND(AVG / 60.0, 0) * 60) to avoid echoing jittery
               @StopByTime-derived values like 21599/21598.
               (4) @CriticalTables hint: when W9
               LOCK_TIMEOUT_INEFFECTIVE has fired, I10 appends a
               suggested @CriticalTables comment to the recommended
               EXEC call listing the top contended tables so the
               operator can apply per-table priority boost.
2026.05.28.1 - Two parallel-mode false-positive fixes.
               (1) sp_StatUpdate-772k: #runs dedup tiebreaker is now
               ORDER BY StartTime DESC, IsKilled ASC, EndTime DESC.
               The prior 2026.04.20.1 EndTime DESC ordering could
               pick a synthetic KILLED orphan-cleanup END over the
               real END (the KILLED row often has a later EndTime),
               falsely flagging completed parallel runs as killed.
               IsKilled ASC guarantees the real END (IsKilled = 0)
               wins.
               (2) sp_StatUpdate-3j5l: completion_pct now returns
               100.0 when StopReason IN (COMPLETED, PARALLEL_COMPLETE).
               In parallel mode StatsFound is the whole-queue total
               while StatsProcessed is this worker's slice, so the
               raw ratio understated completion (e.g. 50%) and
               dragged down the 40%-weighted run-health grade.
2026.04.22.1 - W13 PERPETUALLY_SKIPPED: new WARNING when stats are
               discovered but never updated across N consecutive runs
               due to time limits (gh-504).  Recommends @SortOrder=
               MODIFICATION_VELOCITY or increasing @TimeLimit.
2026.04.20.2 - C1 KILLED_RUNS Evidence truncation fix.  When 90+
               killed runs existed, the STRING_AGG of all killed
               run StartTimes into the Evidence column overflowed
               nvarchar(2000) and raised Msg 2628 'String or binary
               data would be truncated'.  Capped to the 10 most
               recent dates with a "(10 most recent of N)" prefix
               when truncation happens.
2026.04.20.1 - Fix PK_runs violation during #runs INSERT.
               The INSERT LEFT JOINs SP_STATUPDATE_START to
               SP_STATUPDATE_END on RunLabel; CommandLog can
               contain multiple ENDs per RunLabel (orphan-cleanup
               KILLED record + real END from gh-425) or duplicate
               STARTs (same-second retries), producing multiple
               rows per RunLabel and a PK violation before the
               existing dedup could run.  Removed CONSTRAINT
               PK_runs from CREATE TABLE; added UX_runs_RunLabel
               unique index after the dedup CTE.  Dedup tiebreaker
               now ORDER BY StartTime DESC, EndTime DESC so the
               real END is preferred over the KILLED orphan
               record.
2026.04.18.1 - Parallel false-positive fix (c53, 3 checks):
               C3 TIME_LIMIT_EXHAUSTION: suppress when all non-killed runs have
                 StatsInParallel='Y' and aggregate StatsRemaining=0 (all workers
                 used parallel coordination; per-worker TIME_LIMIT exits with zero
                 aggregate remaining are not a genuine time-limit signal).
               W3 STALE_BACKLOG: extend @w3_parallel_complete to also set=1 when
                 @parallel_aggregate_done=1, covering TIME_LIMIT exits alongside
                 the existing PARALLEL_COMPLETE-only path.
               COMPLETION grade: inherits improvement via @w3_parallel_complete
                 (already awards score=95 when @w3_parallel_complete=1).
2026.04.17.1 - Diag bug batch Phase 2 (10 issues):
               gh-471: Sync @procedure_version with header Version.
               gh-472: Auto-upgrade StatUpdateDiagHistory via COL_LENGTH-gated ALTER.
               gh-473: C3 denominator/numerator parity -- filter @time_limit_runs by
                 IsKilled=0 to match @total_completed_runs.
               gh-474: Move @latest_qs_run DECLARE above IF/ELSE so SingleResultSet
                 path can reference it.
               gh-475: W12 clamp StatsProcessed-MopUpProcessed to >=0 via CASE WHEN.
               gh-476: W7 evidence filter uses @w7_median_position (ISNULL,0) instead
                 of hard-coded > 5.
               gh-477: C5 evidence GROUP BY includes DatabaseName/SchemaName/ObjectName
                 to prevent collision across tables sharing a stat name.
               gh-478: W6 clamp overhead_pct to [0,100] to avoid negative display
                 when stat_ms slightly exceeds wall-clock due to clock skew.
               gh-479: C4 cross join requires recent_w.parallel_mode = prior_w.parallel_mode
                 to prevent cross-mode throughput comparisons.
               gh-480: Dashboard SingleResultSet path -- initialize @avg_completion
                 and @tl_run_pct in @w3_parallel_complete=1 branch so downstream
                 COMPLETION detail text always has a value.
2026.04.13.2 - PARALLEL_COMPLETE parity and new checks (4 issues):
               NaturalEndRuns (RS3): now counts PARALLEL_COMPLETE in addition to
                 COMPLETED so parallel runs are recognised as natural completions.
               workload_pct (#qs_efficacy): PARALLEL_COMPLETE treated as 100% coverage
                 same as COMPLETED (all work distributed across workers finished).
               W1d mop-up suggestion: fires for PARALLEL_COMPLETE runs in addition to
                 COMPLETED/NATURAL_END runs (parallel runs with time budget unused).
               W12 PRIORITY_PASS_EMPTY: new WARNING when mop-up does all the work
                 (priority pass averages <5 stats across 3+ runs while mop-up runs).
               SPEED grade parallel-aware: when all non-killed runs are parallel
                 (@StatsInParallel='Y'), grade is based on GB/min throughput instead
                 of sec/stat wall-clock (which underestimates per-worker speed).
2026.04.13.1 - Parallel-mode false positives (7 issues):
               W3 STALE_BACKLOG: suppress when all non-killed runs are PARALLEL_COMPLETE
                 with AvgStatsRemaining=0 (per-worker remaining is not a real backlog).
               W4 OVERLAPPING_RUNS: suppress when both overlapping runs have
                 StatsInParallel='Y' (intentional two-schedule parallel pattern).
               COMPLETION grade: set score to 95 when all non-killed runs end with
                 PARALLEL_COMPLETE and AvgStatsRemaining=0 (aggregate = done).
               C3 TIME_LIMIT_EXHAUSTION: also count COMPLETED runs with StatsRemaining>0
                 as functionally time-limited.
               RS3 AvgDurationSec: exclude killed runs with StatsProcessed=0 (48-hour
                 phantom durations from orphan-cleanup synthetic END records).
               I10 RECOMMENDED_CONFIG: removed stray @TieredThresholds and
                 @CleanupOrphanedRuns references from I10 code path (v2 params).
               I11 FAILURE_CLUSTERING: escalate from INFO to WARNING when C2
                 REPEATED_FAILURES is also active and top error covers >80% of failures.
2026.03.26.1 - RS 13 memory grant and tempdb spill trending (#368): FirstMemoryGrantKB,
               LastMemoryGrantKB, MemoryGrantChangePct, MemoryGrantTrend, FirstTempdbPages,
               LastTempdbPages, TempdbChangePct, TempdbTrend columns.  I8 headline includes
               memory grant trends when data available.  New #stat_updates columns:
               QSTotalMemoryGrantKB, QSTotalTempdbPages (extracted from ExtendedInfo XML).
               Cache table auto-upgrade for new columns.
2026.03.26   - W8 MOPUP_INEFFECTIVE: warn when mop-up consistently finds 0 stats (#347).
               W9 LOCK_TIMEOUT_INEFFECTIVE: persistent lock timeout victims (#348).
               W10 PARAMETER_CHURN: key parameters changing frequently (#350).
               C5 SAMPLE_RATE_DEGRADATION: effective sample rate dropped >50% (#345).
               I11 FAILURE_CLUSTERING: dominant error code detection (#346).
               I12 QS_COVERAGE_DRIFT: QS plan count dropping >50% week-over-week (#349).
               I13 PARALLEL_OPPORTUNITY: serial runs using <40% of TimeLimit (#351).
               I14 MOPUP_MISSING_PAGECOUNT: throughput underestimation warning (#342).
               I10 mutual exclusion validation guard (#343).
2026.03.24   - Mop-up awareness: #runs gets MopUpTriggered/MopUpFound/MopUpProcessed
               from SP_STATUPDATE_END summary XML.  RS4 Run Detail includes mop-up columns.
               W1d recommendation: suggest @MopUpPass when runs complete early (<50% of
               TimeLimit used).  I10 RECOMMENDED_CONFIG includes @MopUpPass when W1d fires.
               Fix: split #runs INSERT dynamic SQL to avoid nvarchar(4000) literal truncation.
             - GB throughput metrics: TotalGB computed from per-stat PageCount aggregation.
               RS3 Run Health Summary: AvgTotalGB, AvgGBPerMin.
               RS4 Run Detail: TotalGB, GBPerMin per run.
               RS10 Efficacy Trend: AvgGBPerMin per week.
               C4 DEGRADING_THROUGHPUT: Evidence includes GB/min for both windows.
               Dashboard SPEED: headlines include GB/min and GB/run volume context.
2026.03.23.1 - I10 RECOMMENDED_CONFIG: synthesized parameter set based on diagnostic
               findings and parameter history.  Preserves safeguards from prior runs,
               adjusts flagged parameters, suggests untracked safeguards (@MinTempdbFreeMB,
               @MaxConsecutiveFailures, @MaxAGRedoQueueMB).
2026.03.23   - NULL RunLabel fix for legacy CommandLog entries (ISNULL fallback).
             - INSERT...EXEC safety: auto @SkipHistory=1, DB_ID guard, TRY/CATCH on map write.
             - @ObfuscationMapTable dedup (NOT EXISTS prevents duplicate rows on repeated runs).
             - Arithmetic overflow fix: nvarchar(10) widened to nvarchar(20) for decimal/ratio conversions.
             - Unicode arrow (U+2192) replaced with ASCII '->'.
             - Enhanced @Debug=1: timing at every stage, effective parameter echo, temp table row
               counts and samples (#qs_efficacy, #executive_dashboard, #recommendations severity
               breakdown), CommandLog date range, legacy label count, execution summary.
             - Enhanced @Help=1: grading scale docs, prerequisites/limitations, grade override
               examples, version history (9 result sets, was 5).
             - Em dash removal (~60 occurrences) for PowerShell 5.1 compatibility.
2026.03.20   - 26-issue bulk resolution across 5 phases:
             Phase 1: Cache watermark sentinel fix (#312), C4 parallelism-aware (#306).
             Phase 2: ObjectId/StatsId extraction (#268), RS 2 Summary (#283),
               RS 7 StatsInParallel (#287), RS 12 WorkloadRankPct/CumulativeCpuPct (#262),
               RS 10 Top5ConcentrationPct (#269), cursor-to-set-based parsing (#302).
             Phase 3: #workload_impact staging table, ImpactScore in RS 5/7 (#255),
               WorkloadImpactPct in RS 5 (#261), IsVolatile flag (#259),
               W7 HIGH_IMPACT_STATS_DEPRIORITIZED (#263), I9 WORKLOAD_CONCENTRATION (#264),
               C3 workload impact enhancement (#265).
             Phase 4: W2 CRITICAL escalation (#266), W5 partial read-only (#274),
               W5/I6 capture mode guidance (#278), I8 improve-then-regress (#276),
               W6 memory pressure guidance (#285), dashboard non-QS scoring (#267),
               ExpertMode=0 workload context (#270).
             Phase 5: AG secondary replica detection (#296), RS 3 ReplicaRole column,
               15 new tests (162 -> 177).
2026.03.13   - Fix RS 12 WorkloadRank always=1 in SingleResultSet mode (#280).
             - Fix RS 11 DeltaVsPrior missing minutes-to-critical delta in SingleResultSet mode (#282).
2026.03.11   - Fix empty-data RS 3 schema mismatch (#304): 7 columns -> 17 columns
             matching production RS 3 (Run Health Summary).
2026.03.10.2 - RS 13 forced plan awareness (#292): ForcedPlanCount column, PlanTrend
             distinguishes 'MORE PLANS (forced plan at risk)' from generic proliferation.
             I8 recommendation warns when degrading stats have forced plans.
             Cross-database sys.query_store_plan lookup (graceful on QS-disabled DBs).
2026.03.10.1 - @GradeOverrides and @GradeWeights parameters for Executive Dashboard.
             Force grades (RELIABILITY=A), exclude categories (SPEED=IGNORE),
             or change weights (COMPLETION=50, auto-normalized to 100%).
             I6 QS_EFFICACY: graceful message when QS runs exist but no CPU data
             (was emitting question marks).
2026.03.10   - DECLARE-in-loop fix for @map_table_msg variable.
             RAISERROR decode query output when @ObfuscationMapTable is used.
2026.03.09.3 - W6 EXCESSIVE_OVERHEAD diagnostic check: flags runs where discovery/environment
             overhead exceeds 40% of wall-clock time.
             PAGE compression on StatUpdateDiagHistory PK and IX.
2026.03.09.2 - I8 silent failure: inserts INFO recommendation when no QS CPU data exists (#239).
             @SingleResultSet=1 auto-promotes @ExpertMode=1 for stable ResultSetID contract (#236).
2026.03.09.1 - RunLabel dedup prevents PK violation on duplicate START entries (#216).
             Watermark gap detection resets when CommandLog archived (#232).
             I5 VERSION_HISTORY implemented (was documented but missing).
             I8 QS_PERFORMANCE_TREND: per-stat per-execution CPU trend.
             StatUpdateDiagCache persistent table avoids XML re-parse.
             New @Help note for automation / @SingleResultSet.
2026.03.08.1 - Executive Dashboard (RS 1): letter grades A-F, health score 0-100,
             5 categories (Overall, Completion, Reliability, Speed, Workload Focus).
             @ExpertMode parameter: 0 = management view (2 RS), 1 = DBA deep-dive (13 RS).
             Persistent history table (dbo.StatUpdateDiagHistory) with watermark-based
             incremental inserts. @SkipHistory parameter to opt out.
             QS Performance Correlation (I8 + RS 13): per-stat CPU trend detail.
             QS Efficacy Trending (I6/I7 + RS 10/11/12): weekly aggregates,
             per-run detail, high-CPU stat positions.
             RS renumbered: RS 1 = Dashboard, RS 2 = Recommendations (always),
             RS 3-13 = ExpertMode=1 only.
2026.03.06   - Security/correctness: SQL injection fix in @CommandLogDatabase,
             missing AG keyword in @Help, parameter validation warnings.
             12 bug fixes from SME review (BUG-01 through BUG-12).
             @ObfuscationSeed and @ObfuscationMapTable parameters for
             deterministic, reproducible obfuscation across runs.
             @Help enhanced with valid_inputs, examples, operational notes.
2026.03.04.1 - Version format adopted CalVer (YYYY.MM.DD).
             @SingleResultSet parameter: wraps all output into one table with
             stable ResultSetID column for INSERT...EXEC automation.
             Fix: empty-data path respects @SingleResultSet + @Obfuscate.
2026.02.12   - Initial release. 8 diagnostic checks (C1-C4, W1-W5, I1-I4),
             obfuscation mode, @Help, @Debug.
             9 result sets. 53 tests.
```
