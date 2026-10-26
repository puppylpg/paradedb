# MPP Leader Participation and `paradedb.mpp_local_partitions` Plan

## Goal

Enable running multi-partition DataFusion MPP plans (including `DistributedExec`, `NetworkShuffleExec`, `NetworkCoalesceExec`, and `RangePartitioningRule` co-partitioning) inside a single Postgres backend process (`proc 0`, the leader) for profiling and debugging, with full wire serialization (protobuf plan/control frames, Arrow IPC batch frames, and `DsmMpscRing` transport) between tasks.

We split this work into two reviewable phases:

1. Phase 1 (Prework): Always have the leader (`proc 0`) participate as a producer in MPP plans alongside any launched background workers.
2. Phase 2: Force serialization across self-channels and add the `paradedb.mpp_local_partitions = N` GUC to run `N`-partition MPP plans entirely in the leader process without launching background workers.

---

## Phase 1 (Prework): Leader (`proc 0`) Participation in MPP Plans

### 1.1 `datafusion-distributed` (`/Users/stuhood/src/df-d-0`)

1. `src/shm/dsm.rs`:
   - Relax `compute_dsm_layout` from `n_procs < 2` to `n_procs < 1` so a 1-proc mesh (leader only) is valid.
   - Update comments reflecting that `proc 0` is both coordinator/consumer and a participating producer proc (`0..n_procs`).
2. `src/shm/runtime.rs`:
   - Change `proc_for_task(n_workers: u32, task_idx: u32) -> u32` from `1 + (task_idx % n_workers.max(1))` to `task_idx % n_workers.max(1)`.
   - Change `MppMesh::n_workers(&self) -> u32` from `self.n_procs - 1` to `self.n_procs` (with `debug_assert!(self.n_procs >= 1)`).
   - Add `MppMesh::send_task_metrics` (or self-loop handling) and update `run_execute_task_loop` (`src/shm/setup.rs`) so that if a task on `this_proc` fails while serving a requester on `this_proc`, `mesh.inbound_receiver().fail_scope(...)` is invoked directly when no remote error sender is used.
3. `src/shm/setup.rs`:
   - Expose `pub plan_bytes: Vec<u8>` on `LeaderSession` (matching `WorkerSession`) so the leader's fragment runner can use the same session inputs as a background worker.
4. `src/shm/in_process.rs` and `src/shm/self_hosted.rs`:
   - Update `in_process.rs` and `self_hosted.rs` so `proc 0` also runs its assigned fragments from `fragments_for_proc(&entries, 0, n_workers)` alongside `1..n_procs`.
   - Update unit tests in `src/shm/` that assert `proc_for_task` assignments or `n_workers()` values.

### 1.2 `pg_search` (`/Users/stuhood/src/paradedb-0`)

1. `pg_search/src/postgres/customscan/mpp/glue.rs`:
   - Lower `MIN_TOTAL_WORKER_COUNT` from `3` (consumer-only leader + 2 background workers) to `2` (participating leader + at least 1 background worker, i.e. 2 total producers).
   - Keep `producer_worker_cap()` as `min(max_parallel_workers_per_gather, max_parallel_workers)` representing the total producer cap (`n_procs`), so `mpp_is_active()` requires `producer_worker_cap() >= MIN_TOTAL_WORKER_COUNT` (`>= 2`), preserving existing `EXPLAIN` plan shapes and `target_partitions`.
2. `pg_search/src/postgres/customscan/mpp/launch.rs`:
   - In `launch_mpp`:
     - `producer_count = (max_tasks as u32).clamp(MIN_TOTAL_WORKER_COUNT, cap)` is the total number of participating producer procs (`n_procs`), which includes the leader (`proc 0`) plus `bg_workers_to_spawn = producer_count - 1` background workers.
     - Reserve DSM for `producer_count` procs (`estimate_dsm_size(producer_count, payload.len())`).
     - Spawn `bg_workers_to_spawn` background workers via `ParallelProcessBuilder::build`.
     - `mpp_attach_outcome` succeeds when `attached_bg_workers + 1 >= MIN_TOTAL_WORKER_COUNT` (i.e. at least 1 background worker attached, giving `total_procs = attached_bg_workers + 1 >= 2`).
     - Call `leader_setup(mesh_ptr, attached_bg_workers + 1, payload)` and record `timing.workers = attached_bg_workers + 1` (total active producers) and `scan_ptr` on `MppLeaderState`.
3. `pg_search/src/postgres/customscan/mpp/exec_worker.rs`:
   - Refactor the fragment execution body of `run_mpp_worker` into a reusable async driver (`run_mpp_fragments`) that:
     - Takes `plan_bytes`, `seed_ctx`, `mesh`, `outbound_senders`, `parallel_state` (wrapped in a `Copy + Send` newtype like `SpillNotifier`), and `plan_sources_count`.
     - Computes `fragments_for_worker` for `mesh.this_proc`. If `fragments` is empty, returns `Ok(())`.
     - Awaits `take_set_plan_draining` for each fragment, decodes each plan synchronously via `deserialize_physical_plan_with_runtime`, validates `source_idx < plan_sources_count`, runs `FuturesUnordered` of `spawn_fragment_execution_task`, and reports `TaskMetrics` to `proc 0`.
   - `run_mpp_worker` (for background workers `proc >= 1`) simply calls `runtime.block_on(run_mpp_fragments(...))` under `HeldInterrupts`.
   - Add a helper to spawn `run_mpp_fragments` as a background task (`tokio::task::JoinHandle<Result<(), DataFusionError>>`) onto the leader's `tokio::runtime::Runtime` and store that handle on `MppLeaderState`.
4. `pg_search/src/postgres/customscan/joinscan/mod.rs` and `pg_search/src/postgres/customscan/aggregatescan/mod.rs`:
   - Enable Tokio timers (`.enable_all()`) on `JoinScan`'s current-thread runtime (matching `AggregateScan` and `run_launched_worker`).
   - When `launch_mpp` returns `Some(leader)`, spawn the leader's fragment task onto `runtime` before polling the `DistributedExec` stream.
   - Because the leader's fragments open their own `PgSearchScanPlan` instances from `ParallelScanState` during `SetPlan` decode (while stage 0 may also contain leader-local scans stamped via `stamp_parallel_state`), both paths share `scan_ptr` cleanly.
   - In `shutdown_custom_scan` and `finish_mpp_execution`, abort/join the leader fragment task handle when the stream finishes or drops early (e.g. `LIMIT`), before `drain_worker_metrics`.

---

## Phase 2: Forced Self-Channel Serialization and `paradedb.mpp_local_partitions`

### 2.1 `datafusion-distributed` (`/Users/stuhood/src/df-d-0`)

1. `src/shm/dsm.rs` & `src/shm/setup.rs`:
   - In `attach_proc`, also attach a self-sender (`DsmMpscSender::new_control`, which does not increment `sender_count`) to `this_proc`'s own `DsmMpscRing` inbox (`header.inbox_offset(this_proc)`).
   - Populate `outbound_senders[this_proc]` and `cancel_senders[this_proc]` with `MppSender` wrapping that self-sender.
2. `src/shm/runtime.rs` & `src/shm/setup.rs`:
   - Route self-targeted `send_set_plan`, `send_execute_task`, `open_partition_sinks`, `pull_partition_stream`, and `TaskMetrics` through `outbound_senders[this_proc]` / `cancel_senders[this_proc]` (`MppSender` -> `DsmMpscRing` -> `DrainHandle::try_drain_pass` -> `register_data_channel`) instead of the in-memory `route_set_plan` / `route_execute_task` / `LocalDrainPartitionSink` / `register_local_channel` bypasses.
   - Verify with a dedicated single-process (`n_procs = 1`, `N` tasks) test in `src/shm/in_process.rs` that all control and data frames serialize through `inbox(0)`'s `DsmMpscRing` without deadlock.

### 2.2 `pg_search` (`/Users/stuhood/src/paradedb-0`)

1. `pg_search/src/gucs.rs`:
   - Add `paradedb.mpp_local_partitions` integer GUC (default `0`, min `0`, max `64`).
2. `pg_search/src/postgres/customscan/mpp/glue.rs` & `launch.rs`:
   - When `paradedb.mpp_local_partitions() > 0`:
     - `producer_worker_cap()` returns `paradedb.mpp_local_partitions() as u32`.
     - `mpp_is_active()` returns `true` (even for `N = 1` or when `max_parallel_workers_per_gather = 0`).
     - `mpp_gated_by_min_rows` returns `false` (bypassing `paradedb.mpp_min_rows`, while leaving `paradedb.hash_join_single_partition_threshold_rows` and other plan-shape GUCs untouched).
     - `launch_mpp` allocates the DSM + `ParallelScanState` in a leader-owned aligned heap buffer (no `ParallelProcessBuilder` / `LaunchParallelWorkers` background workers), initializes `leader_setup` with `n_procs = 1`, and runs all `N` tasks on `proc 0` in the leader process.
3. `pg_search/src/postgres/customscan/joinscan/range_partitioning_rule.rs` & `pg_search/src/scan/execution_plan.rs`:
   - Allow `RangePartitioningRule` and `stages_have_data_parallelism` to keep distributed stages and range co-partitioning active when `paradedb.mpp_local_partitions() > 0` (even if `N = 1`).
