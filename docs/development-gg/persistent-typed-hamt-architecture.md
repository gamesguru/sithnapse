# Persistent augmented HAMT state storage

Status: adopted design. Persistent incremental updates are implemented via
`apply_flat_state_updates` and `_persist_state_hamt_incremental_txn` in
`rust/src/state_hamt.rs` (see the empirical validation table in
[Empirical validation](#empirical-validation) below). The typed-root identity
primitives and flat-to-typed migration remain in progress. This document is the
reference to build against; PRs implementing pieces of it should link back here
rather than re-deriving the design.

## Summary

Stop treating SQL and TiKV as different state algorithms. Define one logical
state-store interface and implement it over:

```text
canonical state identity
        ↓
persistent typed CHAMP/HAMT roots
        ↓
content-addressed immutable nodes
        ↓
SQL or TiKV node backend
```

The HAMT is the live snapshot/update structure. The typed directory reduces bulk
reads by event type. LtHash supplies the cross-server state identity. SQL
remains the default backend; TiKV is an alternative backend, not the design
center — **and it is not a secret dependency of the SQL default**: see Storage
ownership below, which was corrected from an earlier draft that split roots
(SQL) from nodes (TiKV) unconditionally.

Target costs (`S` = total state size, `S_T` = one type's state size, `D` =
differing leaves between two roots, `K` = changed keys in an update):

| operation            | cost                 |
| -------------------- | -------------------- |
| one state-key update | O(log₃₂ S) new nodes |
| K changed keys       | O(K log₃₂ S)         |
| point lookup         | O(log₃₂ S)           |
| all state of type T  | O(log₃₂ S + S_T)     |
| full snapshot        | O(S)                 |
| root equality        | O(1)                 |
| structural diff      | O(D log₃₂ S)         |

A brand-new state imported from a flat map necessarily costs O(S) — that's an
information lower bound (every entry must be read at least once), not a
storage-engine failure to fix.

**Empirical validation:** the "one state-key update" row above is no longer just
a target — `rust/src/state_hamt.rs`'s `apply_flat_state_updates` is the
persistent update API described in
[Update and write workflow](#update-and-write-workflow) below, and
`rust/benches/state_hamt.rs` measures it directly against full-map rebuild
(`build_root_handle_with_lattice`) over a cumulative build from an empty room to
4096 entries (see that file's module doc for methodology, and
`rust/benches/README.md` for how to run it). Results (single run, debug
workstation, not a controlled benchmark environment — treat the _shape_, not the
absolute numbers, as the claim):

| state size (S) | full rebuild (cumulative) | incremental apply (cumulative) | speedup | cumulative node reads |
| -------------- | ------------------------: | -----------------------------: | ------: | --------------------: |
| 16             |                    1.2 ms |                         0.3 ms |    4.0× |                    16 |
| 64             |                   17.3 ms |                         1.5 ms |   11.4× |                    90 |
| 256            |                  270.0 ms |                         5.6 ms |   48.4× |                   454 |
| 1024           |                 4353.9 ms |                        25.5 ms |  171.0× |                  2098 |
| 2048           |                17685.7 ms |                        54.9 ms |  321.9× |                  4587 |
| 4096           |                70661.1 ms |                       113.0 ms |  625.4× |                 10299 |

The speedup _grows_ with S rather than converging to a constant, which is the
actual signature of O(K log S) beating O(S) rather than just "a faster constant
factor." Cumulative node reads track the prediction closely: 4096 single-key
updates against a HAMT with branching factor 32 predicts
`4096 * log₃₂(4096) ≈ 9830` total node reads if each update only ever touches
its own root-to-leaf path; the measured 10299 is within ~5% of that, with the
gap attributable to typed-directory/root overhead and updates that happen to
land near trie boundaries. This node-read count matters as much as the
wall-clock number: it's what the bench's fetch-on-demand loop actually forces
`apply_flat_state_updates` to request from a backing store standing in for
SQL/TiKV, so it's a direct measurement of "how many storage round-trips does one
state update really cost," not just a CPU-bound proxy for it.

## Core representation

The typed persistent root is the preferred physical representation:

```text
TypedRoot
 ├── structural_hash       keyed/local tree identity (never cross-server)
 ├── state_group_id        BLAKE2b-256(unkeyed LtHash) — the canonical identity
 ├── root_lattice          full LtHash, retained for O(1) homomorphic updates
 └── directory (sorted)
       event_type_id → subtree_hash
```

Each event-type subtree is a persistent CHAMP/HAMT:
`state_key_id -> short_event_id`. A subtree update path-copies only the affected
nodes; unchanged nodes remain shared by structural hash.

The existing flat HAMT (format `0x01`, `build_root_handle`) remains a
read-compatible format during migration — it is not a second semantic state
model. Both formats MUST produce the same canonical `state_group_id` for the
same logical state (this is what `typed_root_state_group_id_matches_flat_root`
in `state_hamt.rs` already asserts for one fixture; see Test and acceptance
criteria for the full matrix still needed).

**Never combine collapsed 32-byte `state_group_id` values.** The 32-byte ID is
`BLAKE2b-256(lattice)` — a digest, not itself homomorphic. Homomorphic
composition (incremental updates, per-subtree combination) must operate on the
retained `LtHash` lattice, then collapse to `state_group_id` once, at the end.

## Interning and canonical identity

Add compact local IDs where useful:

```text
event_type_id  <-> event type
state_key_id   <-> (event type, state key)
short_event_id <-> event ID
```

IDs MUST be permanent and never remapped — a historical root must decode
identically forever. Prefer interning the complete `(event_type, state_key)`
pair over a separate global user-ID dictionary; user IDs are common state keys
but not the whole state-key domain, and splitting them out adds classification
complexity for no clear win.

**Identity constraint:** local integer IDs are a storage/cache representation
only. The canonical tuple `(event_type, state_key, event_id)` — the real strings
— is what `LtHash`/`state_group_id` is computed over, unless the protocol itself
defines the IDs as globally stable (it doesn't today). Two servers assigning
different local IDs must still compute identical `state_group_id` for identical
logical state.

## Update and write workflow

Expose a persistent update API instead of full-map rebuild:

```text
old_root + [(key, insert | replace | delete)] -> new_root
```

Per change:

1. Resolve or create the permanent compact IDs.
2. Update the relevant typed subtree by path-copying.
3. Update the root `LtHash` by subtracting the old canonical tuple and adding
   the new one (`LtHash::replace`/`insert`/`remove` already exist in
   `rezzy::state::lthash`).
4. Rebuild only the typed directory path and the changed subtree paths.
5. Derive the new `state_group_id` from the resulting lattice.
6. Persist only new nodes plus the root pointer.

**This is the actual fix for the O(S)-per-PDU tax**, not the typed root by
itself. A normal state PDU changes one logical entry; the current Synapse code
path (`store_state_group_for_events` et al.) materializes the full previous
state map and calls `build_root_handle`/`build_typed_root` over all S entries
again — the builder has no "old root + one mutation" entry point, so it
re-processes everything every time regardless of how few entries actually
changed. Fixing this means adding and wiring a persistent insert/replace/remove
API against an existing root, not just improving the full-rebuild path's
constant factor.

Use full-map construction only for: initial import, legacy migration, state
resolution results with no usable persistent base root, and repair/rebuild
operations. For K changed entries with a known prior root, always update from
that root.

## Storage ownership (backend-neutral — corrected)

Earlier draft considered "SQL owns roots, TiKV owns nodes" as the default.
Rejected: it makes the SQL-default backend secretly depend on TiKV and recreates
exactly the cross-store publication hazard ("a visible root must imply every
node reachable from it is readable") that motivated this document. The adopted
split is backend-local:

- **SQL backend:** typed root records AND content-addressed subtree nodes both
  live in SQL, committed in the same transaction.
- **TiKV backend:** typed root records AND subtree nodes both live in TiKV,
  written in the same TiKV transaction/batch.
- **SQL-root/TiKV-node mode** (the shape the current short-term fix already
  uses, per `docs/development-gg/tikv-state-root-longterm.txt`) is a
  _compatibility_ mode for the existing deployment, not the target architecture
  for making SQL independently efficient.

SQL shape (roots as indexed metadata, not opaque blobs, so common columns are
queryable without a decode step):

```sql
typed_roots(
    state_group      bigint primary key,
    state_group_id   bytea not null,   -- 32B, canonical cross-server identity
    structural_hash  bytea not null,   -- 16B, local/keyed
    root_lattice     bytea not null,   -- 2048B, retained for O(1) updates
    directory_bytes  bytea not null    -- small; a child table is also viable
);

typed_hamt_nodes(
    structural_hash  bytea primary key,
    node_bytes       bytea not null
);
```

Invariant, either backend: **a visible/committed root must never reference a
node that isn't durable yet.** SQL gets this for free from the transaction; TiKV
needs the batch to land before the pointer becomes visible to any reader (or a
retryable pending-publication protocol if it can't be one atomic operation).

TiKV key layout, if/when the TiKV backend is built out to this design:

```text
hamt:node:<room_prefix>:<structural_hash>
hamt:root:<room_prefix>:<state_group_or_root_reference>
dict:event_type:<id>
dict:state_key:<id>
dict:event_id:<id>
```

Do not assume TiKV's sorted keyspace gives logical state ranges — HAMT hashes
are not ordered state keys (this was a real correction made earlier in the
design discussion; see git history on this branch). Type-scoped bulk reads still
traverse the selected subtree; they don't become range scans.

Don't move event IDs into TiKV merely to avoid SQL lookups. Only do it when: the
mapping is permanent/immutable, all readers can resolve it from the same TiKV
namespace, measured SQL round trips actually dominate the workload, and
ownership/repair semantics are specified.

## Storage Engine Selection & libmdbx Embedded Architecture

`libmdbx` was selected as Synapse's embedded storage engine alongside
PostgreSQL. `fjall` was evaluated first and dropped -- see `git log` for
`ab59dd8ba6` ("storage(embedded): drop fjall, commit to mdbx as the embedded
engine") -- both on measured latency and architecturally: mdbx supports native
multi-process `mmap` access on a shared filesystem, while fjall's LSM tree can
only be opened by one OS process, which would require a separate RPC/socket
bridge for every non-owning worker. **No such bridge was ever built or
benchmarked** -- an earlier draft of this doc quoted a "fjall + UDS Bridge"
figure as if it were measured; it wasn't (there is no bridge implementation
anywhere in this repo's history, reachable or not). That figure has been removed
rather than re-estimated.

### Warm-cache benchmark summary (2,000,000 nodes, 512 bytes/node)

Re-run from scratch on `scripts-dev/benchmark_hamt_mdbx_vs_postgres.py` (mdbx
and postgres are both still live in the tree and directly reproducible; fjall's
crate/bindings were fully removed in `ab59dd8ba6`, so its figures below are
unverified historical data rather than newly measured results):

```text
=====================================================================================
Batch Size    fjall (in-process)*   libmdbx (direct mmap)    postgres    speedup**
-------------------------------------------------------------------------------------
batch = 1          14.2 us               6.5 us                64.7 us      9.9x
batch = 5          79.0 us              18.5 us               116.2 us      6.3x
batch = 10         77.5 us              27.4 us               157.5 us      5.7x
commit (batch=5)      n/a               64.2 us               119.3 us      1.9x
bulk-load rows/s      n/a              192,129                 58,531       3.3x
=====================================================================================
* UNVERIFIED. batch=10 beating batch=5 here is non-monotonic and suspicious;
  these figures trace to the same doc commit (1725c043fb9) that also
  contained the fabricated "fjall + UDS Bridge" row above, and the script
  that could have produced them is deleted, so there's no raw log left to
  check them against and no way to rerun them (fjall's crate is gone too).
  Treat as unconfirmed, not as measured fact -- fjall lost to mdbx clearly
  enough on every other axis that nothing here hinges on these two numbers.
** mdbx vs postgres, the two columns re-run together (see below).
```

(p50 latencies; see the script for p99 and full methodology. mdbx and postgres
are reproducible:
`eval "$(scripts-dev/start_test_postgres.sh)"; python3 scripts-dev/benchmark_hamt_mdbx_vs_postgres.py`.
Includes both fixes below: sorted `batch_put` and a single explicit transaction
for the postgres bulk-load, so mdbx and postgres each pay exactly one commit for
the whole corpus -- the postgres bulk-load number barely moved from the earlier,
per-page-autocommit measurement (58,202 -> 58,531 rows/s) because
`start_test_postgres.sh` already disables fsync/synchronous_commit on this
RAM-disk cluster, so per-page commit overhead was already near-zero here; the
methodology bug was real, its effect on these particular numbers wasn't.)

fjall's read numbers are unverified (see note above) -- they come from the
now-deleted `benchmark_hamt_storage_engines.py`, allegedly run before
`ab59dd8ba6` removed the crate/bindings, but no raw output survives to confirm
it. If true, they'd already put fjall slower than mdbx in-process, before
accounting for the bridge a multi-process deployment would have additionally
required (never built, see above) -- but don't cite these two figures as solid.
Its bulk-load throughput and commit latency were never recorded anywhere in this
repo's history (checked commit messages and every doc revision) and can no
longer be measured now that the crate is gone -- marked `n/a` rather than
guessed.

### Two methodology fixes behind these numbers

**Bulk-load throughput (`73283299ed`)**: the one leg mdbx originally _lost_.
`batch_put` inserted content-addressed keys (structural hashes / event ids) in
whatever random order they arrived, costing a B-tree search/possible page-split
per row with no locality. Sorting each batch by key before insertion (the
standard mdbx/LMDB bulk-load pattern, safe against a non-empty table -- unlike
`WriteFlags::APPEND`, not used here since it additionally requires every key to
sort above the table's current max, a guarantee a reused database doesn't give
us) took mdbx from 43,612 to ~185,000-192,000 rows/s across repeated runs, at
the cost of a small, reproducible rise in point-read p50 at batch=1 (2.1us ->
~6.5-6.6us; still comfortably faster than postgres either way). That
read-latency shift isn't fully root-caused -- current best guess is a
benchmark-sampling artifact (the read benchmark's `keys_pool` is drawn in
pre-sort key-generation order, not a random cross-section of the post-sort
tree), not a real regression, but it hasn't been isolated further.

**Postgres bulk-load commit granularity**: `execute_values(..., page_size=1000)`
issues a separate `cur.execute()` per 1000-row page; under
`conn.autocommit = True` each page was its own implicit transaction/commit --
2,000 commits for the 2M-row corpus, vs. mdbx's `batch_put` doing the whole
corpus in one transaction. Fixed by wrapping the postgres bulk-load in one
explicit transaction (`conn.autocommit = False` around the load, single
`conn.commit()` after) so both sides pay one commit for one corpus. As noted
above, this particular test cluster already disables fsync/synchronous_commit,
so the measured effect was within noise here -- but the asymmetry was real and
would matter on a durable (non-RAM-disk) postgres instance, so it's fixed in the
script regardless of whether it moved these specific numbers.

### Warm-cache `event_json` benchmark (mixed-size payloads, realistic event ids)

Same conclusion, separately validated against the actual `event_json` access
pattern (event-id keys, 65/25/10% small/medium/large JSON size mix, not the HAMT
node bench's uniform 512B) via `scripts-dev/benchmark_event_json_storage.py` --
carries both fixes above (sorted `batch_put`, single-transaction postgres
bulk-load) -- re-run at n=2,000,000 (steady state, after the 200k warm-up leg):

```text
n=2,000,000                mdbx        postgres     speedup
------------------------------------------------------------
bulk-load (rows/s, +1.8M)  99,100      54,684        1.8x
read(batch=1)                2.3us      65.6us      28.5x
read(batch=20)               43.3us    236.5us       5.5x
read(batch=100)             185.2us    678.6us       3.7x
commit(batch=5)              63.1us    168.6us       2.7x
```

(p50 latencies. Reproduce with:
`eval "$(scripts-dev/start_test_postgres.sh)"; python3 scripts-dev/benchmark_event_json_storage.py`.
mdbx's bulk-load jumped from 24,158 to 99,100 rows/s on this workload from the
same sort-before-insert fix as the HAMT-node bench, going from postgres's
biggest lead to another mdbx win; postgres's own bulk-load number moved
negligibly, 54,316 -> 54,684, for the same reason noted above.)

### Cold-read status

The tables above are **warm-cache / steady-state** measurements. They must not
be used to claim that MDBX delivers microsecond reads when the required pages
are absent from memory. MDBX reads through the kernel's file-backed page cache;
an absent B-tree page causes storage I/O just as it does for other disk-backed
engines.

`scripts-dev/benchmark_mdbx_cold_reads.py` measures independent MDBX point
lookups after a fresh process opens the environment and `POSIX_FADV_DONTNEED`
has been issued for only the temporary MDBX files. Its temporary database
defaults to the current working directory rather than `/tmp`, since `/tmp` is
often tmpfs and would invalidate an I/O-cold test.

A small, illustrative run on this development host's on-disk ext4 filesystem
(10,000 512-byte values; 5 samples) reported:

| Engine     | Corpus / value size |     p50 |     p95 |     p99 | Status                                                |
| ---------- | ------------------- | ------: | ------: | ------: | ----------------------------------------------------- |
| MDBX       | 10,000 / 512 bytes  | 22.7 ms | 40.8 ms | 40.8 ms | Evicted-page sample; not a strict device-cold result. |
| PostgreSQL | 10,000 / 512 bytes  | 14.1 ms | 61.1 ms | 61.1 ms | Dedicated disk cluster restarted between samples.     |

This is an **evicted-page** result, not a perfectly device-cold guarantee:
`POSIX_FADV_DONTNEED` is advisory, and the result depends strongly on the
storage device, filesystem, background I/O, data size, and B-tree locality. For
a stricter device-cold run, use a controlled host or a corpus larger than RAM. A
representative command is:

```sh
python3 scripts-dev/benchmark_mdbx_cold_reads.py \
  --rows 2000000 --samples 200 --value-size 512 --workdir /path/on/target-disk
```

The two rows above are the first apples-to-apples cold sample, not a general
winner declaration: the sample is too small to resolve tail latency and cold I/O
varies sharply by storage device, filesystem, data size, and B-tree locality. On
this host PostgreSQL had the lower p50, while MDBX had the lower p95. Re-run
both harnesses with a larger corpus and sample count before making a product
decision.

The normal `start_test_postgres.sh` instance used by the warm comparison stores
`PGDATA` on tmpfs and disables durability, so it cannot measure storage misses.
A fair PostgreSQL comparison uses a disk-backed cluster, restarts it to clear
PostgreSQL `shared_buffers`, and evicts its dedicated cluster files before each
measured lookup. `scripts-dev/benchmark_postgres_cold_reads.py` implements that
procedure; it stops and restarts the dedicated cluster between samples.

`scripts-dev/start_test_postgres.sh` now supports the required cluster mode; the
directory is deliberately mandatory because the `stop` action removes it:

```sh
SYNAPSE_TEST_PG_STORAGE=disk \
SYNAPSE_TEST_PG_DATA=/path/on/target-disk/throwaway-postgres \
scripts-dev/start_test_postgres.sh
```

This disables the launcher's tmpfs placement and its `fsync = off`,
`synchronous_commit = off`, and `full_page_writes = off` test overrides. It does
not by itself make an already-read relation cold; the restart and targeted
page-cache eviction in `benchmark_postgres_cold_reads.py` remain part of the
benchmark procedure. Run it with the matching cluster path and port, for
example:

```sh
python3 scripts-dev/benchmark_postgres_cold_reads.py \
  --pgdata /path/on/target-disk/throwaway-postgres --port 5443 \
  --rows 2000000 --samples 200 --value-size 512
```

### Key Architectural Advantages of `libmdbx`:

1. **Direct Zero-Copy `mmap` Read Latency (~6.5us at batch=1)**:
   - Values are returned directly as borrowed `&[u8]` pointers in OS page cache
     without memory allocations, deserialization wrappers, or IPC overhead.
2. **Zero RPC Daemon / Zero Bridge Complexity**:
   - All Synapse worker processes (`sync`, `federation`, `state_res`, `api`)
     open the `libmdbx` environment files directly via kernel `mmap`. This
     avoids the operational overhead a single-process engine like fjall would
     have required (a daemon or socket bridge for non-owning workers) -- but
     note that overhead was never built, so it's an architectural argument, not
     a measured one.

### Dual-Store Crash-Consistency Protocol

```text
                  +------------------------------------+
                  |  Synapse State Persister Worker    |
                  +------------------------------------+
                                    |
                   +----------------+----------------+
                   | (Step 1: Write HAMT Nodes)      | (Step 2: Commit Transaction)
                   v                                 v
      +--------------------------+       +--------------------------+
      |  libmdbx Embedded Engine |       |   PostgreSQL Database    |
      |   (Shared Kernel mmap)   |       |   (`state_groups` table) |
      +--------------------------+       +--------------------------+
                   |                                 |
                   +----------------+----------------+
                                    |
                                    v
                  +------------------------------------+
                  |     Postgres Commit = Truth        |
                  | Unreferenced HAMT nodes are inert  |
                  +------------------------------------+
```

1. **Write HAMT Nodes First**: Structural HAMT nodes are persisted to `libmdbx`
   before committing the `state_groups` transaction in PostgreSQL.
2. **PostgreSQL Commit is Truth**: A state group exists if and only if its row
   in PostgreSQL `state_groups` is committed. If a crash occurs after writing
   `libmdbx` but before PostgreSQL commits, unreferenced HAMT nodes in `libmdbx`
   are inert content-addressed blobs that harm nothing.
3. **Startup Self-Healing**: On startup, Synapse validates that the highest
   `state_group_id` in PostgreSQL has a matching HAMT root in `libmdbx`,
   automatically re-materializing missing roots if a crash occurred during
   commit.

## Read workflows

**Point lookup:** root handle -> typed directory lookup by `event_type_id` ->
HAMT path lookup by `state_key_id` -> `short_event_id` -> `event_id`. Flat
legacy roots use the existing flat-HAMT path.

**Type-scoped bulk read:** root handle -> typed directory lookup -> traverse
only the selected type's subtree -> decode compact IDs. Cost proportional to
that event type, not total room state.

**Full read:** traverse all reachable typed subtrees, batch-fetch nodes by BFS
level (not one round trip per node — this is the SQL/TiKV resolver contract both
backends must implement; see `tikv-state-root-longterm.txt`'s access-pattern
note for the existing TiKV BFS shape to match). Remains O(S) — unavoidable when
the caller actually requests all state.

**Comparison:** compare `state_group_id` (or the root lattice digest) first —
equal means equal logical state, no tree walk needed. Different identity falls
through to a structural typed-HAMT diff: pointer-identity skip when available,
then descend only differing directory entries/subtree paths.

**State resolution / arbitrary merged roots:** apply K persistent updates if
resolution supplies a changed set relative to a known root. Full O(S)
construction only when resolution supplies just a materialized map with no
usable base root — again, an input lower bound, not an engine defect.

## Compatibility and migration

- Keep flat-root (`0x01`) decoding indefinitely during rollout.
- Write typed roots (`0x02`) only behind an explicit capability/config gate
  initially.
- Dual-write mode: persist both representations, assert equal `state_group_id`
  (this is exactly what `typed_root_state_group_id_matches_flat_root` checks at
  the unit level; needs an integration-level version of the same assertion once
  wired to real storage).
- Prefer typed roots for type-scoped reads once verified; fall back to flat
  roots for unsupported readers or a missing typed object.
- Don't eagerly migrate existing roots — migrate on read/write, or via a
  background rebuild (see Synapse's existing background-update machinery,
  `synapse/storage/schema/`).
- Version typed-root serialization (the `TYPED_ROOT_FORMAT` byte) separately
  from the semantic state identity (`state_group_id`'s meaning doesn't change
  across format versions).

## Test and acceptance criteria

Beyond the single-fixture cross-check already landed
(`typed_root_state_group_id_matches_flat_root`), still needed:

- flat/typed identity equality across randomized states and event orderings
- empty state, single-entry, replacement, deletion, duplicate-key rejection
  (confirmed: `rezzy::hamt::build_node` hard-errors on duplicate keys via
  `HashCollision` rather than silently resolving "last wins" — both
  `build_root_handle_and_nodes` and `build_typed_root_and_nodes` propagate that
  error via `?` before any lattice/tree mismatch could surface, so this is
  verified safe, not merely assumed)
- different server secrets producing equal `state_group_id` but different
  `structural_hash`
- encode/decode preserving structural hash, lattice, identity, and directory
- persistent one-key update producing the same state as a full rebuild
- K updates writing only the changed paths (node-count assertion, not just
  correctness)
- typed bulk reads provably excluding unrelated event types (e.g. assert on
  which node hashes were fetched, not just the returned entries)
- SQL BFS reads batching node fetches (one query per level, not per node)
- TiKV reads handling missing/corrupt nodes without silently returning partial
  state
- root publication never exposing unavailable child nodes
- structural diff returning exactly the changed tuples
- legacy flat-root fallback and typed-root migration

Performance acceptance should measure: state-progression cost vs. room size,
point-lookup latency, type-bulk-read latency, full-read throughput, SQL query
count per lookup/materialization, TiKV round trips per lookup/materialization,
and new-node bytes / compaction write amplification.

## Why earlier passes on this kept circling

Each mechanism has exactly one job, and conflating them is what produced the
back-and-forth in the design discussion that preceded this document:

- `LtHash` -> cross-server identity and O(1) root equality only.
- persistent HAMT -> incremental updates, point lookup, structural diff.
- typed root -> prune unrelated event types from bulk reads; nothing more (it
  does not create ordering, and does not make arbitrary state-key range scans
  efficient — that would need a genuinely ordered index, out of scope here).
- SQL/TiKV -> durable node storage and batched resolution, symmetric ownership
  of roots+nodes per backend.
- interning -> compact local representation, never the identity basis.
