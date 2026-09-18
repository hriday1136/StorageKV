# storagekv

A crash-safe, embedded key-value storage engine built on an **LSM-tree**, written from scratch in Rust with **zero runtime dependencies**.

Every write is durable before it's acknowledged, data outlives memory, and the engine recovers to its exact committed state after a crash at any point — verified by property testing against a reference model across hundreds of randomized operation sequences with interleaved crash-recovery.

## Why this exists

This is a from-scratch implementation of the storage architecture behind RocksDB, LevelDB, and Cassandra, built to understand — and demonstrate understanding of — durability, crash recovery, and the LSM read/write/space tradeoff at the level of the actual on-disk bytes. No storage libraries; the only dependency (`proptest`) is a dev-dependency used for testing and never ships in the engine.

## Architecture

The engine is five components:

- **Write-ahead log (WAL)** — append-only, checksummed. Every mutation is durably logged here (via `fsync`) before it is applied anywhere else, so any acknowledged write survives a crash.
- **Memtable** — an in-memory sorted map (`BTreeMap`) holding recent writes. Flushed to disk when it fills.
- **SSTables** — immutable, sorted on-disk tables with a sparse index and a footer. Reads binary-search the in-memory sparse index, then scan one small block.
- **Manifest** — the authoritative record of which SSTables are live. Recovery trusts the manifest, not the directory listing, so crash litter can't corrupt startup.
- **Compaction** — a k-way merge that collapses SSTables into one, keeping the newest version of each key and reclaiming space from overwritten and deleted keys.

**Write path:** assign sequence number → append to WAL (`fsync`) → apply to memtable → flush to a new SSTable when full → rotate the WAL.

**Read path:** memtable first, then SSTables newest-to-oldest; the first layer holding the key wins, and a tombstone there means "not found."

**Recovery:** load the manifest (checkpoint) → open the SSTables it names → replay the WAL (forward log) on top → garbage-collect any crash litter. This is checkpoint-plus-log recovery, the same architecture as Postgres (checkpoint + WAL) and Raft (snapshot + log).

## Durability and crash-safety

Durability is enforced at the filesystem level, and correctness lives in the **ordering** of operations, not the application logic:

- A write is durable only after `fsync`; a file rename is durable only after the containing directory is `fsync`ed. Both are used deliberately.
- New durable state is always made referenceable *before* old state is discarded ("persist-new-before-discard-old"). This invariant governs every operation that swaps durable state — memtable flush, manifest update, and compaction — so a crash at any point recovers to either the pre- or post-operation state, never a broken hybrid.
- SSTable installs and manifest updates are crash-atomic via write-temp → `fsync` → atomic rename → `fsync` directory.

Crash-safety is tested by enumerating the interruption windows of each multi-step operation and asserting recovery resolves each to a consistent state.

## Correctness testing

- **44 tests**, including unit tests for every component and its crash windows.
- **Property tests** (the correctness centerpiece): the engine is run against a `BTreeMap` reference model over hundreds of randomized sequences of put/delete/get/reopen operations, asserting the two never disagree. A small key alphabet forces frequent overwrite/delete/compaction interactions; interleaved reopens test recovery from arbitrary on-disk states. When a property fails, the framework shrinks it to a minimal reproduction.

## Performance

Measured on WSL2 (a `--release` build), 50,000 operations:

- **Point reads:** p50 ~4µs, p99 ~11µs. The p50→p99 spread reflects LSM read amplification (recent data is found immediately; the tail requires checking older SSTables).
- **Cost of durability:** the durable engine (WAL + `fsync`-per-write) is roughly **three orders of magnitude** slower than an in-memory `BTreeMap` baseline on the same machine. This gap is the price of crash-safety: every write pays a physical disk round-trip via `fsync`. Reported as a same-machine ratio to isolate the durability mechanism from hardware.

(Absolute write throughput is dominated by per-write `fsync` cost, which is especially high on WSL2's filesystem layer; group-commit batching, below, is the standard way to raise it.)

## What's next

Deliberately scoped out of the core build, with the architecture designed to accept them:

- **MVCC / snapshot isolation** — the sequence numbers stamped on every record are the foundation; the memtable would become multi-version.
- **Group-commit (batched `fsync`)** — amortize the `fsync` cost across many writes for far higher durable write throughput.
- **Leveled compaction** — bound read/space amplification more tightly than the current size-tiered strategy.
- **Bloom filters per SSTable** — skip tables that definitely lack a key, reducing read amplification.
- **Crash-injection test harness** — subprocess-level kills at instrumented points, a stronger guarantee than the current window-enumeration tests.

## Building and running

```
cargo test # run all tests, including property tests
cargo run --release --bin bench # run the benchmark harness
```

## Design notes

A delete is recorded as a tombstone rather than a physical removal, because the deleted key may still live in an older, immutable SSTable that a delete can't reach in place. During compaction, it is only safe to *drop* a tombstone (and the shadowed values beneath it) when the merge includes the oldest data on disk — otherwise an un-merged older SSTable could still hold a value for that key, and dropping the tombstone would make the deleted key silently reappear. This engine sidesteps the hazard by compacting all tables together, which makes tombstone removal unconditionally safe; the general case is why production engines track the level at which a tombstone becomes droppable.
