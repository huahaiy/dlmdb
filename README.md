# DLMDB

This is the key value (KV) storage engine of
[Datalevin](https://github.com/juji-io/datalevin) database. Based on a fork of
[LMDB](https://www.symas.com/mdb), this KV engine supports these additional
features:

* Order statistics.
  - Efficient range count: `mdb_count_range`, `mdb_count_all`, `mdb_range_count_values`.
  - Random access by rank, i.e. getting ith item: `mdb_get_rank`
  - Rank lookup for existing keys (and specific duplicates): `mdb_get_key_rank`, `mdb_cursor_get_rank`, `mdb_cursor_key_rank`
* Prefix compression.

## Order-statistics API overview

### Getting the element at a rank

`mdb_get_rank` and `mdb_cursor_get_rank` : open database with the
`MDB_COUNTED` flag, then pass a zero-based rank to position the cursor or to copy
the key/data pair out of place.

```c
MDB_val key = {0}, data = {0};
int rc = mdb_get_rank(txn, dbi, rank, &key, &data);
if (rc == MDB_SUCCESS) {
    /* key/data point to the ith entry in sorted order */
}
```

### Finding the rank of an existing key

`mdb_get_key_rank` and `mdb_cursor_key_rank` provide the inverse operation. They
also require `MDB_COUNTED` and return the zero-based rank of a key/value pair.

```c
MDB_val key = {strlen("alpha"), "alpha"};
uint64_t rank = 0;
/* Plain database: data parameter may be NULL */
int rc = mdb_get_key_rank(txn, plain_counted_dbi, &key, NULL, &rank);

MDB_val dup = {strlen("payload-005"), "payload-005"};
rc = mdb_get_key_rank(txn, dupsort_counted_dbi, &key, &dup, &rank);
/* rank now includes all duplicates that precede payload-005 */
```

`mdb_cursor_key_rank` mirrors the cursor-oriented routines for callers that do
not want to leave the cursor's current position.

## Implementation notes

Datalevin extends LMDB's B+tree with optional per-branch cardinalities when a
database is opened using `MDB_COUNTED`. Those counts are already consumed by
`mdb_count_range` and `mdb_get_rank`; the new rank-lookup APIs reuse the same
metadata from the read path:

1. The helper validates that the key (and, in dupsort mode, the individual
   duplicate) exists by using the cursor stack. This guarantees the rank is only
   reported for confirmed entries.
2. Once positioned, the helper calls into the existing prefix-count machinery
   (`mdb_prefix_pair_leq`) to aggregate the number of elements that precede the
   target. This walks the branch counts and, for dupsort pages, performs a local
   traversal of the duplicate sub-tree.
3. Prefix-compressed leaves are handled transparently, because the same decode
   cache and contribution helpers are shared across both directions of the rank
   API.

The result is an inverse-rank query that runs in O(log n) time with the same
performance characteristics as the forward `mdb_get_rank`, without maintaining
any additional on-disk metadata.

## Range counting helpers

Counting records without walking the entire tree is often more useful than
materialising the data. DLMDB exposes three helpers that are backed by the same
counted-branch metadata used for the rank APIs:

* `mdb_count_all(txn, dbi, flags, &total)` – Returns the total number of
  key/value pairs in a counted database. Works for both plain and dupsort DBIs.
* `mdb_count_range(txn, dbi, &low, &high, flags, &total)` – Counts entries with
  keys between two bounds. Flags let you toggle inclusive/exclusive endpoints.
* `mdb_range_count_values(txn, dbi, &key_low, &key_high, key_flags, &total)` –
  Specialised for dupsort data: it counts individual values across a key range,
  honouring duplicate ordering.

All three execute in logarithmic time by traversing the B+tree once to the
relevant boundary nodes and aggregating the precomputed subtree counts stored on
each branch page. Prefix-compressed leaves and duplicate subtrees are handled
transparently.

```c
MDB_val low = {strlen("acct-0500"), "acct-0500"};
MDB_val high = {strlen("acct-0599"), "acct-0599"};
uint64_t total = 0;
int rc = mdb_count_range(txn, dbi, &low, &high, MDB_RANGE_INCLUDE_LOWER, &total);
```

### Range counting performance

`count_bench` exercises both naive cursor scans and the counted APIs. With
50,000 entries, 100 sampled queries, a span of 5,000 keys, and 100 duplicates
per key:

```
./count_bench --entries 50000 --queries 100 --span 5000 --dups 100 --shuffle
Benchmark with 50000 entries, 100 queries, span 5000
Insert order: shuffled

== Plain DB Inserts ==
  plain:   25.44 ms (0.51 us/op)
  counted: 25.04 ms (0.50 us/op)
  overhead: -0.40 ms (-1.58%)

== Range Count (keys) ==
  naive cursor: 5.15 ms (51.49 us/op)
  counted API:  0.07 ms (0.71 us/op)
  speedup: 72.52x

== Rank Lookup ==
  naive (sampled 100): 18.87 ms (188.72 us/op)
  cursor API:        0.02 ms (0.17 us/op)
  mdb_get_rank:      0.02 ms (0.18 us/op)
  speedup: 1110.12x

== Dupsort Inserts ==
  plain:   1826.91 ms (0.37 us/op)
  counted: 1828.93 ms (0.37 us/op)
  overhead: 2.02 ms (0.11%)

== Dupsort Range Count ==
  dup/key: 100
  cursor (mdb_cursor_count): 38.61 ms (386.07 us/op)
  counted API:              0.10 ms (1.04 us/op)
  speedup: 371.22x
```

In short: counted metadata adds negligible write-time overhead while delivering
two to three orders of magnitude acceleration for range counts and rank lookups.

## Prefix compression

Enabling `MDB_PREFIX_COMPRESSION` on a database stores keys using shared
prefixes within each leaf page, reducing page fan-out and disk footprint. DLMDB
further augments the classic prefix compression schema in two ways:

1. **Cached decoding per transaction.** A lightweight cache (`MDB_prefix_scratch`)
   holds decoded keys so repeated cursor walks do not rehydrate byte prefixes.
   This benefit is observable via `mdb_prefix_metrics`, which reports decode and
   cache hit counters.
2. **Cursor-safe navigation.** The unit tests under `mtest_prefix` cover dupsort
   scenarios where multiple cursors iterate forward and backward, mix
   `MDB_NEXT_DUP` / `MDB_PREV_DUP`, and issue `MDB_GET_BOTH[_RANGE]`. The cache
   and rank logic cooperate to ensure prefix-compressed keys behave identically
   to their uncompressed counterparts.

To use prefix compression:

```c
MDB_dbi dbi;
CHECK(mdb_dbi_open(txn, "prefixed", MDB_CREATE | MDB_PREFIX_COMPRESSION, &dbi));
```

Once enabled, read paths continue to expose fully reconstructed keys via the
cursor API; the optimisation is completely internal to the engine.

### Prefix compression performance

`compress_bench` measures workloads with and without prefix compression. With
500,000 entries, 64-byte values, 32-byte shared prefixes, and a 2 GiB map:

```
./compress_bench -n 500000 -r 500000 -v 64 -p 32 -m 2048 -U 100000 -X 100000

=== plain (plain) ===
Insert: 398.366 ms (0.797 us/op)   Random read (warm): 289.653 ms (0.579 us/op)
Range scan (warm): 3.512 ms (0.014 us/key)  Data size: 254.53 MiB, leaf pages: 5405

=== prefix (prefix) ===
Insert: 396.818 ms (0.794 us/op)   Random read (warm): 271.002 ms (0.542 us/op)
Range scan (warm): 5.546 ms (0.022 us/key)  Data size: 166.27 MiB, leaf pages: 3529

Prefix metrics:
  Random Read (cold) decode=3,141,522 (fast=3,141,522) hit=5,184 miss=3,141,522 cached_pages=0
  Range Scan (warm) decode=6,767 (fast=6,767) hit=257,009 miss=6,767 cached_pages=1,486

--- Relative to plain ---
Insert time: 0.996x
Update time: 0.876x
Delete time: 0.926x
Random read (warm): 0.936x
Data size / map usage / leaf pages: ~0.65x
```

Prefix compression therefore shrinks on-disk footprint by ~35 % and improves
cache-friendly workloads (warm random reads) while keeping write throughput on
par with the uncompressed baseline.
