# DLMDB

This is the backing key value (KV) storage engine of
[Datalevin](https://github.com/juji-io/datalevin) database, a simple, fast and
versatile Datalog database. Based on a fork of the esteemed
[LMDB](https://www.symas.com/mdb), this KV engine supports these additional
features:

* Order statistics.
  - Random access by rank, i.e. getting ith item efficiently.
  - Rank lookup for existing keys (and specific duplicates).
  - Efficient range count.
* Prefix compression.

These features are critical for Datalevin's high performance: the order
statistics facilitate query planning, while prefix compression couples
well with triplet storage.

## Order-statistics API

DLMDB extends LMDB's B+tree with optional per-branch cardinalities when a
database is opened using `MDB_COUNTED` flag. Once enabled, the following
functions are supported and they all run in O(log n) time:

### Getting the element at a rank

`mdb_get_rank` and `mdb_cursor_get_rank` : pass a zero-based rank to position
the cursor or to copy the key/data pair out of place.

```c
MDB_val key = {0}, data = {0};
int rc = mdb_get_rank(txn, dbi, rank, &key, &data);
if (rc == MDB_SUCCESS) {
    /* key/data point to the ith entry in sorted order */
}
```
### Finding the rank of an existing key

`mdb_get_key_rank` and `mdb_cursor_key_rank` provide the inverse operation. They
return the zero-based rank of a key/value pair.

```c
MDB_val key = {strlen("alpha"), "alpha"};
uint64_t rank = 0;
/* Plain database: `data` parameter may be NULL */
int rc = mdb_get_key_rank(txn, plain_counted_dbi, &key, NULL, &rank**;

/* DUPSORT database: */
MDB_val dup = {strlen("payload-005"), "payload-005"};
rc = mdb_get_key_rank(txn, dupsort_counted_dbi, &key, &dup, &rank);
/* rank now includes all duplicates that precede payload-005 */
```

`mdb_cursor_key_rank` mirrors the cursor-oriented routines for callers that do
not want to leave the cursor's current position.

## Range Counting API

Counting records without walking the entire tree is more efficient than counting
while materializing the data. DLMDB exposes three helpers that are backed by the
same counted-branch metadata used for the rank APIs:

* `mdb_count_all(txn, dbi, flags, &total)` – Returns the total number of
  key/value pairs in a counted database. Works for both plain and dupsort DBIs.
* `mdb_count_range(txn, dbi, &low, &high, flags, &total)` – Counts entries with
  keys between two bounds. Flags let you toggle inclusive/exclusive endpoints.
* `mdb_range_count_values(txn, dbi, &key_low, &key_high, key_flags, &total)` –
  Specialised for dupsort data: it counts individual values across a key range,
  honouring duplicate ordering.

All three execute in logarithmic time by traversing the B+tree once to the
relevant boundary nodes and aggregating the precomputed subtree counts stored on
each branch page.

```c
MDB_val low = {strlen("acct-0500"), "acct-0500"};
MDB_val high = {strlen("acct-0599"), "acct-0599"};
uint64_t total = 0;
int rc = mdb_count_range(txn, dbi, &low, &high, MDB_RANGE_INCLUDE_LOWER, &total);
```

## Counted DB Benchmark

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

Enabling `MDB_PREFIX_COMPRESSION` flag on a database stores keys using shared
prefixes within each leaf page, reducing page fan-out and disk footprint. For
DUPSORT databases, a higher ratio of compression can be achieved because values
are also compressed.

To use prefix compression:

```c
MDB_dbi dbi;
CHECK(mdb_dbi_open(txn, "prefixed", MDB_CREATE | MDB_PREFIX_COMPRESSION, &dbi));
```

Once enabled, read paths continue to expose fully reconstructed keys via the
cursor API; the optimisation is completely internal to the engine.

### Prefix compression performance

`compress_bench` measures workloads with and without prefix compression. With
1,000,000 entries, 64-byte values, 32-byte shared prefixes, a 2 GiB map, and
duplicate-heavy traffic:

```
./compress_bench -n 1000000 -r 500000 -v 64 -p 32 -m 2048 -U 200000 -X 200000 -D 20

=== plain (plain, unique) ===
Insert: 1205.081 ms (1.205 us/op)   Random read (warm): 409.659 ms (0.819 us/op)
Range scan (warm): 4.905 ms (0.019 us/key)  Data size: 509.12 MiB, leaf pages: 10810

=== prefix (prefix, unique) ===
Insert: 1285.150 ms (1.285 us/op)   Random read (warm): 461.428 ms (0.923 us/op)
Range scan (warm): 5.160 ms (0.020 us/key)  Data size: 331.83 MiB, leaf pages: 7049

=== plain-dups (plain, dupsort) ===
Insert: 1703.188 ms (1.703 us/op)   Random read (warm): 410.138 ms (0.820 us/op)
Range scan (warm): 5.314 ms (0.021 us/key)  Data size: 344.14 MiB, leaf pages: 7302

=== prefix-dups (prefix, dupsort) ===
Insert: 1902.391 ms (1.902 us/op)   Random read (warm): 387.025 ms (0.774 us/op)
Range scan (warm): 4.823 ms (0.019 us/key)  Data size: 74.14 MiB, leaf pages: 1590

Prefix metrics:
  Random Read (cold) decode=3,142,748 (fast=3,142,748) hit=5,137 miss=3,142,748 cached_pages=0
  Range Scan (warm) decode=6,805 (fast=6,805) hit=257,003 miss=6,805 cached_pages=1,492

--- Relative to plain ---
Insert time: 1.066x
Update time: 1.012x
Delete time: 1.109x
Random read (warm): 1.126x
Range scan (warm): 1.052x
Data size / map usage / leaf pages: ~0.65x

--- Relative to plain-dups ---
Insert time: 1.117x
Update time: 1.236x
Delete time: 0.945x
Random read (warm): 0.944x
Range scan (warm): 0.908x
Data size / map usage / leaf pages: ~0.22x
```

Prefix compression therefore shrinks on-disk footprint by ~35 % for regular
database, ~75% for DUPSORT database, and improves
cache-friendly workloads (warm random reads) while keeping write throughput on
par with the uncompressed baseline.
