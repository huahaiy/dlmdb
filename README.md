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

## Prefix Compression

Enabling `MDB_PREFIX_COMPRESSION` flag on a database stores keys using shared
prefixes within each leaf page, reducing page fan-out and disk footprint. For
DUPSORT databases, a higher ratio of compression can be achieved because values
are also compressed.

To use prefix compression:

```c
MDB_dbi dbi;
CHECK(mdb_dbi_open(txn, "db", MDB_CREATE | MDB_PREFIX_COMPRESSION, &dbi));
```

Once enabled, read paths continue to expose fully reconstructed keys via the
cursor API; the optimization is completely internal to the engine.

### Prefix compression performance

`compress_bench` measures workloads with and without prefix compression. With
1,000,000 entries, 64-byte values, 32-byte shared prefixes, a 2 GiB map, and
duplicate-heavy traffic:

```
./compress_bench -n 1000000 -r 500000 -v 64 -p 32 -m 2048  -U 200000 -X 200000 -D 20

=== plain (plain, unique) ===
Insert: 960.479 ms (0.960 us/op, 1041147 op/s over 1000000 ops)
Update: 192.540 ms (0.963 us/op, 1038745 op/s over 200000 ops)
Delete: 279.387 ms (1.397 us/key, 715853 key/s over 200000 keys)
Reinsert: 232.080 ms (1.160 us/key, 861772 key/s over 200000 keys)
Random Read (cold): 373.584 ms (0.747 us/op, 1338387 op/s over 500000 ops)
Random Read (warm): 363.248 ms (0.726 us/op, 1376470 op/s over 500000 ops)
Range Scan (cold): 6.883 ms (0.027 us/key, 37193084 key/s over 256000 keys)
Range Scan (warm): 3.833 ms (0.015 us/key, 66788416 key/s over 256000 keys)
Map: 509.12 MiB used / 2.00 GiB configured

=== prefix (prefix, unique) ===
Insert: 1055.974 ms (1.056 us/op, 946993 op/s over 1000000 ops)
Update: 190.974 ms (0.955 us/op, 1047263 op/s over 200000 ops)
Delete: 355.270 ms (1.776 us/key, 562952 key/s over 200000 keys)
Reinsert: 241.767 ms (1.209 us/key, 827243 key/s over 200000 keys)
Random Read (cold): 389.688 ms (0.779 us/op, 1283078 op/s over 500000 ops)
Random Read (warm): 368.862 ms (0.738 us/op, 1355520 op/s over 500000 ops)
Range Scan (cold): 6.506 ms (0.025 us/key, 39348294 key/s over 256000 keys)
Range Scan (warm): 4.489 ms (0.018 us/key, 57028291 key/s over 256000 keys)
Map: 331.83 MiB used / 2.00 GiB configured

=== plain-dups (plain, dupsort) ===
Insert: 1335.676 ms (1.336 us/op, 748685 op/s over 1000000 ops)
Update: 421.925 ms (2.110 us/op, 474018 op/s over 200000 ops)
Delete: 265.496 ms (1.327 us/key, 753307 key/s over 200000 keys)
Reinsert: 337.803 ms (1.689 us/key, 592061 key/s over 200000 keys)
Random Read (cold): 473.842 ms (0.948 us/op, 1055204 op/s over 500000 ops)
Random Read (warm): 406.730 ms (0.813 us/op, 1229317 op/s over 500000 ops)
Range Scan (cold): 6.411 ms (0.025 us/key, 39931368 key/s over 256000 keys)
Range Scan (warm): 4.827 ms (0.019 us/key, 53035011 key/s over 256000 keys)
Map: 344.14 MiB used / 2.00 GiB configured

=== prefix-dups (prefix, dupsort) ===
Insert: 1407.319 ms (1.407 us/op, 710571 op/s over 1000000 ops)
Update: 509.726 ms (2.549 us/op, 392368 op/s over 200000 ops)
Delete: 252.931 ms (1.265 us/key, 790729 key/s over 200000 keys)
Reinsert: 347.707 ms (1.739 us/key, 575197 key/s over 200000 keys)
Random Read (cold): 354.284 ms (0.709 us/op, 1411297 op/s over 500000 ops)
Random Read (warm): 340.684 ms (0.681 us/op, 1467636 op/s over 500000 ops)
Range Scan (cold): 5.095 ms (0.020 us/key, 50245339 key/s over 256000 keys)
Range Scan (warm): 4.315 ms (0.017 us/key, 59327926 key/s over 256000 keys)
Map: 74.14 MiB used / 2.00 GiB configured

--- Relative to plain ---
Insert time: 1.099x (960.479 ms -> 1055.974 ms)
Update time: 0.992x (192.540 ms -> 190.974 ms)
Delete time: 1.272x (279.387 ms -> 355.270 ms)
Reinsert time: 1.042x (232.080 ms -> 241.767 ms)
Random read (warm): 1.015x
Random read (cold): 1.043x
Range scan (warm): 1.171x
Map used: 0.652x (509.12 MiB -> 331.83 MiB)
Leaf pages: 0.652x (10810 -> 7049)

--- Relative to plain-dups ---
Insert time: 1.054x (1335.676 ms -> 1407.319 ms)
Update time: 1.208x (421.925 ms -> 509.726 ms)
Delete time: 0.953x (265.496 ms -> 252.931 ms)
Reinsert time: 1.029x (337.803 ms -> 347.707 ms)
Random read (warm): 0.838x
Random read (cold): 0.748x
Range scan (warm): 0.894x
Map used: 0.215x (344.14 MiB -> 74.14 MiB)
Leaf pages: 0.218x (7302 -> 1590)

```

Prefix compression therefore shrinks on-disk footprint by ~35 % for regular
database, and ~75 % for DUPSORT database, while keeping read/write throughput on
par with the uncompressed baseline.
