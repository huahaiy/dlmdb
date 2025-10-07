# DLMDB

This is the key value (KV) storage engine of
[Datalevin](https://github.com/juji-io/datalevin) database. Based on a fork of
[LMDB](https://www.symas.com/mdb), this KV engine supports these additional
features:

* Efficient range count: `mdb_count_range`, `mdb_count_all`,
  `mdb_range_count_values`.
* Random access by rank, i.e. getting ith item: `mdb_get_rank`
* Sample items efficiently.
