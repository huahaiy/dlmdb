# DLMDB

This is the key value (KV) storage engine of
[Datalevin](https://github.com/juji-io/datalevin) database. Based on a fork of
[LMDB](https://www.symas.com/mdb), this KV engine supports these additional
features:

* B+ tree is augmented with order statics (subtree counts), so it supports
  efficient range counts in O(log n).
* Getting items by rank, i.e. getting ith item, in O(log n).
* Sample items efficiently.
