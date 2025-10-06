#include "lmdb.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>
#include <unistd.h>
#include <sys/stat.h>
#include <errno.h>

#define CHECK(rc, msg) do { \
    if ((rc) != MDB_SUCCESS) { \
        fprintf(stderr, "%s: %s\n", (msg), mdb_strerror(rc)); \
        exit(EXIT_FAILURE); \
    } \
} while (0)

static int
cmp_key(const MDB_val *a, const MDB_val *b)
{
    size_t min = a->mv_size < b->mv_size ? a->mv_size : b->mv_size;
    int diff = memcmp(a->mv_data, b->mv_data, min);
    if (diff)
        return diff;
    if (a->mv_size < b->mv_size)
        return -1;
    if (a->mv_size > b->mv_size)
        return 1;
    return 0;
}

static uint64_t
naive_count(MDB_txn *txn, MDB_dbi dbi,
            const MDB_val *low, const MDB_val *high,
            int lower_incl, int upper_incl)
{
    MDB_cursor *cur;
    MDB_val key, data;
    uint64_t total = 0;
    int rc = mdb_cursor_open(txn, dbi, &cur);
    CHECK(rc, "mdb_cursor_open");

    rc = mdb_cursor_get(cur, &key, &data, MDB_FIRST);
    while (rc == MDB_SUCCESS) {
        int include = 1;
        if (low) {
            int cmp = cmp_key(&key, low);
            if (cmp < 0 || (cmp == 0 && !lower_incl))
                include = 0;
        }
        if (high) {
            int cmp = cmp_key(&key, high);
            if (cmp > 0 || (cmp == 0 && !upper_incl)) {
                include = 0;
                if (cmp > 0 || (cmp == 0 && !upper_incl))
                    break;
            }
        }
        if (include)
            total++;
        rc = mdb_cursor_get(cur, &key, &data, MDB_NEXT);
    }
    if (rc != MDB_NOTFOUND)
        CHECK(rc, "mdb_cursor_get");

    mdb_cursor_close(cur);
    return total;
}

static void
expect_eq(uint64_t got, uint64_t want, const char *msg)
{
    if (got != want) {
        fprintf(stderr, "%s: expected %" PRIu64 ", got %" PRIu64 "\n",
                msg, want, got);
        exit(EXIT_FAILURE);
    }
}

int
main(void)
{
    const int entries = 512;

    /* Pre-emptively create the directory and chmod it. */
    const char *dir = "./testdb_count";
    if (mkdir(dir, 0775) && errno != EEXIST) {
        perror("mkdir testdb_count");
        return EXIT_FAILURE;
    }
    if (chmod(dir, 0775) && errno != EPERM) {
        perror("chmod testdb_count");
    }
    MDB_env *env;
    MDB_txn *txn;
    MDB_dbi dbi;
    MDB_val key, data;
    char keybuf[16];
    char databuf[16];
    int rc;
    uint64_t total;

    const char *pathbuf = "./testdb_count";
    unlink("./testdb_count/data.mdb");
    unlink("./testdb_count/lock.mdb");

    rc = mdb_env_create(&env);
    CHECK(rc, "mdb_env_create");
    CHECK(mdb_env_set_maxdbs(env, 8), "mdb_env_set_maxdbs");
    CHECK(mdb_env_open(env, pathbuf, MDB_NOLOCK, 0664), "mdb_env_open");

    CHECK(mdb_txn_begin(env, NULL, 0, &txn), "mdb_txn_begin");
    CHECK(mdb_dbi_open(txn, "counted", MDB_CREATE | MDB_COUNTED, &dbi), "mdb_dbi_open");

    for (int i = 0; i < entries; ++i) {
        snprintf(keybuf, sizeof(keybuf), "k%04d", i);
        snprintf(databuf, sizeof(databuf), "v%04d", i);
        key.mv_size = strlen(keybuf);
        key.mv_data = keybuf;
        data.mv_size = strlen(databuf);
        data.mv_data = databuf;
        rc = mdb_put(txn, dbi, &key, &data, 0);
        CHECK(rc, "mdb_put");
    }

    CHECK(mdb_txn_commit(txn), "mdb_txn_commit");

    CHECK(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn), "rd txn");
    CHECK(mdb_count_all(txn, dbi, 0, &total), "mdb_count_all");
    expect_eq(total, entries, "count_all initial");

    MDB_val low, high;
    char lowbuf[] = "k0100";
    char highbuf[] = "k0300";
    low.mv_size = sizeof(lowbuf) - 1;
    low.mv_data = lowbuf;
    high.mv_size = sizeof(highbuf) - 1;
    high.mv_data = highbuf;

    uint64_t naive = naive_count(txn, dbi, &low, &high, 1, 1);
    CHECK(mdb_count_range(txn, dbi, &low, &high,
                          MDB_COUNT_LOWER_INCL | MDB_COUNT_UPPER_INCL, &total),
          "range incl/incl");
    expect_eq(total, naive, "range incl/incl");

    naive = naive_count(txn, dbi, &low, &high, 0, 1);
    CHECK(mdb_count_range(txn, dbi, &low, &high, MDB_COUNT_UPPER_INCL, &total),
          "range excl/incl");
    expect_eq(total, naive, "range excl/incl");

    naive = naive_count(txn, dbi, &low, &high, 1, 0);
    CHECK(mdb_count_range(txn, dbi, &low, &high, MDB_COUNT_LOWER_INCL, &total),
          "range incl/excl");
    expect_eq(total, naive, "range incl/excl");

    naive = naive_count(txn, dbi, &low, &high, 0, 0);
    CHECK(mdb_count_range(txn, dbi, &low, &high, 0, &total),
          "range excl/excl");
    expect_eq(total, naive, "range excl/excl");

    CHECK(mdb_count_range(txn, dbi, &high, &low,
                          MDB_COUNT_LOWER_INCL | MDB_COUNT_UPPER_INCL, &total),
          "range low>high");
    expect_eq(total, 0, "low greater than high");

    CHECK(mdb_count_range(txn, dbi, NULL, &high, MDB_COUNT_UPPER_INCL, &total),
          "range upper only");
    naive = naive_count(txn, dbi, NULL, &high, 0, 1);
    expect_eq(total, naive, "upper bound only");

    CHECK(mdb_count_range(txn, dbi, &low, NULL, MDB_COUNT_LOWER_INCL, &total),
          "range lower only");
    naive = naive_count(txn, dbi, &low, NULL, 1, 0);
    expect_eq(total, naive, "lower bound only");

    mdb_txn_abort(txn);

    /* Delete a slice and re-check counts */
    CHECK(mdb_txn_begin(env, NULL, 0, &txn), "delete txn");
    for (int i = 0; i < 25; ++i) {
        snprintf(keybuf, sizeof(keybuf), "k%04d", i);
        key.mv_size = strlen(keybuf);
        key.mv_data = keybuf;
        rc = mdb_del(txn, dbi, &key, NULL);
        CHECK(rc, "mdb_del");
    }
    CHECK(mdb_txn_commit(txn), "delete commit");

    CHECK(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn), "rd txn 2");
    CHECK(mdb_count_all(txn, dbi, 0, &total), "count after del");
    expect_eq(total, entries - 25, "count_all after deletions");

    naive = naive_count(txn, dbi, &low, &high, 1, 1);
    CHECK(mdb_count_range(txn, dbi, &low, &high,
                          MDB_COUNT_LOWER_INCL | MDB_COUNT_UPPER_INCL, &total),
          "range after del");
    expect_eq(total, naive, "range after deletions");
    mdb_txn_abort(txn);

    mdb_env_close(env);
    return EXIT_SUCCESS;
}
