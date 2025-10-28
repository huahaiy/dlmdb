#include "lmdb.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>
#include <unistd.h>
#include <sys/stat.h>
#include <errno.h>
#include <pthread.h>

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

static unsigned int
next_rand(unsigned int *state)
{
    *state = (*state * 1103515245u) + 12345u;
    return *state;
}

static uint64_t
naive_count(MDB_txn *txn, MDB_dbi dbi,
            const MDB_val *low, const MDB_val *high,
            int lower_incl, int upper_incl,
            MDB_cmp_func *cmp_func)
{
    MDB_cursor *cur;
    MDB_val key, data;
    uint64_t total = 0;
    int rc = mdb_cursor_open(txn, dbi, &cur);
    CHECK(rc, "mdb_cursor_open");

    if (!cmp_func)
        cmp_func = cmp_key;

    rc = mdb_cursor_get(cur, &key, &data, MDB_FIRST);
    while (rc == MDB_SUCCESS) {
        int include = 1;
        if (low) {
            int cmp = cmp_func(&key, low);
            if (cmp < 0 || (cmp == 0 && !lower_incl))
                include = 0;
        }
        if (high) {
            int cmp = cmp_func(&key, high);
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

static uint64_t
naive_count_values(MDB_txn *txn, MDB_dbi dbi,
                   const MDB_val *key_low, const MDB_val *key_high,
                   int key_lower_incl, int key_upper_incl,
                   MDB_cmp_func *kcmp)
{
    MDB_cursor *cur;
    MDB_val key, data;
    uint64_t total = 0;
    int rc = mdb_cursor_open(txn, dbi, &cur);
    CHECK(rc, "naive_count_values cursor_open");

    if (!kcmp)
        kcmp = cmp_key;

    rc = mdb_cursor_get(cur, &key, &data, MDB_FIRST);
    while (rc == MDB_SUCCESS) {
        int include = 1;
        if (key_low) {
            int cmp = kcmp(&key, key_low);
            if (cmp < 0 || (cmp == 0 && !key_lower_incl))
                include = 0;
        }
        if (include && key_high) {
            int cmp = kcmp(&key, key_high);
            if (cmp > 0 || (cmp == 0 && !key_upper_incl))
                break;
        }
        if (include) {
            mdb_size_t dupcount = 0;
            int drc = mdb_cursor_count(cur, &dupcount);
            CHECK(drc, "naive_count_values count");
            total += (uint64_t)dupcount;
        }
        rc = mdb_cursor_get(cur, &key, &data, MDB_NEXT_NODUP);
    }
    if (rc != MDB_NOTFOUND)
        CHECK(rc, "naive_count_values next_nodup");

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

static void
expect_rc(int rc, int expect, const char *msg)
{
    if (rc != expect) {
        fprintf(stderr, "%s: expected %s (%d), got %s (%d)\n",
                msg, mdb_strerror(expect), expect, mdb_strerror(rc), rc);
        exit(EXIT_FAILURE);
    }
}

static void
expect_val_eq(const MDB_val *val, const char *str, const char *msg)
{
    size_t len = strlen(str);
    if (val->mv_size != len || memcmp(val->mv_data, str, len) != 0) {
        fprintf(stderr,
                "%s: expected \"%s\" len=%zu, got \"%.*s\" len=%zu\n",
                msg, str, len,
                (int)val->mv_size, (const char *)val->mv_data, val->mv_size);
        exit(EXIT_FAILURE);
    }
}

static void
expect_val_match(const MDB_val *got, const MDB_val *want, const char *msg)
{
    if (got->mv_size != want->mv_size ||
        memcmp(got->mv_data, want->mv_data, want->mv_size) != 0) {
        fprintf(stderr,
                "%s: mismatch expected len=%zu \"%.*s\" got len=%zu \"%.*s\"\n",
                msg,
                (size_t)want->mv_size, (int)want->mv_size,
                (const char *)want->mv_data,
                (size_t)got->mv_size, (int)got->mv_size,
                (const char *)got->mv_data);
        exit(EXIT_FAILURE);
    }
}

static void
check_range_matches(MDB_txn *txn, MDB_dbi dbi,
                    const MDB_val *low, const MDB_val *high,
                    unsigned int flags, const char *msg)
{
    int lower_incl = (flags & MDB_COUNT_LOWER_INCL) != 0;
    int upper_incl = (flags & MDB_COUNT_UPPER_INCL) != 0;
    uint64_t naive = naive_count(txn, dbi, low, high,
                                 lower_incl, upper_incl, cmp_key);
    uint64_t counted = 0;
    CHECK(mdb_count_range(txn, dbi, low, high, flags, &counted), msg);
    expect_eq(counted, naive, msg);
}

static int
reverse_cmp(const MDB_val *a, const MDB_val *b)
{
    return cmp_key(b, a);
}

struct concurrency_ctx {
    MDB_env *env;
    MDB_dbi dbi;
    int max_keys;
    int per_txn_ops;
    int reader_queries;
    int iterations;
    volatile int stop;
    unsigned char *present;
    int present_total;
};

static void
format_stage(char *buf, size_t buf_size,
             const char *label, const char *stage)
{
    if (buf_size == 0)
        return;
    if (label && *label) {
        if (stage && *stage)
            snprintf(buf, buf_size, "%s %s", label, stage);
        else
            snprintf(buf, buf_size, "%s", label);
    } else if (stage && *stage) {
        snprintf(buf, buf_size, "%s", stage);
    } else {
        buf[0] = '\0';
    }
}

static void *
count_reader_thread(void *arg)
{
    struct concurrency_ctx *ctx = (struct concurrency_ctx *)arg;
    pthread_t self = pthread_self();
    uintptr_t ident = (uintptr_t)self;
    unsigned int seed = (unsigned int)(ident ^ 0x13579bdu);

    while (!ctx->stop) {
        MDB_txn *txn;
        CHECK(mdb_txn_begin(ctx->env, NULL, MDB_RDONLY, &txn),
              "concurrent reader begin");

        for (int q = 0; q < ctx->reader_queries; ++q) {
            MDB_val low, high;
            MDB_val *low_ptr = NULL;
            MDB_val *high_ptr = NULL;
            unsigned int flags = 0;
            char lowbuf[16];
            char highbuf[16];

            if (next_rand(&seed) & 1u) {
                int low_idx = (int)(next_rand(&seed) % ctx->max_keys);
                snprintf(lowbuf, sizeof(lowbuf), "c%05d", low_idx);
                low.mv_size = strlen(lowbuf);
                low.mv_data = lowbuf;
                low_ptr = &low;
                if (next_rand(&seed) & 1u)
                    flags |= MDB_COUNT_LOWER_INCL;
            }

            if (next_rand(&seed) & 1u) {
                int high_idx = (int)(next_rand(&seed) % ctx->max_keys);
                snprintf(highbuf, sizeof(highbuf), "c%05d", high_idx);
                high.mv_size = strlen(highbuf);
                high.mv_data = highbuf;
                high_ptr = &high;
                if (next_rand(&seed) & 1u)
                    flags |= MDB_COUNT_UPPER_INCL;
            }

            int lower_incl = (flags & MDB_COUNT_LOWER_INCL) != 0;
            int upper_incl = (flags & MDB_COUNT_UPPER_INCL) != 0;
            uint64_t naive = naive_count(txn, ctx->dbi, low_ptr, high_ptr,
                                         lower_incl, upper_incl, cmp_key);
            uint64_t counted = 0;
            CHECK(mdb_count_range(txn, ctx->dbi, low_ptr, high_ptr,
                                  flags, &counted),
                  "concurrent reader range");
            expect_eq(counted, naive, "concurrent reader snapshot");
        }

        uint64_t total = 0;
        CHECK(mdb_count_all(txn, ctx->dbi, 0, &total),
              "concurrent reader count_all");
        uint64_t naive_full = naive_count(txn, ctx->dbi, NULL, NULL, 1, 1,
                                          cmp_key);
        expect_eq(total, naive_full, "concurrent reader total");
        mdb_txn_abort(txn);
    }

    return NULL;
}

static void *
count_writer_thread(void *arg)
{
    struct concurrency_ctx *ctx = (struct concurrency_ctx *)arg;
    unsigned int seed = 0x2468aceu;

    for (int iter = 0; iter < ctx->iterations; ++iter) {
        MDB_txn *txn;
        CHECK(mdb_txn_begin(ctx->env, NULL, 0, &txn),
              "concurrent writer begin");

        for (int op = 0; op < ctx->per_txn_ops; ++op) {
            int idx = (int)(next_rand(&seed) % ctx->max_keys);
            char keybuf[16];
            snprintf(keybuf, sizeof(keybuf), "c%05d", idx);

            MDB_val key;
            key.mv_size = strlen(keybuf);
            key.mv_data = keybuf;

            if (ctx->present[idx]) {
                if (next_rand(&seed) & 1u) {
                    int rc = mdb_del(txn, ctx->dbi, &key, NULL);
                    if (rc == MDB_SUCCESS) {
                        ctx->present[idx] = 0;
                        ctx->present_total--;
                    } else if (rc != MDB_NOTFOUND) {
                        CHECK(rc, "concurrent writer delete");
                    }
                } else {
                    MDB_val data;
                    data.mv_size = 8;
                    data.mv_data = NULL;
                    CHECK(mdb_put(txn, ctx->dbi, &key, &data, MDB_RESERVE),
                          "concurrent writer update");
                    memset(data.mv_data, 'u', data.mv_size);
                }
            } else {
                char valbuf[24];
                snprintf(valbuf, sizeof(valbuf), "val%05d-%d",
                         idx, iter);
                MDB_val data;
                data.mv_size = strlen(valbuf);
                data.mv_data = valbuf;
                CHECK(mdb_put(txn, ctx->dbi, &key, &data, 0),
                      "concurrent writer insert");
                ctx->present[idx] = 1;
                ctx->present_total++;
            }
        }

        CHECK(mdb_txn_commit(txn), "concurrent writer commit");

        if ((iter & 0x0f) == 0) {
            MDB_txn *rtxn;
            CHECK(mdb_txn_begin(ctx->env, NULL, MDB_RDONLY, &rtxn),
                  "concurrent verify begin");

            uint64_t total = 0;
            CHECK(mdb_count_all(rtxn, ctx->dbi, 0, &total),
                  "concurrent verify total");
            expect_eq(total, (uint64_t)ctx->present_total,
                      "concurrent writer total check");

            for (int q = 0; q < 4; ++q) {
                MDB_val low, high;
                MDB_val *low_ptr = NULL;
                MDB_val *high_ptr = NULL;
                unsigned int flags = 0;
                char lowbuf[16];
                char highbuf[16];

                if (next_rand(&seed) & 1u) {
                    int low_idx = (int)(next_rand(&seed) % ctx->max_keys);
                    snprintf(lowbuf, sizeof(lowbuf), "c%05d", low_idx);
                    low.mv_size = strlen(lowbuf);
                    low.mv_data = lowbuf;
                    low_ptr = &low;
                    if (next_rand(&seed) & 1u)
                        flags |= MDB_COUNT_LOWER_INCL;
                }

                if (next_rand(&seed) & 1u) {
                    int high_idx = (int)(next_rand(&seed) % ctx->max_keys);
                    snprintf(highbuf, sizeof(highbuf), "c%05d", high_idx);
                    high.mv_size = strlen(highbuf);
                    high.mv_data = highbuf;
                    high_ptr = &high;
                    if (next_rand(&seed) & 1u)
                        flags |= MDB_COUNT_UPPER_INCL;
                }

                int lower_incl = (flags & MDB_COUNT_LOWER_INCL) != 0;
                int upper_incl = (flags & MDB_COUNT_UPPER_INCL) != 0;
                uint64_t naive = naive_count(rtxn, ctx->dbi, low_ptr, high_ptr,
                                             lower_incl, upper_incl, cmp_key);
                uint64_t counted = 0;
                CHECK(mdb_count_range(rtxn, ctx->dbi, low_ptr, high_ptr,
                                      flags, &counted),
                      "concurrent writer range");
                expect_eq(counted, naive,
                          "concurrent writer range cross");
            }

            mdb_txn_abort(rtxn);
        }
    }

    ctx->stop = 1;
    return NULL;
}

static void
test_concurrent_readers(void)
{
    const char *dir = "./testdb_count_concurrent";
    if (mkdir(dir, 0775) && errno != EEXIST) {
        perror("mkdir testdb_count_concurrent");
        exit(EXIT_FAILURE);
    }
    if (chmod(dir, 0775) && errno != EPERM)
        perror("chmod testdb_count_concurrent");

    unlink("./testdb_count_concurrent/data.mdb");
    unlink("./testdb_count_concurrent/lock.mdb");

    MDB_env *env;
    MDB_txn *txn;
    MDB_dbi dbi;
    struct concurrency_ctx ctx;

    memset(&ctx, 0, sizeof(ctx));
    ctx.max_keys = 2048;
    ctx.per_txn_ops = 8;
    ctx.reader_queries = 6;
    ctx.iterations = 256;
    ctx.present = calloc((size_t)ctx.max_keys, sizeof(unsigned char));
    if (!ctx.present) {
        fprintf(stderr, "failed to allocate concurrency bitmap\n");
        exit(EXIT_FAILURE);
    }

    CHECK(mdb_env_create(&env), "concurrent env create");
    CHECK(mdb_env_set_maxdbs(env, 4), "concurrent env maxdbs");
    CHECK(mdb_env_open(env, dir, MDB_NOLOCK, 0664), "concurrent env open");

    CHECK(mdb_txn_begin(env, NULL, 0, &txn), "concurrent setup begin");
    CHECK(mdb_dbi_open(txn, "concurrent", MDB_CREATE | MDB_COUNTED, &dbi),
          "concurrent dbi open");

    for (int i = 0; i < ctx.max_keys / 4; ++i) {
        char keybuf[16];
        char valbuf[24];
        snprintf(keybuf, sizeof(keybuf), "c%05d", i);
        snprintf(valbuf, sizeof(valbuf), "init%05d", i);
        MDB_val key;
        MDB_val data;
        key.mv_size = strlen(keybuf);
        key.mv_data = keybuf;
        data.mv_size = strlen(valbuf);
        data.mv_data = valbuf;
        CHECK(mdb_put(txn, dbi, &key, &data, 0), "concurrent preload");
        ctx.present[i] = 1;
        ctx.present_total++;
    }

    CHECK(mdb_txn_commit(txn), "concurrent setup commit");

    ctx.env = env;
    ctx.dbi = dbi;
    ctx.stop = 0;

    pthread_t readers[3];
    for (size_t r = 0; r < sizeof(readers) / sizeof(readers[0]); ++r) {
        if (pthread_create(&readers[r], NULL, count_reader_thread, &ctx)) {
            fprintf(stderr, "pthread_create reader failed\n");
            exit(EXIT_FAILURE);
        }
    }

    pthread_t writer;
    if (pthread_create(&writer, NULL, count_writer_thread, &ctx)) {
        fprintf(stderr, "pthread_create writer failed\n");
        exit(EXIT_FAILURE);
    }

    if (pthread_join(writer, NULL)) {
        fprintf(stderr, "pthread_join writer failed\n");
        exit(EXIT_FAILURE);
    }

    ctx.stop = 1;

    for (size_t r = 0; r < sizeof(readers) / sizeof(readers[0]); ++r) {
        if (pthread_join(readers[r], NULL)) {
            fprintf(stderr, "pthread_join reader failed\n");
            exit(EXIT_FAILURE);
        }
    }

    CHECK(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn),
          "concurrent final begin");
    uint64_t total = 0;
    CHECK(mdb_count_all(txn, dbi, 0, &total), "concurrent final total");
    expect_eq(total, (uint64_t)ctx.present_total, "concurrent final check");
    mdb_txn_abort(txn);

    CHECK(mdb_txn_begin(env, NULL, 0, &txn), "concurrent drop begin");
    CHECK(mdb_drop(txn, dbi, 0), "concurrent drop");
    CHECK(mdb_txn_commit(txn), "concurrent drop commit");

    mdb_dbi_close(env, dbi);
    mdb_env_close(env);
    free(ctx.present);

}

static void
test_empty_db(MDB_env *env)
{
    MDB_txn *txn;
    MDB_dbi dbi;
    uint64_t total;
    int rc;
    char lowbuf[] = "low";
    char highbuf[] = "high";
    MDB_val low, high;

    CHECK(mdb_txn_begin(env, NULL, 0, &txn), "empty begin");
    CHECK(mdb_dbi_open(txn, "edge_empty", MDB_CREATE | MDB_COUNTED, &dbi),
          "empty open");
    CHECK(mdb_txn_commit(txn), "empty commit");

    CHECK(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn), "empty read begin");
    rc = mdb_count_all(txn, dbi, 0, &total);
    CHECK(rc, "empty count_all");
    expect_eq(total, 0, "empty count_all zero");

    low.mv_data = lowbuf;
    low.mv_size = strlen(lowbuf);
    high.mv_data = highbuf;
    high.mv_size = strlen(highbuf);

    CHECK(mdb_count_range(txn, dbi, NULL, NULL, 0, &total),
          "empty unbounded");
    expect_eq(total, 0, "empty unbounded zero");

    CHECK(mdb_count_range(txn, dbi, &low, &low,
                          MDB_COUNT_LOWER_INCL | MDB_COUNT_UPPER_INCL,
                          &total),
          "empty equal bounds");
    expect_eq(total, 0, "empty equal bounds zero");

    CHECK(mdb_count_range(txn, dbi, &low, NULL, MDB_COUNT_LOWER_INCL, &total),
          "empty lower only");
    expect_eq(total, 0, "empty lower only zero");

    CHECK(mdb_count_range(txn, dbi, NULL, &high, MDB_COUNT_UPPER_INCL, &total),
          "empty upper only");
    expect_eq(total, 0, "empty upper only zero");

    CHECK(mdb_count_range(txn, dbi, &high, &low,
                          MDB_COUNT_LOWER_INCL | MDB_COUNT_UPPER_INCL,
                          &total),
          "empty reversed bounds");
    expect_eq(total, 0, "empty reversed bounds zero");

    mdb_txn_abort(txn);
    mdb_dbi_close(env, dbi);
}

static void
test_single_key(MDB_env *env)
{
    MDB_txn *txn;
    MDB_dbi dbi;
    uint64_t total;
    MDB_val key, data;

    CHECK(mdb_txn_begin(env, NULL, 0, &txn), "single begin");
    CHECK(mdb_dbi_open(txn, "edge_single", MDB_CREATE | MDB_COUNTED, &dbi),
          "single open");

    key.mv_data = "solo";
    key.mv_size = 4;
    data.mv_data = "value";
    data.mv_size = 5;
    CHECK(mdb_put(txn, dbi, &key, &data, 0), "single put");
    CHECK(mdb_txn_commit(txn), "single commit");

    CHECK(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn), "single read begin");
    CHECK(mdb_count_all(txn, dbi, 0, &total), "single count_all");
    expect_eq(total, 1, "single count_all one");

    uint64_t naive = naive_count(txn, dbi, &key, &key, 1, 1, cmp_key);
    CHECK(mdb_count_range(txn, dbi, &key, &key,
                          MDB_COUNT_LOWER_INCL | MDB_COUNT_UPPER_INCL,
                          &total),
          "single incl/incl");
    expect_eq(total, naive, "single incl/incl one");

    naive = naive_count(txn, dbi, &key, &key, 0, 1, cmp_key);
    CHECK(mdb_count_range(txn, dbi, &key, &key, MDB_COUNT_UPPER_INCL, &total),
          "single excl/incl");
    expect_eq(total, naive, "single excl/incl zero");

    naive = naive_count(txn, dbi, &key, &key, 1, 0, cmp_key);
    CHECK(mdb_count_range(txn, dbi, &key, &key, MDB_COUNT_LOWER_INCL, &total),
          "single incl/excl");
    expect_eq(total, naive, "single incl/excl zero");

    naive = naive_count(txn, dbi, &key, &key, 0, 0, cmp_key);
    CHECK(mdb_count_range(txn, dbi, &key, &key, 0, &total),
          "single excl/excl");
    expect_eq(total, naive, "single excl/excl zero");

    naive = naive_count(txn, dbi, NULL, &key, 0, 1, cmp_key);
    CHECK(mdb_count_range(txn, dbi, NULL, &key, MDB_COUNT_UPPER_INCL, &total),
          "single upper only");
    expect_eq(total, naive, "single upper only one");

    naive = naive_count(txn, dbi, NULL, &key, 0, 0, cmp_key);
    CHECK(mdb_count_range(txn, dbi, NULL, &key, 0, &total),
          "single upper excl");
    expect_eq(total, naive, "single upper excl zero");

    naive = naive_count(txn, dbi, &key, NULL, 1, 0, cmp_key);
    CHECK(mdb_count_range(txn, dbi, &key, NULL, MDB_COUNT_LOWER_INCL, &total),
          "single lower only");
    expect_eq(total, naive, "single lower only one");

    naive = naive_count(txn, dbi, &key, NULL, 0, 0, cmp_key);
    CHECK(mdb_count_range(txn, dbi, &key, NULL, 0, &total),
          "single lower excl");
    expect_eq(total, naive, "single lower excl zero");

    mdb_txn_abort(txn);
    mdb_dbi_close(env, dbi);
}

static void
test_extreme_keys(MDB_env *env)
{
    MDB_txn *txn;
    MDB_dbi dbi;
    MDB_val key, data;
    uint64_t total;
    unsigned char tiny_key[1] = { 0x00 };
    char small_key[] = "a";
    char big_key[500];
    memset(big_key, 'Z', sizeof(big_key));

    CHECK(mdb_txn_begin(env, NULL, 0, &txn), "extreme begin");
    CHECK(mdb_dbi_open(txn, "edge_extreme", MDB_CREATE | MDB_COUNTED, &dbi),
          "extreme open");

    key.mv_data = tiny_key;
    key.mv_size = sizeof(tiny_key);
    data.mv_data = "tiny";
    data.mv_size = 4;
    CHECK(mdb_put(txn, dbi, &key, &data, 0), "extreme put tiny");

    key.mv_data = small_key;
    key.mv_size = sizeof(small_key) - 1;
    data.mv_data = "small";
    data.mv_size = 5;
    CHECK(mdb_put(txn, dbi, &key, &data, 0), "extreme put small");

    key.mv_data = big_key;
    key.mv_size = sizeof(big_key);
    data.mv_data = "large";
    data.mv_size = 5;
    CHECK(mdb_put(txn, dbi, &key, &data, 0), "extreme put large");

    CHECK(mdb_txn_commit(txn), "extreme commit");

    CHECK(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn), "extreme read begin");
    CHECK(mdb_count_all(txn, dbi, 0, &total), "extreme count_all");
    expect_eq(total, 3, "extreme total three");

    MDB_val low, high;
    low.mv_data = tiny_key;
    low.mv_size = sizeof(tiny_key);
    high.mv_data = big_key;
    high.mv_size = sizeof(big_key);

    uint64_t naive = naive_count(txn, dbi, &low, &high, 1, 1, cmp_key);
    CHECK(mdb_count_range(txn, dbi, &low, &high,
                          MDB_COUNT_LOWER_INCL | MDB_COUNT_UPPER_INCL,
                          &total),
          "extreme full inclusive");
    expect_eq(total, naive, "extreme full inclusive three");

    naive = naive_count(txn, dbi, &low, &high, 0, 1, cmp_key);
    CHECK(mdb_count_range(txn, dbi, &low, &high, MDB_COUNT_UPPER_INCL, &total),
          "extreme lower excl");
    expect_eq(total, naive, "extreme lower excl two");

    MDB_val mid;
    mid.mv_data = small_key;
    mid.mv_size = sizeof(small_key) - 1;

    naive = naive_count(txn, dbi, &mid, &high, 1, 0, cmp_key);
    CHECK(mdb_count_range(txn, dbi, &mid, &high, MDB_COUNT_LOWER_INCL, &total),
          "extreme upper excl");
    expect_eq(total, naive, "extreme upper excl one");

    naive = naive_count(txn, dbi, &high, &mid, 1, 1, cmp_key);
    CHECK(mdb_count_range(txn, dbi, &high, &mid,
                          MDB_COUNT_LOWER_INCL | MDB_COUNT_UPPER_INCL,
                          &total),
          "extreme reversed");
    expect_eq(total, naive, "extreme reversed zero");

    mdb_txn_abort(txn);
    mdb_dbi_close(env, dbi);
}

static void
test_range_outside_bounds(MDB_env *env)
{
    MDB_txn *txn;
    MDB_dbi dbi;
    MDB_val key, data, low, high;
    uint64_t total;
    const char *keys[] = { "key1", "key2", "key3" };
    const char *payload = "value";
    const char *lower_bound = "aa";
    const char *upper_bound = "zzzz";

    CHECK(mdb_txn_begin(env, NULL, 0, &txn), "outside begin");
    CHECK(mdb_dbi_open(txn, "range_outside", MDB_CREATE | MDB_COUNTED, &dbi),
          "outside open");

    data.mv_data = (void *)payload;
    data.mv_size = strlen(payload);

    for (size_t i = 0; i < sizeof(keys) / sizeof(keys[0]); ++i) {
        key.mv_data = (void *)keys[i];
        key.mv_size = strlen(keys[i]);
        CHECK(mdb_put(txn, dbi, &key, &data, 0), "outside put");
    }

    CHECK(mdb_txn_commit(txn), "outside commit");

    CHECK(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn), "outside read begin");

    low.mv_data = (void *)lower_bound;
    low.mv_size = strlen(lower_bound);
    high.mv_data = (void *)upper_bound;
    high.mv_size = strlen(upper_bound);

    uint64_t naive = naive_count(txn, dbi, &low, &high, 1, 1, cmp_key);
    CHECK(mdb_count_range(txn, dbi, &low, &high,
                          MDB_COUNT_LOWER_INCL | MDB_COUNT_UPPER_INCL,
                          &total),
          "outside inclusive");
    expect_eq(total, naive, "outside inclusive matches naive");
    expect_eq(total, 3, "outside inclusive spans all entries");

    mdb_txn_abort(txn);
    mdb_dbi_close(env, dbi);
}

static void
test_custom_comparator(MDB_env *env)
{
    MDB_txn *txn;
    MDB_dbi dbi;
    MDB_val key, data;
    uint64_t total;
    const char *keys[] = { "aa", "bb", "cc" };

    CHECK(mdb_txn_begin(env, NULL, 0, &txn), "custom begin");
    CHECK(mdb_dbi_open(txn, "edge_custom", MDB_CREATE | MDB_COUNTED, &dbi),
          "custom open");
    CHECK(mdb_set_compare(txn, dbi, reverse_cmp), "custom compare");

    for (size_t i = 0; i < sizeof(keys) / sizeof(keys[0]); ++i) {
        key.mv_data = (void *)keys[i];
        key.mv_size = strlen(keys[i]);
        data.mv_data = (void *)keys[i];
        data.mv_size = strlen(keys[i]);
        CHECK(mdb_put(txn, dbi, &key, &data, 0), "custom put");
    }

    CHECK(mdb_txn_commit(txn), "custom commit");

    CHECK(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn), "custom read begin");
    CHECK(mdb_count_all(txn, dbi, 0, &total), "custom count_all");
    expect_eq(total, 3, "custom total three");

    MDB_val low, high;
    low.mv_data = (void *)keys[1];
    low.mv_size = strlen(keys[1]);
    high.mv_data = (void *)keys[0];
    high.mv_size = strlen(keys[0]);

    uint64_t naive = naive_count(txn, dbi, &low, &high, 1, 1, reverse_cmp);
    CHECK(mdb_count_range(txn, dbi, &low, &high,
                          MDB_COUNT_LOWER_INCL | MDB_COUNT_UPPER_INCL,
                          &total),
          "custom range bb-aa");
    expect_eq(total, naive, "custom range bb-aa two");

    naive = naive_count(txn, dbi, &low, &high, 0, 1, reverse_cmp);
    CHECK(mdb_count_range(txn, dbi, &low, &high, MDB_COUNT_UPPER_INCL, &total),
          "custom lower excl");
    expect_eq(total, naive, "custom lower excl one");

    naive = naive_count(txn, dbi, &low, &high, 1, 0, reverse_cmp);
    CHECK(mdb_count_range(txn, dbi, &low, &high, MDB_COUNT_LOWER_INCL, &total),
          "custom upper excl");
    expect_eq(total, naive, "custom upper excl one");

    high.mv_data = (void *)keys[2];
    high.mv_size = strlen(keys[2]);
    naive = naive_count(txn, dbi, &low, &high, 1, 1, reverse_cmp);
    CHECK(mdb_count_range(txn, dbi, &low, &high,
                          MDB_COUNT_LOWER_INCL | MDB_COUNT_UPPER_INCL,
                          &total),
          "custom out of order");
    expect_eq(total, naive, "custom out of order zero");

    MDB_val upper;
    upper.mv_data = (void *)keys[1];
    upper.mv_size = strlen(keys[1]);
    naive = naive_count(txn, dbi, NULL, &upper, 0, 1, reverse_cmp);
    CHECK(mdb_count_range(txn, dbi, NULL, &upper, MDB_COUNT_UPPER_INCL, &total),
          "custom head");
    expect_eq(total, naive, "custom head two");

    naive = naive_count(txn, dbi, NULL, &upper, 0, 0, reverse_cmp);
    CHECK(mdb_count_range(txn, dbi, NULL, &upper, 0, &total),
          "custom head excl");
    expect_eq(total, naive, "custom head excl one");

    mdb_txn_abort(txn);
    mdb_dbi_close(env, dbi);
}

static void
test_range_count_values(MDB_env *env)
{
    MDB_txn *txn;
    MDB_dbi dbi;
    MDB_val key, data;
    char keybuf[16];
    char valbuf[2];

    CHECK(mdb_txn_begin(env, NULL, 0, &txn), "dup range begin");
    CHECK(mdb_dbi_open(txn, "dup_values", MDB_CREATE | MDB_DUPSORT | MDB_COUNTED, &dbi),
          "dup range open");

    for (int i = 0; i < 6; ++i) {
        snprintf(keybuf, sizeof(keybuf), "k%02d", i);
        key.mv_size = strlen(keybuf);
        key.mv_data = keybuf;
        for (int j = 0; j < 5; ++j) {
            valbuf[0] = 'a' + j;
            valbuf[1] = '\0';
            data.mv_size = 1;
            data.mv_data = valbuf;
            CHECK(mdb_put(txn, dbi, &key, &data, 0), "dup range put");
        }
    }

    CHECK(mdb_txn_commit(txn), "dup range commit");

    CHECK(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn), "dup range read");

    MDB_val key_low, key_high;
    uint64_t counted = 0;
    uint64_t naive = 0;
    uint64_t total_all = 0;

    key_low.mv_data = "k01";
    key_low.mv_size = 3;
    key_high.mv_data = "k03";
    key_high.mv_size = 3;

    naive = naive_count_values(txn, dbi, &key_low, &key_high, 1, 1, cmp_key);
    CHECK(mdb_range_count_values(txn, dbi, &key_low, &key_high,
                                 MDB_COUNT_LOWER_INCL | MDB_COUNT_UPPER_INCL,
                                 &counted),
          "dup range inclusive");
    expect_eq(counted, naive, "dup range inclusive match");

    naive = naive_count_values(txn, dbi, &key_low, &key_high, 0, 1, cmp_key);
    CHECK(mdb_range_count_values(txn, dbi, &key_low, &key_high,
                                 MDB_COUNT_UPPER_INCL,
                                 &counted),
          "dup range lower exclusive");
    expect_eq(counted, naive, "dup range lower exclusive match");

    naive = naive_count_values(txn, dbi, &key_low, &key_high, 1, 0, cmp_key);
    CHECK(mdb_range_count_values(txn, dbi, &key_low, &key_high,
                                 MDB_COUNT_LOWER_INCL,
                                 &counted),
          "dup range upper exclusive");
    expect_eq(counted, naive, "dup range upper exclusive match");

    naive = naive_count_values(txn, dbi, NULL, NULL, 0, 0, cmp_key);
    CHECK(mdb_range_count_values(txn, dbi, NULL, NULL, 0, &counted),
          "dup range full db");
    expect_eq(counted, naive, "dup range full db match");
    CHECK(mdb_count_all(txn, dbi, 0, &total_all), "dup range count_all total");
    expect_eq(total_all, naive, "dup range count_all match");

    MDB_val reverse_low = key_high;
    MDB_val reverse_high = key_low;
    naive = naive_count_values(txn, dbi, &reverse_low, &reverse_high, 1, 1,
                               cmp_key);
    CHECK(mdb_range_count_values(txn, dbi, &reverse_low, &reverse_high,
                                 MDB_COUNT_LOWER_INCL | MDB_COUNT_UPPER_INCL,
                                 &counted),
          "dup range inverted");
    expect_eq(counted, naive, "dup range inverted zero");

    mdb_txn_abort(txn);

    CHECK(mdb_txn_begin(env, NULL, 0, &txn), "dup range add begin");
    key.mv_data = "k02";
    key.mv_size = 3;
    for (int j = 0; j < 3; ++j) {
        valbuf[0] = (char)('f' + j);
        valbuf[1] = '\0';
        data.mv_size = 1;
        data.mv_data = valbuf;
        CHECK(mdb_put(txn, dbi, &key, &data, 0), "dup range add dup");
    }
    CHECK(mdb_txn_commit(txn), "dup range add commit");

    CHECK(mdb_txn_begin(env, NULL, 0, &txn), "dup range delete begin");
    key.mv_data = "k01";
    key.mv_size = 3;
    data.mv_size = 1;
    data.mv_data = "a";
    CHECK(mdb_del(txn, dbi, &key, &data), "dup range delete a");
    data.mv_data = "b";
    CHECK(mdb_del(txn, dbi, &key, &data), "dup range delete b");
    CHECK(mdb_txn_commit(txn), "dup range delete commit");

    CHECK(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn), "dup range reread");

    naive = naive_count_values(txn, dbi, &key_low, &key_high, 1, 1, cmp_key);
    CHECK(mdb_range_count_values(txn, dbi, &key_low, &key_high,
                                 MDB_COUNT_LOWER_INCL | MDB_COUNT_UPPER_INCL,
                                 &counted),
          "dup range post mutate full");
    expect_eq(counted, naive, "dup range post mutate full match");
    CHECK(mdb_count_all(txn, dbi, 0, &total_all), "dup range count_all post mutate");
    uint64_t naive_total = naive_count_values(txn, dbi, NULL, NULL, 0, 0, cmp_key);
    expect_eq(total_all, naive_total, "dup range count_all post mutate total");

    mdb_txn_abort(txn);

    CHECK(mdb_txn_begin(env, NULL, 0, &txn), "dup range drop begin");
    CHECK(mdb_drop(txn, dbi, 0), "dup range drop");
    CHECK(mdb_txn_commit(txn), "dup range drop commit");
    mdb_dbi_close(env, dbi);
}

static void
test_count_all_plain(MDB_env *env)
{
    MDB_txn *txn;
    MDB_dbi dbi;
    MDB_val key, data;
    char keybuf[16];
    char valbuf[16];
    uint64_t total = 0;

    CHECK(mdb_txn_begin(env, NULL, 0, &txn), "count_all plain begin");
    CHECK(mdb_dbi_open(txn, "plain_all", MDB_CREATE | MDB_COUNTED, &dbi),
          "count_all plain open");

    CHECK(mdb_txn_commit(txn), "count_all plain commit empty");

    CHECK(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn), "count_all plain rd empty");
    CHECK(mdb_count_all(txn, dbi, 0, &total), "count_all plain empty");
    expect_eq(total, 0, "count_all plain empty total");
    mdb_txn_abort(txn);

    CHECK(mdb_txn_begin(env, NULL, 0, &txn), "count_all plain load");
    for (int i = 0; i < 25; ++i) {
        snprintf(keybuf, sizeof(keybuf), "k%03d", i);
        snprintf(valbuf, sizeof(valbuf), "v%03d", i);
        key.mv_size = strlen(keybuf);
        key.mv_data = keybuf;
        data.mv_size = strlen(valbuf);
        data.mv_data = valbuf;
        CHECK(mdb_put(txn, dbi, &key, &data, 0), "count_all plain put");
    }
    CHECK(mdb_txn_commit(txn), "count_all plain load commit");

    CHECK(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn), "count_all plain rd full");
    CHECK(mdb_count_all(txn, dbi, 0, &total), "count_all plain full");
    expect_eq(total, 25, "count_all plain total");
    mdb_txn_abort(txn);

    CHECK(mdb_txn_begin(env, NULL, 0, &txn), "count_all plain delete");
    for (int i = 0; i < 10; ++i) {
        snprintf(keybuf, sizeof(keybuf), "k%03d", i);
        key.mv_size = strlen(keybuf);
        key.mv_data = keybuf;
        CHECK(mdb_del(txn, dbi, &key, NULL), "count_all plain del");
    }
    CHECK(mdb_txn_commit(txn), "count_all plain delete commit");

    CHECK(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn), "count_all plain rd trim");
    CHECK(mdb_count_all(txn, dbi, 0, &total), "count_all plain trimmed");
    expect_eq(total, 15, "count_all plain trimmed total");
    mdb_txn_abort(txn);

    CHECK(mdb_txn_begin(env, NULL, 0, &txn), "count_all plain drop begin");
    CHECK(mdb_drop(txn, dbi, 0), "count_all plain drop");
    CHECK(mdb_txn_commit(txn), "count_all plain drop commit");
    mdb_dbi_close(env, dbi);
}

static void
test_count_all_dupsort(MDB_env *env)
{
    MDB_txn *txn;
    MDB_dbi dbi;
    MDB_val key, data;
    char keybuf[8];
    char valbuf[8];
    uint64_t total = 0;

    CHECK(mdb_txn_begin(env, NULL, 0, &txn), "count_all dup begin");
    CHECK(mdb_dbi_open(txn, "dup_all", MDB_CREATE | MDB_DUPSORT | MDB_COUNTED, &dbi),
          "count_all dup open");

    CHECK(mdb_txn_commit(txn), "count_all dup commit empty");

    CHECK(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn), "count_all dup rd empty");
    CHECK(mdb_count_all(txn, dbi, 0, &total), "count_all dup empty");
    expect_eq(total, 0, "count_all dup empty total");
    mdb_txn_abort(txn);

    CHECK(mdb_txn_begin(env, NULL, 0, &txn), "count_all dup load");
    for (int k = 0; k < 4; ++k) {
        snprintf(keybuf, sizeof(keybuf), "k%d", k);
        key.mv_size = strlen(keybuf);
        key.mv_data = keybuf;
        for (int v = 0; v < 6; ++v) {
            snprintf(valbuf, sizeof(valbuf), "v%d", v);
            data.mv_size = strlen(valbuf);
            data.mv_data = valbuf;
            CHECK(mdb_put(txn, dbi, &key, &data, 0), "count_all dup put");
        }
    }
    CHECK(mdb_txn_commit(txn), "count_all dup load commit");

    CHECK(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn), "count_all dup rd full");
    CHECK(mdb_count_all(txn, dbi, 0, &total), "count_all dup full");
    expect_eq(total, 24, "count_all dup total");
    mdb_txn_abort(txn);

    CHECK(mdb_txn_begin(env, NULL, 0, &txn), "count_all dup delete");
    key.mv_size = 2;
    key.mv_data = "k1";
    data.mv_size = 2;
    data.mv_data = "v0";
    CHECK(mdb_del(txn, dbi, &key, &data), "count_all dup del");
    data.mv_data = "v1";
    CHECK(mdb_del(txn, dbi, &key, &data), "count_all dup del2");
    CHECK(mdb_txn_commit(txn), "count_all dup delete commit");

    CHECK(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn), "count_all dup rd trim");
    CHECK(mdb_count_all(txn, dbi, 0, &total), "count_all dup trimmed");
    expect_eq(total, 22, "count_all dup trimmed total");
    mdb_txn_abort(txn);

    CHECK(mdb_txn_begin(env, NULL, 0, &txn), "count_all dup drop begin");
    CHECK(mdb_drop(txn, dbi, 0), "count_all dup drop");
    CHECK(mdb_txn_commit(txn), "count_all dup drop commit");
    mdb_dbi_close(env, dbi);
}

#define RANK_DUP_MAX 64

struct kv_pair {
    MDB_val key;
    MDB_val data;
};

static int
pair_cmp(const void *a, const void *b)
{
    const struct kv_pair *pa = (const struct kv_pair *)a;
    const struct kv_pair *pb = (const struct kv_pair *)b;
    int rc = cmp_key(&pa->key, &pb->key);
    if (rc)
        return rc;
    return cmp_key(&pa->data, &pb->data);
}

static void
test_random_access_plain(MDB_env *env)
{
    const int count = 120;
    MDB_txn *txn;
    MDB_dbi dbi;
    MDB_val key, data;
    MDB_cursor *cur;

    CHECK(mdb_txn_begin(env, NULL, 0, &txn), "rank plain begin");
    CHECK(mdb_dbi_open(txn, "rank_plain", MDB_CREATE | MDB_COUNTED, &dbi),
          "rank plain open");

    for (int i = 0; i < count; ++i) {
        char keybuf[32];
        char databuf[32];
        snprintf(keybuf, sizeof(keybuf), "rp%05d", i);
        snprintf(databuf, sizeof(databuf), "rv%05d", i);
        key.mv_size = strlen(keybuf);
        key.mv_data = keybuf;
        data.mv_size = strlen(databuf);
        data.mv_data = databuf;
        CHECK(mdb_put(txn, dbi, &key, &data, MDB_APPEND), "rank plain put");
    }

    CHECK(mdb_txn_commit(txn), "rank plain commit");

    CHECK(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn), "rank plain rd");
    CHECK(mdb_cursor_open(txn, dbi, &cur), "rank plain cursor");

    for (uint64_t idx = 0; idx < (uint64_t)count; ++idx) {
        MDB_val got_key, got_data;
        CHECK(mdb_cursor_get_rank(cur, idx, &got_key, &got_data, 0),
              "rank plain cursor get");

        char expect_key[32];
        char expect_data[32];
        snprintf(expect_key, sizeof(expect_key), "rp%05" PRIu64, idx);
        snprintf(expect_data, sizeof(expect_data), "rv%05" PRIu64, idx);

        expect_val_eq(&got_key, expect_key, "rank plain cursor key");
        expect_val_eq(&got_data, expect_data, "rank plain cursor data");

        MDB_val lookup_key = {strlen(expect_key), (void *)expect_key};
        uint64_t key_rank = UINT64_MAX;
        CHECK(mdb_get_key_rank(txn, dbi, &lookup_key, NULL, &key_rank),
              "rank plain api key rank");
        expect_eq(key_rank, idx, "rank plain api key rank value");

        MDB_val lookup_data = {strlen(expect_data), (void *)expect_data};
        key_rank = UINT64_MAX;
        CHECK(mdb_get_key_rank(txn, dbi, &lookup_key, &lookup_data, &key_rank),
              "rank plain api key/data rank");
        expect_eq(key_rank, idx, "rank plain api key/data rank value");

        uint64_t cursor_rank = UINT64_MAX;
        CHECK(mdb_cursor_key_rank(cur, &lookup_key, NULL, 0, &cursor_rank),
              "rank plain cursor key rank");
        expect_eq(cursor_rank, idx, "rank plain cursor key rank value");

        cursor_rank = UINT64_MAX;
        CHECK(mdb_cursor_key_rank(cur, &lookup_key, &lookup_data, 0, &cursor_rank),
              "rank plain cursor key/data rank");
        expect_eq(cursor_rank, idx, "rank plain cursor key/data rank value");

        MDB_val api_key, api_data;
        CHECK(mdb_get_rank(txn, dbi, idx, &api_key, &api_data),
              "rank plain api");
        expect_val_eq(&api_key, expect_key, "rank plain api key");
        expect_val_eq(&api_data, expect_data, "rank plain api data");

        if (idx == 7) {
            MDB_val key_only;
            CHECK(mdb_cursor_get_rank(cur, idx, &key_only, NULL, 0),
                  "rank plain key only");
            expect_val_eq(&key_only, expect_key, "rank plain key only value");
        }

        if (idx == 23) {
            MDB_val key_only;
            CHECK(mdb_cursor_get_rank(cur, idx, &key_only, &got_data, 0),
                  "rank plain reuse data");
            expect_val_eq(&key_only, expect_key, "rank plain reuse key");
        }
    }

    uint64_t dummy_rank = 0;
    const char *flag_key_str = "rp00000";
    MDB_val flag_key = {strlen(flag_key_str), (void *)flag_key_str};
    int rc = mdb_cursor_key_rank(cur, &flag_key, NULL, 1, &dummy_rank);
    expect_rc(rc, EINVAL, "rank plain cursor key rank flags");

    const char *missing_key_str = "rp99999";
    MDB_val missing_key = {strlen(missing_key_str), (void *)missing_key_str};
    rc = mdb_get_key_rank(txn, dbi, &missing_key, NULL, &dummy_rank);
    expect_rc(rc, MDB_NOTFOUND, "rank plain missing key");

    const char *mismatch_key_str = "rp00010";
    const char *mismatch_data_str = "rv99999";
    MDB_val mismatch_key = {strlen(mismatch_key_str), (void *)mismatch_key_str};
    MDB_val mismatch_data = {strlen(mismatch_data_str), (void *)mismatch_data_str};
    rc = mdb_get_key_rank(txn, dbi, &mismatch_key, &mismatch_data, &dummy_rank);
    expect_rc(rc, MDB_NOTFOUND, "rank plain mismatched data");

    rc = mdb_cursor_get_rank(cur, count, &key, &data, 0);
    expect_rc(rc, MDB_NOTFOUND, "rank plain eof");

    rc = mdb_cursor_get_rank(cur, 0, NULL, NULL, 1);
    expect_rc(rc, EINVAL, "rank plain bad flags");

    mdb_cursor_close(cur);
    mdb_txn_abort(txn);

    CHECK(mdb_txn_begin(env, NULL, 0, &txn), "rank plain drop begin");
    CHECK(mdb_drop(txn, dbi, 1), "rank plain drop");
    CHECK(mdb_txn_commit(txn), "rank plain drop commit");
    mdb_dbi_close(env, dbi);

    CHECK(mdb_txn_begin(env, NULL, 0, &txn), "rank plain uncounted begin");
    MDB_dbi uncounted;
    CHECK(mdb_dbi_open(txn, "rank_plain_uncounted", MDB_CREATE, &uncounted),
          "rank plain uncounted open");
    char uk = 'x';
    char ud = 'y';
    key.mv_size = 1;
    key.mv_data = &uk;
    data.mv_size = 1;
    data.mv_data = &ud;
    CHECK(mdb_put(txn, uncounted, &key, &data, 0), "rank plain uncounted put");
    CHECK(mdb_txn_commit(txn), "rank plain uncounted commit");

    CHECK(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn), "rank plain uncounted rd");
    rc = mdb_get_rank(txn, uncounted, 0, &key, &data);
    expect_rc(rc, MDB_INCOMPATIBLE, "rank plain incompatible");
    uint64_t rank_check = 0;
    MDB_val sample_key = {1, &uk};
    rc = mdb_get_key_rank(txn, uncounted, &sample_key, NULL, &rank_check);
    expect_rc(rc, MDB_INCOMPATIBLE, "rank plain key incompatible");
    mdb_txn_abort(txn);

    CHECK(mdb_txn_begin(env, NULL, 0, &txn), "rank plain uncounted drop begin");
    CHECK(mdb_drop(txn, uncounted, 1), "rank plain uncounted drop");
    CHECK(mdb_txn_commit(txn), "rank plain uncounted drop commit");
    mdb_dbi_close(env, uncounted);
}

static void
test_random_access_dupsort(MDB_env *env)
{
    const int dup_counts[] = {1, 3, 5, 2, 4};
    const int num_keys = (int)(sizeof(dup_counts) / sizeof(dup_counts[0]));
    size_t total = 0;
    for (int i = 0; i < num_keys; ++i)
        total += (size_t)dup_counts[i];
    if (total > RANK_DUP_MAX) {
        fprintf(stderr, "rank dup configuration too large\n");
        exit(EXIT_FAILURE);
    }

    char keybufs[RANK_DUP_MAX][16];
    char databufs[RANK_DUP_MAX][16];
    struct kv_pair expected[RANK_DUP_MAX];
    struct kv_pair shuffled[RANK_DUP_MAX];

    size_t index = 0;
    for (int k = 0; k < num_keys; ++k) {
        for (int d = 0; d < dup_counts[k]; ++d) {
            snprintf(keybufs[index], sizeof(keybufs[index]), "dk%02d", k);
            snprintf(databufs[index], sizeof(databufs[index]), "dv%02d_%02d", k, d);
            expected[index].key.mv_size = strlen(keybufs[index]);
            expected[index].key.mv_data = keybufs[index];
            expected[index].data.mv_size = strlen(databufs[index]);
            expected[index].data.mv_data = databufs[index];
            ++index;
        }
    }

    memcpy(shuffled, expected, sizeof(struct kv_pair) * total);
    unsigned int shuffle_seed = 0x9e3779b9u;
    for (size_t i = 0; i < total; ++i) {
        size_t swap_idx = next_rand(&shuffle_seed) % total;
        struct kv_pair tmp = shuffled[i];
        shuffled[i] = shuffled[swap_idx];
        shuffled[swap_idx] = tmp;
    }

    MDB_txn *txn;
    MDB_dbi dbi;
    CHECK(mdb_txn_begin(env, NULL, 0, &txn), "rank dup begin");
    CHECK(mdb_dbi_open(txn, "rank_dup", MDB_CREATE | MDB_COUNTED | MDB_DUPSORT, &dbi),
          "rank dup open");

    for (size_t i = 0; i < total; ++i) {
        CHECK(mdb_put(txn, dbi, &shuffled[i].key, &shuffled[i].data, 0),
              "rank dup put");
    }

    CHECK(mdb_txn_commit(txn), "rank dup commit");

    qsort(expected, total, sizeof(expected[0]), pair_cmp);
    size_t prefix[num_keys + 1];
    prefix[0] = 0;
    for (int i = 0; i < num_keys; ++i)
        prefix[i + 1] = prefix[i] + (size_t)dup_counts[i];

    CHECK(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn), "rank dup rd");
    MDB_cursor *cur;
    CHECK(mdb_cursor_open(txn, dbi, &cur), "rank dup cursor");

    size_t group = 0;
    for (uint64_t idx = 0; idx < total; ++idx) {
        MDB_val got_key, got_data;
        CHECK(mdb_cursor_get_rank(cur, idx, &got_key, &got_data, 0),
              "rank dup cursor get");
        expect_val_match(&got_key, &expected[idx].key, "rank dup key");
        expect_val_match(&got_data, &expected[idx].data, "rank dup data");

        uint64_t pair_rank = UINT64_MAX;
        CHECK(mdb_get_key_rank(txn, dbi, &expected[idx].key, &expected[idx].data, &pair_rank),
              "rank dup api key/data rank");
        expect_eq(pair_rank, idx, "rank dup api key/data rank value");

        uint64_t cursor_pair_rank = UINT64_MAX;
        CHECK(mdb_cursor_key_rank(cur, &expected[idx].key, &expected[idx].data, 0, &cursor_pair_rank),
              "rank dup cursor key/data rank");
        expect_eq(cursor_pair_rank, idx, "rank dup cursor key/data rank value");

        while (group + 1 <= (size_t)num_keys && idx >= prefix[group + 1])
            group++;
        if (idx == prefix[group]) {
            uint64_t first_rank = UINT64_MAX;
            CHECK(mdb_get_key_rank(txn, dbi, &expected[idx].key, NULL, &first_rank),
                  "rank dup api key rank");
            expect_eq(first_rank, idx, "rank dup api key rank value");

            uint64_t cursor_first = UINT64_MAX;
            CHECK(mdb_cursor_key_rank(cur, &expected[idx].key, NULL, 0, &cursor_first),
                  "rank dup cursor key rank");
            expect_eq(cursor_first, idx, "rank dup cursor key rank value");
        }

        if (idx == 3) {
            MDB_val key_only;
            CHECK(mdb_cursor_get_rank(cur, idx, &key_only, NULL, 0),
                  "rank dup key only");
            expect_val_match(&key_only, &expected[idx].key,
                             "rank dup key only value");
        }
    }

    uint64_t dup_dummy = 0;
    int rc = mdb_cursor_key_rank(cur, &expected[0].key, &expected[0].data, 1, &dup_dummy);
    expect_rc(rc, EINVAL, "rank dup cursor key rank flags");

    const char *missing_dup_key = "zz";
    MDB_val missing_dup = {strlen(missing_dup_key), (void *)missing_dup_key};
    rc = mdb_get_key_rank(txn, dbi, &missing_dup, NULL, &dup_dummy);
    expect_rc(rc, MDB_NOTFOUND, "rank dup missing key");

    const char *bad_data_str = "dv_bad";
    MDB_val bad_data = {strlen(bad_data_str), (void *)bad_data_str};
    rc = mdb_get_key_rank(txn, dbi, &expected[0].key, &bad_data, &dup_dummy);
    expect_rc(rc, MDB_NOTFOUND, "rank dup missing data");

    unsigned int verify_seed = 0x1234abcdu;
    for (int i = 0; i < 10; ++i) {
        uint64_t idx = next_rand(&verify_seed) % total;
        MDB_val gkey, gdata;
        CHECK(mdb_get_rank(txn, dbi, idx, &gkey, &gdata),
              "rank dup api");
        expect_val_match(&gkey, &expected[idx].key, "rank dup api key");
        expect_val_match(&gdata, &expected[idx].data, "rank dup api data");
    }

    MDB_val sample_data;
    CHECK(mdb_get_rank(txn, dbi, total - 1, NULL, &sample_data),
          "rank dup tail data");
    expect_val_match(&sample_data, &expected[total - 1].data,
                     "rank dup tail match");

    MDB_val key, data;
    rc = mdb_cursor_get_rank(cur, total, &key, &data, 0);
    expect_rc(rc, MDB_NOTFOUND, "rank dup eof");

    rc = mdb_cursor_get_rank(cur, 0, &key, &data, 1);
    expect_rc(rc, EINVAL, "rank dup bad flags");

    rc = mdb_get_rank(txn, dbi, total, &key, &data);
    expect_rc(rc, MDB_NOTFOUND, "rank dup api eof");

    mdb_cursor_close(cur);
    mdb_txn_abort(txn);

    CHECK(mdb_txn_begin(env, NULL, 0, &txn), "rank dup drop begin");
    CHECK(mdb_drop(txn, dbi, 1), "rank dup drop");
    CHECK(mdb_txn_commit(txn), "rank dup drop commit");
    mdb_dbi_close(env, dbi);
}

static void
test_count_all_persistence(void)
{
    const char *path = "./count_all_persist";
    const char *data_path = "./count_all_persist/data.mdb";
    const char *lock_path = "./count_all_persist/lock.mdb";
    MDB_env *env;
    MDB_txn *txn;
    MDB_dbi db_plain, db_dup;
    MDB_val key, data;
    char keybuf[16];
    char valbuf[16];
    uint64_t total = 0;

    if (mkdir(path, 0777) && errno != EEXIST) {
        fprintf(stderr, "mkdir %s failed: %s\n", path, strerror(errno));
        exit(EXIT_FAILURE);
    }
    unlink(data_path);
    unlink(lock_path);

    CHECK(mdb_env_create(&env), "persistence env create");
    CHECK(mdb_env_set_maxdbs(env, 8), "persistence maxdbs");
    CHECK(mdb_env_open(env, path, MDB_NOLOCK, 0664), "persistence env open");

    CHECK(mdb_txn_begin(env, NULL, 0, &txn), "persistence load begin");
    CHECK(mdb_dbi_open(txn, "persist_plain", MDB_CREATE | MDB_COUNTED, &db_plain),
          "persistence open plain");
    CHECK(mdb_dbi_open(txn, "persist_dup", MDB_CREATE | MDB_COUNTED | MDB_DUPSORT,
                       &db_dup),
          "persistence open dup");

    for (int i = 0; i < 16; ++i) {
        snprintf(keybuf, sizeof(keybuf), "p%03d", i);
        snprintf(valbuf, sizeof(valbuf), "v%03d", i);
        key.mv_size = strlen(keybuf);
        key.mv_data = keybuf;
        data.mv_size = strlen(valbuf);
        data.mv_data = valbuf;
        CHECK(mdb_put(txn, db_plain, &key, &data, 0), "persistence put plain");
    }

    for (int k = 0; k < 3; ++k) {
        snprintf(keybuf, sizeof(keybuf), "dk%02d", k);
        key.mv_size = strlen(keybuf);
        key.mv_data = keybuf;
        for (int v = 0; v < 5; ++v) {
            snprintf(valbuf, sizeof(valbuf), "dv%02d", v);
            data.mv_size = strlen(valbuf);
            data.mv_data = valbuf;
            CHECK(mdb_put(txn, db_dup, &key, &data, 0), "persistence put dup");
        }
    }

    CHECK(mdb_txn_commit(txn), "persistence load commit");
    mdb_dbi_close(env, db_plain);
    mdb_dbi_close(env, db_dup);
    mdb_env_close(env);

    CHECK(mdb_env_create(&env), "persistence env create rd");
    CHECK(mdb_env_set_maxdbs(env, 8), "persistence maxdbs rd");
    CHECK(mdb_env_open(env, path, MDB_NOLOCK, 0664), "persistence env open rd");
    CHECK(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn), "persistence read begin");
    CHECK(mdb_dbi_open(txn, "persist_plain", MDB_COUNTED, &db_plain),
          "persistence reopen plain");
    CHECK(mdb_dbi_open(txn, "persist_dup", MDB_COUNTED | MDB_DUPSORT, &db_dup),
          "persistence reopen dup");

    CHECK(mdb_count_all(txn, db_plain, 0, &total), "persistence plain initial");
    expect_eq(total, 16, "persistence plain initial total");
    CHECK(mdb_count_all(txn, db_dup, 0, &total), "persistence dup initial");
    expect_eq(total, 15, "persistence dup initial total");

    mdb_txn_abort(txn);
    mdb_dbi_close(env, db_plain);
    mdb_dbi_close(env, db_dup);
    mdb_env_close(env);

    CHECK(mdb_env_create(&env), "persistence env create mutate");
    CHECK(mdb_env_set_maxdbs(env, 8), "persistence maxdbs mutate");
    CHECK(mdb_env_open(env, path, MDB_NOLOCK, 0664), "persistence env open mutate");
    CHECK(mdb_txn_begin(env, NULL, 0, &txn), "persistence mutate begin");
    CHECK(mdb_dbi_open(txn, "persist_plain", MDB_COUNTED, &db_plain),
          "persistence mutate plain");
    CHECK(mdb_dbi_open(txn, "persist_dup", MDB_COUNTED | MDB_DUPSORT, &db_dup),
          "persistence mutate dup");

    for (int i = 0; i < 6; ++i) {
        snprintf(keybuf, sizeof(keybuf), "p%03d", i);
        key.mv_size = strlen(keybuf);
        key.mv_data = keybuf;
        CHECK(mdb_del(txn, db_plain, &key, NULL), "persistence delete plain");
    }

    key.mv_size = 4;
    key.mv_data = "dk01";
    for (int v = 0; v < 2; ++v) {
        snprintf(valbuf, sizeof(valbuf), "dv%02d", v);
        data.mv_size = strlen(valbuf);
        data.mv_data = valbuf;
        CHECK(mdb_del(txn, db_dup, &key, &data), "persistence delete dup");
    }

    CHECK(mdb_txn_commit(txn), "persistence mutate commit");
    mdb_dbi_close(env, db_plain);
    mdb_dbi_close(env, db_dup);
    mdb_env_close(env);

    CHECK(mdb_env_create(&env), "persistence env create verify");
    CHECK(mdb_env_set_maxdbs(env, 8), "persistence maxdbs verify");
    CHECK(mdb_env_open(env, path, MDB_NOLOCK, 0664), "persistence env open verify");
    CHECK(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn), "persistence verify begin");
    CHECK(mdb_dbi_open(txn, "persist_plain", MDB_COUNTED, &db_plain),
          "persistence verify plain");
    CHECK(mdb_dbi_open(txn, "persist_dup", MDB_COUNTED | MDB_DUPSORT, &db_dup),
          "persistence verify dup");

    CHECK(mdb_count_all(txn, db_plain, 0, &total), "persistence plain final");
    expect_eq(total, 10, "persistence plain final total");
    CHECK(mdb_count_all(txn, db_dup, 0, &total), "persistence dup final");
    expect_eq(total, 13, "persistence dup final total");

    mdb_txn_abort(txn);
    mdb_dbi_close(env, db_plain);
    mdb_dbi_close(env, db_dup);
    mdb_env_close(env);

    CHECK(mdb_env_create(&env), "persistence env create cleanup");
    CHECK(mdb_env_set_maxdbs(env, 8), "persistence maxdbs cleanup");
    CHECK(mdb_env_open(env, path, MDB_NOLOCK, 0664), "persistence env open cleanup");
    CHECK(mdb_txn_begin(env, NULL, 0, &txn), "persistence cleanup begin");
    CHECK(mdb_dbi_open(txn, "persist_plain", MDB_COUNTED, &db_plain),
          "persistence cleanup plain");
    CHECK(mdb_drop(txn, db_plain, 0), "persistence drop plain");
    CHECK(mdb_dbi_open(txn, "persist_dup", MDB_COUNTED | MDB_DUPSORT, &db_dup),
          "persistence cleanup dup");
    CHECK(mdb_drop(txn, db_dup, 0), "persistence drop dup");
    CHECK(mdb_txn_commit(txn), "persistence cleanup commit");
    mdb_dbi_close(env, db_plain);
    mdb_dbi_close(env, db_dup);
    mdb_env_close(env);

    unlink(data_path);
    unlink(lock_path);
    rmdir(path);
}

static void
run_fuzz_random(MDB_env *env, const char *db_name,
                unsigned int open_flags, const char *key_prefix,
                unsigned int seed_init, const char *label)
{
    const int max_keys = 2048;
    const int rounds = 200;
    const int ops_per_round = 24;
    unsigned int seed = seed_init;
    unsigned char *present;
    int live = 0;

    present = calloc((size_t)max_keys, sizeof(unsigned char));
    if (!present) {
        fprintf(stderr, "%s: failed to allocate fuzz bitmap\n",
                label ? label : "fuzz");
        exit(EXIT_FAILURE);
    }

    MDB_txn *txn;
    MDB_dbi dbi;
    char stage_msg[96];
    format_stage(stage_msg, sizeof(stage_msg), label, "begin");
    CHECK(mdb_txn_begin(env, NULL, 0, &txn), stage_msg);
    format_stage(stage_msg, sizeof(stage_msg), label, "open");
    CHECK(mdb_dbi_open(txn, db_name, open_flags, &dbi), stage_msg);
    format_stage(stage_msg, sizeof(stage_msg), label, "commit open");
    CHECK(mdb_txn_commit(txn), stage_msg);

    for (int round = 0; round < rounds; ++round) {
        format_stage(stage_msg, sizeof(stage_msg), label, "round begin");
        CHECK(mdb_txn_begin(env, NULL, 0, &txn), stage_msg);
        for (int op = 0; op < ops_per_round; ++op) {
            int idx = (int)(next_rand(&seed) % max_keys);
            char keybuf[16];
            snprintf(keybuf, sizeof(keybuf), "%s%05d", key_prefix, idx);

            MDB_val key;
            key.mv_size = strlen(keybuf);
            key.mv_data = keybuf;

            unsigned int action = next_rand(&seed) % 3u;
            if (action == 0) {
                char valbuf[24];
                snprintf(valbuf, sizeof(valbuf), "%sval%05d-%03u",
                         key_prefix, idx,
                         (unsigned)(next_rand(&seed) & 0x3ffu));
                MDB_val data;
                data.mv_size = strlen(valbuf);
                data.mv_data = valbuf;
                int rc = mdb_put(txn, dbi, &key, &data, MDB_NOOVERWRITE);
                if (rc == MDB_KEYEXIST) {
                    data.mv_size = 12;
                    data.mv_data = NULL;
                    format_stage(stage_msg, sizeof(stage_msg), label,
                                 "update reserve");
                    CHECK(mdb_put(txn, dbi, &key, &data, MDB_RESERVE),
                          stage_msg);
                    memset(data.mv_data, 'r', data.mv_size);
                } else {
                    format_stage(stage_msg, sizeof(stage_msg), label,
                                 "insert");
                    CHECK(rc, stage_msg);
                    if (!present[idx]) {
                        present[idx] = 1;
                        live++;
                    }
                }
            } else if (action == 1) {
                if (present[idx]) {
                    int rc = mdb_del(txn, dbi, &key, NULL);
                    if (rc == MDB_SUCCESS) {
                        present[idx] = 0;
                        live--;
                    } else if (rc != MDB_NOTFOUND) {
                        format_stage(stage_msg, sizeof(stage_msg), label,
                                     "delete");
                        CHECK(rc, stage_msg);
                    }
                }
            } else {
                if (present[idx]) {
                    MDB_val data;
                    data.mv_size = 16;
                    data.mv_data = NULL;
                    format_stage(stage_msg, sizeof(stage_msg), label,
                                 "overwrite reserve");
                    CHECK(mdb_put(txn, dbi, &key, &data, MDB_RESERVE),
                          stage_msg);
                    memset(data.mv_data, 's', data.mv_size);
                } else {
                    char valbuf[24];
                    snprintf(valbuf, sizeof(valbuf), "%salt%05d",
                             key_prefix, idx);
                    MDB_val data;
                    data.mv_size = strlen(valbuf);
                    data.mv_data = valbuf;
                    format_stage(stage_msg, sizeof(stage_msg), label,
                                 "alt insert");
                    CHECK(mdb_put(txn, dbi, &key, &data, 0), stage_msg);
                    present[idx] = 1;
                    live++;
                }
            }
        }

        format_stage(stage_msg, sizeof(stage_msg), label, "round commit");
        CHECK(mdb_txn_commit(txn), stage_msg);

        format_stage(stage_msg, sizeof(stage_msg), label, "verify begin");
        CHECK(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn), stage_msg);
        uint64_t total;
        format_stage(stage_msg, sizeof(stage_msg), label, "count_all");
        CHECK(mdb_count_all(txn, dbi, 0, &total), stage_msg);
        format_stage(stage_msg, sizeof(stage_msg), label, "total matches");
        expect_eq(total, (uint64_t)live, stage_msg);

        for (int q = 0; q < 12; ++q) {
            MDB_val low, high;
            MDB_val *low_ptr = NULL;
            MDB_val *high_ptr = NULL;
            unsigned int flags = 0;
            char lowbuf[16];
            char highbuf[16];

            if (next_rand(&seed) & 1u) {
                int low_idx = (int)(next_rand(&seed) % max_keys);
                snprintf(lowbuf, sizeof(lowbuf), "%s%05d",
                         key_prefix, low_idx);
                low.mv_size = strlen(lowbuf);
                low.mv_data = lowbuf;
                low_ptr = &low;
                if (next_rand(&seed) & 1u)
                    flags |= MDB_COUNT_LOWER_INCL;
            }

            if (next_rand(&seed) & 1u) {
                int high_idx = (int)(next_rand(&seed) % max_keys);
                snprintf(highbuf, sizeof(highbuf), "%s%05d",
                         key_prefix, high_idx);
                high.mv_size = strlen(highbuf);
                high.mv_data = highbuf;
                high_ptr = &high;
                if (next_rand(&seed) & 1u)
                    flags |= MDB_COUNT_UPPER_INCL;
            }

            int lower_incl = (flags & MDB_COUNT_LOWER_INCL) != 0;
            int upper_incl = (flags & MDB_COUNT_UPPER_INCL) != 0;
            uint64_t naive = naive_count(txn, dbi, low_ptr, high_ptr,
                                         lower_incl, upper_incl, cmp_key);
            uint64_t counted = 0;
            format_stage(stage_msg, sizeof(stage_msg), label, "range");
            CHECK(mdb_count_range(txn, dbi, low_ptr, high_ptr,
                                  flags, &counted),
                  stage_msg);
            format_stage(stage_msg, sizeof(stage_msg), label, "range matches");
            expect_eq(counted, naive, stage_msg);
        }
        mdb_txn_abort(txn);
    }

    format_stage(stage_msg, sizeof(stage_msg), label, "final begin");
    CHECK(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn), stage_msg);
    uint64_t final_total;
    format_stage(stage_msg, sizeof(stage_msg), label, "final total");
    CHECK(mdb_count_all(txn, dbi, 0, &final_total), stage_msg);
    format_stage(stage_msg, sizeof(stage_msg), label, "final count");
    expect_eq(final_total, (uint64_t)live, stage_msg);
    mdb_txn_abort(txn);

    format_stage(stage_msg, sizeof(stage_msg), label, "drop begin");
    CHECK(mdb_txn_begin(env, NULL, 0, &txn), stage_msg);
    format_stage(stage_msg, sizeof(stage_msg), label, "drop");
    CHECK(mdb_drop(txn, dbi, 0), stage_msg);
    format_stage(stage_msg, sizeof(stage_msg), label, "drop commit");
    CHECK(mdb_txn_commit(txn), stage_msg);

    mdb_dbi_close(env, dbi);
    free(present);
}

static void
test_fuzz_random(MDB_env *env)
{
    run_fuzz_random(env, "edge_fuzz_random",
                    MDB_CREATE | MDB_COUNTED, "f",
                    0x7f4a7u, "fuzz");
}

static void
test_fuzz_random_prefix(MDB_env *env)
{
    run_fuzz_random(env, "edge_fuzz_prefix",
                    MDB_CREATE | MDB_COUNTED | MDB_PREFIX_COMPRESSION,
                    "pf", 0x51c0cau, "fuzz prefix");
}

static void
test_overwrite_stability(MDB_env *env)
{
    MDB_txn *txn;
    MDB_dbi dbi;
    MDB_val key, data;
    uint64_t total;

    CHECK(mdb_txn_begin(env, NULL, 0, &txn), "overwrite begin");
    CHECK(mdb_dbi_open(txn, "edge_overwrite", MDB_CREATE | MDB_COUNTED, &dbi),
          "overwrite open");

    key.mv_data = "dup";
    key.mv_size = 3;
    data.mv_data = "v1";
    data.mv_size = 2;
    CHECK(mdb_put(txn, dbi, &key, &data, 0), "overwrite put initial");
    CHECK(mdb_txn_commit(txn), "overwrite commit initial");

    CHECK(mdb_txn_begin(env, NULL, 0, &txn), "overwrite update begin");
    data.mv_size = 6;
    CHECK(mdb_put(txn, dbi, &key, &data, MDB_RESERVE), "overwrite reserve");
    memset(data.mv_data, 'x', data.mv_size);
    CHECK(mdb_txn_commit(txn), "overwrite update commit");

    CHECK(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn), "overwrite read");
    CHECK(mdb_count_all(txn, dbi, 0, &total), "overwrite count_all");
    expect_eq(total, 1, "overwrite count remains one");

    uint64_t counted = 0;
    CHECK(mdb_count_range(txn, dbi, &key, &key,
                          MDB_COUNT_LOWER_INCL | MDB_COUNT_UPPER_INCL,
                          &counted),
          "overwrite range");
    expect_eq(counted, 1, "overwrite range count one");

    mdb_txn_abort(txn);

    CHECK(mdb_txn_begin(env, NULL, 0, &txn), "overwrite drop begin");
    CHECK(mdb_drop(txn, dbi, 0), "overwrite drop");
    CHECK(mdb_txn_commit(txn), "overwrite drop commit");

    mdb_dbi_close(env, dbi);
}

static void
test_cursor_deletions(MDB_env *env)
{
    const int limit = 1024;
    MDB_txn *txn;
    MDB_dbi dbi;
    MDB_val key, data;

    CHECK(mdb_txn_begin(env, NULL, 0, &txn), "cursor del begin");
    CHECK(mdb_dbi_open(txn, "edge_cursor_del", MDB_CREATE | MDB_COUNTED, &dbi),
          "cursor del open");

    char kbuf[16];
    char vbuf[16];
    for (int i = 0; i < limit; ++i) {
        snprintf(kbuf, sizeof(kbuf), "c%04d", i);
        snprintf(vbuf, sizeof(vbuf), "val%04d", i);
        key.mv_size = strlen(kbuf);
        key.mv_data = kbuf;
        data.mv_size = strlen(vbuf);
        data.mv_data = vbuf;
        CHECK(mdb_put(txn, dbi, &key, &data, MDB_APPEND), "cursor del append");
    }
    CHECK(mdb_txn_commit(txn), "cursor del commit load");

    CHECK(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn), "cursor del read init");
    uint64_t total;
    CHECK(mdb_count_all(txn, dbi, 0, &total), "cursor del count_all init");
    uint64_t naive_total = naive_count(txn, dbi, NULL, NULL, 1, 1, cmp_key);
    expect_eq(naive_total, limit, "cursor del naive total");
    expect_eq(total, limit, "cursor del initial total");

    MDB_val low, high;
    char lowbuf[16];
    char highbuf[16];
    snprintf(lowbuf, sizeof(lowbuf), "c%04d", 0);
    snprintf(highbuf, sizeof(highbuf), "c%04d", 9);
    low.mv_size = strlen(lowbuf);
    low.mv_data = lowbuf;
    high.mv_size = strlen(highbuf);
    high.mv_data = highbuf;
    uint64_t naive = naive_count(txn, dbi, &low, &high, 1, 1, cmp_key);
    uint64_t counted = 0;
    CHECK(mdb_count_range(txn, dbi, &low, &high,
                          MDB_COUNT_LOWER_INCL | MDB_COUNT_UPPER_INCL,
                          &counted),
          "cursor del front range");
    expect_eq(counted, naive, "cursor del front compare");

    snprintf(lowbuf, sizeof(lowbuf), "c%04d", limit - 10);
    snprintf(highbuf, sizeof(highbuf), "c%04d", limit - 1);
    low.mv_size = strlen(lowbuf);
    low.mv_data = lowbuf;
    high.mv_size = strlen(highbuf);
    high.mv_data = highbuf;
    naive = naive_count(txn, dbi, &low, &high, 1, 1, cmp_key);
    CHECK(mdb_count_range(txn, dbi, &low, &high,
                          MDB_COUNT_LOWER_INCL | MDB_COUNT_UPPER_INCL,
                          &counted),
          "cursor del back range");
    expect_eq(counted, naive, "cursor del back compare");

    mdb_txn_abort(txn);

    int remaining = limit;
    while (remaining > 0) {
        CHECK(mdb_txn_begin(env, NULL, 0, &txn), "cursor del loop begin");
        MDB_cursor *cur;
        CHECK(mdb_cursor_open(txn, dbi, &cur), "cursor del cursor open");
        MDB_val ckey, cdata;
        int inner = 0;
        while (remaining > 0 && inner < 32) {
            MDB_cursor_op op = inner ? MDB_PREV : MDB_LAST;
            int rc = mdb_cursor_get(cur, &ckey, &cdata, op);
            if (rc == MDB_NOTFOUND)
                break;
            CHECK(rc, "cursor del get");
            CHECK(mdb_cursor_del(cur, 0), "cursor del cursor_del");
            remaining--;
            inner++;
        }
        mdb_cursor_close(cur);
        CHECK(mdb_txn_commit(txn), "cursor del loop commit");

        CHECK(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn),
              "cursor del verify begin");
        CHECK(mdb_count_all(txn, dbi, 0, &total), "cursor del verify total");
        expect_eq(total, (uint64_t)remaining, "cursor del verify count");
        if (remaining > 0) {
            int start = remaining > 32 ? remaining - 32 : 0;
            snprintf(lowbuf, sizeof(lowbuf), "c%04d", start);
            snprintf(highbuf, sizeof(highbuf), "c%04d", remaining - 1);
            low.mv_size = strlen(lowbuf);
            low.mv_data = lowbuf;
            high.mv_size = strlen(highbuf);
            high.mv_data = highbuf;
            naive = naive_count(txn, dbi, &low, &high, 1, 1, cmp_key);
            CHECK(mdb_count_range(txn, dbi, &low, &high,
                                  MDB_COUNT_LOWER_INCL | MDB_COUNT_UPPER_INCL,
                                  &counted),
                  "cursor del verify tail");
            expect_eq(counted, naive, "cursor del tail compare");
        }
        mdb_txn_abort(txn);
    }

    CHECK(mdb_txn_begin(env, NULL, 0, &txn), "cursor del drop begin");
    CHECK(mdb_drop(txn, dbi, 0), "cursor del drop");
    CHECK(mdb_txn_commit(txn), "cursor del drop commit");

    mdb_dbi_close(env, dbi);
}

static void
verify_windows(MDB_txn *txn, MDB_dbi dbi, const char *tag)
{
    static const struct {
        int low;
        int high;
        unsigned int flags;
    } cases[] = {
        { -1, 64, MDB_COUNT_UPPER_INCL },
        { 0, 127, MDB_COUNT_LOWER_INCL | MDB_COUNT_UPPER_INCL },
        { 96, 160, MDB_COUNT_LOWER_INCL },
        { 96, 160, MDB_COUNT_UPPER_INCL },
        { 512, 256, MDB_COUNT_LOWER_INCL | MDB_COUNT_UPPER_INCL },
        { 1500, -1, MDB_COUNT_LOWER_INCL },
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
        MDB_val low, high;
        MDB_val *low_ptr = NULL;
        MDB_val *high_ptr = NULL;
        char lowbuf[16];
        char highbuf[16];

        if (cases[i].low >= 0) {
            snprintf(lowbuf, sizeof(lowbuf), "s%05d", cases[i].low);
            low.mv_size = strlen(lowbuf);
            low.mv_data = lowbuf;
            low_ptr = &low;
        }
        if (cases[i].high >= 0) {
            snprintf(highbuf, sizeof(highbuf), "s%05d", cases[i].high);
            high.mv_size = strlen(highbuf);
            high.mv_data = highbuf;
            high_ptr = &high;
        }

        char msg[128];
        snprintf(msg, sizeof(msg), "%s case %zu", tag, i);
        check_range_matches(txn, dbi, low_ptr, high_ptr,
                            cases[i].flags, msg);
    }
}

static void
test_split_merge(MDB_env *env)
{
    const int initial = 2048;
    MDB_txn *txn;
    MDB_dbi dbi;
    MDB_val key, data;
    uint64_t total;

    CHECK(mdb_txn_begin(env, NULL, 0, &txn), "split begin");
    CHECK(mdb_dbi_open(txn, "edge_split_merge", MDB_CREATE | MDB_COUNTED,
                       &dbi), "split open");

    char kbuf[16];
    char vbuf[16];
    for (int i = 0; i < initial; ++i) {
        snprintf(kbuf, sizeof(kbuf), "s%05d", i);
        snprintf(vbuf, sizeof(vbuf), "val%05d", i);
        key.mv_size = strlen(kbuf);
        key.mv_data = kbuf;
        data.mv_size = strlen(vbuf);
        data.mv_data = vbuf;
        CHECK(mdb_put(txn, dbi, &key, &data, MDB_APPEND),
              "split load append");
    }
    CHECK(mdb_txn_commit(txn), "split load commit");

    CHECK(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn), "split read init");
    CHECK(mdb_count_all(txn, dbi, 0, &total), "split count_all init");
    expect_eq(total, initial, "split initial total");
    verify_windows(txn, dbi, "split initial");
    mdb_txn_abort(txn);

    int remaining = initial;
    for (int base = 0; base < initial; base += 128) {
        CHECK(mdb_txn_begin(env, NULL, 0, &txn), "split delete begin");
        MDB_cursor *cur;
        CHECK(mdb_cursor_open(txn, dbi, &cur), "split cursor open");
        snprintf(kbuf, sizeof(kbuf), "s%05d", base);
        key.mv_size = strlen(kbuf);
        key.mv_data = kbuf;
        MDB_val pdata;
        int rc = mdb_cursor_get(cur, &key, &pdata, MDB_SET_RANGE);
        if (rc == MDB_SUCCESS) {
            int deleted = 0;
            while (deleted < 64 && remaining > 0) {
                CHECK(mdb_cursor_del(cur, 0), "split cursor del");
                remaining--;
                deleted++;
                rc = mdb_cursor_get(cur, &key, &pdata, MDB_NEXT);
                if (rc != MDB_SUCCESS)
                    break;
            }
            if (rc != MDB_SUCCESS && rc != MDB_NOTFOUND)
                CHECK(rc, "split cursor next");
        } else if (rc != MDB_NOTFOUND) {
            CHECK(rc, "split cursor seek");
        }
        mdb_cursor_close(cur);
        CHECK(mdb_txn_commit(txn), "split delete commit");

        CHECK(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn),
              "split verify read");
        CHECK(mdb_count_all(txn, dbi, 0, &total),
              "split count_all verify");
        expect_eq(total, (uint64_t)remaining, "split verify total");
        verify_windows(txn, dbi, "split verify");
        mdb_txn_abort(txn);

        if (remaining <= 512)
            break;
    }

    CHECK(mdb_txn_begin(env, NULL, 0, &txn), "split drop begin");
    CHECK(mdb_drop(txn, dbi, 0), "split drop");
    CHECK(mdb_txn_commit(txn), "split drop commit");

    mdb_dbi_close(env, dbi);
}

static void
test_nested_transactions(MDB_env *env)
{
    MDB_txn *parent;
    MDB_txn *child;
    MDB_txn *txn;
    MDB_dbi dbi;
    MDB_val key, data;
    uint64_t total;

    CHECK(mdb_txn_begin(env, NULL, 0, &parent), "nested parent begin");
    CHECK(mdb_dbi_open(parent, "edge_nested", MDB_CREATE | MDB_COUNTED,
                       &dbi), "nested open");

    char keybuf[16];
    char valbuf[16];
    for (int i = 0; i < 10; ++i) {
        snprintf(keybuf, sizeof(keybuf), "n%04d", i);
        snprintf(valbuf, sizeof(valbuf), "p%04d", i);
        key.mv_size = strlen(keybuf);
        key.mv_data = keybuf;
        data.mv_size = strlen(valbuf);
        data.mv_data = valbuf;
        CHECK(mdb_put(parent, dbi, &key, &data, MDB_APPEND),
              "nested parent put");
    }

    CHECK(mdb_count_all(parent, dbi, 0, &total),
          "nested parent initial count");
    expect_eq(total, 10, "nested parent initial total");

    CHECK(mdb_txn_begin(env, parent, 0, &child),
          "nested child begin abort");
    for (int i = 10; i < 15; ++i) {
        snprintf(keybuf, sizeof(keybuf), "n%04d", i);
        snprintf(valbuf, sizeof(valbuf), "c%04d", i);
        key.mv_size = strlen(keybuf);
        key.mv_data = keybuf;
        data.mv_size = strlen(valbuf);
        data.mv_data = valbuf;
        CHECK(mdb_put(child, dbi, &key, &data, MDB_APPEND),
              "nested child add");
    }
    CHECK(mdb_count_all(child, dbi, 0, &total),
          "nested child count add");
    expect_eq(total, 15, "nested child add total");

    snprintf(keybuf, sizeof(keybuf), "n%04d", 2);
    key.mv_size = strlen(keybuf);
    key.mv_data = keybuf;
    CHECK(mdb_del(child, dbi, &key, NULL), "nested child del");
    CHECK(mdb_count_all(child, dbi, 0, &total),
          "nested child count del");
    expect_eq(total, 14, "nested child after del");

    MDB_val low, high;
    char lowbuf[16];
    char highbuf[16];
    snprintf(lowbuf, sizeof(lowbuf), "n%04d", 1);
    low.mv_size = strlen(lowbuf);
    low.mv_data = lowbuf;
    snprintf(highbuf, sizeof(highbuf), "n%04d", 12);
    high.mv_size = strlen(highbuf);
    high.mv_data = highbuf;
    check_range_matches(child, dbi, &low, &high,
                        MDB_COUNT_LOWER_INCL | MDB_COUNT_UPPER_INCL,
                        "nested child abort window");
    mdb_txn_abort(child);

    CHECK(mdb_count_all(parent, dbi, 0, &total),
          "nested parent after abort");
    expect_eq(total, 10, "nested parent retained");

    CHECK(mdb_txn_begin(env, parent, 0, &child),
          "nested child begin commit");
    for (int i = 10; i < 20; ++i) {
        snprintf(keybuf, sizeof(keybuf), "n%04d", i);
        snprintf(valbuf, sizeof(valbuf), "d%04d", i);
        key.mv_size = strlen(keybuf);
        key.mv_data = keybuf;
        data.mv_size = strlen(valbuf);
        data.mv_data = valbuf;
        CHECK(mdb_put(child, dbi, &key, &data, MDB_APPEND),
              "nested child append");
    }
    for (int i = 0; i < 5; ++i) {
        snprintf(keybuf, sizeof(keybuf), "n%04d", i);
        key.mv_size = strlen(keybuf);
        key.mv_data = keybuf;
        CHECK(mdb_del(child, dbi, &key, NULL), "nested child prune");
    }
    CHECK(mdb_count_all(child, dbi, 0, &total),
          "nested child post prune");
    expect_eq(total, 15, "nested child post prune total");

    snprintf(lowbuf, sizeof(lowbuf), "n%04d", 0);
    low.mv_size = strlen(lowbuf);
    low.mv_data = lowbuf;
    snprintf(highbuf, sizeof(highbuf), "n%04d", 18);
    high.mv_size = strlen(highbuf);
    high.mv_data = highbuf;
    check_range_matches(child, dbi, &low, &high,
                        MDB_COUNT_LOWER_INCL | MDB_COUNT_UPPER_INCL,
                        "nested child commit window");

    snprintf(highbuf, sizeof(highbuf), "n%04d", 5);
    high.mv_size = strlen(highbuf);
    high.mv_data = highbuf;
    check_range_matches(child, dbi, NULL, &high,
                        MDB_COUNT_UPPER_INCL,
                        "nested child commit head");

    CHECK(mdb_txn_commit(child), "nested child commit");

    CHECK(mdb_count_all(parent, dbi, 0, &total),
          "nested parent after child");
    expect_eq(total, 15, "nested parent after child total");

    snprintf(lowbuf, sizeof(lowbuf), "n%04d", 6);
    low.mv_size = strlen(lowbuf);
    low.mv_data = lowbuf;
    snprintf(highbuf, sizeof(highbuf), "n%04d", 19);
    high.mv_size = strlen(highbuf);
    high.mv_data = highbuf;
    check_range_matches(parent, dbi, &low, &high,
                        MDB_COUNT_LOWER_INCL,
                        "nested parent post child window");

    CHECK(mdb_txn_commit(parent), "nested parent commit");

    CHECK(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn),
          "nested verify read");
    CHECK(mdb_count_all(txn, dbi, 0, &total), "nested final count");
    expect_eq(total, 15, "nested final total");

    snprintf(lowbuf, sizeof(lowbuf), "n%04d", 7);
    low.mv_size = strlen(lowbuf);
    low.mv_data = lowbuf;
    check_range_matches(txn, dbi, &low, NULL,
                        MDB_COUNT_LOWER_INCL,
                        "nested final tail");
    mdb_txn_abort(txn);

    CHECK(mdb_txn_begin(env, NULL, 0, &txn), "nested drop begin");
    CHECK(mdb_drop(txn, dbi, 0), "nested drop");
    CHECK(mdb_txn_commit(txn), "nested drop commit");

    mdb_dbi_close(env, dbi);
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

    uint64_t naive = naive_count(txn, dbi, &low, &high, 1, 1, cmp_key);
    CHECK(mdb_count_range(txn, dbi, &low, &high,
                          MDB_COUNT_LOWER_INCL | MDB_COUNT_UPPER_INCL, &total),
          "range incl/incl");
    expect_eq(total, naive, "range incl/incl");

    naive = naive_count(txn, dbi, &low, &high, 0, 1, cmp_key);
    CHECK(mdb_count_range(txn, dbi, &low, &high, MDB_COUNT_UPPER_INCL, &total),
          "range excl/incl");
    expect_eq(total, naive, "range excl/incl");

    naive = naive_count(txn, dbi, &low, &high, 1, 0, cmp_key);
    CHECK(mdb_count_range(txn, dbi, &low, &high, MDB_COUNT_LOWER_INCL, &total),
          "range incl/excl");
    expect_eq(total, naive, "range incl/excl");

    naive = naive_count(txn, dbi, &low, &high, 0, 0, cmp_key);
    CHECK(mdb_count_range(txn, dbi, &low, &high, 0, &total),
          "range excl/excl");
    expect_eq(total, naive, "range excl/excl");

    CHECK(mdb_count_range(txn, dbi, &high, &low,
                          MDB_COUNT_LOWER_INCL | MDB_COUNT_UPPER_INCL, &total),
          "range low>high");
    expect_eq(total, 0, "low greater than high");

    CHECK(mdb_count_range(txn, dbi, NULL, &high, MDB_COUNT_UPPER_INCL, &total),
          "range upper only");
    naive = naive_count(txn, dbi, NULL, &high, 0, 1, cmp_key);
    expect_eq(total, naive, "upper bound only");

    CHECK(mdb_count_range(txn, dbi, &low, NULL, MDB_COUNT_LOWER_INCL, &total),
          "range lower only");
    naive = naive_count(txn, dbi, &low, NULL, 1, 0, cmp_key);
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

    naive = naive_count(txn, dbi, &low, &high, 1, 1, cmp_key);
    CHECK(mdb_count_range(txn, dbi, &low, &high,
                          MDB_COUNT_LOWER_INCL | MDB_COUNT_UPPER_INCL, &total),
          "range after del");
    expect_eq(total, naive, "range after deletions");
    mdb_txn_abort(txn);

    CHECK(mdb_txn_begin(env, NULL, 0, &txn), "random clear txn");
    CHECK(mdb_drop(txn, dbi, 0), "random mdb_drop");
    CHECK(mdb_txn_commit(txn), "random clear commit");

    enum { max_keys = 1024 };
    unsigned char present[max_keys];
    memset(present, 0, sizeof(present));
    int live = 0;
    const int operations = 4000;
    int performed = 0;

    srand(7);

    while (performed < operations) {
        int idx = rand() % max_keys;
        int want_insert = rand() & 1;
        int changed = 0;

        CHECK(mdb_txn_begin(env, NULL, 0, &txn), "random op begin");
        snprintf(keybuf, sizeof(keybuf), "r%04d", idx);
        key.mv_size = strlen(keybuf);
        key.mv_data = keybuf;
        if (want_insert) {
            if (!present[idx]) {
                snprintf(databuf, sizeof(databuf), "val%04d", idx);
                data.mv_size = strlen(databuf);
                data.mv_data = databuf;
                rc = mdb_put(txn, dbi, &key, &data, 0);
                CHECK(rc, "random mdb_put");
                present[idx] = 1;
                live++;
                changed = 1;
            }
        } else {
            if (present[idx]) {
                rc = mdb_del(txn, dbi, &key, NULL);
                CHECK(rc, "random mdb_del");
                present[idx] = 0;
                live--;
                changed = 1;
            }
        }
        if (changed) {
            CHECK(mdb_txn_commit(txn), "random op commit");
            performed++;

            if ((performed & 7) == 0) {
                MDB_txn *rtxn;
                CHECK(mdb_txn_begin(env, NULL, MDB_RDONLY, &rtxn),
                      "random read txn");
                for (int q = 0; q < 6; ++q) {
                    MDB_val *low_ptr = NULL;
                    MDB_val *high_ptr = NULL;
                    MDB_val lowv, highv;
                    char lowtmp[16];
                    char hightmp[16];
                    unsigned range_flags = 0;

                    if (rand() & 1) {
                        int low_idx = rand() % max_keys;
                        snprintf(lowtmp, sizeof(lowtmp), "r%04d", low_idx);
                        lowv.mv_size = strlen(lowtmp);
                        lowv.mv_data = lowtmp;
                        low_ptr = &lowv;
                        if (rand() & 1)
                            range_flags |= MDB_COUNT_LOWER_INCL;
                    }
                    if (rand() & 1) {
                        int high_idx = rand() % max_keys;
                        snprintf(hightmp, sizeof(hightmp), "r%04d", high_idx);
                        highv.mv_size = strlen(hightmp);
                        highv.mv_data = hightmp;
                        high_ptr = &highv;
                        if (rand() & 1)
                            range_flags |= MDB_COUNT_UPPER_INCL;
                    }

                    int lower_incl = (range_flags & MDB_COUNT_LOWER_INCL) != 0;
                    int upper_incl = (range_flags & MDB_COUNT_UPPER_INCL) != 0;
                    uint64_t naive = naive_count(rtxn, dbi, low_ptr, high_ptr,
                                                 lower_incl, upper_incl,
                                                 cmp_key);
                    char low_desc[24];
                    char high_desc[24];
                    if (low_ptr) {
                        size_t len = low_ptr->mv_size;
                        if (len >= sizeof(low_desc))
                            len = sizeof(low_desc) - 1;
                        memcpy(low_desc, low_ptr->mv_data, len);
                        low_desc[len] = '\0';
                    } else {
                        strcpy(low_desc, "<nil>");
                    }
                    if (high_ptr) {
                        size_t len = high_ptr->mv_size;
                        if (len >= sizeof(high_desc))
                            len = sizeof(high_desc) - 1;
                        memcpy(high_desc, high_ptr->mv_data, len);
                        high_desc[len] = '\0';
                    } else {
                        strcpy(high_desc, "<nil>");
                    }
                    uint64_t counted = 0;
                    CHECK(mdb_count_range(rtxn, dbi, low_ptr, high_ptr,
                                          range_flags, &counted),
                          "mdb_count_range random");
                    char msg[128];
                    snprintf(msg, sizeof(msg),
                             "random check %d.%d low=%s high=%s flags=%u",
                             performed, q, low_desc, high_desc, range_flags);
                    expect_eq(counted, naive, msg);
                }
                CHECK(mdb_count_all(rtxn, dbi, 0, &total),
                      "count_all random");
                expect_eq(total, (uint64_t)live, "random total matches");
                mdb_txn_abort(rtxn);
            }
        } else {
            mdb_txn_abort(txn);
        }
    }

    CHECK(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn), "random final read");
    CHECK(mdb_count_all(txn, dbi, 0, &total), "random final total");
    expect_eq(total, (uint64_t)live, "random final count");
    for (int q = 0; q < 8; ++q) {
        MDB_val *low_ptr = NULL;
        MDB_val *high_ptr = NULL;
        MDB_val lowv, highv;
        char lowtmp[16];
        char hightmp[16];
        unsigned range_flags = 0;

        if (rand() & 1) {
            int low_idx = rand() % max_keys;
            snprintf(lowtmp, sizeof(lowtmp), "r%04d", low_idx);
            lowv.mv_size = strlen(lowtmp);
            lowv.mv_data = lowtmp;
            low_ptr = &lowv;
            if (rand() & 1)
                range_flags |= MDB_COUNT_LOWER_INCL;
        }
        if (rand() & 1) {
            int high_idx = rand() % max_keys;
            snprintf(hightmp, sizeof(hightmp), "r%04d", high_idx);
            highv.mv_size = strlen(hightmp);
            highv.mv_data = hightmp;
            high_ptr = &highv;
            if (rand() & 1)
                range_flags |= MDB_COUNT_UPPER_INCL;
        }

        int lower_incl = (range_flags & MDB_COUNT_LOWER_INCL) != 0;
        int upper_incl = (range_flags & MDB_COUNT_UPPER_INCL) != 0;
        uint64_t naive = naive_count(txn, dbi, low_ptr, high_ptr,
                                     lower_incl, upper_incl, cmp_key);
        uint64_t counted = 0;
        CHECK(mdb_count_range(txn, dbi, low_ptr, high_ptr,
                              range_flags, &counted),
              "mdb_count_range random final");
        char low_desc[24];
        char high_desc[24];
        if (low_ptr) {
            size_t len = low_ptr->mv_size;
            if (len >= sizeof(low_desc))
                len = sizeof(low_desc) - 1;
            memcpy(low_desc, low_ptr->mv_data, len);
            low_desc[len] = '\0';
        } else {
            strcpy(low_desc, "<nil>");
        }
        if (high_ptr) {
            size_t len = high_ptr->mv_size;
            if (len >= sizeof(high_desc))
                len = sizeof(high_desc) - 1;
            memcpy(high_desc, high_ptr->mv_data, len);
            high_desc[len] = '\0';
        } else {
            strcpy(high_desc, "<nil>");
        }
        char msg[128];
        snprintf(msg, sizeof(msg),
                 "random final %d low=%s high=%s flags=%u",
                 q, low_desc, high_desc, range_flags);
        expect_eq(counted, naive, msg);
    }
    mdb_txn_abort(txn);

    mdb_dbi_close(env, dbi);

    test_empty_db(env);
    test_single_key(env);
    test_extreme_keys(env);
    test_range_outside_bounds(env);
    test_custom_comparator(env);
    test_range_count_values(env);
    test_count_all_plain(env);
    test_count_all_dupsort(env);
    test_count_all_persistence();
    test_random_access_plain(env);
    test_random_access_dupsort(env);
    test_overwrite_stability(env);
    test_cursor_deletions(env);
    test_split_merge(env);
    test_nested_transactions(env);
    test_fuzz_random(env);
    test_fuzz_random_prefix(env);
    test_concurrent_readers();

    mdb_env_close(env);
    return EXIT_SUCCESS;
}
