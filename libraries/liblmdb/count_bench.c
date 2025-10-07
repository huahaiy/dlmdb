#include "lmdb.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>
#include <time.h>
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
naive_count(MDB_cursor *cur, const MDB_val *low, const MDB_val *high)
{
    MDB_val key, data;
    int rc;
    uint64_t total = 0;

    if (low && high && cmp_key(low, high) > 0)
        return 0;

    if (low) {
        key = *low;
        rc = mdb_cursor_get(cur, &key, &data, MDB_SET_RANGE);
    } else {
        rc = mdb_cursor_get(cur, &key, &data, MDB_FIRST);
    }

    if (rc == MDB_NOTFOUND)
        return 0;
    CHECK(rc, "mdb_cursor_get range");

    for (;;) {
        if (!low || cmp_key(&key, low) >= 0) {
            if (high && cmp_key(&key, high) > 0)
                break;
            total++;
        }

        rc = mdb_cursor_get(cur, &key, &data, MDB_NEXT);
        if (rc == MDB_NOTFOUND)
            break;
        CHECK(rc, "mdb_cursor_get next");
    }

    return total;
}

static int
naive_count_values(MDB_txn *txn, MDB_dbi dbi,
    MDB_cursor *keycur, MDB_cursor *dupcur,
    const MDB_val *key_low, const MDB_val *key_high, unsigned key_flags,
    const MDB_val *val_low, const MDB_val *val_high, unsigned val_flags,
    uint64_t *out)
{
    MDB_val key = {0}, data = {0};
    uint64_t total = 0;
    int key_lower_incl = (key_flags & MDB_COUNT_LOWER_INCL) != 0;
    int key_upper_incl = (key_flags & MDB_COUNT_UPPER_INCL) != 0;
    int val_lower_incl = (val_flags & MDB_COUNT_LOWER_INCL) != 0;
    int val_upper_incl = (val_flags & MDB_COUNT_UPPER_INCL) != 0;
    int rc;

    if (!out)
        return EINVAL;

    *out = 0;

    if (key_low && key_high) {
        int c = mdb_cmp(txn, dbi, key_low, key_high);
        if (c > 0 || (c == 0 && !(key_lower_incl && key_upper_incl)))
            return MDB_SUCCESS;
    }

    if (val_low && val_high) {
        int c = mdb_dcmp(txn, dbi, val_low, val_high);
        if (c > 0 || (c == 0 && !(val_lower_incl && val_upper_incl)))
            return MDB_SUCCESS;
    }

    if (key_low) {
        key = *key_low;
        rc = mdb_cursor_get(keycur, &key, &data, MDB_SET_RANGE);
        if (rc == MDB_NOTFOUND)
            return MDB_SUCCESS;
        if (rc != MDB_SUCCESS)
            return rc;
        if (!key_lower_incl) {
            if (mdb_cmp(txn, dbi, &key, key_low) == 0) {
                rc = mdb_cursor_get(keycur, &key, &data, MDB_NEXT_NODUP);
                if (rc == MDB_NOTFOUND)
                    return MDB_SUCCESS;
                if (rc != MDB_SUCCESS)
                    return rc;
            }
        }
    } else {
        rc = mdb_cursor_get(keycur, &key, &data, MDB_FIRST);
        if (rc == MDB_NOTFOUND)
            return MDB_SUCCESS;
        if (rc != MDB_SUCCESS)
            return rc;
    }

    while (1) {
        if (key_high) {
            int c = mdb_cmp(txn, dbi, &key, key_high);
            if (c > 0 || (c == 0 && !key_upper_incl))
                break;
        }

        if (!val_low && !val_high) {
            mdb_size_t dupcount = 0;
            rc = mdb_cursor_count(keycur, &dupcount);
            if (rc != MDB_SUCCESS)
                return rc;
            total += (uint64_t)dupcount;
        } else {
            MDB_val dkey = key;
            MDB_val dval = data;

            rc = mdb_cursor_get(dupcur, &dkey, &dval, MDB_SET_KEY);
            if (rc != MDB_SUCCESS)
                return rc;

            rc = mdb_cursor_get(dupcur, &dkey, &dval, MDB_FIRST_DUP);
            if (rc == MDB_NOTFOUND)
                goto next_key;
            if (rc != MDB_SUCCESS)
                return rc;

            while (1) {
                if (val_high) {
                    int cmp_high = mdb_dcmp(txn, dbi, &dval, val_high);
                    if (cmp_high > 0 ||
                        (cmp_high == 0 && !val_upper_incl))
                        break;
                }

                if (val_low) {
                    int cmp_low = mdb_dcmp(txn, dbi, &dval, val_low);
                    if (cmp_low < 0 ||
                        (cmp_low == 0 && !val_lower_incl))
                        goto advance_dup;
                }

                total++;

advance_dup:
                rc = mdb_cursor_get(dupcur, &dkey, &dval, MDB_NEXT_DUP);
                if (rc == MDB_NOTFOUND)
                    break;
                if (rc != MDB_SUCCESS)
                    return rc;
            }
        }

next_key:
        rc = mdb_cursor_get(keycur, &key, &data, MDB_NEXT_NODUP);
        if (rc == MDB_NOTFOUND)
            break;
        if (rc != MDB_SUCCESS)
            return rc;
    }

    *out = total;
    return MDB_SUCCESS;
}

static double
elapsed_ms(const struct timespec *start, const struct timespec *end)
{
    return (end->tv_sec - start->tv_sec) * 1000.0 +
           (end->tv_nsec - start->tv_nsec) / 1.0e6;
}

static uint64_t
bench_rand(uint64_t *state)
{
    uint64_t x = *state;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    *state = x;
    return x * 2685821657736338717ULL;
}

static void
prepare_dir(const char *path)
{
    char data_path[256];
    char lock_path[256];

    if (mkdir(path, 0775) && errno != EEXIST) {
        perror("mkdir benchmark env");
        exit(EXIT_FAILURE);
    }
    if (chmod(path, 0775) && errno != EPERM) {
        perror("chmod benchmark env");
    }
    snprintf(data_path, sizeof(data_path), "%s/data.mdb", path);
    snprintf(lock_path, sizeof(lock_path), "%s/lock.mdb", path);
    unlink(data_path);
    unlink(lock_path);
}

static void
remove_dir(const char *path)
{
    char data_path[256];
    char lock_path[256];

    snprintf(data_path, sizeof(data_path), "%s/data.mdb", path);
    snprintf(lock_path, sizeof(lock_path), "%s/lock.mdb", path);
    unlink(data_path);
    unlink(lock_path);

    if (rmdir(path) && errno != ENOENT) {
        perror("rmdir benchmark env");
    }
}

static double
populate_db(const char *path, int counted, size_t entries,
        const size_t *order, MDB_env **env_out, MDB_dbi *dbi_out)
{
    MDB_env *env;
    MDB_txn *txn;
    MDB_dbi dbi;
    MDB_val key, data;
    char keybuf[32];
    char databuf[32];
    unsigned int db_flags = MDB_CREATE;
    struct timespec t0, t1;

    CHECK(mdb_env_create(&env), "mdb_env_create");
    CHECK(mdb_env_set_maxdbs(env, 4), "mdb_env_set_maxdbs");
    CHECK(mdb_env_set_mapsize(env, entries ? entries * 128 : 128),
        "mdb_env_set_mapsize");
    CHECK(mdb_env_open(env, path, MDB_NOLOCK, 0664), "mdb_env_open");
    CHECK(mdb_txn_begin(env, NULL, 0, &txn), "mdb_txn_begin populate");
    if (counted)
        db_flags |= MDB_COUNTED;
    CHECK(mdb_dbi_open(txn, "bench", db_flags, &dbi), "mdb_dbi_open");

    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (size_t i = 0; i < entries; ++i) {
        size_t idx = order ? order[i] : i;
        snprintf(keybuf, sizeof(keybuf), "k%06zu", idx);
        snprintf(databuf, sizeof(databuf), "v%06zu", idx);
        key.mv_size = strlen(keybuf);
        key.mv_data = keybuf;
        data.mv_size = strlen(databuf);
        data.mv_data = databuf;
        CHECK(mdb_put(txn, dbi, &key, &data, 0), "mdb_put");
    }
    CHECK(mdb_txn_commit(txn), "mdb_txn_commit populate");
    clock_gettime(CLOCK_MONOTONIC, &t1);

    if (env_out && dbi_out) {
        *env_out = env;
        *dbi_out = dbi;
    } else {
        mdb_dbi_close(env, dbi);
        mdb_env_close(env);
    }

    return elapsed_ms(&t0, &t1);
}

static double
populate_dup_db(const char *path, int counted, size_t entries,
    const size_t *order, size_t dupcount,
    MDB_env **env_out, MDB_dbi *dbi_out)
{
    MDB_env *env;
    MDB_txn *txn;
    MDB_dbi dbi;
    MDB_val key, data;
    char keybuf[32];
    char valbuf[32];
    unsigned int db_flags = MDB_CREATE | MDB_DUPSORT;
    struct timespec t0, t1;

    if (!dupcount)
        return 0.0;

    CHECK(mdb_env_create(&env), "mdb_env_create dup");
    CHECK(mdb_env_set_maxdbs(env, 4), "mdb_env_set_maxdbs dup");
    CHECK(mdb_env_set_mapsize(env,
        entries ? entries * dupcount * 128 : 128), "mdb_env_set_mapsize dup");
    CHECK(mdb_env_open(env, path, MDB_NOLOCK, 0664), "mdb_env_open dup");
    CHECK(mdb_txn_begin(env, NULL, 0, &txn), "mdb_txn_begin dup populate");
    if (counted)
        db_flags |= MDB_COUNTED;
    CHECK(mdb_dbi_open(txn, "bench_dup", db_flags, &dbi), "mdb_dbi_open dup");

    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (size_t i = 0; i < entries; ++i) {
        size_t idx = order ? order[i] : i;
        snprintf(keybuf, sizeof(keybuf), "k%06zu", idx);
        key.mv_size = strlen(keybuf);
        key.mv_data = keybuf;
        for (size_t d = 0; d < dupcount; ++d) {
            snprintf(valbuf, sizeof(valbuf), "val%08zu", d);
            data.mv_size = strlen(valbuf);
            data.mv_data = valbuf;
            CHECK(mdb_put(txn, dbi, &key, &data, 0), "mdb_put dup");
        }
    }
    CHECK(mdb_txn_commit(txn), "mdb_txn_commit dup populate");
    clock_gettime(CLOCK_MONOTONIC, &t1);

    if (env_out && dbi_out) {
        *env_out = env;
        *dbi_out = dbi;
    } else {
        mdb_dbi_close(env, dbi);
        mdb_env_close(env);
    }

    return elapsed_ms(&t0, &t1);
}

static void
measure_insert_costs(size_t entries, const size_t *order, double *plain_ms,
        double *counted_ms, int *samples_out)
{
    const int repeats = 6;
    double plain_total = 0.0;
    double counted_total = 0.0;
    const char *debug = getenv("COUNT_BENCH_DEBUG");

    if (!entries) {
        *plain_ms = 0.0;
        *counted_ms = 0.0;
        return;
    }

    prepare_dir("./bench_tmp_plain");
    double warm_plain = populate_db("./bench_tmp_plain", 0, entries, order,
        NULL, NULL);
    if (debug) {
        fprintf(stderr, "warm-plain: %.2f ms\n", warm_plain);
    }
    remove_dir("./bench_tmp_plain");

    prepare_dir("./bench_tmp_counted");
    double warm_counted = populate_db("./bench_tmp_counted", 1, entries,
        order, NULL, NULL);
    if (debug) {
        fprintf(stderr, "warm-counted: %.2f ms\n", warm_counted);
    }
    remove_dir("./bench_tmp_counted");

    for (int pass = 0; pass < repeats; ++pass) {
        int counted_first = pass & 1;

        if (counted_first) {
            prepare_dir("./bench_tmp_counted");
            double counted_run = populate_db("./bench_tmp_counted", 1, entries,
                order, NULL, NULL);
            counted_total += counted_run;
            if (debug) {
                fprintf(stderr, "counted-run[%d]: %.2f ms\n", pass,
                    counted_run);
            }
            prepare_dir("./bench_tmp_plain");
            double plain_run = populate_db("./bench_tmp_plain", 0, entries,
                order, NULL, NULL);
            plain_total += plain_run;
            if (debug) {
                fprintf(stderr, "plain-run[%d]: %.2f ms\n", pass,
                    plain_run);
            }
        } else {
            prepare_dir("./bench_tmp_plain");
            double plain_run = populate_db("./bench_tmp_plain", 0, entries,
                order, NULL, NULL);
            plain_total += plain_run;
            if (debug) {
                fprintf(stderr, "plain-run[%d]: %.2f ms\n", pass,
                    plain_run);
            }
            prepare_dir("./bench_tmp_counted");
            double counted_run = populate_db("./bench_tmp_counted", 1, entries,
                order, NULL, NULL);
            counted_total += counted_run;
            if (debug) {
                fprintf(stderr, "counted-run[%d]: %.2f ms\n", pass,
                    counted_run);
            }
        }
    }

    remove_dir("./bench_tmp_plain");
    remove_dir("./bench_tmp_counted");

    *plain_ms = plain_total / repeats;
    *counted_ms = counted_total / repeats;
    if (samples_out)
        *samples_out = repeats;
}

static void
measure_dup_insert_costs(size_t entries, size_t dupcount, const size_t *order,
        double *plain_ms, double *counted_ms, int *samples_out)
{
    const int repeats = 6;
    double plain_total = 0.0;
    double counted_total = 0.0;
    const char *debug = getenv("COUNT_BENCH_DEBUG");

    if (!entries || !dupcount) {
        if (plain_ms)
            *plain_ms = 0.0;
        if (counted_ms)
            *counted_ms = 0.0;
        if (samples_out)
            *samples_out = 0;
        return;
    }

    prepare_dir("./bench_tmp_plain_dup");
    double warm_plain = populate_dup_db("./bench_tmp_plain_dup", 0, entries,
        order, dupcount, NULL, NULL);
    if (debug) {
        fprintf(stderr, "warm-plain-dup: %.2f ms\n", warm_plain);
    }
    remove_dir("./bench_tmp_plain_dup");

    prepare_dir("./bench_tmp_counted_dup");
    double warm_counted = populate_dup_db("./bench_tmp_counted_dup", 1,
        entries, order, dupcount, NULL, NULL);
    if (debug) {
        fprintf(stderr, "warm-counted-dup: %.2f ms\n", warm_counted);
    }
    remove_dir("./bench_tmp_counted_dup");

    for (int pass = 0; pass < repeats; ++pass) {
        int counted_first = pass & 1;

        if (counted_first) {
            prepare_dir("./bench_tmp_counted_dup");
            double counted_run = populate_dup_db("./bench_tmp_counted_dup", 1,
                entries, order, dupcount, NULL, NULL);
            counted_total += counted_run;
            if (debug) {
                fprintf(stderr, "counted-dup-run[%d]: %.2f ms\n", pass,
                    counted_run);
            }
            prepare_dir("./bench_tmp_plain_dup");
            double plain_run = populate_dup_db("./bench_tmp_plain_dup", 0,
                entries, order, dupcount, NULL, NULL);
            plain_total += plain_run;
            if (debug) {
                fprintf(stderr, "plain-dup-run[%d]: %.2f ms\n", pass,
                    plain_run);
            }
        } else {
            prepare_dir("./bench_tmp_plain_dup");
            double plain_run = populate_dup_db("./bench_tmp_plain_dup", 0,
                entries, order, dupcount, NULL, NULL);
            plain_total += plain_run;
            if (debug) {
                fprintf(stderr, "plain-dup-run[%d]: %.2f ms\n", pass,
                    plain_run);
            }
            prepare_dir("./bench_tmp_counted_dup");
            double counted_run = populate_dup_db("./bench_tmp_counted_dup", 1,
                entries, order, dupcount, NULL, NULL);
            counted_total += counted_run;
            if (debug) {
                fprintf(stderr, "counted-dup-run[%d]: %.2f ms\n", pass,
                    counted_run);
            }
        }
    }

    remove_dir("./bench_tmp_plain_dup");
    remove_dir("./bench_tmp_counted_dup");

    if (plain_ms)
        *plain_ms = plain_total / repeats;
    if (counted_ms)
        *counted_ms = counted_total / repeats;
    if (samples_out)
        *samples_out = repeats;
}

static void
usage(const char *prog)
{
    fprintf(stderr,
        "Usage: %s [--entries N] [--queries Q] [--span W] [--dups D] [--shuffle]\n",
        prog);
}

int
main(int argc, char **argv)
{
    size_t entries = 50000;
    size_t queries = 5000;
    size_t span = 200;
    size_t dupcount = 8;
    int shuffle = 0;
    size_t *order = NULL;
    const char *debug = getenv("COUNT_BENCH_DEBUG");
    int status = EXIT_SUCCESS;

    for (int i = 1; i < argc; ++i) {
        if (!strcmp(argv[i], "--entries") && i + 1 < argc) {
            entries = (size_t)strtoull(argv[++i], NULL, 10);
        } else if (!strcmp(argv[i], "--queries") && i + 1 < argc) {
            queries = (size_t)strtoull(argv[++i], NULL, 10);
        } else if (!strcmp(argv[i], "--span") && i + 1 < argc) {
            span = (size_t)strtoull(argv[++i], NULL, 10);
        } else if (!strcmp(argv[i], "--dups") && i + 1 < argc) {
            dupcount = (size_t)strtoull(argv[++i], NULL, 10);
        } else if (!strcmp(argv[i], "--shuffle")) {
            shuffle = 1;
        } else {
            usage(argv[0]);
            return EXIT_FAILURE;
        }
    }

    if (!entries) {
        queries = 0;
        span = 0;
    } else if (span >= entries) {
        span = entries > 1 ? entries - 1 : 0;
    }

    if (shuffle && entries) {
        order = malloc(entries * sizeof(size_t));
        if (!order) {
            fprintf(stderr, "allocation failure\n");
            return EXIT_FAILURE;
        }
        for (size_t i = 0; i < entries; ++i)
            order[i] = i;
        uint64_t state = 0x9e3779b97f4a7c15ULL;
        for (size_t i = entries - 1; i > 0; --i) {
            size_t j = (size_t)(bench_rand(&state) % (i + 1));
            size_t tmp = order[i];
            order[i] = order[j];
            order[j] = tmp;
        }
    }

    double plain_ms = 0.0;
    double counted_build_ms = 0.0;
    int insert_samples = 0;
    double dup_plain_ms = 0.0;
    double dup_counted_build_ms = 0.0;
    int dup_insert_samples = 0;

    measure_insert_costs(entries, order, &plain_ms, &counted_build_ms,
        &insert_samples);

    if (entries && dupcount) {
        measure_dup_insert_costs(entries, dupcount, order, &dup_plain_ms,
            &dup_counted_build_ms, &dup_insert_samples);
        prepare_dir("./bench_tmp_plain_dup");
        double dup_plain_extra = populate_dup_db("./bench_tmp_plain_dup", 0,
            entries, order, dupcount, NULL, NULL);
        dup_plain_ms = (dup_plain_ms * dup_insert_samples + dup_plain_extra) /
            (dup_insert_samples + 1);
        remove_dir("./bench_tmp_plain_dup");
        dup_insert_samples++;
    }

    if (entries) {
        prepare_dir("./bench_tmp_plain");
        double plain_extra_ms = populate_db("./bench_tmp_plain", 0, entries,
            order, NULL, NULL);
        if (debug) {
            fprintf(stderr, "plain-extra: %.2f ms\n", plain_extra_ms);
        }
        plain_ms = (plain_ms * insert_samples + plain_extra_ms) /
            (insert_samples + 1);
        remove_dir("./bench_tmp_plain");
        insert_samples++;
    }

    prepare_dir("./benchdb_count");

    MDB_env *env = NULL;
    MDB_txn *txn = NULL;
    MDB_dbi dbi = 0;
    int have_env = 0;
    int have_txn = 0;
    MDB_env *dup_env = NULL;
    MDB_txn *dup_txn = NULL;
    MDB_dbi dup_dbi = 0;
    int have_dup_env = 0;
    int have_dup_txn = 0;

    double counted_insert_ms = populate_db("./benchdb_count", 1, entries, order,
        &env, &dbi);
    have_env = 1;
    if (debug) {
        fprintf(stderr, "counted-final: %.2f ms\n", counted_insert_ms);
    }
    if (entries) {
        counted_build_ms = (counted_build_ms * (insert_samples - 1) +
            counted_insert_ms) / insert_samples;
    } else {
        counted_build_ms = counted_insert_ms;
    }
    double plain_us = entries ? (plain_ms * 1000.0) / entries : 0.0;
    double counted_insert_us = entries ? (counted_build_ms * 1000.0) / entries : 0.0;
    double insert_overhead_ms = counted_build_ms - plain_ms;
    double insert_overhead_pct = plain_ms ? (insert_overhead_ms / plain_ms) * 100.0 : 0.0;

    MDB_val *lows = calloc(queries, sizeof(MDB_val));
    MDB_val *highs = calloc(queries, sizeof(MDB_val));
    char **lowbufs = calloc(queries, sizeof(char *));
    char **highbufs = calloc(queries, sizeof(char *));
    MDB_val *val_lows = calloc(queries, sizeof(MDB_val));
    MDB_val *val_highs = calloc(queries, sizeof(MDB_val));
    MDB_val **val_low_refs = calloc(queries, sizeof(MDB_val *));
    MDB_val **val_high_refs = calloc(queries, sizeof(MDB_val *));
    char **val_lowbufs = calloc(queries, sizeof(char *));
    char **val_highbufs = calloc(queries, sizeof(char *));
    if (!lows || !highs || !lowbufs || !highbufs || !val_lows || !val_highs ||
        !val_low_refs || !val_high_refs || !val_lowbufs || !val_highbufs) {
        fprintf(stderr, "allocation failure\n");
        free(order);
        free(lows);
        free(highs);
        free(lowbufs);
        free(highbufs);
        free(val_lows);
        free(val_highs);
        free(val_low_refs);
        free(val_high_refs);
        free(val_lowbufs);
        free(val_highbufs);
        return EXIT_FAILURE;
    }

    srand(1);
    for (size_t i = 0; i < queries; ++i) {
        size_t start = rand() % entries;
        size_t stop = start + span;
        if (stop >= entries)
            stop = entries - 1;

        lowbufs[i] = malloc(16);
        highbufs[i] = malloc(16);
        if (!lowbufs[i] || !highbufs[i]) {
            fprintf(stderr, "allocation failure\n");
            status = EXIT_FAILURE;
            goto cleanup;
        }
        snprintf(lowbufs[i], 16, "k%06zu", start);
        snprintf(highbufs[i], 16, "k%06zu", stop);
        lows[i].mv_size = strlen(lowbufs[i]);
        lows[i].mv_data = lowbufs[i];
        highs[i].mv_size = strlen(highbufs[i]);
        highs[i].mv_data = highbufs[i];

        val_low_refs[i] = NULL;
        val_high_refs[i] = NULL;
        if (dupcount && (rand() & 1)) {
            size_t vstart = rand() % dupcount;
            size_t vend = vstart + (rand() % (dupcount - vstart));
            val_lowbufs[i] = malloc(16);
            val_highbufs[i] = malloc(16);
            if (!val_lowbufs[i] || !val_highbufs[i]) {
                fprintf(stderr, "allocation failure\n");
                status = EXIT_FAILURE;
                goto cleanup;
            }
            snprintf(val_lowbufs[i], 16, "val%08zu", vstart);
            snprintf(val_highbufs[i], 16, "val%08zu", vend);
            val_lows[i].mv_size = strlen(val_lowbufs[i]);
            val_lows[i].mv_data = val_lowbufs[i];
            val_highs[i].mv_size = strlen(val_highbufs[i]);
            val_highs[i].mv_data = val_highbufs[i];
            val_low_refs[i] = &val_lows[i];
            val_high_refs[i] = &val_highs[i];
        }
    }

    CHECK(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn), "mdb_txn_begin read");
    have_txn = 1;

    struct timespec t0, t1;
    uint64_t sink = 0;

    MDB_cursor *scan_cur;
    CHECK(mdb_cursor_open(txn, dbi, &scan_cur), "mdb_cursor_open naive");

    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (size_t i = 0; i < queries; ++i) {
        sink += naive_count(scan_cur, &lows[i], &highs[i]);
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double naive_ms = elapsed_ms(&t0, &t1);

    mdb_cursor_close(scan_cur);

    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (size_t i = 0; i < queries; ++i) {
        uint64_t counted = 0;
        CHECK(mdb_count_range(txn, dbi, &lows[i], &highs[i],
                              MDB_COUNT_LOWER_INCL | MDB_COUNT_UPPER_INCL,
                              &counted), "mdb_count_range");
        sink += counted;
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double counted_ms = elapsed_ms(&t0, &t1);

    mdb_txn_abort(txn);
    have_txn = 0;
    mdb_dbi_close(env, dbi);
    mdb_env_close(env);
    have_env = 0;
    remove_dir("./benchdb_count");

    double naive_us = queries ? (naive_ms * 1000.0) / queries : 0.0;
    double counted_us = queries ? (counted_ms * 1000.0) / queries : 0.0;
    double range_speedup = (counted_ms > 0.0) ? naive_ms / counted_ms : 0.0;

    printf("Benchmark with %zu entries, %zu queries, span %zu\n", entries, queries, span);
    printf("Insert order: %s\n", shuffle ? "shuffled" : "monotonic");
    printf("Insert plain DB:   %.2f ms (%.2f us/op)\n", plain_ms, plain_us);
    printf("Insert counted DB: %.2f ms (%.2f us/op)\n", counted_build_ms,
        counted_insert_us);
    printf("Counted overhead:  %.2f ms (%.2f%%)\n", insert_overhead_ms,
        insert_overhead_pct);
    printf("Naive cursor scan: %.2f ms (%.2f us/op)\n", naive_ms, naive_us);
    printf("Counted API:      %.2f ms (%.2f us/op)\n", counted_ms, counted_us);
    if (counted_ms > 0.0)
        printf("Range count speedup: %.2fx\n", range_speedup);
    else
        printf("Range count speedup: N/A\n");

    if (entries && dupcount) {
        size_t dup_ops = entries * dupcount;
        double dup_plain_us = dup_ops ? (dup_plain_ms * 1000.0) / dup_ops : 0.0;
        double dup_counted_us = dup_ops ? (dup_counted_build_ms * 1000.0) /
            dup_ops : 0.0;
        double dup_overhead_ms = dup_counted_build_ms - dup_plain_ms;
        double dup_overhead_pct = dup_plain_ms ?
            (dup_overhead_ms / dup_plain_ms) * 100.0 : 0.0;
        printf("Dupsort plain DB:   %.2f ms (%.2f us/op)\n", dup_plain_ms,
            dup_plain_us);
        printf("Dupsort counted DB: %.2f ms (%.2f us/op)\n",
            dup_counted_build_ms, dup_counted_us);
        printf("Dupsort overhead:   %.2f ms (%.2f%%)\n", dup_overhead_ms,
            dup_overhead_pct);
    }

    double naive_val_ms = 0.0;
    double counted_val_ms = 0.0;
    double naive_val_us = 0.0;
    double counted_val_us = 0.0;
    double value_speedup = 0.0;

    if (entries && dupcount) {
        prepare_dir("./benchdb_count_dup");
        double dup_counted_insert_ms = populate_dup_db("./benchdb_count_dup", 1,
            entries, order, dupcount, &dup_env, &dup_dbi);
        have_dup_env = 1;
        if (debug) {
            fprintf(stderr, "dup-counted: %.2f ms\n", dup_counted_insert_ms);
        }
        if (dup_insert_samples > 0) {
            dup_counted_build_ms = (dup_counted_build_ms * (dup_insert_samples - 1) +
                dup_counted_insert_ms) / dup_insert_samples;
        } else {
            dup_counted_build_ms = dup_counted_insert_ms;
        }
    }

    if (entries && queries && dupcount && have_dup_env) {
        MDB_cursor *dup_keycur;
        MDB_cursor *dup_dupcur;

        CHECK(mdb_txn_begin(dup_env, NULL, MDB_RDONLY, &dup_txn),
            "mdb_txn_begin dup read");
        have_dup_txn = 1;
        CHECK(mdb_cursor_open(dup_txn, dup_dbi, &dup_keycur),
            "mdb_cursor_open dup key");
        CHECK(mdb_cursor_open(dup_txn, dup_dbi, &dup_dupcur),
            "mdb_cursor_open dup dup");

        clock_gettime(CLOCK_MONOTONIC, &t0);
        for (size_t i = 0; i < queries; ++i) {
            uint64_t counted = 0;
            int rc = naive_count_values(dup_txn, dup_dbi, dup_keycur,
                dup_dupcur,
                queries ? &lows[i] : NULL,
                queries ? &highs[i] : NULL,
                MDB_COUNT_LOWER_INCL | MDB_COUNT_UPPER_INCL,
                val_low_refs[i], val_high_refs[i],
                MDB_COUNT_LOWER_INCL | MDB_COUNT_UPPER_INCL,
                &counted);
            CHECK(rc, "naive_count_values");
            sink += counted;
        }
        clock_gettime(CLOCK_MONOTONIC, &t1);
        naive_val_ms = elapsed_ms(&t0, &t1);

        clock_gettime(CLOCK_MONOTONIC, &t0);
        for (size_t i = 0; i < queries; ++i) {
            uint64_t counted = 0;
            CHECK(mdb_range_count_values(dup_txn, dup_dbi,
                queries ? &lows[i] : NULL,
                queries ? &highs[i] : NULL,
                MDB_COUNT_LOWER_INCL | MDB_COUNT_UPPER_INCL,
                val_low_refs[i], val_high_refs[i],
                MDB_COUNT_LOWER_INCL | MDB_COUNT_UPPER_INCL,
                &counted), "mdb_range_count_values");
            sink += counted;
        }
        clock_gettime(CLOCK_MONOTONIC, &t1);
        counted_val_ms = elapsed_ms(&t0, &t1);

        mdb_cursor_close(dup_dupcur);
        mdb_cursor_close(dup_keycur);
        mdb_txn_abort(dup_txn);
        have_dup_txn = 0;
        mdb_dbi_close(dup_env, dup_dbi);
        mdb_env_close(dup_env);
        have_dup_env = 0;
        remove_dir("./benchdb_count_dup");

        naive_val_us = (naive_val_ms * 1000.0) / queries;
        counted_val_us = (counted_val_ms * 1000.0) / queries;
        value_speedup = (counted_val_ms > 0.0) ?
            (naive_val_ms / counted_val_ms) : 0.0;

        printf("Dupsort dup/key:  %zu\n", dupcount);
        printf("Value cursor scan: %.2f ms (%.2f us/op)\n",
            naive_val_ms, naive_val_us);
        printf("Value counted API: %.2f ms (%.2f us/op)\n",
            counted_val_ms, counted_val_us);
        if (counted_val_ms > 0.0)
            printf("Value range speedup: %.2fx\n", value_speedup);
        else
            printf("Value range speedup: N/A\n");
    }

    volatile uint64_t sink_guard = sink;
    (void)sink_guard;

    if (!shuffle && insert_overhead_ms < 0.0) {
        printf("Sequential inserts minimize overhead; use --shuffle to randomize load.\n");
    }

cleanup:
    if (have_txn)
        mdb_txn_abort(txn);
    if (have_env) {
        mdb_dbi_close(env, dbi);
        mdb_env_close(env);
        remove_dir("./benchdb_count");
    }
    if (have_dup_txn)
        mdb_txn_abort(dup_txn);
    if (have_dup_env) {
        mdb_dbi_close(dup_env, dup_dbi);
        mdb_env_close(dup_env);
        remove_dir("./benchdb_count_dup");
    }
    for (size_t i = 0; i < queries; ++i) {
        free(lowbufs[i]);
        free(highbufs[i]);
        free(val_lowbufs[i]);
        free(val_highbufs[i]);
    }
    free(lowbufs);
    free(highbufs);
    free(val_lowbufs);
    free(val_highbufs);
    free(lows);
    free(highs);
    free(val_lows);
    free(val_highs);
    free(val_low_refs);
    free(val_high_refs);

    free(order);

    return status;
}
