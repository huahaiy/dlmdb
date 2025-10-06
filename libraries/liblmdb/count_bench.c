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
naive_count(MDB_txn *txn, MDB_dbi dbi,
            const MDB_val *low, const MDB_val *high)
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
            if (cmp < 0)
                include = 0;
        }
        if (include && high) {
            int cmp = cmp_key(&key, high);
            if (cmp > 0) {
                include = 0;
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

static double
elapsed_ms(const struct timespec *start, const struct timespec *end)
{
    return (end->tv_sec - start->tv_sec) * 1000.0 +
           (end->tv_nsec - start->tv_nsec) / 1.0e6;
}

static void
usage(const char *prog)
{
    fprintf(stderr, "Usage: %s [--entries N] [--queries Q] [--span W]\n", prog);
}

int
main(int argc, char **argv)
{
    size_t entries = 50000;
    size_t queries = 5000;
    size_t span = 200;

    for (int i = 1; i < argc; ++i) {
        if (!strcmp(argv[i], "--entries") && i + 1 < argc) {
            entries = (size_t)strtoull(argv[++i], NULL, 10);
        } else if (!strcmp(argv[i], "--queries") && i + 1 < argc) {
            queries = (size_t)strtoull(argv[++i], NULL, 10);
        } else if (!strcmp(argv[i], "--span") && i + 1 < argc) {
            span = (size_t)strtoull(argv[++i], NULL, 10);
        } else {
            usage(argv[0]);
            return EXIT_FAILURE;
        }
    }

    if (span >= entries)
        span = entries / 2;

    MDB_env *env;
    MDB_txn *txn;
    MDB_dbi dbi;
    MDB_val key, data;
    char keybuf[32];
    char databuf[32];
    int rc;

    const char *pathbuf = "./benchdb_count";
    if (mkdir(pathbuf, 0775) && errno != EEXIST) {
        perror("mkdir benchdb_count");
        return EXIT_FAILURE;
    }
    if (chmod(pathbuf, 0775) && errno != EPERM) {
        perror("chmod benchdb_count");
    }
    unlink("./benchdb_count/data.mdb");
    unlink("./benchdb_count/lock.mdb");

    CHECK(mdb_env_create(&env), "mdb_env_create");
    CHECK(mdb_env_set_maxdbs(env, 4), "mdb_env_set_maxdbs");
    CHECK(mdb_env_set_mapsize(env, entries * 128), "mdb_env_set_mapsize");
    CHECK(mdb_env_open(env, pathbuf, MDB_NOLOCK, 0664), "mdb_env_open");

    CHECK(mdb_txn_begin(env, NULL, 0, &txn), "mdb_txn_begin write");
    CHECK(mdb_dbi_open(txn, "bench", MDB_CREATE | MDB_COUNTED, &dbi), "mdb_dbi_open");

    for (size_t i = 0; i < entries; ++i) {
        snprintf(keybuf, sizeof(keybuf), "k%06zu", i);
        snprintf(databuf, sizeof(databuf), "v%06zu", i);
        key.mv_size = strlen(keybuf);
        key.mv_data = keybuf;
        data.mv_size = strlen(databuf);
        data.mv_data = databuf;
        rc = mdb_put(txn, dbi, &key, &data, 0);
        CHECK(rc, "mdb_put");
    }
    CHECK(mdb_txn_commit(txn), "commit population");

    MDB_val *lows = calloc(queries, sizeof(MDB_val));
    MDB_val *highs = calloc(queries, sizeof(MDB_val));
    char **lowbufs = calloc(queries, sizeof(char *));
    char **highbufs = calloc(queries, sizeof(char *));
    if (!lows || !highs || !lowbufs || !highbufs) {
        fprintf(stderr, "allocation failure\n");
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
            return EXIT_FAILURE;
        }
        snprintf(lowbufs[i], 16, "k%06zu", start);
        snprintf(highbufs[i], 16, "k%06zu", stop);
        lows[i].mv_size = strlen(lowbufs[i]);
        lows[i].mv_data = lowbufs[i];
        highs[i].mv_size = strlen(highbufs[i]);
        highs[i].mv_data = highbufs[i];
    }

    CHECK(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn), "mdb_txn_begin read");

    struct timespec t0, t1;
    uint64_t sink = 0;

    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (size_t i = 0; i < queries; ++i) {
        sink += naive_count(txn, dbi, &lows[i], &highs[i]);
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);
    double naive_ms = elapsed_ms(&t0, &t1);

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

    printf("Benchmark with %zu entries, %zu queries, span %zu\n", entries, queries, span);
    printf("Naive cursor scan: %.2f ms\n", naive_ms);
    printf("Counted API:      %.2f ms\n", counted_ms);
    printf("(Ignore sink %" PRIu64 " to prevent dead-code elimination)\n", sink);

    for (size_t i = 0; i < queries; ++i) {
        free(lowbufs[i]);
        free(highbufs[i]);
    }
    free(lowbufs);
    free(highbufs);
    free(lows);
    free(highs);

    mdb_env_close(env);
    return EXIT_SUCCESS;
}
