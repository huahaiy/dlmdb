#include "lmdb.h"

#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define CHECK(rc, msg)                                                          \
	do {                                                                    \
		if ((rc) != MDB_SUCCESS) {                                      \
			fprintf(stderr, "%s:%d: %s: %s\n", __FILE__, __LINE__,  \
			    (msg), mdb_strerror(rc));                             \
			exit(EXIT_FAILURE);                                       \
		}                                                               \
	} while (0)

#define CHECK_CALL(expr)                                                        \
	do {                                                                        \
		int __rc = (expr);                                                  \
		CHECK(__rc, #expr);                                                 \
	} while (0)

#define ARRAY_SIZE(a) (sizeof(a) / sizeof((a)[0]))

#define PF_KEY_MAX_LEN   256
#define PF_VALUE_MAX_LEN 512
#define PF_MAX_ENTRIES   2048

/* Dupsort fuzz limits */
#define DF_MAX_KEYS   256
#define DF_MAX_DUPS   512

typedef struct {
	size_t key_len;
	char key[PF_KEY_MAX_LEN];
	size_t val_len;
	unsigned char value[PF_VALUE_MAX_LEN];
} PFEntry;

typedef struct {
	size_t len;
	unsigned char value[PF_VALUE_MAX_LEN];
} DFDuplicate;

typedef struct {
	size_t key_len;
	char key[PF_KEY_MAX_LEN];
	size_t dup_count;
	DFDuplicate dups[DF_MAX_DUPS];
} DFEntry;

static PFEntry pf_entries[PF_MAX_ENTRIES];
static size_t pf_entry_count;
static uint64_t pf_rng_state = UINT64_C(0x9e3779b97f4a7c15);
static uint64_t pf_key_nonce;
static size_t pf_op_index;
static int pf_trace_ops_enabled = -1;

static DFEntry df_entries[DF_MAX_KEYS];
static size_t df_entry_count;
static uint64_t df_rng_state = UINT64_C(0xd2b74407b1ce6e93);
static uint64_t df_key_nonce;
static size_t df_op_index;

static void df_model_reset(void);
static uint64_t df_rng_next(void);
static size_t df_make_value(unsigned char *buf, size_t max_len);
static void df_model_insert(const char *key, size_t key_len,
    const unsigned char *value, size_t val_len);
static void df_verify_model(MDB_env *env, MDB_dbi dbi);
static void df_do_insert(MDB_env *env, MDB_dbi dbi);
static void df_do_delete(MDB_env *env, MDB_dbi dbi);

static int
pf_trace_ops(void)
{
	if (pf_trace_ops_enabled < 0) {
		const char *env = getenv("PF_TRACE_OPS");
		pf_trace_ops_enabled = (env && env[0] != '\0');
	}
	return pf_trace_ops_enabled;
}

static void test_prefix_leaf_splits(void);
static void test_prefix_alternating_prefixes(void);
static void test_prefix_update_reinsert(void);
static void test_prefix_dupsort_cursor_walk(void);
static void test_prefix_dupsort_get_both_range(void);
static void test_prefix_dupsort_smoke(void);
static void assert_dup_sequence(MDB_env *env, MDB_dbi dbi, const char *key,
    const char *const *expected, size_t expected_count);
static void test_prefix_dupsort_inline_basic_ops(void);
static void test_prefix_dupsort_inline_promote(void);
static void test_prefix_dupsort_trunk_swap_inline(void);
static void test_prefix_dupsort_trunk_swap_promote(void);
static void test_prefix_dupsort_trunk_key_shift_no_value_change(void);

static void
reset_dir(const char *dir)
{
	if (mkdir(dir, 0755) && errno != EEXIST) {
		fprintf(stderr, "mkdir %s failed: %s\n", dir, strerror(errno));
		exit(EXIT_FAILURE);
	}
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/data.mdb", dir);
	unlink(path);
	snprintf(path, sizeof(path), "%s/lock.mdb", dir);
	unlink(path);
}

static MDB_env *
create_env(const char *dir)
{
	MDB_env *env = NULL;
	reset_dir(dir);
	CHECK_CALL(mdb_env_create(&env));
	CHECK_CALL(mdb_env_set_maxdbs(env, 4));
	CHECK_CALL(mdb_env_set_mapsize(env, 64UL * 1024 * 1024));
	CHECK_CALL(mdb_env_open(env, dir, MDB_NOLOCK, 0664));
	return env;
}


static void
test_config_validation(void)
{
	MDB_env *env = NULL;
	CHECK_CALL(mdb_env_create(&env));
	CHECK_CALL(mdb_env_set_maxdbs(env, 4));
	CHECK_CALL(mdb_env_set_mapsize(env, 64UL * 1024 * 1024));
	int maxkey = mdb_env_get_maxkeysize(env);
	if (maxkey <= 0) {
		fprintf(stderr, "config validation: unexpected max key size %d\n",
		    maxkey);
		exit(EXIT_FAILURE);
	}
	mdb_env_close(env);
}

static void
test_edge_cases(void)
{
	static const char *dir = "testdb_prefix_edges";
	MDB_env *env = create_env(dir);
	MDB_txn *txn = NULL;
	MDB_dbi dbi;
	MDB_cursor *cur = NULL;
	MDB_val key = {0, NULL};
	MDB_val data = {0, NULL};
	int rc;

	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	CHECK_CALL(mdb_dbi_open(txn, NULL, MDB_PREFIX_COMPRESSION, &dbi));
	const char *single_key = "solo-entry";
	MDB_val single = {strlen(single_key), (void *)single_key};
	CHECK_CALL(mdb_put(txn, dbi, &single, &single, 0));
	CHECK_CALL(mdb_txn_commit(txn));

	CHECK_CALL(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn));
	CHECK_CALL(mdb_dbi_open(txn, NULL, MDB_PREFIX_COMPRESSION, &dbi));
	key.mv_data = NULL;
	key.mv_size = 0;
	data.mv_data = NULL;
	data.mv_size = 0;
	CHECK_CALL(mdb_cursor_open(txn, dbi, &cur));
	rc = mdb_cursor_get(cur, &key, &data, MDB_FIRST);
	if (rc != MDB_SUCCESS || key.mv_size != single.mv_size ||
	    memcmp(key.mv_data, single.mv_data, key.mv_size) != 0) {
		fprintf(stderr, "edge cases: failed to fetch single key entry\n");
		exit(EXIT_FAILURE);
	}
	mdb_cursor_close(cur);
	mdb_txn_abort(txn);

	mdb_env_close(env);
	env = create_env(dir);

	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	CHECK_CALL(mdb_dbi_open(txn, NULL, MDB_PREFIX_COMPRESSION, &dbi));
	const char *short_key = "ab";
	MDB_val short_val = {strlen(short_key), (void *)short_key};
	CHECK_CALL(mdb_put(txn, dbi, &short_val, &short_val, 0));

	CHECK_CALL(mdb_txn_commit(txn));

	CHECK_CALL(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn));
	CHECK_CALL(mdb_dbi_open(txn, NULL, MDB_PREFIX_COMPRESSION, &dbi));
	CHECK_CALL(mdb_cursor_open(txn, dbi, &cur));
	MDB_val short_lookup = {short_val.mv_size, short_val.mv_data};
	rc = mdb_cursor_get(cur, &short_lookup, &data, MDB_SET_KEY);
	if (rc != MDB_SUCCESS || short_lookup.mv_size != short_val.mv_size ||
	    memcmp(short_lookup.mv_data, short_val.mv_data, short_lookup.mv_size) != 0) {
		fprintf(stderr, "edge cases: short key lookup failed\n");
		exit(EXIT_FAILURE);
	}
	mdb_cursor_close(cur);
	mdb_txn_abort(txn);

	mdb_env_close(env);
}

static void
test_range_scans(void)
{
	static const char *dir = "testdb_prefix_ranges";
	MDB_env *env = create_env(dir);
	MDB_txn *txn = NULL;
	MDB_dbi dbi;
	MDB_cursor *cur = NULL;

	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	CHECK_CALL(mdb_dbi_open(txn, NULL, MDB_PREFIX_COMPRESSION, &dbi));
	for (unsigned int i = 0; i < 16; ++i) {
		char keybuf[32];
		snprintf(keybuf, sizeof(keybuf), "acct-%04u-range", i);
		MDB_val key = {strlen(keybuf), keybuf};
		MDB_val val = {strlen(keybuf), keybuf};
		CHECK_CALL(mdb_put(txn, dbi, &key, &val, 0));
	}
	CHECK_CALL(mdb_txn_commit(txn));

	CHECK_CALL(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn));
	CHECK_CALL(mdb_dbi_open(txn, NULL, 0, &dbi));
	CHECK_CALL(mdb_cursor_open(txn, dbi, &cur));

	MDB_val key = {0, NULL};
	MDB_val data = {0, NULL};

	char target_key[] = "acct-0005-range";
	key.mv_size = strlen(target_key);
	key.mv_data = target_key;
	CHECK_CALL(mdb_cursor_get(cur, &key, &data, MDB_SET_RANGE));
	if (key.mv_size != strlen(target_key) ||
	    memcmp(key.mv_data, target_key, key.mv_size) != 0) {
		fprintf(stderr, "range scans: MDB_SET_RANGE exact failed\n");
		exit(EXIT_FAILURE);
	}
	if (data.mv_size != key.mv_size ||
	    memcmp(data.mv_data, key.mv_data, key.mv_size) != 0) {
		fprintf(stderr, "range scans: MDB_SET_RANGE exact value mismatch\n");
		exit(EXIT_FAILURE);
	}

	char between_key[] = "acct-0005-rangezzz";
	key.mv_size = strlen(between_key);
	key.mv_data = between_key;
	CHECK_CALL(mdb_cursor_get(cur, &key, &data, MDB_SET_RANGE));
	const char *expect_next = "acct-0006-range";
	if (key.mv_size != strlen(expect_next) ||
	    memcmp(key.mv_data, expect_next, key.mv_size) != 0) {
		fprintf(stderr, "range scans: MDB_SET_RANGE upper bound failed\n");
		exit(EXIT_FAILURE);
	}

	char low_key[] = "acct-0000-range";
	key.mv_size = strlen(low_key);
	key.mv_data = low_key;
	CHECK_CALL(mdb_cursor_get(cur, &key, &data, MDB_SET_KEY));
	if (key.mv_size != strlen(low_key) ||
	    memcmp(key.mv_data, low_key, key.mv_size) != 0) {
		fprintf(stderr, "range scans: MDB_SET_KEY first entry failed\n");
		exit(EXIT_FAILURE);
	}

	CHECK_CALL(mdb_cursor_get(cur, &key, &data, MDB_LAST));
	const char *last_key = "acct-0015-range";
	if (key.mv_size != strlen(last_key) ||
	    memcmp(key.mv_data, last_key, key.mv_size) != 0) {
		fprintf(stderr, "range scans: MDB_LAST failed\n");
		exit(EXIT_FAILURE);
	}
	CHECK_CALL(mdb_cursor_get(cur, &key, &data, MDB_PREV));
	const char *prev_key = "acct-0014-range";
	if (key.mv_size != strlen(prev_key) ||
	    memcmp(key.mv_data, prev_key, key.mv_size) != 0) {
		fprintf(stderr, "range scans: MDB_PREV failed\n");
		exit(EXIT_FAILURE);
	}

	char beyond_key[] = "acct-9999-range";
	key.mv_size = strlen(beyond_key);
	key.mv_data = beyond_key;
	int rc = mdb_cursor_get(cur, &key, &data, MDB_SET_RANGE);
	if (rc != MDB_NOTFOUND) {
		fprintf(stderr,
		    "range scans: expected MDB_NOTFOUND for upper bound, saw %s\n",
		    mdb_strerror(rc));
		exit(EXIT_FAILURE);
	}

	mdb_cursor_close(cur);
	mdb_txn_abort(txn);
	mdb_env_close(env);
}




static void
test_threshold_behavior(void)
{
	static const char *dir = "testdb_prefix_threshold";
	MDB_env *env = create_env(dir);
	MDB_txn *txn = NULL;
	MDB_dbi dbi;
	const char *keys[] = {
		"aaaa-0000",
		"aaaa-0001",
		"aaab-0002"
	};

	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	CHECK_CALL(mdb_dbi_open(txn, NULL, MDB_PREFIX_COMPRESSION, &dbi));
	for (size_t i = 0; i < ARRAY_SIZE(keys); ++i) {
		const char *k = keys[i];
		MDB_val key = {strlen(k), (void *)k};
		MDB_val val = {strlen(k), (void *)k};
		CHECK_CALL(mdb_put(txn, dbi, &key, &val, 0));
	}
	CHECK_CALL(mdb_txn_commit(txn));

	CHECK_CALL(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn));
	CHECK_CALL(mdb_dbi_open(txn, NULL, 0, &dbi));

	MDB_stat st;
	CHECK_CALL(mdb_stat(txn, dbi, &st));
	if ((size_t)st.ms_entries != ARRAY_SIZE(keys)) {
		fprintf(stderr, "threshold test: expected %zu entries, saw %" MDB_PRIy(u) "\n",
		    ARRAY_SIZE(keys), st.ms_entries);
		exit(EXIT_FAILURE);
	}
	if (st.ms_leaf_pages != 1) {
		fprintf(stderr, "threshold test: expected single leaf page, saw %" MDB_PRIy(u) "\n",
		    st.ms_leaf_pages);
		exit(EXIT_FAILURE);
	}

	mdb_txn_abort(txn);
	mdb_env_close(env);
}



static void
test_mixed_pattern_and_unicode(void)
{
	static const char *dir = "testdb_prefix_mixed_patterns";
	MDB_env *env = create_env(dir);
	MDB_txn *txn = NULL;
	MDB_dbi dbi;

	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	CHECK_CALL(mdb_dbi_open(txn, NULL, MDB_PREFIX_COMPRESSION, &dbi));
	const char *keys[] = {
		"sh",
		"shared-alpha-000000000000",
		"shared-alpha-000000000001",
		"shared-alpha-zzzzzzzzzzzz",
		"shared-beta-000000000004",
		"\xE2\x82\xAC-shared-euro-0002",
		"\xE6\xBC\xA2\xE5\xAD\x97-long-prefix-0005",
		"\xF0\x9F\x97\x9D-shared-box-0003",
	};
	for (size_t i = 0; i < ARRAY_SIZE(keys); ++i) {
		const char *k = keys[i];
		char valbuf[PF_KEY_MAX_LEN];
		int vlen = snprintf(valbuf, sizeof(valbuf), "VAL-%s", k);
		if (vlen < 0) {
			fprintf(stderr, "mixed patterns: value formatting failed\n");
			exit(EXIT_FAILURE);
		}
		MDB_val key = {strlen(k), (void *)k};
		MDB_val data = {(size_t)vlen, valbuf};
		int put_rc = mdb_put(txn, dbi, &key, &data, 0);
		if (put_rc != MDB_SUCCESS) {
			fprintf(stderr, "mixed patterns: insert failed for key '%s' rc=%s\n",
			    k, mdb_strerror(put_rc));
			exit(EXIT_FAILURE);
		}
	}
	CHECK_CALL(mdb_txn_commit(txn));

	CHECK_CALL(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn));
	CHECK_CALL(mdb_dbi_open(txn, NULL, 0, &dbi));
	MDB_cursor *cur = NULL;
	CHECK_CALL(mdb_cursor_open(txn, dbi, &cur));

	MDB_val key = {0, NULL};
	MDB_val data = {0, NULL};
	int rc = mdb_cursor_get(cur, &key, &data, MDB_FIRST);
	size_t seen = 0;
	while (rc == MDB_SUCCESS) {
		char valbuf[PF_KEY_MAX_LEN];
		int vlen = snprintf(valbuf, sizeof(valbuf), "VAL-%.*s",
		    (int)key.mv_size, (char *)key.mv_data);
		if (vlen < 0) {
			fprintf(stderr, "mixed patterns: snprintf failed during validation\n");
			exit(EXIT_FAILURE);
		}
		if ((size_t)vlen != data.mv_size ||
		    memcmp(data.mv_data, valbuf, data.mv_size) != 0) {
			fprintf(stderr,
			    "mixed patterns: value mismatch for key %.*s\n",
			    (int)key.mv_size, (char *)key.mv_data);
			exit(EXIT_FAILURE);
		}
		seen++;
		rc = mdb_cursor_get(cur, &key, &data, MDB_NEXT);
	}
	if (rc != MDB_NOTFOUND)
		CHECK(rc, "mdb_cursor_get");
	if (seen != ARRAY_SIZE(keys)) {
		fprintf(stderr,
		    "mixed patterns: expected %zu keys during scan, saw %zu\n",
		    ARRAY_SIZE(keys), seen);
		exit(EXIT_FAILURE);
	}

	for (size_t i = 0; i < ARRAY_SIZE(keys); ++i) {
		const char *k = keys[i];
		char valbuf[PF_KEY_MAX_LEN];
		int vlen = snprintf(valbuf, sizeof(valbuf), "VAL-%s", k);
		if (vlen < 0) {
			fprintf(stderr, "mixed patterns: lookup format failed\n");
			exit(EXIT_FAILURE);
		}
		MDB_val lookup = {strlen(k), (void *)k};
		MDB_val value = {0, NULL};
		CHECK_CALL(mdb_get(txn, dbi, &lookup, &value));
		if ((size_t)vlen != value.mv_size ||
		    memcmp(value.mv_data, valbuf, value.mv_size) != 0) {
			fprintf(stderr, "mixed patterns: lookup mismatch for %s\n", k);
			exit(EXIT_FAILURE);
		}
	}

	mdb_cursor_close(cur);
	mdb_txn_abort(txn);
	mdb_env_close(env);
}





static void
test_cursor_buffer_sharing(void)
{
	static const char *dir = "testdb_prefix_cursor_sharing";
	MDB_env *env = create_env(dir);
	MDB_txn *txn = NULL;
	MDB_dbi dbi;

	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	CHECK_CALL(mdb_dbi_open(txn, NULL, MDB_PREFIX_COMPRESSION, &dbi));
	for (unsigned int i = 0; i < 12; ++i) {
		char keybuf[64];
		snprintf(keybuf, sizeof(keybuf), "cursor-shared-%03u", i);
		MDB_val key = {strlen(keybuf), keybuf};
		MDB_val val = {strlen(keybuf), keybuf};
		CHECK_CALL(mdb_put(txn, dbi, &key, &val, MDB_APPEND));
	}
	CHECK_CALL(mdb_txn_commit(txn));

	MDB_txn *rtxn = NULL;
	CHECK_CALL(mdb_txn_begin(env, NULL, MDB_RDONLY, &rtxn));
	MDB_cursor *primary = NULL;
	CHECK_CALL(mdb_cursor_open(rtxn, dbi, &primary));
	MDB_val key = {0, NULL};
	MDB_val data = {0, NULL};
	CHECK_CALL(mdb_cursor_get(primary, &key, &data, MDB_FIRST));

	MDB_cursor *shadow = NULL;
	CHECK_CALL(mdb_cursor_open(rtxn, dbi, &shadow));
	MDB_val shadow_key = {0, NULL};
	MDB_val shadow_data = {0, NULL};
	CHECK_CALL(mdb_cursor_get(shadow, &shadow_key, &shadow_data, MDB_FIRST));

	CHECK_CALL(mdb_cursor_get(shadow, &shadow_key, &shadow_data, MDB_NEXT));
	MDB_val verify = {0, NULL};
	MDB_val verify_data = {0, NULL};
	CHECK_CALL(mdb_cursor_get(primary, &verify, &verify_data, MDB_GET_CURRENT));
	if (verify.mv_size != verify_data.mv_size ||
	    memcmp(verify.mv_data, verify_data.mv_data, verify.mv_size) != 0) {
		fprintf(stderr, "cursor sharing: primary cursor lost its buffer after peer advance\n");
		exit(EXIT_FAILURE);
	}

	MDB_txn *wtxn = NULL;
	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &wtxn));
	const char *new_key = "cursor-shared-999-new";
	MDB_val nkey = {strlen(new_key), (void *)new_key};
	MDB_val nval = {strlen(new_key), (void *)new_key};
	CHECK_CALL(mdb_put(wtxn, dbi, &nkey, &nval, 0));
	CHECK_CALL(mdb_txn_commit(wtxn));

	mdb_cursor_close(shadow);
	mdb_txn_abort(rtxn);

	MDB_txn *rtxn2 = NULL;
	CHECK_CALL(mdb_txn_begin(env, NULL, MDB_RDONLY, &rtxn2));
	CHECK_CALL(mdb_cursor_renew(rtxn2, primary));

	CHECK_CALL(mdb_cursor_get(primary, &key, &data, MDB_LAST));
	if (key.mv_size != nkey.mv_size ||
	    memcmp(key.mv_data, nkey.mv_data, key.mv_size) != 0) {
		fprintf(stderr, "cursor sharing: renewed cursor failed to see new key\n");
		exit(EXIT_FAILURE);
	}

	mdb_cursor_close(primary);
	mdb_txn_abort(rtxn2);
	mdb_env_close(env);
}

static void
test_prefix_dupsort_transitions(void)
{
	static const char *dir = "testdb_prefix_dupsort_transitions";
	MDB_env *env = create_env(dir);
	MDB_txn *txn = NULL;
	MDB_dbi dbi;

	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	unsigned int open_flags = MDB_CREATE | MDB_PREFIX_COMPRESSION | MDB_DUPSORT;
	CHECK_CALL(mdb_dbi_open(txn, "prefixed", open_flags, &dbi));
	const char *dup_key_str = "prefixed-dup-target";
	MDB_val dup_key = {strlen(dup_key_str), (void *)dup_key_str};
	MDB_val dup_val1 = {5, "dup-a"};
	MDB_val dup_val2 = {5, "dup-b"};
	CHECK_CALL(mdb_put(txn, dbi, &dup_key, &dup_val1, 0));
	CHECK_CALL(mdb_put(txn, dbi, &dup_key, &dup_val2, 0));
	CHECK_CALL(mdb_txn_commit(txn));

	CHECK_CALL(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn));
	CHECK_CALL(mdb_dbi_open(txn, "prefixed", 0, &dbi));
	MDB_cursor *cur = NULL;
	CHECK_CALL(mdb_cursor_open(txn, dbi, &cur));
	MDB_val seek_key = {strlen(dup_key_str), (void *)dup_key_str};
	MDB_val data = {0, NULL};
	CHECK_CALL(mdb_cursor_get(cur, &seek_key, &data, MDB_SET_KEY));
	int seen_first = 0;
	int seen_second = 0;
	for (;;) {
		if (data.mv_size == dup_val1.mv_size &&
		    memcmp(data.mv_data, dup_val1.mv_data, data.mv_size) == 0) {
			seen_first = 1;
		} else if (data.mv_size == dup_val2.mv_size &&
		    memcmp(data.mv_data, dup_val2.mv_data, data.mv_size) == 0) {
			seen_second = 1;
		} else {
			fprintf(stderr, "dupsort transitions: unexpected duplicate payload\n");
			exit(EXIT_FAILURE);
		}
		int dup_rc = mdb_cursor_get(cur, &seek_key, &data, MDB_NEXT_DUP);
		if (dup_rc == MDB_NOTFOUND)
			break;
		if (dup_rc != MDB_SUCCESS)
			CHECK(dup_rc, "mdb_cursor_get");
	}
	if (!seen_first || !seen_second) {
		fprintf(stderr, "dupsort transitions: missing duplicate entries\n");
		exit(EXIT_FAILURE);
	}
	mdb_cursor_close(cur);
	mdb_txn_abort(txn);

	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	int rc = mdb_dbi_open(txn, "prefixed",
	    MDB_PREFIX_COMPRESSION | MDB_DUPFIXED, &dbi);
	if (rc == MDB_SUCCESS)
		mdb_txn_commit(txn);
	else
		mdb_txn_abort(txn);

	mdb_env_close(env);
}

static void
test_prefix_dupsort_cursor_walk(void)
{
	static const char *dir = "testdb_prefix_dupsort_walk";
	MDB_env *env = create_env(dir);
	MDB_txn *txn = NULL;
	MDB_dbi dbi;

	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	unsigned int flags = MDB_PREFIX_COMPRESSION | MDB_DUPSORT;
	CHECK_CALL(mdb_dbi_open(txn, "walkdb", MDB_CREATE | flags, &dbi));

	static const char *keys[] = {
		"dup-walk-alpha",
		"dup-walk-bravo",
		"dup-walk-charlie"
	};
	static const char *dup_values[][3] = {
		{"dup-alpha-001", "dup-alpha-002", "dup-alpha-003"},
		{"dup-bravo-001", "dup-bravo-002", "dup-bravo-003"},
		{"dup-charlie-001", "dup-charlie-002", "dup-charlie-003"}
	};
	const size_t dup_count = ARRAY_SIZE(dup_values[0]);

	for (size_t i = 0; i < ARRAY_SIZE(keys); ++i) {
		MDB_val key = {strlen(keys[i]), (void *)keys[i]};
		for (size_t j = 0; j < dup_count; ++j) {
			MDB_val data = {strlen(dup_values[i][j]), (void *)dup_values[i][j]};
			CHECK_CALL(mdb_put(txn, dbi, &key, &data, 0));
		}
	}
	CHECK_CALL(mdb_txn_commit(txn));

	CHECK_CALL(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn));
	CHECK_CALL(mdb_dbi_open(txn, "walkdb", flags, &dbi));
	MDB_cursor *cur = NULL;
	CHECK_CALL(mdb_cursor_open(txn, dbi, &cur));
	MDB_val key = {0, NULL};
	MDB_val data = {0, NULL};

	CHECK_CALL(mdb_cursor_get(cur, &key, &data, MDB_FIRST));
	if (key.mv_size != strlen(keys[0]) ||
	    memcmp(key.mv_data, keys[0], key.mv_size) != 0 ||
	    data.mv_size != strlen(dup_values[0][0]) ||
	    memcmp(data.mv_data, dup_values[0][0], data.mv_size) != 0) {
		fprintf(stderr, "dupsort walk: unexpected first entry\n");
		exit(EXIT_FAILURE);
	}

	CHECK_CALL(mdb_cursor_get(cur, &key, &data, MDB_NEXT_DUP));
	if (data.mv_size != strlen(dup_values[0][1]) ||
	    memcmp(data.mv_data, dup_values[0][1], data.mv_size) != 0) {
		fprintf(stderr, "dupsort walk: second duplicate mismatch\n");
		exit(EXIT_FAILURE);
	}

	CHECK_CALL(mdb_cursor_get(cur, &key, &data, MDB_NEXT_DUP));
	if (data.mv_size != strlen(dup_values[0][2]) ||
	    memcmp(data.mv_data, dup_values[0][2], data.mv_size) != 0) {
		fprintf(stderr, "dupsort walk: third duplicate mismatch\n");
		exit(EXIT_FAILURE);
	}

	int rc = mdb_cursor_get(cur, &key, &data, MDB_NEXT_DUP);
	if (rc != MDB_NOTFOUND) {
		fprintf(stderr, "dupsort walk: expected end of duplicates\n");
		exit(EXIT_FAILURE);
	}

	CHECK_CALL(mdb_cursor_get(cur, &key, &data, MDB_NEXT));
	if (key.mv_size != strlen(keys[1]) ||
	    memcmp(key.mv_data, keys[1], key.mv_size) != 0 ||
	    data.mv_size != strlen(dup_values[1][0]) ||
	    memcmp(data.mv_data, dup_values[1][0], data.mv_size) != 0) {
		fprintf(stderr, "dupsort walk: MDB_NEXT did not reach next key\n");
		exit(EXIT_FAILURE);
	}

	CHECK_CALL(mdb_cursor_get(cur, &key, &data, MDB_NEXT_DUP));
	if (data.mv_size != strlen(dup_values[1][1]) ||
	    memcmp(data.mv_data, dup_values[1][1], data.mv_size) != 0) {
		fprintf(stderr, "dupsort walk: MDB_NEXT_DUP failed within second key\n");
		exit(EXIT_FAILURE);
	}

	CHECK_CALL(mdb_cursor_get(cur, &key, &data, MDB_PREV_DUP));
	if (data.mv_size != strlen(dup_values[1][0]) ||
	    memcmp(data.mv_data, dup_values[1][0], data.mv_size) != 0) {
		fprintf(stderr, "dupsort walk: MDB_PREV_DUP failed to rewind\n");
		exit(EXIT_FAILURE);
	}

	CHECK_CALL(mdb_cursor_get(cur, &key, &data, MDB_NEXT_DUP));
	CHECK_CALL(mdb_cursor_get(cur, &key, &data, MDB_NEXT_DUP));
	if (data.mv_size != strlen(dup_values[1][2]) ||
	    memcmp(data.mv_data, dup_values[1][2], data.mv_size) != 0) {
		fprintf(stderr, "dupsort walk: MDB_NEXT_DUP missed last duplicate\n");
		exit(EXIT_FAILURE);
	}

	CHECK_CALL(mdb_cursor_get(cur, &key, &data, MDB_NEXT_NODUP));
	if (key.mv_size != strlen(keys[2]) ||
	    memcmp(key.mv_data, keys[2], key.mv_size) != 0 ||
	    data.mv_size != strlen(dup_values[2][0]) ||
	    memcmp(data.mv_data, dup_values[2][0], data.mv_size) != 0) {
		fprintf(stderr, "dupsort walk: MDB_NEXT_NODUP did not land on third key\n");
		exit(EXIT_FAILURE);
	}

	CHECK_CALL(mdb_cursor_get(cur, &key, &data, MDB_NEXT_DUP));
	if (data.mv_size != strlen(dup_values[2][1]) ||
	    memcmp(data.mv_data, dup_values[2][1], data.mv_size) != 0) {
		fprintf(stderr, "dupsort walk: MDB_NEXT_DUP mismatch in third key\n");
		exit(EXIT_FAILURE);
	}

	CHECK_CALL(mdb_cursor_get(cur, &key, &data, MDB_NEXT_DUP));
	if (data.mv_size != strlen(dup_values[2][2]) ||
	    memcmp(data.mv_data, dup_values[2][2], data.mv_size) != 0) {
		fprintf(stderr, "dupsort walk: MDB_NEXT_DUP missed tail duplicate\n");
		exit(EXIT_FAILURE);
	}

	CHECK_CALL(mdb_cursor_get(cur, &key, &data, MDB_PREV_NODUP));
	if (key.mv_size != strlen(keys[1]) ||
	    memcmp(key.mv_data, keys[1], key.mv_size) != 0 ||
	    data.mv_size != strlen(dup_values[1][2]) ||
	    memcmp(data.mv_data, dup_values[1][2], data.mv_size) != 0) {
		fprintf(stderr, "dupsort walk: MDB_PREV_NODUP did not target prior key tail\n");
		exit(EXIT_FAILURE);
	}

	CHECK_CALL(mdb_cursor_get(cur, &key, &data, MDB_PREV_DUP));
	if (data.mv_size != strlen(dup_values[1][1]) ||
	    memcmp(data.mv_data, dup_values[1][1], data.mv_size) != 0) {
		fprintf(stderr, "dupsort walk: MDB_PREV_DUP failed in reverse iteration\n");
		exit(EXIT_FAILURE);
	}

	CHECK_CALL(mdb_cursor_get(cur, &key, &data, MDB_PREV_DUP));
	if (data.mv_size != strlen(dup_values[1][0]) ||
	    memcmp(data.mv_data, dup_values[1][0], data.mv_size) != 0) {
		fprintf(stderr, "dupsort walk: MDB_PREV_DUP missed earliest duplicate\n");
		exit(EXIT_FAILURE);
	}

	rc = mdb_cursor_get(cur, &key, &data, MDB_PREV_DUP);
	if (rc != MDB_NOTFOUND) {
		fprintf(stderr, "dupsort walk: expected start of dup chain\n");
		exit(EXIT_FAILURE);
	}

	CHECK_CALL(mdb_cursor_get(cur, &key, &data, MDB_PREV_NODUP));
	if (key.mv_size != strlen(keys[0]) ||
	    memcmp(key.mv_data, keys[0], key.mv_size) != 0 ||
	    data.mv_size != strlen(dup_values[0][2]) ||
	    memcmp(data.mv_data, dup_values[0][2], data.mv_size) != 0) {
		fprintf(stderr, "dupsort walk: MDB_PREV_NODUP missed previous key tail\n");
		exit(EXIT_FAILURE);
	}

	CHECK_CALL(mdb_cursor_get(cur, &key, &data, MDB_PREV));
	if (data.mv_size != strlen(dup_values[0][1]) ||
	    memcmp(data.mv_data, dup_values[0][1], data.mv_size) != 0) {
		fprintf(stderr, "dupsort walk: MDB_PREV did not step within key\n");
		exit(EXIT_FAILURE);
	}

	CHECK_CALL(mdb_cursor_get(cur, &key, &data, MDB_PREV));
	if (data.mv_size != strlen(dup_values[0][0]) ||
	    memcmp(data.mv_data, dup_values[0][0], data.mv_size) != 0) {
		fprintf(stderr, "dupsort walk: MDB_PREV missed first duplicate\n");
		exit(EXIT_FAILURE);
	}

	rc = mdb_cursor_get(cur, &key, &data, MDB_PREV);
	if (rc != MDB_NOTFOUND) {
		fprintf(stderr, "dupsort walk: expected start of database\n");
		exit(EXIT_FAILURE);
	}

	mdb_cursor_close(cur);
	mdb_txn_abort(txn);
	mdb_env_close(env);
}

static void
test_prefix_dupsort_get_both_range(void)
{
	static const char *dir = "testdb_prefix_dupsort_get_both";
	MDB_env *env = create_env(dir);
	MDB_txn *txn = NULL;
	MDB_dbi dbi;

	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	unsigned int flags = MDB_PREFIX_COMPRESSION | MDB_DUPSORT;
	CHECK_CALL(mdb_dbi_open(txn, "bothdb", MDB_CREATE | flags, &dbi));

	static const char *keys[] = {
		"range-key-alpha",
		"range-key-beta"
	};
	static const char *alpha_dups[] = {
		"dup-0001",
		"dup-0005",
		"dup-0010"
	};
	static const char *beta_dups[] = {
		"dup-0100",
		"dup-0200",
		"dup-0300"
	};

	MDB_val key = {strlen(keys[0]), (void *)keys[0]};
	for (size_t i = 0; i < ARRAY_SIZE(alpha_dups); ++i) {
		MDB_val data = {strlen(alpha_dups[i]), (void *)alpha_dups[i]};
		CHECK_CALL(mdb_put(txn, dbi, &key, &data, 0));
	}

	key.mv_size = strlen(keys[1]);
	key.mv_data = (void *)keys[1];
	for (size_t i = 0; i < ARRAY_SIZE(beta_dups); ++i) {
		MDB_val data = {strlen(beta_dups[i]), (void *)beta_dups[i]};
		CHECK_CALL(mdb_put(txn, dbi, &key, &data, 0));
	}
	CHECK_CALL(mdb_txn_commit(txn));

	CHECK_CALL(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn));
	CHECK_CALL(mdb_dbi_open(txn, "bothdb", flags, &dbi));
	MDB_cursor *cur = NULL;
	CHECK_CALL(mdb_cursor_open(txn, dbi, &cur));

	MDB_val exact_key = {strlen(keys[0]), (void *)keys[0]};
	MDB_val exact_data = {strlen(alpha_dups[1]), (void *)alpha_dups[1]};
	CHECK_CALL(mdb_cursor_get(cur, &exact_key, &exact_data, MDB_GET_BOTH));
	if (exact_key.mv_size != strlen(keys[0]) ||
	    memcmp(exact_key.mv_data, keys[0], exact_key.mv_size) != 0 ||
	    exact_data.mv_size != strlen(alpha_dups[1]) ||
	    memcmp(exact_data.mv_data, alpha_dups[1], exact_data.mv_size) != 0) {
		fprintf(stderr, "dupsort get_both: exact lookup failed\n");
		exit(EXIT_FAILURE);
	}

	MDB_val range_key = {strlen(keys[0]), (void *)keys[0]};
	MDB_val range_data = {strlen("dup-0004"), "dup-0004"};
	CHECK_CALL(mdb_cursor_get(cur, &range_key, &range_data, MDB_GET_BOTH_RANGE));
	if (range_data.mv_size != strlen(alpha_dups[1]) ||
	    memcmp(range_data.mv_data, alpha_dups[1], range_data.mv_size) != 0) {
		fprintf(stderr, "dupsort get_both: range lookup did not advance to dup-0005\n");
		exit(EXIT_FAILURE);
	}

	range_key.mv_size = strlen(keys[0]);
	range_key.mv_data = (void *)keys[0];
	range_data.mv_size = strlen("dup-0011");
	range_data.mv_data = "dup-0011";
	int rc = mdb_cursor_get(cur, &range_key, &range_data, MDB_GET_BOTH_RANGE);
	if (rc != MDB_NOTFOUND) {
		fprintf(stderr, "dupsort get_both: expected no match beyond last duplicate\n");
		exit(EXIT_FAILURE);
	}

	MDB_val beta_key = {strlen(keys[1]), (void *)keys[1]};
	MDB_val beta_data = {strlen("dup-0000"), "dup-0000"};
	CHECK_CALL(mdb_cursor_get(cur, &beta_key, &beta_data, MDB_GET_BOTH_RANGE));
	if (beta_data.mv_size != strlen(beta_dups[0]) ||
	    memcmp(beta_data.mv_data, beta_dups[0], beta_data.mv_size) != 0) {
		fprintf(stderr, "dupsort get_both: range lookup on second key failed\n");
		exit(EXIT_FAILURE);
	}

	CHECK_CALL(mdb_cursor_get(cur, &beta_key, &beta_data, MDB_NEXT_DUP));
	if (beta_data.mv_size != strlen(beta_dups[1]) ||
	    memcmp(beta_data.mv_data, beta_dups[1], beta_data.mv_size) != 0) {
		fprintf(stderr, "dupsort get_both: MDB_NEXT_DUP did not continue range walk\n");
		exit(EXIT_FAILURE);
	}

	MDB_val missing_key = {strlen(keys[1]), (void *)keys[1]};
	MDB_val missing_data = {strlen("dup-9999"), "dup-9999"};
	rc = mdb_cursor_get(cur, &missing_key, &missing_data, MDB_GET_BOTH);
	if (rc != MDB_NOTFOUND) {
		fprintf(stderr, "dupsort get_both: unexpected success for missing duplicate\n");
		exit(EXIT_FAILURE);
	}

	mdb_cursor_close(cur);
	mdb_txn_abort(txn);
	mdb_env_close(env);
}

static void
test_prefix_leaf_splits(void)
{
	static const char *dir = "testdb_prefix_split";
	MDB_env *env = create_env(dir);
	MDB_txn *txn = NULL;
	MDB_dbi dbi;
	const size_t total = 4096;

	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	CHECK_CALL(mdb_dbi_open(txn, NULL, MDB_PREFIX_COMPRESSION, &dbi));
	for (size_t i = 0; i < total; ++i) {
		char keybuf[64];
		snprintf(keybuf, sizeof(keybuf), "shared-split-%08zu", i);
		MDB_val key = {strlen(keybuf), keybuf};
		MDB_val data = {strlen(keybuf), keybuf};
		CHECK_CALL(mdb_put(txn, dbi, &key, &data, 0));
	}
	CHECK_CALL(mdb_txn_commit(txn));

	CHECK_CALL(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn));
	CHECK_CALL(mdb_dbi_open(txn, NULL, 0, &dbi));

	for (size_t i = 0; i < total; i += 1023) {
		char keybuf[64];
		snprintf(keybuf, sizeof(keybuf), "shared-split-%08zu", i);
		MDB_val key = {strlen(keybuf), keybuf};
		MDB_val data = {0, NULL};
		CHECK_CALL(mdb_get(txn, dbi, &key, &data));
		if (data.mv_size != key.mv_size ||
		    memcmp(data.mv_data, key.mv_data, key.mv_size) != 0) {
			fprintf(stderr, "leaf splits: mismatch for %s\n", keybuf);
			exit(EXIT_FAILURE);
		}
	}

	MDB_cursor *cur = NULL;
	CHECK_CALL(mdb_cursor_open(txn, dbi, &cur));
	MDB_val key = {0, NULL};
	MDB_val data = {0, NULL};
	int rc = mdb_cursor_get(cur, &key, &data, MDB_FIRST);
	size_t seen = 0;
	while (rc == MDB_SUCCESS) {
		char expect[64];
		snprintf(expect, sizeof(expect), "shared-split-%08zu", seen);
		if (key.mv_size != strlen(expect) ||
		    memcmp(key.mv_data, expect, key.mv_size) != 0) {
			fprintf(stderr, "leaf splits: iteration mismatch at %zu\n", seen);
			exit(EXIT_FAILURE);
		}
		rc = mdb_cursor_get(cur, &key, &data, MDB_NEXT);
		seen++;
	}
	if (rc != MDB_NOTFOUND)
		CHECK(rc, "mdb_cursor_get");
	if (seen != total) {
		fprintf(stderr, "leaf splits: expected %zu entries, saw %zu\n", total, seen);
		exit(EXIT_FAILURE);
	}
	mdb_cursor_close(cur);
	mdb_txn_abort(txn);
	mdb_env_close(env);
}

static void
test_prefix_alternating_prefixes(void)
{
	static const char *dir = "testdb_prefix_alternating";
	MDB_env *env = create_env(dir);
	MDB_txn *txn = NULL;
	MDB_dbi dbi;
	const size_t total = 512;

	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	CHECK_CALL(mdb_dbi_open(txn, NULL, MDB_PREFIX_COMPRESSION, &dbi));
	for (size_t i = 0; i < total; ++i) {
		char keybuf[64];
		const char *prefix = (i & 1) ? "omega-" : "alpha-";
		snprintf(keybuf, sizeof(keybuf), "%s%08zu", prefix, i);
		MDB_val key = {strlen(keybuf), keybuf};
		MDB_val data = {strlen(prefix), (void *)prefix};
		CHECK_CALL(mdb_put(txn, dbi, &key, &data, 0));
	}
	CHECK_CALL(mdb_txn_commit(txn));

	CHECK_CALL(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn));
	CHECK_CALL(mdb_dbi_open(txn, NULL, 0, &dbi));
	MDB_cursor *cur = NULL;
	CHECK_CALL(mdb_cursor_open(txn, dbi, &cur));
	MDB_val key = {0, NULL};
	MDB_val data = {0, NULL};
	int rc = mdb_cursor_get(cur, &key, &data, MDB_FIRST);
	char prev[64] = {0};
	size_t seen = 0;
	while (rc == MDB_SUCCESS) {
		if (seen > 0 &&
		    (strlen(prev) != key.mv_size ||
		        memcmp(prev, key.mv_data, key.mv_size) > 0)) {
			fprintf(stderr, "alternating prefixes: order violation\n");
			exit(EXIT_FAILURE);
		}
		memcpy(prev, key.mv_data, key.mv_size);
		prev[key.mv_size] = '\0';
		rc = mdb_cursor_get(cur, &key, &data, MDB_NEXT);
		seen++;
	}
	if (rc != MDB_NOTFOUND)
		CHECK(rc, "mdb_cursor_get");
	if (seen != total) {
		fprintf(stderr, "alternating prefixes: expected %zu entries, saw %zu\n",
		    total, seen);
		exit(EXIT_FAILURE);
	}
	mdb_cursor_close(cur);
	mdb_txn_abort(txn);
	mdb_env_close(env);
}

static void
test_prefix_update_reinsert(void)
{
	static const char *dir = "testdb_prefix_update";
	MDB_env *env = create_env(dir);
	MDB_txn *txn = NULL;
	MDB_dbi dbi;
	const char *key_str = "update-key-constant";
	MDB_val key = {strlen(key_str), (void *)key_str};

	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	CHECK_CALL(mdb_dbi_open(txn, NULL, MDB_PREFIX_COMPRESSION, &dbi));
	for (int i = 0; i < 5; ++i) {
		char valbuf[32];
		int vlen = snprintf(valbuf, sizeof(valbuf), "value-%d", i);
		MDB_val val = {(size_t)vlen, valbuf};
		CHECK_CALL(mdb_put(txn, dbi, &key, &val, 0));
	}
	CHECK_CALL(mdb_txn_commit(txn));

	CHECK_CALL(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn));
	CHECK_CALL(mdb_dbi_open(txn, NULL, 0, &dbi));
	MDB_val data = {0, NULL};
	CHECK_CALL(mdb_get(txn, dbi, &key, &data));
	if (data.mv_size != strlen("value-4") ||
	    memcmp(data.mv_data, "value-4", data.mv_size) != 0) {
		fprintf(stderr, "update reinsert: unexpected value after updates\n");
		exit(EXIT_FAILURE);
	}
	mdb_txn_abort(txn);

	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	CHECK_CALL(mdb_dbi_open(txn, NULL, MDB_PREFIX_COMPRESSION, &dbi));
	CHECK_CALL(mdb_del(txn, dbi, &key, NULL));
	CHECK_CALL(mdb_txn_commit(txn));

	CHECK_CALL(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn));
	CHECK_CALL(mdb_dbi_open(txn, NULL, 0, &dbi));
	int rc = mdb_get(txn, dbi, &key, &data);
	if (rc != MDB_NOTFOUND) {
		fprintf(stderr, "update reinsert: key still present (%s)\n", mdb_strerror(rc));
		exit(EXIT_FAILURE);
	}
	mdb_txn_abort(txn);

	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	CHECK_CALL(mdb_dbi_open(txn, NULL, MDB_PREFIX_COMPRESSION, &dbi));
	MDB_val rein_val = {strlen("value-reinsert"), "value-reinsert"};
	CHECK_CALL(mdb_put(txn, dbi, &key, &rein_val, 0));
	CHECK_CALL(mdb_txn_commit(txn));

	CHECK_CALL(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn));
	CHECK_CALL(mdb_dbi_open(txn, NULL, 0, &dbi));
	CHECK_CALL(mdb_get(txn, dbi, &key, &data));
	if (data.mv_size != rein_val.mv_size ||
	    memcmp(data.mv_data, rein_val.mv_data, data.mv_size) != 0) {
		fprintf(stderr, "update reinsert: reinsertion mismatch\n");
		exit(EXIT_FAILURE);
	}
	mdb_txn_abort(txn);
	mdb_env_close(env);
}

static void
test_prefix_dupsort_smoke(void)
{
	static const char *dir = "testdb_prefix_dupsort_smoke";
	MDB_env *env = create_env(dir);
	MDB_txn *txn = NULL;
	MDB_dbi dbi;

	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	unsigned int flags = MDB_PREFIX_COMPRESSION | MDB_DUPSORT;
	CHECK_CALL(mdb_dbi_open(txn, "dupdb", MDB_CREATE | flags, &dbi));

	const char *key_str = "dup-key-alpha";
	MDB_val key = {strlen(key_str), (void *)key_str};
	const char *dups[] = {"dup-01", "dup-02", "dup-03", "dup-04"};
	for (size_t i = 0; i < ARRAY_SIZE(dups); ++i) {
		MDB_val data = {strlen(dups[i]), (void *)dups[i]};
		CHECK_CALL(mdb_put(txn, dbi, &key, &data, 0));
	}
	CHECK_CALL(mdb_txn_commit(txn));

	CHECK_CALL(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn));
	CHECK_CALL(mdb_dbi_open(txn, "dupdb", flags, &dbi));
	MDB_cursor *cur = NULL;
	CHECK_CALL(mdb_cursor_open(txn, dbi, &cur));
	MDB_val data = {0, NULL};
	CHECK_CALL(mdb_cursor_get(cur, &key, &data, MDB_SET_KEY));
	size_t seen = 0;
	int rc = mdb_cursor_get(cur, &key, &data, MDB_GET_CURRENT);
	while (rc == MDB_SUCCESS) {
		const char *expect = dups[seen];
		if (data.mv_size != strlen(expect) ||
		    memcmp(data.mv_data, expect, data.mv_size) != 0) {
			fprintf(stderr, "dupsort smoke: mismatch at %zu\n", seen);
			exit(EXIT_FAILURE);
		}
		rc = mdb_cursor_get(cur, &key, &data, MDB_NEXT_DUP);
		seen++;
	}
	if (rc != MDB_NOTFOUND)
		CHECK(rc, "mdb_cursor_get");
	if (seen != ARRAY_SIZE(dups)) {
		fprintf(stderr, "dupsort smoke: expected %zu duplicates, saw %zu\n",
		    ARRAY_SIZE(dups), seen);
		exit(EXIT_FAILURE);
	}
	mdb_cursor_close(cur);
	mdb_txn_abort(txn);

	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	CHECK_CALL(mdb_dbi_open(txn, "dupdb", flags, &dbi));
	MDB_val deldup = {strlen(dups[1]), (void *)dups[1]};
	CHECK_CALL(mdb_del(txn, dbi, &key, &deldup));
	CHECK_CALL(mdb_txn_commit(txn));

	CHECK_CALL(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn));
	CHECK_CALL(mdb_dbi_open(txn, "dupdb", flags, &dbi));
	CHECK_CALL(mdb_cursor_open(txn, dbi, &cur));
	CHECK_CALL(mdb_cursor_get(cur, &key, &data, MDB_SET_KEY));
	rc = mdb_cursor_get(cur, &key, &data, MDB_GET_CURRENT);
	seen = 0;
	while (rc == MDB_SUCCESS) {
		const char *expect = (seen == 0) ? dups[0] : dups[seen + 1];
		if (data.mv_size != strlen(expect) ||
		    memcmp(data.mv_data, expect, data.mv_size) != 0) {
			fprintf(stderr, "dupsort smoke: mismatch after delete\n");
			exit(EXIT_FAILURE);
		}
		rc = mdb_cursor_get(cur, &key, &data, MDB_NEXT_DUP);
		seen++;
	}
	if (rc != MDB_NOTFOUND)
		CHECK(rc, "mdb_cursor_get");
	if (seen != ARRAY_SIZE(dups) - 1) {
		fprintf(stderr, "dupsort smoke: expected %zu duplicates after delete, saw %zu\n",
		    ARRAY_SIZE(dups) - 1, seen);
		exit(EXIT_FAILURE);
	}
	mdb_cursor_close(cur);
	mdb_txn_abort(txn);
	mdb_env_close(env);
}

static void
test_prefix_dupsort_inline_basic_ops(void)
{
	static const char *dir = "testdb_prefix_inline_basic";
	static const char *key = "dup-inline-basic";
	static const char *dup_sequence[] = { "a", "b", "c", "d", "e" };
	const char *after_delete_d[] = { "a", "b", "c" };
	const char *after_delete_c[] = { "a", "b" };
	const char *after_readd_c[] = { "a", "b", "c" };
	const size_t initial_dup_count = ARRAY_SIZE(dup_sequence) - 1;
	const size_t full_dup_count = ARRAY_SIZE(dup_sequence);

	MDB_env *env = create_env(dir);
	MDB_txn *txn = NULL;
	MDB_dbi dbi;
	MDB_val mkey = { strlen(key), (void *)key };

	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	CHECK_CALL(mdb_dbi_open(txn, NULL,
	    MDB_CREATE | MDB_PREFIX_COMPRESSION | MDB_DUPSORT, &dbi));
	for (size_t i = 0; i < initial_dup_count; ++i) {
		const char *dup = dup_sequence[i];
		MDB_val data = { strlen(dup), (void *)dup };
		CHECK_CALL(mdb_put(txn, dbi, &mkey, &data, 0));
	}
	CHECK_CALL(mdb_txn_commit(txn));

	assert_dup_sequence(env, dbi, key,
	    dup_sequence, initial_dup_count);

	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	{
		const char *dup = dup_sequence[initial_dup_count];
		MDB_val data = { strlen(dup), (void *)dup };
		CHECK_CALL(mdb_put(txn, dbi, &mkey, &data, 0));
	}
	CHECK_CALL(mdb_txn_commit(txn));

	assert_dup_sequence(env, dbi, key,
	    dup_sequence, full_dup_count);

	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	{
		const char *dup = dup_sequence[initial_dup_count];
		MDB_val data = { strlen(dup), (void *)dup };
		CHECK_CALL(mdb_del(txn, dbi, &mkey, &data));
	}
	CHECK_CALL(mdb_txn_commit(txn));

	assert_dup_sequence(env, dbi, key,
	    dup_sequence, initial_dup_count);

	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	{
		const char *dup = dup_sequence[initial_dup_count - 1];
		MDB_val data = { strlen(dup), (void *)dup };
		CHECK_CALL(mdb_del(txn, dbi, &mkey, &data));
	}
	CHECK_CALL(mdb_txn_commit(txn));

	assert_dup_sequence(env, dbi, key,
	    after_delete_d, ARRAY_SIZE(after_delete_d));

	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	{
		const char *dup = dup_sequence[initial_dup_count - 2];
		MDB_val data = { strlen(dup), (void *)dup };
		CHECK_CALL(mdb_del(txn, dbi, &mkey, &data));
	}
	CHECK_CALL(mdb_txn_commit(txn));

	assert_dup_sequence(env, dbi, key,
	    after_delete_c, ARRAY_SIZE(after_delete_c));

	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	{
		const char *dup = dup_sequence[initial_dup_count - 2];
		MDB_val data = { strlen(dup), (void *)dup };
		CHECK_CALL(mdb_put(txn, dbi, &mkey, &data, 0));
	}
	CHECK_CALL(mdb_txn_commit(txn));

	assert_dup_sequence(env, dbi, key,
	    after_readd_c, ARRAY_SIZE(after_readd_c));

	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	{
		const char *dup = dup_sequence[initial_dup_count - 1];
		MDB_val data = { strlen(dup), (void *)dup };
		CHECK_CALL(mdb_put(txn, dbi, &mkey, &data, 0));
	}
	CHECK_CALL(mdb_txn_commit(txn));

	assert_dup_sequence(env, dbi, key,
	    dup_sequence, initial_dup_count);

	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	{
		const char *dup = dup_sequence[initial_dup_count];
		MDB_val data = { strlen(dup), (void *)dup };
		CHECK_CALL(mdb_put(txn, dbi, &mkey, &data, 0));
	}
	CHECK_CALL(mdb_txn_commit(txn));

	assert_dup_sequence(env, dbi, key,
	    dup_sequence, full_dup_count);

	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	{
		const char *dup = dup_sequence[initial_dup_count];
		MDB_val data = { strlen(dup), (void *)dup };
		CHECK_CALL(mdb_del(txn, dbi, &mkey, &data));
	}
	CHECK_CALL(mdb_txn_commit(txn));

	assert_dup_sequence(env, dbi, key,
	    dup_sequence, initial_dup_count);

	mdb_env_close(env);
}

static void
test_prefix_dupsort_inline_promote(void)
{
	static const char *dir = "testdb_prefix_inline_promote";
	static const char *key = "dup-inline-promote";
	static const char *small_dups[] = {
		"inline-small-1",
		"inline-small-2",
		"inline-small-3"
	};
	const size_t large_len1 = 500;
	const size_t large_len2 = 508;
	char *large_dup1 = malloc(large_len1 + 1);
	char *large_dup2 = malloc(large_len2 + 1);

	if (!large_dup1 || !large_dup2) {
		fprintf(stderr, "inline promote: allocation failure\n");
		exit(EXIT_FAILURE);
	}

	memset(large_dup1, 'z', large_len1);
	large_dup1[large_len1 - 1] = '1';
	large_dup1[large_len1] = '\0';

	memset(large_dup2, 'z', large_len2);
	large_dup2[large_len2 - 1] = '2';
	large_dup2[large_len2] = '\0';

	const char *expected_after_promotion[] = {
		small_dups[0],
		small_dups[1],
		small_dups[2],
		large_dup1,
		large_dup2
	};

	MDB_env *env = create_env(dir);
	MDB_txn *txn = NULL;
	MDB_dbi dbi;
	MDB_val mkey = { strlen(key), (void *)key };

	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	CHECK_CALL(mdb_dbi_open(txn, NULL,
	    MDB_CREATE | MDB_PREFIX_COMPRESSION | MDB_DUPSORT, &dbi));
	for (size_t i = 0; i < ARRAY_SIZE(small_dups); ++i) {
		const char *dup = small_dups[i];
		MDB_val data = { strlen(dup), (void *)dup };
		CHECK_CALL(mdb_put(txn, dbi, &mkey, &data, 0));
	}
	CHECK_CALL(mdb_txn_commit(txn));

	assert_dup_sequence(env, dbi, key,
	    small_dups, ARRAY_SIZE(small_dups));

	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	{
		MDB_val data = { large_len1, large_dup1 };
		CHECK_CALL(mdb_put(txn, dbi, &mkey, &data, 0));
	}
	CHECK_CALL(mdb_txn_commit(txn));

	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	{
		MDB_val data = { large_len2, large_dup2 };
		CHECK_CALL(mdb_put(txn, dbi, &mkey, &data, 0));
	}
	CHECK_CALL(mdb_txn_commit(txn));

	assert_dup_sequence(env, dbi, key,
	    expected_after_promotion, ARRAY_SIZE(expected_after_promotion));

	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	{
		MDB_val data = { strlen(small_dups[1]), (void *)small_dups[1] };
		CHECK_CALL(mdb_del(txn, dbi, &mkey, &data));
	}
	CHECK_CALL(mdb_txn_commit(txn));

	const char *after_delete_mid[] = {
		small_dups[0],
		small_dups[2],
		large_dup1,
		large_dup2
	};
	assert_dup_sequence(env, dbi, key,
	    after_delete_mid, ARRAY_SIZE(after_delete_mid));

	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	{
		MDB_val data = { strlen(small_dups[1]), (void *)small_dups[1] };
		CHECK_CALL(mdb_put(txn, dbi, &mkey, &data, 0));
	}
	CHECK_CALL(mdb_txn_commit(txn));

	assert_dup_sequence(env, dbi, key,
	    expected_after_promotion, ARRAY_SIZE(expected_after_promotion));

	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	{
		MDB_val data = { large_len1, large_dup1 };
		CHECK_CALL(mdb_del(txn, dbi, &mkey, &data));
	}
	CHECK_CALL(mdb_txn_commit(txn));

	const char *after_delete_large[] = {
		small_dups[0],
		small_dups[1],
		small_dups[2],
		large_dup2
	};
	assert_dup_sequence(env, dbi, key,
	    after_delete_large, ARRAY_SIZE(after_delete_large));

	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	{
		MDB_val data = { large_len1, large_dup1 };
		CHECK_CALL(mdb_put(txn, dbi, &mkey, &data, 0));
	}
	CHECK_CALL(mdb_txn_commit(txn));

	assert_dup_sequence(env, dbi, key,
	    expected_after_promotion, ARRAY_SIZE(expected_after_promotion));

	CHECK_CALL(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn));
	{
		MDB_cursor *cur = NULL;
		MDB_val search_key = { strlen(key), (void *)key };
		MDB_val search_data = { large_len2, large_dup2 };
		CHECK_CALL(mdb_cursor_open(txn, dbi, &cur));
		int rc = mdb_cursor_get(cur, &search_key, &search_data, MDB_GET_BOTH_RANGE);
		if (rc != MDB_SUCCESS) {
			fprintf(stderr,
			    "inline promote: MDB_GET_BOTH failed for promoted duplicate (%s)\n",
			    mdb_strerror(rc));
			exit(EXIT_FAILURE);
		}
		mdb_cursor_close(cur);
	}
	mdb_txn_abort(txn);

	mdb_env_close(env);
	env = NULL;

	CHECK_CALL(mdb_env_create(&env));
	CHECK_CALL(mdb_env_set_maxdbs(env, 4));
	CHECK_CALL(mdb_env_set_mapsize(env, 64UL * 1024 * 1024));
	CHECK_CALL(mdb_env_open(env, dir, MDB_NOLOCK, 0664));

	CHECK_CALL(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn));
	CHECK_CALL(mdb_dbi_open(txn, NULL,
	    MDB_PREFIX_COMPRESSION | MDB_DUPSORT, &dbi));
	assert_dup_sequence(env, dbi, key,
	    expected_after_promotion, ARRAY_SIZE(expected_after_promotion));
	mdb_txn_abort(txn);

	free(large_dup1);
	free(large_dup2);
	mdb_env_close(env);
}

static void
test_prefix_dupsort_trunk_key_shift_no_value_change(void)
{
	static const char *dir = "testdb_prefix_trunk_key_shift";
	static const char *key = "dup-trunk-key-shift";
	static const char *initial_dups[] = {
		"trunk-inline-0100",
		"trunk-inline-0200",
		"trunk-inline-0300"
	};
	static const char *new_trunk = "trunk-inline-0005";
	const char *expected_before[] = {
		"trunk-inline-0100",
		"trunk-inline-0200",
		"trunk-inline-0300"
	};
	const char *expected_after[] = {
		"trunk-inline-0005",
		"trunk-inline-0100",
		"trunk-inline-0200",
		"trunk-inline-0300"
	};
	const char *after_delete[] = {
		"trunk-inline-0005",
		"trunk-inline-0100",
		"trunk-inline-0200"
	};
	const char *after_insert[] = {
		"trunk-inline-0005",
		"trunk-inline-0100",
		"trunk-inline-0150",
		"trunk-inline-0200"
	};

	MDB_env *env = create_env(dir);
	MDB_txn *txn = NULL;
	MDB_dbi dbi;
	MDB_val mkey = { strlen(key), (void *)key };

	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	CHECK_CALL(mdb_dbi_open(txn, NULL,
	    MDB_CREATE | MDB_PREFIX_COMPRESSION | MDB_DUPSORT, &dbi));
	for (size_t i = 0; i < ARRAY_SIZE(initial_dups); ++i) {
		const char *dup = initial_dups[i];
		MDB_val data = { strlen(dup), (void *)dup };
		CHECK_CALL(mdb_put(txn, dbi, &mkey, &data, 0));
	}
	CHECK_CALL(mdb_txn_commit(txn));

	assert_dup_sequence(env, dbi, key,
	    expected_before, ARRAY_SIZE(expected_before));

	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	{
		MDB_val data = { strlen(new_trunk), (void *)new_trunk };
		CHECK_CALL(mdb_put(txn, dbi, &mkey, &data, 0));
	}
	CHECK_CALL(mdb_txn_commit(txn));

	assert_dup_sequence(env, dbi, key,
	    expected_after, ARRAY_SIZE(expected_after));

	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	{
		MDB_val tail = { strlen(initial_dups[2]), (void *)initial_dups[2] };
		CHECK_CALL(mdb_del(txn, dbi, &mkey, &tail));
	}
	CHECK_CALL(mdb_txn_commit(txn));

	assert_dup_sequence(env, dbi, key,
	    after_delete, ARRAY_SIZE(after_delete));

	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	{
		const char *insert_dup = "trunk-inline-0150";
		MDB_val data = { strlen(insert_dup), (void *)insert_dup };
		CHECK_CALL(mdb_put(txn, dbi, &mkey, &data, 0));
	}
	CHECK_CALL(mdb_txn_commit(txn));

	assert_dup_sequence(env, dbi, key,
	    after_insert, ARRAY_SIZE(after_insert));

	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	{
		const char *mid_dup = "trunk-inline-0150";
		MDB_val data = { strlen(mid_dup), (void *)mid_dup };
		CHECK_CALL(mdb_del(txn, dbi, &mkey, &data));
	}
	CHECK_CALL(mdb_txn_commit(txn));

	assert_dup_sequence(env, dbi, key,
	    after_delete, ARRAY_SIZE(after_delete));

	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	{
		const char *readd_tail = "trunk-inline-0300";
		MDB_val data = { strlen(readd_tail), (void *)readd_tail };
		CHECK_CALL(mdb_put(txn, dbi, &mkey, &data, 0));
	}
	CHECK_CALL(mdb_txn_commit(txn));

	assert_dup_sequence(env, dbi, key,
	    expected_after, ARRAY_SIZE(expected_after));

	mdb_env_close(env);
}

static void
test_prefix_dupsort_trunk_swap_inline(void)
{
	static const char *dir = "testdb_prefix_trunk_inline";
	static const char *key = "dup-trunk-inline";
	const char *initial_dups[] = {
		"mango-inline-tail-0001",
		"mango-inline-tail-0002"
	};
	const char *new_trunk = "aardvark-inline-root-0000";
	const char *expected_after_initial[] = {
		"mango-inline-tail-0001",
		"mango-inline-tail-0002"
	};
	const char *expected_after_swap[] = {
		"aardvark-inline-root-0000",
		"mango-inline-tail-0001",
		"mango-inline-tail-0002"
	};

	MDB_env *env = create_env(dir);
	MDB_txn *txn = NULL;
	MDB_dbi dbi;
	MDB_val mkey = { strlen(key), (void *)key };

	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	CHECK_CALL(mdb_dbi_open(txn, NULL,
	    MDB_CREATE | MDB_PREFIX_COMPRESSION | MDB_DUPSORT, &dbi));
	for (size_t i = 0; i < ARRAY_SIZE(initial_dups); ++i) {
		const char *dup = initial_dups[i];
		MDB_val data = { strlen(dup), (void *)dup };
		CHECK_CALL(mdb_put(txn, dbi, &mkey, &data, 0));
	}
	CHECK_CALL(mdb_txn_commit(txn));

	assert_dup_sequence(env, dbi, key,
	    expected_after_initial, ARRAY_SIZE(expected_after_initial));

	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	{
		MDB_val data = { strlen(new_trunk), (void *)new_trunk };
		CHECK_CALL(mdb_put(txn, dbi, &mkey, &data, 0));
	}
	CHECK_CALL(mdb_txn_commit(txn));

	assert_dup_sequence(env, dbi, key,
	    expected_after_swap, ARRAY_SIZE(expected_after_swap));

	mdb_env_close(env);
}

static void
test_prefix_dupsort_trunk_swap_promote(void)
{
	static const char *dir = "testdb_prefix_trunk_promote";
	static const char *key = "dup-trunk-promote";
	enum { VALUE_LEN = 192, INITIAL_COUNT = 24 };
	char values[INITIAL_COUNT][VALUE_LEN + 1];
	const char *expected_initial[INITIAL_COUNT];

	MDB_env *env = create_env(dir);
	MDB_txn *txn = NULL;
	MDB_dbi dbi;
	MDB_val mkey = { strlen(key), (void *)key };

	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	CHECK_CALL(mdb_dbi_open(txn, NULL,
	    MDB_CREATE | MDB_PREFIX_COMPRESSION | MDB_DUPSORT, &dbi));

	for (size_t i = 0; i < INITIAL_COUNT; ++i) {
		size_t prefix = snprintf(values[i], sizeof(values[i]),
		    "shared-promote-base-%04zu-", i);
		if (prefix >= VALUE_LEN)
			prefix = VALUE_LEN - 1;
		memset(values[i] + prefix, 'v' - (int)(i % 12), VALUE_LEN - prefix);
		values[i][VALUE_LEN] = '\0';
		MDB_val data = { VALUE_LEN, values[i] };
		CHECK_CALL(mdb_put(txn, dbi, &mkey, &data, MDB_APPENDDUP));
		expected_initial[i] = values[i];
	}
	CHECK_CALL(mdb_txn_commit(txn));

	assert_dup_sequence(env, dbi, key, expected_initial, INITIAL_COUNT);

	char promote_trunk[VALUE_LEN + 1];
	{
		size_t prefix = snprintf(promote_trunk, sizeof(promote_trunk),
		    "alpha-promote-root-0000-");
		if (prefix >= VALUE_LEN)
			prefix = VALUE_LEN - 1;
		memset(promote_trunk + prefix, 'a', VALUE_LEN - prefix);
		promote_trunk[VALUE_LEN] = '\0';
	}

	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	{
		MDB_val data = { VALUE_LEN, promote_trunk };
		CHECK_CALL(mdb_put(txn, dbi, &mkey, &data, 0));
	}
	CHECK_CALL(mdb_txn_commit(txn));

	const char *expected_after_swap[INITIAL_COUNT + 2];
	expected_after_swap[0] = promote_trunk;
	for (size_t i = 0; i < INITIAL_COUNT; ++i)
		expected_after_swap[i + 1] = expected_initial[i];
	assert_dup_sequence(env, dbi, key,
	    expected_after_swap, INITIAL_COUNT + 1);

	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	{
		char tail_value[VALUE_LEN + 1];
		size_t prefix = snprintf(tail_value, sizeof(tail_value),
		    "shared-promote-base-%04zu-", (size_t)INITIAL_COUNT);
		if (prefix >= VALUE_LEN)
			prefix = VALUE_LEN - 1;
		memset(tail_value + prefix, 'z', VALUE_LEN - prefix);
		tail_value[VALUE_LEN] = '\0';
		MDB_val data = { VALUE_LEN, tail_value };
		CHECK_CALL(mdb_put(txn, dbi, &mkey, &data, 0));
		char *tail_copy = malloc(VALUE_LEN + 1);
		if (!tail_copy) {
			fprintf(stderr, "dup trunk promote: malloc failed\n");
			exit(EXIT_FAILURE);
		}
		memcpy(tail_copy, tail_value, VALUE_LEN + 1);
		expected_after_swap[INITIAL_COUNT + 1] = tail_copy;
	}
	CHECK_CALL(mdb_txn_commit(txn));

	assert_dup_sequence(env, dbi, key,
	    expected_after_swap, INITIAL_COUNT + 2);

	/* Free strdup'ed tail entry */
	free((void *)expected_after_swap[INITIAL_COUNT + 1]);

	mdb_env_close(env);
}

static void
test_prefix_dupsort_fuzz(void)
{
	static const char *dir = "testdb_prefix_dupsort_fuzz";
	static const char *dbname = "dupfuzz";
	MDB_env *env = create_env(dir);
	MDB_txn *txn = NULL;
	MDB_dbi dbi;
	unsigned int flags = MDB_PREFIX_COMPRESSION | MDB_DUPSORT;

	const char *seed_env = getenv("PF_SEED");
	uint64_t seed = UINT64_C(0x9e3779b97f4a7c15);
	if (seed_env && *seed_env) {
		char *end = NULL;
		uint64_t parsed = strtoull(seed_env, &end, 0);
		if (end && *end == '\0')
			seed = parsed;
	}
	df_rng_state = seed ^ UINT64_C(0x517cc1b727220a95);
	df_model_reset();
	df_op_index = 0;

	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	CHECK_CALL(mdb_dbi_open(txn, dbname, MDB_CREATE | flags, &dbi));
	CHECK_CALL(mdb_txn_commit(txn));

	const size_t operations = 1500;
	for (size_t op = 0; op < operations; ++op) {
		df_op_index = op;
		int do_insert = (df_entry_count == 0) || (df_rng_next() & 1);
		if (do_insert)
			df_do_insert(env, dbi);
		else
			df_do_delete(env, dbi);
		df_verify_model(env, dbi);
	}

	/* Force a large inline duplicate set to promote into a sub-DB. */
	const char *promo_key = "prefix-longer-gamma-promo-anchor";
	size_t promo_len = strlen(promo_key);
	for (size_t i = 0; i < 320; ++i) {
		df_op_index = operations + i;
		unsigned char valbuf[PF_VALUE_MAX_LEN];
		size_t val_len = df_make_value(valbuf, sizeof(valbuf));
		CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
		MDB_val key = { promo_len, (void *)promo_key };
		MDB_val data = { val_len, valbuf };
		int rc = mdb_put(txn, dbi, &key, &data, 0);
		if (rc != MDB_SUCCESS) {
			fprintf(stderr, "dupsort fuzz: promo insert failed (%s)\n",
			    mdb_strerror(rc));
			mdb_txn_abort(txn);
			exit(EXIT_FAILURE);
		}
		CHECK_CALL(mdb_txn_commit(txn));
		df_model_insert(promo_key, promo_len, valbuf, val_len);
		df_verify_model(env, dbi);
	}

	/* Drain the database to confirm delete paths behave. */
	while (df_entry_count > 0) {
		df_op_index++;
		df_do_delete(env, dbi);
		df_verify_model(env, dbi);
	}

	mdb_env_close(env);
}

static uint64_t
pf_rng_next(void)
{
	uint64_t x = pf_rng_state;
	x ^= x >> 12;
	x ^= x << 25;
	x ^= x >> 27;
	pf_rng_state = x;
	return x * UINT64_C(2685821657736338717);
}

static size_t
pf_rng_range(size_t min, size_t max)
{
	if (max <= min)
		return min;
	uint64_t span = (uint64_t)(max - min + 1);
	return min + (size_t)(pf_rng_next() % span);
}

static int
pf_key_compare(const char *a, size_t alen, const char *b, size_t blen)
{
	size_t n = alen < blen ? alen : blen;
	int cmp = memcmp(a, b, n);
	if (cmp)
		return cmp;
	if (alen < blen)
		return -1;
	if (alen > blen)
		return 1;
	return 0;
}

static int
pf_entry_search(const char *key, size_t key_len, int *found)
{
	size_t lo = 0;
	size_t hi = pf_entry_count;
	while (lo < hi) {
		size_t mid = lo + ((hi - lo) >> 1);
		int cmp = pf_key_compare(key, key_len,
		    pf_entries[mid].key, pf_entries[mid].key_len);
		if (cmp == 0) {
			*found = 1;
			return (int)mid;
		}
		if (cmp < 0)
			hi = mid;
		else
			lo = mid + 1;
	}
	*found = 0;
	return (int)lo;
}

static void
pf_entry_insert(const char *key, size_t key_len,
    const unsigned char *value, size_t val_len)
{
	int found = 0;
	int idx = pf_entry_search(key, key_len, &found);
	if (found) {
		PFEntry *e = &pf_entries[idx];
		e->val_len = val_len;
		memcpy(e->value, value, val_len);
		return;
	}
	if (pf_entry_count >= PF_MAX_ENTRIES) {
		fprintf(stderr, "prefix fuzz: model capacity exceeded\n");
		exit(EXIT_FAILURE);
	}
	for (size_t i = pf_entry_count; i > (size_t)idx; --i)
		pf_entries[i] = pf_entries[i - 1];
	PFEntry *dst = &pf_entries[idx];
	dst->key_len = key_len;
	memcpy(dst->key, key, key_len);
	dst->key[key_len] = '\0';
	dst->val_len = val_len;
	memcpy(dst->value, value, val_len);
	pf_entry_count++;
}

static void
pf_entry_delete_at(size_t idx)
{
	if (idx >= pf_entry_count) {
		fprintf(stderr, "prefix fuzz: delete index out of range\n");
		exit(EXIT_FAILURE);
	}
	for (size_t i = idx; i + 1 < pf_entry_count; ++i)
		pf_entries[i] = pf_entries[i + 1];
	pf_entry_count--;
}

static void
df_model_reset(void)
{
	df_entry_count = 0;
	df_key_nonce = 0;
}

static uint64_t
df_rng_next(void)
{
	uint64_t x = df_rng_state;
	x ^= x >> 12;
	x ^= x << 25;
	x ^= x >> 27;
	df_rng_state = x;
	return x * UINT64_C(2685821657736338717);
}

static size_t
df_rng_range(size_t min, size_t max)
{
	if (max <= min)
		return min;
	uint64_t span = (uint64_t)(max - min + 1);
	return min + (size_t)(df_rng_next() % span);
}

static int
df_entry_search(const char *key, size_t key_len, int *found)
{
	size_t lo = 0;
	size_t hi = df_entry_count;
	while (lo < hi) {
		size_t mid = lo + ((hi - lo) >> 1);
		int cmp = pf_key_compare(key, key_len,
		    df_entries[mid].key, df_entries[mid].key_len);
		if (cmp == 0) {
			*found = 1;
			return (int)mid;
		}
		if (cmp < 0)
			hi = mid;
		else
			lo = mid + 1;
	}
	*found = 0;
	return (int)lo;
}

static size_t
df_make_key(char *out, size_t max_len)
{
	static const char *prefixes[] = {
		"shared-alpha",
		"shared-beta",
		"shared-gamma",
		"prefix-longer-alpha",
		"prefix-longer-beta",
		"prefix-longer-gamma"
	};
	size_t which = (size_t)(df_rng_next() % (sizeof(prefixes) / sizeof(prefixes[0])));
	const char *prefix = prefixes[which];
	size_t prefix_len = strlen(prefix);
	if (prefix_len + 1 >= max_len)
		prefix_len = max_len > 1 ? max_len - 1 : 0;
	memcpy(out, prefix, prefix_len);
	out[prefix_len++] = '-';

	uint64_t nonce = df_key_nonce++;
	int written = snprintf(out + prefix_len, max_len - prefix_len, "%016" PRIx64, nonce);
	if (written < 0) {
		fprintf(stderr, "dupsort fuzz: snprintf failed while formatting key\n");
		exit(EXIT_FAILURE);
	}
	size_t len = prefix_len + (size_t)written;
	if (len >= max_len)
		len = max_len - 1;

	size_t extra = df_rng_range(0, 6);
	for (size_t i = 0; i < extra && len + 1 < max_len; ++i)
		out[len++] = (char)('a' + (df_rng_next() % 26));

	out[len] = '\0';
	return len;
}

static size_t
df_make_value(unsigned char *buf, size_t max_len)
{
	size_t len = df_rng_range(12, max_len > 192 ? 192 : max_len);
	for (size_t i = 0; i < len; ++i)
		buf[i] = (unsigned char)('A' + (df_rng_next() % 26));
	return len;
}

static void
df_model_insert(const char *key, size_t key_len,
    const unsigned char *value, size_t val_len)
{
	int found = 0;
	int idx = df_entry_search(key, key_len, &found);
	DFEntry *entry = NULL;
	if (!found) {
		if (df_entry_count >= DF_MAX_KEYS) {
			fprintf(stderr, "dupsort fuzz: key capacity exceeded\n");
			exit(EXIT_FAILURE);
		}
		for (size_t i = df_entry_count; i > (size_t)idx; --i)
		{
			df_entries[i] = df_entries[i - 1];
		}
		entry = &df_entries[idx];
		entry->key_len = key_len;
		memcpy(entry->key, key, key_len);
		entry->key[key_len] = '\0';
		entry->dup_count = 0;
		df_entry_count++;
	} else {
		entry = &df_entries[idx];
	}

	if (entry->dup_count >= DF_MAX_DUPS) {
		fprintf(stderr, "dupsort fuzz: duplicate capacity exceeded for key %.*s\n",
		    (int)entry->key_len, entry->key);
		exit(EXIT_FAILURE);
	}

	size_t pos = entry->dup_count;
	while (pos > 0) {
		size_t prev = pos - 1;
		int cmp = pf_key_compare((const char *)entry->dups[prev].value,
		    entry->dups[prev].len, (const char *)value, val_len);
		if (cmp <= 0)
			break;
		entry->dups[pos] = entry->dups[prev];
		pos = prev;
	}
	entry->dups[pos].len = val_len;
	memcpy(entry->dups[pos].value, value, val_len);
	entry->dup_count++;
}

static void
df_model_delete(const char *key, size_t key_len,
    const unsigned char *value, size_t val_len)
{
	int found = 0;
	int idx = df_entry_search(key, key_len, &found);
	if (!found) {
		fprintf(stderr, "dupsort fuzz: attempted to delete missing key %.*s\n",
		    (int)key_len, key);
		exit(EXIT_FAILURE);
	}
	DFEntry *entry = &df_entries[idx];
	size_t pos = SIZE_MAX;
	for (size_t i = 0; i < entry->dup_count; ++i) {
		if (entry->dups[i].len == val_len &&
		    memcmp(entry->dups[i].value, value, val_len) == 0) {
			pos = i;
			break;
		}
	}
	if (pos == SIZE_MAX) {
		fprintf(stderr, "dupsort fuzz: value not found for delete on key %.*s\n",
		    (int)entry->key_len, entry->key);
		exit(EXIT_FAILURE);
	}
	for (size_t i = pos; i + 1 < entry->dup_count; ++i)
		entry->dups[i] = entry->dups[i + 1];
	entry->dup_count--;
	if (entry->dup_count == 0) {
		for (size_t i = (size_t)idx; i + 1 < df_entry_count; ++i)
			df_entries[i] = df_entries[i + 1];
		df_entry_count--;
	}
}

static void
df_verify_model(MDB_env *env, MDB_dbi dbi)
{
	MDB_txn *txn = NULL;
	MDB_cursor *cur = NULL;
	CHECK_CALL(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn));
	CHECK_CALL(mdb_cursor_open(txn, dbi, &cur));

	MDB_val key = {0, NULL};
	MDB_val data = {0, NULL};
	int rc = mdb_cursor_get(cur, &key, &data, MDB_FIRST);
	size_t key_index = 0;

	while (rc == MDB_SUCCESS) {
		if (key_index >= df_entry_count) {
			fprintf(stderr, "dupsort fuzz: database has unexpected extra key\n");
			exit(EXIT_FAILURE);
		}
		DFEntry *entry = &df_entries[key_index];
		if (key.mv_size != entry->key_len ||
		    memcmp(key.mv_data, entry->key, key.mv_size) != 0) {
			fprintf(stderr, "dupsort fuzz: key mismatch at index %zu\n", key_index);
			exit(EXIT_FAILURE);
		}
		mdb_size_t dupcount = 0;
		CHECK_CALL(mdb_cursor_count(cur, &dupcount));
		if (dupcount != entry->dup_count) {
			fprintf(stderr, "dupsort fuzz: duplicate count mismatch after op%zu for key %.*s "
			    "(expected %zu, got %" PRIuPTR ")\n",
			    df_op_index, (int)entry->key_len, entry->key, entry->dup_count,
			    (uintptr_t)dupcount);
			exit(EXIT_FAILURE);
		}
		for (size_t dup = 0; dup < entry->dup_count; ++dup) {
			if (data.mv_size != entry->dups[dup].len ||
			    memcmp(data.mv_data, entry->dups[dup].value, data.mv_size) != 0) {
				fprintf(stderr,
				    "dupsort fuzz: duplicate mismatch after op%zu at key %.*s idx %zu "
				    "(cursor dupcount=%" PRIuPTR ")\n",
				    df_op_index, (int)entry->key_len, entry->key, dup,
				    (uintptr_t)dupcount);
				fprintf(stderr, "  expected (%zu bytes):", entry->dups[dup].len);
				for (size_t j = 0; j < entry->dups[dup].len; ++j)
					fprintf(stderr, " %02x", entry->dups[dup].value[j]);
				fprintf(stderr, "\n  actual (%zu bytes):", data.mv_size);
				const unsigned char *raw = (const unsigned char *)data.mv_data;
				for (size_t j = 0; j < data.mv_size; ++j)
					fprintf(stderr, " %02x", raw[j]);
				fprintf(stderr, "\n");
				exit(EXIT_FAILURE);
			}
			if (dup + 1 < entry->dup_count) {
				int drc = mdb_cursor_get(cur, &key, &data, MDB_NEXT_DUP);
				if (drc != MDB_SUCCESS) {
					fprintf(stderr, "dupsort fuzz: MDB_NEXT_DUP failed (%s)\n",
					    mdb_strerror(drc));
					exit(EXIT_FAILURE);
				}
			}
		}
		rc = mdb_cursor_get(cur, &key, &data, MDB_NEXT_NODUP);
		key_index++;
	}
	if (rc != MDB_NOTFOUND)
		CHECK(rc, "mdb_cursor_get");
	if (key_index != df_entry_count) {
		fprintf(stderr, "dupsort fuzz: database returned %zu keys, expected %zu\n",
		    key_index, df_entry_count);
		exit(EXIT_FAILURE);
	}
	mdb_cursor_close(cur);
	mdb_txn_abort(txn);
}

static void
assert_dup_sequence(MDB_env *env, MDB_dbi dbi, const char *key,
    const char *const *expected, size_t expected_count)
{
	MDB_txn *txn = NULL;
	MDB_cursor *cur = NULL;
	MDB_val lookup = { strlen(key), (void *)key };
	MDB_val data = { 0, NULL };
	int rc;

	CHECK_CALL(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn));
	CHECK_CALL(mdb_cursor_open(txn, dbi, &cur));

	rc = mdb_cursor_get(cur, &lookup, &data, MDB_SET_KEY);
	if (expected_count == 0) {
		if (rc != MDB_NOTFOUND) {
			fprintf(stderr, "dup sequence: expected key %s to be absent\n", key);
			exit(EXIT_FAILURE);
		}
		goto done;
	}
	if (rc != MDB_SUCCESS) {
		fprintf(stderr, "dup sequence: failed to find key %s (%s)\n",
		    key, mdb_strerror(rc));
		exit(EXIT_FAILURE);
	}

	mdb_size_t dupcount = 0;
	CHECK_CALL(mdb_cursor_count(cur, &dupcount));
	if ((size_t)dupcount != expected_count) {
		fprintf(stderr,
		    "dup sequence: key %s expected %zu duplicates, observed %" PRIuPTR "\n",
		    key, expected_count, (uintptr_t)dupcount);
		exit(EXIT_FAILURE);
	}

	for (size_t i = 0; i < expected_count; ++i) {
		const char *expect = expected[i];
		size_t expect_len = strlen(expect);
		if (data.mv_size != expect_len ||
		    memcmp(data.mv_data, expect, expect_len) != 0) {
			fprintf(stderr, "observed duplicate size %zu\n", (size_t)data.mv_size);
			fprintf(stderr, "observed duplicate bytes:");
			for (size_t b = 0; b < data.mv_size; ++b) {
				fprintf(stderr, " %02x", ((const unsigned char *)data.mv_data)[b]);
			}
			fprintf(stderr, "\n");
			fprintf(stderr,
			    "dup sequence: key %s mismatch at dup index %zu (expected \"%s\", got %.*s)\n",
			    key, i, expect, (int)data.mv_size, (const char *)data.mv_data);
			exit(EXIT_FAILURE);
		}
		if (i + 1 < expected_count) {
			rc = mdb_cursor_get(cur, &lookup, &data, MDB_NEXT_DUP);
			if (rc != MDB_SUCCESS) {
				fprintf(stderr,
				    "dup sequence: MDB_NEXT_DUP failed at index %zu (%s)\n",
				    i, mdb_strerror(rc));
				exit(EXIT_FAILURE);
			}
		}
	}
	rc = mdb_cursor_get(cur, &lookup, &data, MDB_NEXT_DUP);
	if (rc != MDB_NOTFOUND) {
		fprintf(stderr,
		    "dup sequence: expected end of duplicates for key %s, got %s\n",
		    key, mdb_strerror(rc));
		exit(EXIT_FAILURE);
	}

done:
	mdb_cursor_close(cur);
	mdb_txn_abort(txn);
}

static void
df_do_insert(MDB_env *env, MDB_dbi dbi)
{
	char keybuf[PF_KEY_MAX_LEN];
	size_t key_len = 0;
	int picked_new_key = 0;

	if (df_entry_count == 0 ||
	    (df_entry_count < DF_MAX_KEYS && (df_rng_next() & 7) == 0)) {
		do {
			key_len = df_make_key(keybuf, sizeof(keybuf));
			int found = 0;
			df_entry_search(keybuf, key_len, &found);
			if (!found) {
				picked_new_key = 1;
				break;
			}
		} while (1);
	} else {
		DFEntry *entry = NULL;
		for (int attempt = 0; attempt < 8; ++attempt) {
			size_t idx = (size_t)(df_rng_next() % df_entry_count);
			if (df_entries[idx].dup_count < DF_MAX_DUPS) {
				entry = &df_entries[idx];
				memcpy(keybuf, entry->key, entry->key_len);
				key_len = entry->key_len;
				break;
			}
		}
		if (!entry) {
			do {
				key_len = df_make_key(keybuf, sizeof(keybuf));
				int found = 0;
				df_entry_search(keybuf, key_len, &found);
				if (!found) {
					picked_new_key = 1;
					break;
				}
			} while (1);
		}
	}

	unsigned char valbuf[PF_VALUE_MAX_LEN];
	size_t val_len = df_make_value(valbuf, sizeof(valbuf));

	if (pf_trace_ops()) {
		fprintf(stderr, "dfuzz op%zu: insert key=%.*s len=%zu%s\n",
		    df_op_index, (int)key_len, keybuf, val_len,
		    picked_new_key ? " (new)" : "");
	}

	MDB_txn *txn = NULL;
	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	MDB_val key = { key_len, keybuf };
	MDB_val data = { val_len, valbuf };
	int rc = mdb_put(txn, dbi, &key, &data, 0);
	if (rc != MDB_SUCCESS) {
		fprintf(stderr, "dupsort fuzz: mdb_put failed (%s)\n", mdb_strerror(rc));
		mdb_txn_abort(txn);
		exit(EXIT_FAILURE);
	}
	CHECK_CALL(mdb_txn_commit(txn));
	df_model_insert(keybuf, key_len, valbuf, val_len);

	if (pf_trace_ops() && key_len == strlen("shared-alpha-0000000000000001jv") &&
	    memcmp(keybuf, "shared-alpha-0000000000000001jv", key_len) == 0) {
		MDB_txn *rtxn = NULL;
		MDB_cursor *cur = NULL;
		CHECK_CALL(mdb_txn_begin(env, NULL, MDB_RDONLY, &rtxn));
		CHECK_CALL(mdb_cursor_open(rtxn, dbi, &cur));
		MDB_val rkey = { key_len, keybuf };
		MDB_val rdata = {0, NULL};
		int grc = mdb_cursor_get(cur, &rkey, &rdata, MDB_SET_KEY);
		if (grc == MDB_SUCCESS) {
			mdb_size_t dupcount = 0;
			CHECK_CALL(mdb_cursor_count(cur, &dupcount));
			fprintf(stderr, "dfuzz debug: key %.*s txndups=%" PRIuPTR "\n",
			    (int)key_len, keybuf, (uintptr_t)dupcount);
		} else {
			fprintf(stderr, "dfuzz debug: key %.*s lookup rc=%d\n",
			    (int)key_len, keybuf, grc);
		}
		mdb_cursor_close(cur);
		mdb_txn_abort(rtxn);
	}
}

static void
df_do_delete(MDB_env *env, MDB_dbi dbi)
{
	if (df_entry_count == 0)
		return;

	size_t key_idx = (size_t)(df_rng_next() % df_entry_count);
	DFEntry snapshot = df_entries[key_idx]; /* copy header for safe use after model update */
	if (snapshot.dup_count == 0)
		return;
	size_t dup_idx = (size_t)(df_rng_next() % snapshot.dup_count);

	unsigned char value_copy[PF_VALUE_MAX_LEN];
	memcpy(value_copy, snapshot.dups[dup_idx].value, snapshot.dups[dup_idx].len);

	if (pf_trace_ops()) {
		fprintf(stderr, "dfuzz op%zu: delete key=%.*s dup_len=%zu\n",
		    df_op_index, (int)snapshot.key_len, snapshot.key, snapshot.dups[dup_idx].len);
	}

	MDB_txn *txn = NULL;
	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	MDB_val key = { snapshot.key_len, snapshot.key };
	MDB_val data = { snapshot.dups[dup_idx].len, snapshot.dups[dup_idx].value };
	int rc = mdb_del(txn, dbi, &key, &data);
	if (rc != MDB_SUCCESS) {
		fprintf(stderr, "dupsort fuzz: mdb_del failed (%s)\n", mdb_strerror(rc));
		mdb_txn_abort(txn);
		exit(EXIT_FAILURE);
	}
	CHECK_CALL(mdb_txn_commit(txn));
	df_model_delete(snapshot.key, snapshot.key_len, value_copy, snapshot.dups[dup_idx].len);
}

static size_t
pf_make_key(char *out, size_t max_len)
{
	static const char *prefixes[] = {
		"shared-alpha",
		"shared-beta",
		"shared-gamma",
		"prefix-longer-alpha",
		"prefix-longer-beta",
		"prefix-longer-gamma"
	};
	size_t which = (size_t)(pf_rng_next() % (sizeof(prefixes) / sizeof(prefixes[0])));
	const char *prefix = prefixes[which];
	size_t prefix_len = strlen(prefix);
	if (prefix_len + 1 >= max_len)
		prefix_len = max_len > 1 ? max_len - 1 : 0;
	memcpy(out, prefix, prefix_len);
	out[prefix_len++] = '-';

	uint64_t nonce = pf_key_nonce++;
	int written = snprintf(out + prefix_len, max_len - prefix_len, "%016" PRIx64, nonce);
	if (written < 0) {
		fprintf(stderr, "prefix fuzz: snprintf failed while formatting key\n");
		exit(EXIT_FAILURE);
	}
	size_t len = prefix_len + (size_t)written;
	if (len >= max_len)
		len = max_len - 1;

	size_t extra = pf_rng_range(0, 8);
	for (size_t i = 0; i < extra && len + 1 < max_len; ++i)
		out[len++] = (char)('a' + (pf_rng_next() % 26));

	out[len] = '\0';
	return len;
}

static size_t
pf_make_value(unsigned char *buf, size_t max_len)
{
    size_t len = pf_rng_range(4, max_len > 96 ? 96 : max_len);
    for (size_t i = 0; i < len; ++i) {
        unsigned char ch = (unsigned char)('A' + (pf_rng_next() % 26));
        buf[i] = ch;
    }
    return len;
}

static void
pf_verify_model(MDB_env *env, MDB_dbi dbi)
{
	MDB_txn *txn = NULL;
	MDB_cursor *cur = NULL;
	CHECK_CALL(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn));
	MDB_dbi verify_dbi = dbi;
	if (dbi == 0) {
		CHECK_CALL(mdb_dbi_open(txn, NULL, MDB_PREFIX_COMPRESSION, &verify_dbi));
	}
	CHECK_CALL(mdb_cursor_open(txn, verify_dbi, &cur));

	MDB_val key = {0, NULL};
	MDB_val data = {0, NULL};
	size_t idx = 0;
	int rc = mdb_cursor_get(cur, &key, &data, MDB_FIRST);
	while (rc == MDB_SUCCESS) {
		if (idx >= pf_entry_count) {
			fprintf(stderr, "prefix fuzz: database has unexpected extra key\n");
			exit(EXIT_FAILURE);
		}
		PFEntry *e = &pf_entries[idx];
		if (key.mv_size != e->key_len ||
		    memcmp(key.mv_data, e->key, key.mv_size) != 0) {
			fprintf(stderr, "prefix fuzz: key mismatch at index %zu\n", idx);
			exit(EXIT_FAILURE);
		}
		if (data.mv_size != e->val_len ||
		    memcmp(data.mv_data, e->value, data.mv_size) != 0) {
		fprintf(stderr,
		    "prefix fuzz: op=%zu value mismatch at index %zu (entries=%zu)\n",
		    pf_op_index, idx, pf_entry_count);
			fprintf(stderr, "  key: %.*s\n", (int)key.mv_size, (char *)key.mv_data);
			fprintf(stderr, "  expected (%zu bytes):", e->val_len);
			for (size_t i = 0; i < e->val_len; ++i)
				fprintf(stderr, " %02x", e->value[i]);
			fprintf(stderr, "\n  actual (%zu bytes):", data.mv_size);
			for (size_t i = 0; i < data.mv_size; ++i)
				fprintf(stderr, " %02x", ((unsigned char *)data.mv_data)[i]);
			if (data.mv_size && ((unsigned char *)data.mv_data) != NULL) {
				unsigned char *raw = (unsigned char *)data.mv_data;
				fprintf(stderr, "\n  preceding byte: %02x", raw[-1]);
			}
			fprintf(stderr, "\n");
			exit(EXIT_FAILURE);
		}
		idx++;
		rc = mdb_cursor_get(cur, &key, &data, MDB_NEXT);
	}
	if (rc != MDB_NOTFOUND)
		CHECK(rc, "mdb_cursor_get");
	if (idx != pf_entry_count) {
		fprintf(stderr, "prefix fuzz: database returned %zu keys, expected %zu\n",
		    idx, pf_entry_count);
		exit(EXIT_FAILURE);
	}
	mdb_cursor_close(cur);
	mdb_txn_abort(txn);
}

static void
pf_do_insert(MDB_env *env, MDB_dbi dbi)
{
	char keybuf[PF_KEY_MAX_LEN];
	unsigned char valbuf[PF_VALUE_MAX_LEN];
	size_t key_len = pf_make_key(keybuf, sizeof(keybuf));
	size_t val_len = pf_make_value(valbuf, sizeof(valbuf));
	if (pf_trace_ops())
		fprintf(stderr, "op%zu: insert %.*s len=%zu\n",
		    pf_op_index, (int)key_len, keybuf, val_len);

	MDB_val key = { key_len, keybuf };
	MDB_val val = { val_len, valbuf };

	MDB_txn *txn = NULL;
	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	int rc = mdb_put(txn, dbi, &key, &val, 0);
	if (rc != MDB_SUCCESS) {
		fprintf(stderr, "prefix fuzz: insert failed (%s)\n", mdb_strerror(rc));
		mdb_txn_abort(txn);
		exit(EXIT_FAILURE);
	}
	CHECK_CALL(mdb_txn_commit(txn));

	pf_entry_insert(keybuf, key_len, valbuf, val_len);
}

static void
pf_do_delete(MDB_env *env, MDB_dbi dbi)
{
	if (pf_entry_count == 0)
		return;
	size_t idx = (size_t)(pf_rng_next() % pf_entry_count);
	PFEntry *entry = &pf_entries[idx];
	MDB_val key = { entry->key_len, entry->key };
	if (pf_trace_ops())
		fprintf(stderr, "op%zu: delete %.*s\n",
		    pf_op_index, (int)entry->key_len, entry->key);

	MDB_txn *txn = NULL;
	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	int rc = mdb_del(txn, dbi, &key, NULL);
	if (rc != MDB_SUCCESS) {
		fprintf(stderr, "prefix fuzz: delete failed (%s)\n", mdb_strerror(rc));
		mdb_txn_abort(txn);
		exit(EXIT_FAILURE);
	}
	CHECK_CALL(mdb_txn_commit(txn));

	pf_entry_delete_at(idx);
}

static void
test_prefix_fuzz(void)
{
	static const char *dir = "testdb_prefix_fuzz";
	MDB_env *env = create_env(dir);
	MDB_txn *txn = NULL;
	MDB_dbi dbi;

	const char *seed_env = getenv("PF_SEED");
	uint64_t seed = UINT64_C(0x9e3779b97f4a7c15);
	if (seed_env && *seed_env) {
		char *end = NULL;
		uint64_t parsed = strtoull(seed_env, &end, 0);
		if (end && *end == '\0')
			seed = parsed;
	}
	pf_entry_count = 0;
	pf_key_nonce = 0;
	pf_rng_state = seed;

	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &txn));
	CHECK_CALL(mdb_dbi_open(txn, NULL, MDB_PREFIX_COMPRESSION, &dbi));
	CHECK_CALL(mdb_txn_commit(txn));

	const size_t operations = 10000;
	for (size_t op = 0; op < operations; ++op) {
		pf_op_index = op;
		int do_insert = (pf_entry_count < PF_MAX_ENTRIES) &&
		    (pf_entry_count == 0 || (pf_rng_next() & 1));
		if (do_insert)
			pf_do_insert(env, dbi);
		else
			pf_do_delete(env, dbi);
		pf_verify_model(env, dbi);
	}

	/* Final cleanup verification. */
	while (pf_entry_count > 0) {
		pf_do_delete(env, dbi);
		pf_verify_model(env, dbi);
	}

	mdb_env_close(env);
}

static void
test_nested_txn_rollback(void)
{
	static const char *dir = "testdb_prefix_nested";
	MDB_env *env = create_env(dir);
	MDB_txn *parent = NULL;
	MDB_dbi dbi;

	CHECK_CALL(mdb_txn_begin(env, NULL, 0, &parent));
	CHECK_CALL(mdb_dbi_open(parent, NULL, MDB_PREFIX_COMPRESSION, &dbi));
	const char *base_key = "nested-base";
	MDB_val key = {strlen(base_key), (void *)base_key};
	MDB_val val = {strlen(base_key), (void *)base_key};
	CHECK_CALL(mdb_put(parent, dbi, &key, &val, 0));

	MDB_txn *child = NULL;
	CHECK_CALL(mdb_txn_begin(env, parent, 0, &child));
	const char *child_key = "nested-child";
	MDB_val ckey = {strlen(child_key), (void *)child_key};
	MDB_val cval = {strlen(child_key), (void *)child_key};
	CHECK_CALL(mdb_put(child, dbi, &ckey, &cval, 0));
	mdb_txn_abort(child);

	MDB_val lookup = {strlen(child_key), (void *)child_key};
	MDB_val data = {0, NULL};
	int rc = mdb_get(parent, dbi, &lookup, &data);
	if (rc != MDB_NOTFOUND) {
		fprintf(stderr,
		    "nested txn: aborted child write still visible (%s)\n",
		    mdb_strerror(rc));
		exit(EXIT_FAILURE);
	}

	const char *parent_key = "nested-parent";
	MDB_val pkey = {strlen(parent_key), (void *)parent_key};
	MDB_val pval = {strlen(parent_key), (void *)parent_key};
	CHECK_CALL(mdb_put(parent, dbi, &pkey, &pval, 0));
	CHECK_CALL(mdb_txn_commit(parent));

	CHECK_CALL(mdb_txn_begin(env, NULL, MDB_RDONLY, &parent));
	CHECK_CALL(mdb_dbi_open(parent, NULL, 0, &dbi));
	rc = mdb_get(parent, dbi, &lookup, &data);
	if (rc != MDB_NOTFOUND) {
		fprintf(stderr,
		    "nested txn: child insert survived abort (%s)\n",
		    mdb_strerror(rc));
		exit(EXIT_FAILURE);
	}
	CHECK_CALL(mdb_get(parent, dbi, &pkey, &data));
	if (data.mv_size != pkey.mv_size ||
	    memcmp(data.mv_data, pkey.mv_data, data.mv_size) != 0) {
		fprintf(stderr, "nested txn: parent insert mismatch\n");
		exit(EXIT_FAILURE);
	}
	CHECK_CALL(mdb_get(parent, dbi, &key, &data));
	if (data.mv_size != key.mv_size ||
	    memcmp(data.mv_data, key.mv_data, data.mv_size) != 0) {
		fprintf(stderr, "nested txn: base insert mismatch\n");
		exit(EXIT_FAILURE);
	}
	mdb_txn_abort(parent);
	mdb_env_close(env);
}





int
main(void)
{
    test_config_validation();
    test_edge_cases();
    test_range_scans();
    test_threshold_behavior();
    test_mixed_pattern_and_unicode();
    test_cursor_buffer_sharing();
    // test_prefix_dupsort_transitions();
	// test_prefix_dupsort_cursor_walk();
	// test_prefix_dupsort_get_both_range();
	test_prefix_leaf_splits();
	test_prefix_alternating_prefixes();
	test_prefix_update_reinsert();
	test_prefix_dupsort_smoke();
	test_prefix_dupsort_inline_basic_ops();
	test_prefix_dupsort_inline_promote();
	test_prefix_dupsort_trunk_key_shift_no_value_change();
	// test_prefix_dupsort_trunk_swap_inline();
	// test_prefix_dupsort_trunk_swap_promote();
	// test_prefix_dupsort_fuzz();
	test_nested_txn_rollback();
	test_prefix_fuzz();
	printf("mtest_prefix: all tests passed\n");
	return 0;
}
