# Repository Guidelines

## Project Structure & Module Organization
- `libraries/liblmdb` holds the core C sources (`mdb.c`, `midl.c`), headers, CLI utilities, and man pages; builds emit `liblmdb.a` and `.so` artifacts here.
- `libraries/liblmdb/mtest*.c` are functional smoke tests and usage examples—keep them compiling with new features.
- Temporary databases (`testdb`) land under `libraries/liblmdb`; wipe them with `make -C libraries/liblmdb clean`.

## Build, Test, and Development Commands
- `make -C libraries/liblmdb` compiles the static/shared libraries and helper binaries with GCC and pthread support.
- `make -C libraries/liblmdb test` rebuilds, seeds a disposable `testdb`, runs `mtest`, then inspects it with `mdb_stat`.
- `make -C libraries/liblmdb clean` removes objects, binaries, and temp data—run before packaging patches.
- `make -C libraries/liblmdb coverage` produces gcov binaries and reports via `gcov`.

## Coding Style & Naming Conventions
- Stick to ANSI C; use hard tabs and mirror the alignment patterns in `mdb.c` and `midl.c`.
- Keep macros uppercase snake case (`MDB_*`), file-scope functions lowercase snake case, and struct/enum tags on existing prefixes (`MDB`, `MIDL`).
- Target ~80 columns, wrap multi-line comments with `/* ... */`, and preserve Doxygen tags on public APIs.
- Treat compiler warnings as failures—ensure a clean `make -C libraries/liblmdb` build before sending patches.

## Testing Guidelines
- Extend the `mtest*` programs when adding features or regression fixes; reuse the numbered naming scheme and short-circuit on errors.
- Integration tests rely on the generated `testdb`; reset state with `make -C libraries/liblmdb clean` after failures.
- Use `make -C libraries/liblmdb coverage` to gauge hot-path coverage (`xmdb.c.gcov`, `xmidl.c.gcov`) when adjusting cursor or transaction code.

## Commit & Pull Request Guidelines
- Use `DTVL lmdb: descriptive change` prefix in commit message (e.g., `DTLV lmdb: add mdb_range_count functionality`).
- Commit related code, docs (`lmdb.h`, man pages), and tests together so reviewers can validate behavior quickly.
- PR descriptions should summarize intent, list validation steps (`make`, `make test`, coverage if relevant), and call out ABI or format impacts.
- Link upstream tickets and include before/after CLI output when modifying tools such as `mdb_dump`, `mdb_load`, or `mdb_stat`.

## Configuration Tips
- Platform tweaks belong in `CPPFLAGS`; document new macros near related `#ifdef` blocks in `mdb.c`.
- Keep defaults portable and guard OS-specific logic with existing feature macros (`MDB_USE_POSIX_MUTEX`, `MDB_USE_ROBUST`, etc.).
