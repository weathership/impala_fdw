# impala_fdw

PostgreSQL foreign data wrapper for **Apache Impala** (HS2): **Kudu** hot
tier (`kudu_scan` INSERT/scan) and **Iceberg** cold tier (`impala_sql`).

Part of the [signals-360](https://github.com/weathership/signals) stack: query
**Kudu tables** (via Impala HS2) from Postgres so AGE graph / governance SQL can
join the hot data plane without copying bulk data into Postgres.

## Storage scope

| In scope | Out of scope |
|----------|-------------------|
| Kudu hot tables (`kudu_scan` + HS2) | Parquet/ORC/HDFS that is not Iceberg cold |
| Iceberg cold + UNION views (`impala_sql`) | INSERT over `impala_sql` / Iceberg |
| `DROP RANGE PARTITION` via `impala_fdw_exec` | Dropping a hash bucket or a partial range slice |

Impala is the **query frontend** (HS2); Kudu is the only **storage backend** we
need to support for foreign tables.

## Specification

**Binding design:** [docs/SPEC.md](docs/SPEC.md) — dual access paths
(`impala_sql` + `kudu_scan`), governance op catalog, Kudu-only storage,
phased implementation.

## Status

Scaffold / MVP. Builds as a PostgreSQL 16 extension (**C/C++** clients).
Kerberos is first-class for Impala and Kudu; multi-tenancy is out of scope
for the FDW (use Postgres GRANT/RLS). Foreign table scans are stubs until
HS2/Kudu executors land.

| Target | Default (signals devenv) |
|--------|---------------------------|
| Impala HS2 | `localhost:21050` |
| Storage | **Kudu only** (masters `127.0.0.1:7051` in signals stack) |
| Auth | `noSasl` (local); Kerberos `VISTA.ZNDX.ORG` later |
| Postgres | signals devenv PG **5455** |

## Build

Requires PostgreSQL dev headers (`pg_config`), **Apache Thrift**, and **Boost** headers
(thrift 0.22 depends on boost). In signals devenv these are provided as packages.

```bash
# devenv (recommended)
just impala-fdw-build
# or: devenv tasks run impala-fdw:build

# manual
export THRIFT_HOME=... BOOST_HOME=...   # nix paths or system prefixes
make with_llvm=no
make install   # into PG prefix
```

HS2 smoke (Impala must be listening on :21050, auth nosasl):

```bash
make hs2-smoke && ./tools/hs2_smoke 127.0.0.1 21050
```

Phase 1 uses **NOSASL** thrift to de-risk deps; product path is still Kerberos (1b).


## Usage (planned)

```sql
CREATE EXTENSION impala_fdw;

CREATE SERVER impala_kudu_srv
  FOREIGN DATA WRAPPER impala_fdw
  OPTIONS (host '127.0.0.1', port '21050', auth 'nosasl');

CREATE USER MAPPING FOR CURRENT_USER SERVER impala_kudu_srv;

-- Foreign tables must map to Impala tables backed by Kudu storage
CREATE FOREIGN TABLE my_kudu_table (
  id bigint,
  name text
) SERVER impala_kudu_srv
  OPTIONS (database 'default', table 'my_kudu_table');
```

## Layout

```
impala_fdw.control   extension control
sql/                 extension scripts
src/                 FDW C sources
docs/design.md       architecture notes
Makefile             PGXS build
```

## License

Apache License 2.0 — see [LICENSE](LICENSE).
