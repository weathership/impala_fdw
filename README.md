# impala_fdw

PostgreSQL foreign data wrapper for **Apache Impala** (HS2), scoped to **Kudu
storage only**.

Part of the [signals-360](https://github.com/weathership/signals) stack: query
**Kudu tables** (via Impala HS2) from Postgres so AGE graph / governance SQL can
join the hot data plane without copying bulk data into Postgres.

## Storage scope

| In scope | Out of scope (v1) |
|----------|-------------------|
| Impala tables stored on **Kudu** | Iceberg / Parquet / other Impala table formats |
| Read path through HS2 | Direct Kudu C++ client (optional later) |

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

Requires PostgreSQL development headers (`postgresql_16.dev` / `pg_config`).

```bash
make          # uses pg_config from PATH
make install  # may need DESTDIR / sudo depending on prefix
```

In signals devenv:

```bash
just impala-fdw-build
# or: devenv tasks run impala-fdw:build
```

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
