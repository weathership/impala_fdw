# impala_fdw

PostgreSQL foreign data wrapper for **Apache Impala** (HS2).

Part of the [signals-360](https://github.com/weathership/signals) stack: query Impala /
Kudu tables from Postgres so AGE graph / governance SQL can join the data plane
without copying bulk data into Postgres.

## Status

Scaffold / MVP. Builds as a PostgreSQL 16 extension; foreign table scans are
stubs until the HS2 client path lands.

| Target | Default (signals devenv) |
|--------|---------------------------|
| Impala HS2 | `localhost:21050` |
| Auth | `noSasl` (local); Kerberos `VISTA.ZNDX.ORG` later |
| Postgres | signals devenv PG **5455** |

## Build

Requires PostgreSQL development headers (`postgresql_16.dev` / `pg_config`).

```bash
make          # uses pg_config from PATH
make install  # may need DESTDIR / sudo depending on prefix
```

In signals devenv (once submodule is present):

```bash
devenv tasks run impala-fdw:build   # when wired
```

## Usage (planned)

```sql
CREATE EXTENSION impala_fdw;

CREATE SERVER impala_srv
  FOREIGN DATA WRAPPER impala_fdw
  OPTIONS (host '127.0.0.1', port '21050', auth 'nosasl');

CREATE USER MAPPING FOR CURRENT_USER SERVER impala_srv;

CREATE FOREIGN TABLE my_table (
  id bigint,
  name text
) SERVER impala_srv
  OPTIONS (database 'default', table 'my_table');
```

## Layout

```
impala_fdw.control   extension control
sql/                 extension scripts
src/                 FDW C sources
Makefile             PGXS build
```

## License

Apache License 2.0 — see [LICENSE](LICENSE).
