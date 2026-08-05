# impala_fdw design notes

## Role in signals-360

```
PostgreSQL (:5455)
  ├─ AGE graph (Atlas entities, atlas_graph)
  ├─ signals_catalog (Impala HMS-free registry)
  └─ impala_fdw ──HS2──► Impala (:21050) ──► Kudu / Iceberg
```

Governance and graph queries stay in Postgres; bulk table data stays in Impala.
FDW is complementary to Atlas (metadata), not a replacement.

## Protocol

- Prefer **HiveServer2** thrift protocol (same as Impala HS2 / JDBC).
- Auth modes: `nosasl` (devenv default), later `kerberos` with
  `impala/tinybox.dev.vista.zndx.org@VISTA.ZNDX.ORG`.

## Implementation phases

1. **Scaffold** (this commit) — extension, options validator, planner hooks, clear error on scan.
2. **HS2 client** — thrift or link Impala/Hive JDBC via a thin C bridge; `SELECT *` pushdown.
3. **Predicate pushdown** — remote quals in `GetForeignPlan`.
4. **IMPORT FOREIGN SCHEMA** — map Impala `SHOW TABLES` / `DESCRIBE`.
5. **Kerberos** — keytab / ticket cache from signals devenv KDC.

## Non-goals

- Writing DML to Impala in v0 (read-only scans first).
- Replacing the Python catalog bridge or Atlas REST tagging path.
