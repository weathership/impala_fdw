# impala_fdw design notes

## Role in signals-360

```
PostgreSQL (:5455)
  ├─ AGE graph (Atlas entities, atlas_graph)
  ├─ signals_catalog (Impala HMS-free registry)
  └─ impala_fdw ──HS2──► Impala (:21050) ──► Kudu only (:7051)
```

Governance and graph queries stay in Postgres; **hot-tier bulk data stays in
Kudu**, addressed only through Impala HS2. FDW is complementary to Atlas
(metadata), not a replacement.

## Storage scope (binding)

**v1 target: Kudu-backed Impala tables only.**

- Foreign tables correspond 1:1 to Impala tables whose storage handler is Kudu.
- No requirement to support Iceberg, HDFS/Parquet, or other Impala table types.
- Optional later: Kudu client bypass for simple PK lookups; not the default path.

This keeps the FDW contract small: HS2 SQL against tables that already exist on
the signals Kudu cluster.

## Protocol

- **HiveServer2** thrift (same as Impala HS2 / JDBC) — not a native Kudu wire
  protocol for the first cut.
- Auth modes: `nosasl` (devenv default), later `kerberos` with
  `impala/tinybox.dev.vista.zndx.org@VISTA.ZNDX.ORG`.

## Implementation phases

1. **Scaffold** — extension, options validator, planner hooks, clear error on scan.
2. **HS2 client** — thrift or thin bridge; `SELECT *` from Kudu-backed tables.
3. **Predicate pushdown** — remote quals in `GetForeignPlan` (push what Kudu/Impala can use).
4. **IMPORT FOREIGN SCHEMA** — map Impala `SHOW TABLES` / `DESCRIBE`, filter or
   document Kudu-only tables.
5. **Kerberos** — keytab / ticket cache from signals devenv KDC.

## Non-goals

- Iceberg / multi-format Impala foreign tables.
- Writing DML to Impala/Kudu in v0 (read-only scans first).
- Replacing the Python catalog bridge or Atlas REST tagging path.
- Standing in for Ranger (authorization stays on Impala/Ranger, not the FDW).
