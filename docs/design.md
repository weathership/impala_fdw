# impala_fdw design notes

**Canonical specification:** [SPEC.md](./SPEC.md) (binding intent for implementation).

This file is a short orientation; do not diverge from SPEC.md.

## One-liner

PostgreSQL FDW for **Kudu-only** tables: **Impala HS2 by default** (SQL-shaped,
Cloudera-aligned), with **direct Kudu scans** for the closed Atlas / Ranger /
AGE / sigint governance algebra. Implemented in **C/C++** (`libkudu_client` +
HS2 thrift). **Kerberos** is first-class end-to-end: Postgres **GSSAPI** login
and Impala/Kudu outbound use the **same principal** when possible. **No
multi-tenancy** in the FDW—use Postgres GRANT/RLS for local access control.

## Topology

```
PostgreSQL (:5455)
  ├─ AGE / Atlas graph
  ├─ signals_catalog
  └─ impala_fdw
        ├─ impala_sql ──HS2──► Impala ──► Kudu
        └─ kudu_scan  ───────► Kudu
```

## Storage

**Kudu only.** No Iceberg / HDFS / other Impala formats in v1.

## Status

| Phase | State |
|-------|--------|
| 0 Scaffold | Done (extension loads; scan errors clearly) |
| 1+ HS2 / Kudu paths | Per SPEC.md §14 |

## Non-goals (summary)

No HMS, no standalone HiveServer2, no DML in v0, no multi-format storage,
no replacing Atlas/Ranger engines.
