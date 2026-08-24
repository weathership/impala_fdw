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

**Kudu hot + Iceberg cold.** `kudu_scan` is Kudu-only. `impala_sql` reads Iceberg and UNION views. Expire with `DROP RANGE PARTITION`.

## Status

| Phase | State |
|-------|--------|
| 0 Scaffold | Done |
| 1a HS2 pushdown (proj/IN/ANY/EXPLAIN) | Done |
| **3 kudu_scan (libkudu_client)** | **Lab-ready (K0–K4 + chkpt-02)** — [kudu_scan.md](./kudu_scan.md); **PR-K5** Kerberos plan verified (K5a–K5e) |
| Kerberos / pool / TC | Later (K5 after latency proof) |

Atlas projections + frontier bench (~2.5s/hop HS2 floor) drive **kudu_scan** priority over more HS2 pooling.

## Non-goals (summary)

No HMS, no standalone HiveServer2, no DML in v0, no multi-format storage,
no replacing Atlas/Ranger engines.
