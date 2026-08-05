# impala_fdw specification

**Status:** Draft v0.2 (binding intent for implementation)  
**Storage scope:** Kudu-backed Impala tables only  
**Implementation language:** **C/C++** (PostgreSQL FDW + libkudu_client + HS2 thrift client)—no Java runtime in the extension process  
**Tenancy:** **Single-tenant / governance-plane** (no multi-tenant isolation inside the FDW)  
**Consumers:** signals-360 Postgres (AGE / Atlas graph co-location, Ranger policy helpers, sigint sampling)

## 1. Purpose

`impala_fdw` is the PostgreSQL foreign-data interface to the **Impala + Kudu** data plane.

- **Public contract:** SQL-shaped access to Kudu tables for Postgres clients (governance, AGE joins, analytics).
- **Default remote engine:** **Apache Impala** over HiveServer2 (HS2)—the co-developed SQL engine on Kudu, including planner and in-memory execution benefits for SQL-shaped work.
- **Specialized paths:** For the **closed Atlas + Ranger + AGE + sigint** query algebra, the extension may execute **direct Kudu client** scans when that is faster and semantically equivalent—without exposing a second FDW product.
- **Native stack:** Executors and clients are **C/C++**, matching Impala/Kudu’s native libraries and PostgreSQL’s FDW ABI—avoid JVM bridges in-process.

Postgres remains the primary front end for graph/governance SQL. Kudu remains the only storage backend in scope. Impala is the default SQL adapter and the escape hatch for ad-hoc / multi-table SQL.

```
PostgreSQL (:5455)
  ├─ AGE / Atlas graph (atlas_graph)
  ├─ signals_catalog (HMS-free registry)
  └─ impala_fdw
        ├─ path: impala_sql  ──HS2──► Impala (:21050) ──► Kudu (:7051)
        └─ path: kudu_scan   ──────────Kudu client──────► Kudu (:7051)
```

## 2. Goals

| ID | Goal |
|----|------|
| G1 | Read Kudu table data from Postgres via foreign tables / foreign scans |
| G2 | Prefer Impala HS2 for general SQL-shaped queries (joins, complex predicates, partner demos) |
| G3 | Recognize governance/sigint scan shapes and run them on a **Kudu fast path** when safe |
| G4 | Single extension, single type-mapping layer, dual executors |
| G5 | Align with signals devenv: PG 16 :5455, Impala HS2 :21050, Kudu masters :7051, realm `VISTA.ZNDX.ORG` |
| G6 | Kudu storage only—no Iceberg / HDFS / other Impala formats in v1 |
| G7 | **C/C++ only** for extension code and remote clients (PGXS, libkudu_client, HS2 thrift/C++) |
| G8 | **Kerberos as a first-class identity path** for Impala HS2 and Kudu (aligned with signals KDC), even if rollout is phased |
| G9 | Interoperate cleanly with **PostgreSQL RLS** and standard PG privilege patterns (USAGE/SELECT on foreign tables)—without multi-tenant FDW logic |

## 3. Non-goals

| ID | Non-goal |
|----|----------|
| N1 | Full general-purpose BI FDW (arbitrary Impala SQL dialects beyond pushed scans) |
| N2 | Iceberg or multi-format Impala tables |
| N3 | DML (INSERT/UPDATE/DELETE) in v0–v1—read-only scans first |
| N4 | Replacing Atlas REST, AGE schema, or Ranger policy evaluation engines |
| N5 | Running Hive Metastore or standalone HiveServer2 |
| N6 | Beeswax client support |
| N7 | Implementing a full authz engine *inside* the FDW (no second Ranger) |
| N8 | **Multi-tenancy** inside impala_fdw (no per-tenant connection isolation, no tenant row filters in the FDW) |
| N9 | Java/JDBC-in-process client for HS2 or Kudu |

## 4. Design principles

1. **Impala-first default** — Unrecognized plans use HS2. Never silently drop into a wrong Kudu path.
2. **Closed-world optimization** — Governance ops are enumerated (see §6). Only those promote to `kudu_scan`.
3. **Semantic equivalence** — For any promoted shape, Kudu path results must match Impala path results under the test suite (same projection, filter, row set modulo ordering unless ORDER BY pushed).
4. **One type system** — Impala/Kudu/Postgres type mapping lives in one module used by both executors.
5. **Explainability** — `EXPLAIN` / `EXPLAIN (VERBOSE)` must show which access method was chosen and why (or a shape id).
6. **Fail closed on storage** — If a foreign table is not Kudu-backed, create/import or first scan errors clearly.
7. **C/C++ native** — Prefer linking official/native clients; no JVM in the backend process.
8. **Kerberos-first identity model** — Design options, user mapping, and connection setup for Kerberos from day one; `nosasl` is a devenv convenience, not the long-term default story.
9. **Postgres-native access control** — Rely on PG roles, GRANT, and RLS on foreign tables / wrapping views; FDW does not invent tenants.

## 5. Objects and options

### 5.1 Extension

```sql
CREATE EXTENSION impala_fdw;  -- version 0.1.0+
```

### 5.2 Server

```sql
CREATE SERVER impala_kudu
  FOREIGN DATA WRAPPER impala_fdw
  OPTIONS (
    host '127.0.0.1',
    port '21050',
    auth 'nosasl',           -- nosasl | kerberos (later)
    kudu_masters '127.0.0.1:7051',
    default_access 'auto'    -- auto | impala_sql | kudu_scan
  );
```

| Option | Required | Default | Notes |
|--------|----------|---------|--------|
| `host` | no | `127.0.0.1` | Impala HS2 host |
| `port` | no | `21050` | Impala HS2 port |
| `auth` | no | `nosasl` | Kerberos later: principal/keytab via env or mapping |
| `kudu_masters` | no | `127.0.0.1:7051` | Comma-separated; used by `kudu_scan` path |
| `default_access` | no | `auto` | Force path for debugging |

### 5.3 User mapping

```sql
CREATE USER MAPPING FOR CURRENT_USER SERVER impala_kudu
  OPTIONS (
    /* reserved: kerberos principal, password if ever needed */
  );
```

v1: no options for `nosasl`.

### 5.4 Foreign table

```sql
CREATE FOREIGN TABLE f_events (
  id bigint OPTIONS (kudu_column 'id'),
  name text OPTIONS (kudu_column 'name'),
  ts bigint OPTIONS (kudu_column 'ts')
) SERVER impala_kudu
  OPTIONS (
    database 'default',
    table 'events',
    kudu_table '',           -- optional override; default = Impala table → Kudu name
    access 'auto'              -- auto | impala_sql | kudu_scan
  );
```

| Option | Required | Default | Notes |
|--------|----------|---------|--------|
| `database` | yes* | `default` | Impala database |
| `table` | yes* | — | Impala table name (must be `STORED AS KUDU`) |
| `kudu_table` | no | resolved from Impala/Kudu metadata | Explicit Kudu table name if different |
| `access` | no | `auto` | Per-table override |

\* Or supplied via `IMPORT FOREIGN SCHEMA`.

Column option `kudu_column` maps PG attribute → Kudu/Impala column name when they differ.

### 5.5 IMPORT FOREIGN SCHEMA (phase ≥ 2)

```sql
IMPORT FOREIGN SCHEMA default
  FROM SERVER impala_kudu
  INTO public
  OPTIONS (kudu_only 'true');
```

Only import tables verified as Kudu storage. Skip or error on others when `kudu_only=true`.

## 6. Governance operation catalog (closed query space)

These are the **only** shapes eligible for automatic `kudu_scan` promotion when `access=auto`.  
Everything else → `impala_sql`.

Shape IDs are stable strings for EXPLAIN, metrics, and tests.

### 6.1 Ops

| Shape ID | Intent | Typical caller | Input | Output | Preferred path |
|----------|--------|----------------|-------|--------|----------------|
| `gov.pk_lookup` | Point read by full primary key | AGE enrich, entity hydrate | Equality quals on all PK cols | 0..1 row, projected cols | **kudu_scan** |
| `gov.unique_lookup` | Point read by unique key / business key | Catalog bridge helpers | Equality on unique column set | 0..1 row | **kudu_scan** |
| `gov.filtered_scan` | Selective scan, no join | Policy / validation helpers | Pushable AND of simple preds | Stream rows | **kudu_scan** if selectivity heuristic ok; else impala_sql |
| `gov.column_sample` | Bounded sample for classification | sigint `ImpalaSampler` / features | table + column list + `LIMIT n` / sample budget | ≤ n rows, projected cols | **kudu_scan** |
| `gov.projection_only` | Full or large scan but few columns | Feature extract without filter | tlist ≪ width, weak/no filter | Stream | **impala_sql** default; optional kudu if LIMIT |
| `gov.schema` | Column names/types | IMPORT, DESCRIBE | database + table | Catalog row(s) | **metadata API** (Kudu/Impala describe)—not a data scan |
| `sql.general` | Anything else: joins, agg, expr, multi-table, unrecognized | BI, partner SQL, ad-hoc | Arbitrary plan | — | **impala_sql** |

### 6.2 Recognition rules (normative sketch)

A scan is `gov.pk_lookup` when all of:

- Single foreign table baserel (no join in the foreign path)
- Quals are equality (`=`) on a complete primary-key column set (PK from Kudu schema)
- No volatile functions in tlist/quals
- No aggregates in the foreign path

A scan is `gov.column_sample` when all of:

- Single foreign table
- `LIMIT` present and `LIMIT ≤ sample_max` (server GUC, default 1000)
- No aggregates; quals empty or only pushable simple preds
- tlist is a subset of columns (projection)

A scan is `gov.filtered_scan` when:

- Single foreign table
- Quals ⊆ pushable set (eq, ineq, IS NULL, simple AND)—see §8
- Estimated selectivity below `kudu_scan_sel_threshold` (GUC) **or** explicit `access=kudu_scan`

Otherwise → `sql.general` → Impala.

### 6.3 Explicit force

```sql
-- Force Impala even for PK lookups (compare plans / cache behavior)
OPTIONS (access 'impala_sql')

-- Force Kudu (error if shape not implementable on Kudu path)
OPTIONS (access 'kudu_scan')
```

Server `default_access` applies when table option is `auto`.

## 7. Access methods

### 7.1 `impala_sql`

1. Build remote SQL from foreign scan: projection + pushed quals + LIMIT if safe.
2. Open HS2 session (pool); execute; stream rows into `TupleTableSlot`.
3. Auth: `nosasl` or Kerberos (`impala/tinybox.dev.vista.zndx.org@VISTA.ZNDX.ORG`).
4. Reject or refuse import if table is not Kudu-backed (check via Impala metadata / TBLPROPERTIES).

**Strengths:** joins (when planned as remote—or multi-scan + local join in PG), complex SQL, Impala–Kudu integration and execution caching for SQL-shaped workloads.

### 7.2 `kudu_scan`

1. Resolve Impala table → Kudu table name + schema (cache).
2. Build Kudu scanner: projected columns, predicates from pushed quals, primary-key encoding for point get when applicable.
3. Stream results; map types via shared mapper.
4. No Impala process required for this path (only Kudu masters/tservers)—optional operational mode for governance-only deploys.

**Strengths:** low latency point/sample; avoids HS2 session overhead for closed ops.

### 7.3 Path selector

```
Input: RelOptInfo / ForeignScan (tlist, quals, limit, table options)
  → classify shape id (§6)
  → if access forced: use force (error if impossible)
  → if auto: map shape → method (§6.1)
  → record shape id + method in fdw_private for EXPLAIN and metrics
```

## 8. Predicate and projection pushdown

### 8.1 Pushable (both paths where supported)

- Attribute op Const for op in `=`, `<>`, `<`, `<=`, `>`, `>=`
- `IS NULL` / `IS NOT NULL`
- AND of pushable clauses
- `IN (const list)` (optional phase 2)
- Column projection (tlist subset)

### 8.2 Not pushed (evaluate in Postgres or require impala_sql)

- OR across different columns (unless path implements)
- Stable/volatile functions, casts that change semantics
- Subqueries, joins inside remote plan (v1: joins local in PG or remote only via full impala_sql rewrite phase later)
- LIKE/regex (phase 2+ if Kudu/Impala support mapped cleanly)

### 8.3 LIMIT

- Pushed on `kudu_scan` for `gov.column_sample` and optional filtered scans
- Pushed on `impala_sql` when no local quals remain after pushdown

## 9. Type mapping

Shared module; both executors use the same rules.

| Kudu / Impala | PostgreSQL |
|---------------|------------|
| BOOL | boolean |
| INT8, INT16, INT32 | integer / smallint as appropriate |
| INT64 | bigint |
| FLOAT, DOUBLE | real / double precision |
| STRING, VARCHAR | text |
| BINARY | bytea |
| UNIXTIME_MICROS | timestamp (UTC) |
| DECIMAL | numeric |
| DATE | date |

Unmapped types: error at CREATE FOREIGN TABLE or first scan with clear message.

## 10. GUCs (extension parameters)

| GUC | Default | Meaning |
|-----|---------|---------|
| `impala_fdw.sample_max` | 1000 | Max LIMIT for `gov.column_sample` Kudu path |
| `impala_fdw.kudu_scan_sel_threshold` | 0.1 | Selectivity below which filtered_scan prefers Kudu |
| `impala_fdw.default_access` | auto | Global default if server option omitted |
| `impala_fdw.log_path_choice` | off | LOG chosen shape id + method |
| `impala_fdw.hs2_pool_size` | 4 | HS2 session pool |
| `impala_fdw.require_kudu_storage` | on | Fail if table not Kudu-backed |

## 11. Security and identity

| Concern | Behavior |
|---------|----------|
| PG privileges | Standard FDW: USAGE on server/table |
| Impala/Kudu auth | Server + user mapping; Kerberos later with signals KDC |
| Ranger | Not enforced inside FDW; production query paths that need Ranger should use Impala HS2 with plugin, or accept that FDW reads are trusted-operator/governance plane |
| Credentials | No secrets in table options; keytabs via env / mapping |

Document clearly: **governance-plane FDW access is a privileged path** in devenv; production hardening may require Impala-only + Ranger for untrusted tenants.

## 12. Observability

- EXPLAIN shows: `AccessMethod: kudu_scan|impala_sql`, `ShapeId: gov.pk_lookup|…`
- Optional metrics counters: scans_by_shape, rows, errors, fallbacks auto→impala after kudu error
- On Kudu path failure when shape was promoted: **fallback to impala_sql** once (if `access=auto`) and log; if `access=kudu_scan`, surface error

## 13. Failure modes

| Condition | Result |
|-----------|--------|
| Table not Kudu storage | ERROR at import or Open |
| HS2 down, path impala_sql | ERROR connection |
| Kudu down, path kudu_scan, access=auto | Fallback HS2 if Impala up; else ERROR |
| Kudu down, access=kudu_scan | ERROR |
| Type mismatch | ERROR |
| Forced kudu_scan on sql.general shape | ERROR not supported |

## 14. Implementation phases

| Phase | Deliverable | Exit criteria |
|-------|-------------|---------------|
| **0** | Scaffold (current) | Extension loads; validator; clear error on scan |
| **1** | Type map + HS2 client + simple foreign scan | `SELECT` projected cols from one Kudu table via Impala |
| **2** | Pushdown + LIMIT + EXPLAIN path label | Predicate push; EXPLAIN shows impala_sql |
| **3** | Kudu client + `gov.pk_lookup` + `gov.column_sample` | Equivalence tests vs HS2; EXPLAIN shows kudu_scan |
| **4** | Full shape catalog + selector GUCs + fallback | All §6 shapes classified; BDD in signals |
| **5** | IMPORT FOREIGN SCHEMA (kudu_only) | Import default DB Kudu tables |
| **6** | Kerberos | HS2 + optional Kudu ticket path with `VISTA.ZNDX.ORG` |

Phase 1 may ship before any Kudu client code; phase 3 is where governance optimization lands.

## 15. Testing strategy

| Layer | What |
|-------|------|
| Unit | Shape classifier (quals, PK, LIMIT → shape id) |
| Unit | Type mapping round-trip |
| Integration | HS2 scan against signals devenv Impala+Kudu |
| Integration | Kudu path vs HS2 path same PK/sample (equivalence) |
| BDD (signals) | Tagger sampling via FDW; AGE join to foreign table; catalog lifecycle |

Fixture: small Kudu table `gov_fdw_probe(id INT PK, name STRING, ts BIGINT)` loaded via Impala DDL.

## 16. Signals stack integration

| Component | Interaction |
|-----------|-------------|
| `signals_catalog` | Optional source of table list / FQ names for IMPORT |
| Atlas / AGE | Graph stays in PG; FDW supplies row/sample data for enrichment |
| sigint sampler | Prefer `gov.column_sample` over ad-hoc HS2 in Python long-term |
| Ranger | Policy metadata in Ranger/Atlas; value checks may use FDW reads |
| devenv | `impala-fdw:build`; PG 16; HS2 :21050; Kudu :7051 |

## 17. Directory layout (target)

```
components/impala_fdw/
  docs/SPEC.md              ← this document
  docs/design.md            ← short pointer + changelog of intent
  src/
    impala_fdw.c            ← FDW entry / planner glue
    path_select.c/h         ← shape id + access method
    exec_impala.c/h         ← HS2 executor
    exec_kudu.c/h           ← Kudu scanner executor
    type_map.c/h
    options.c/h
  sql/
  Makefile
```

## 18. Open decisions (resolve in phase 1–3)

| # | Decision | Options | Lean |
|---|----------|---------|------|
| D1 | HS2 client implementation | C thrift / linked JDBC bridge / subprocess | C thrift or Impala-compatible HS2 lib if available |
| D2 | Kudu client | Official C++ `libkudu_client` vs Java bridge | C++ client in FDW .so (matches Kudu subtree) |
| D3 | Join pushdown to Impala | Never in v1 / rewrite multi-table foreign joins | Never in v1—PG local join |
| D4 | Sample algorithm | First-N scan vs random | First-N with optional predicate; random later |
| D5 | Production multi-tenant | FDW trusted only / must use Impala+Ranger | Document trusted governance plane for devenv |

## 19. Success metrics

- Governance PK lookup p99 latency: Kudu path ≪ HS2 path on warm local devenv  
- Equivalence: 0 row mismatches on probe table for §6 Kudu-eligible shapes  
- Partner/demo path: `impala_sql` executes multi-table Kudu SQL without using Kudu path  
- No Iceberg tables importable when `kudu_only=true`

---

**Document history**

| Ver | Date | Note |
|-----|------|------|
| 0.1 | 2026-08-05 | Initial binding spec: dual path, governance catalog, Kudu-only |
