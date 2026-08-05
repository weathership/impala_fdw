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
    auth 'kerberos',         -- kerberos | nosasl (devenv convenience)
    krb_service 'impala',    -- HS2 service principal name component
    krb_host 'tinybox.dev.vista.zndx.org',
    krb_realm 'VISTA.ZNDX.ORG',
    kudu_masters '127.0.0.1:7051',
    default_access 'auto'    -- auto | impala_sql | kudu_scan
  );
```

| Option | Required | Default | Notes |
|--------|----------|---------|--------|
| `host` | no | `127.0.0.1` | Impala HS2 host |
| `port` | no | `21050` | Impala HS2 port |
| `auth` | no | `nosasl` in devenv docs; **design for `kerberos`** | See §11 |
| `krb_service` | if kerberos | `impala` | Builds `impala/krb_host@REALM` |
| `krb_host` | if kerberos | `SIGNALS_KRB_HOST` / `tinybox.dev.vista.zndx.org` | SPN instance |
| `krb_realm` | if kerberos | `VISTA.ZNDX.ORG` | Must match signals KDC |
| `kudu_masters` | no | `127.0.0.1:7051` | Comma-separated; used by `kudu_scan` path |
| `default_access` | no | `auto` | Force path for debugging |

### 5.3 User mapping

```sql
CREATE USER MAPPING FOR CURRENT_USER SERVER impala_kudu
  OPTIONS (
    principal 'signals@VISTA.ZNDX.ORG',   -- optional override
    keytab '/path/to/signals.keytab'      -- or rely on process ticket cache
  );
```

| Option | Notes |
|--------|--------|
| `principal` | Client principal for HS2/Kudu; default = OS/krb5 default principal |
| `keytab` | Optional; prefer `KRB5CCNAME` ticket cache in devenv (`kinit`) |
| *(none)* | Valid for `auth=nosasl` |

Kerberos is a **first-class** mapping concern: both Impala and Kudu expect GSSAPI-capable clients. Implementation may stage `nosasl` first for CI, but options and code paths must not paint into a corner that assumes no Kerberos.

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

### 11.1 Tenancy model

**Single-tenant / governance plane.**  
`impala_fdw` does **not** implement multi-tenant isolation (no per-tenant connection pools, no tenant_id injection, no FDW-level row tenancy). One operators/governance identity plane talks to one Impala/Kudu cluster (signals devenv or a single lab realm).

If multi-tenant productization appears later, it belongs in **Postgres roles + RLS + separate servers/mappings**, or in Impala/Ranger—not ad-hoc logic inside the FDW scan path.

### 11.2 PostgreSQL privileges and RLS

The FDW must **compose with normal Postgres access control**:

| Mechanism | Expected use |
|-----------|----------------|
| `GRANT USAGE ON FOREIGN SERVER` | Who may use the server |
| `GRANT SELECT ON FOREIGN TABLE` | Who may read which foreign tables |
| **RLS on foreign tables** | PG enforces policies for the local user on the foreign table relation around FDW scan (to the extent PG supports policies on foreign tables) |
| **RLS / security-barrier views** over foreign tables | Preferred when policies are complex: wrap foreign tables; expose views to less-privileged roles |
| `SET ROLE` | Session user for PG checks; **remote** Kerberos principal comes from **user mapping**, not from RLS |

**Implementer requirements:**

1. Regression tests: “policy denies → no remote data returned”; document that **RLS expressions are not automatically pushed** as Kudu/Impala predicates unless a future design explicitly does so.
2. Prefer **security barrier views** when exposing governance data to weaker PG roles.
3. Never bypass PG permission checks in C code (standard FDW hooks only).

RLS gates **who in Postgres** may initiate a remote read—it is not a substitute for Impala/Kudu Kerberos.

### 11.3 Kerberos (first-class for Impala and Kudu)

Kerberos is a **first-class construct** in both Impala and Kudu. The FDW treats GSSAPI identity as core design, not a bolt-on.

| Path | Kerberos usage |
|------|----------------|
| `impala_sql` | HS2 client authenticates as client principal to `impala/<krb_host>@REALM` (SASL/GSSAPI) |
| `kudu_scan` | `libkudu_client` with Kerberos (Kudu SPN per cluster, typically `kudu/<krb_host>@REALM`) |
| Devenv | Realm `VISTA.ZNDX.ORG`, host `tinybox.dev.vista.zndx.org`; keytabs under `.devenv/kdc/`; `KRB5_CONFIG` / `KRB5CCNAME` from signals shell |

**Common Postgres + Kerberos patterns we align with:**

| Pattern | Application |
|---------|-------------|
| Ticket cache (`KRB5CCNAME`) | Backend inherits env after `kinit`; mapping may omit keytab |
| Keytab in user mapping | Headless / service role |
| SPN host = FQDN | `krb_host` = `tinybox.dev.vista.zndx.org`, not only `localhost` |
| One realm per lab | `VISTA.ZNDX.ORG`; no cross-realm in v1 |
| `nosasl` | Local CI/devenv only |

**Incremental security rollout (allowed):**

| Stage | Auth | Notes |
|-------|------|--------|
| S0 | `nosasl` | Phase 1–2 CI; no KDC required for FDW unit/integration smoke |
| S1 | Kerberos on **Impala HS2** | Phase 1b—GSSAPI HS2 with signals KDC |
| S2 | Kerberos on **Kudu client** | Phase 3b with `kudu_scan` |
| S3 | Optional Impala Ranger on HS2 path | Orthogonal to FDW |
| S4 | Disable `nosasl` outside dev profiles | Config/docs |

Stages may ship independently; **Kerberos options and code structure exist from the start**.

### 11.4 Summary table

| Concern | Behavior |
|---------|----------|
| Multi-tenancy | **Out of scope** for the FDW |
| PG privileges | USAGE / SELECT; standard FDW |
| PG RLS | Supported via PG; test deny/allow; no silent RLS→remote predicate push |
| Impala auth | Kerberos first-class; nosasl for devenv |
| Kudu auth | Kerberos first-class with client; insecure only if cluster allows (devenv) |
| Ranger | Not inside FDW; optional on Impala for HS2 later |
| Secrets | Keytabs/paths in mapping or env—not in table options |

### 11.5 Language and libraries (binding)

| Component | Technology |
|-----------|------------|
| FDW entry / planner | C (PostgreSQL FDW API / PGXS) |
| Path selector, type map | C |
| `impala_sql` client | **C/C++** HS2 thrift client (no JDBC-in-process) |
| `kudu_scan` client | **C++** `libkudu_client` (from `components/kudu` or devenv prefix) |
| Build | g++/clang as required by libkudu_client; Makefile/PGXS for C++ objects |

Java remains acceptable for **out-of-process** tooling (tests, Impala FE), not for the extension `.so`.

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
| **1** | Type map + **C/C++ HS2** client + simple foreign scan (`nosasl`) | `SELECT` projected cols from one Kudu table via Impala |
| **1b** | Kerberos options + **HS2 GSSAPI** against signals KDC | `auth=kerberos` works with `kinit` / keytab mapping |
| **2** | Pushdown + LIMIT + EXPLAIN path label | Predicate push; EXPLAIN shows impala_sql |
| **3** | **C++ libkudu_client** + `gov.pk_lookup` + `gov.column_sample` | Equivalence tests vs HS2; EXPLAIN shows kudu_scan |
| **3b** | Kerberos on Kudu client path | `kudu_scan` with secured cluster |
| **4** | Full shape catalog + selector GUCs + fallback | All §6 shapes classified; BDD in signals |
| **5** | IMPORT FOREIGN SCHEMA (kudu_only) | Import default DB Kudu tables |
| **6** | RLS regression suite + security-barrier view recipes | Documented patterns; CI policies deny/allow |

Phase 1 may ship with `nosasl` only if 1b is scheduled immediately after; Kerberos is not optional long-term.

## 15. Testing strategy

| Layer | What |
|-------|------|
| Unit | Shape classifier (quals, PK, LIMIT → shape id) |
| Unit | Type mapping round-trip |
| Integration | HS2 scan against signals devenv Impala+Kudu (`nosasl` + Kerberos) |
| Integration | Kudu path vs HS2 path same PK/sample (equivalence) |
| Integration | RLS: policy blocks SELECT → no data; barrier view recipes |
| BDD (signals) | Tagger sampling via FDW; AGE join to foreign table; catalog lifecycle |
| Kerberos | `kinit signals@VISTA.ZNDX.ORG`; HS2 and Kudu paths with real SPNs |

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
    impala_fdw.c            ← FDW entry / planner glue (C)
    path_select.c/h         ← shape id + access method (C)
    exec_impala.cpp/h       ← HS2 executor (C++)
    exec_kudu.cpp/h         ← Kudu scanner (C++ / libkudu_client)
    type_map.c/h
    options.c/h
    krb_util.c/h            ← ticket cache / keytab helpers
  sql/
  Makefile                  ← PGXS + C++ link against kudu_client
```

## 18. Decisions

| # | Decision | Resolution |
|---|----------|------------|
| D1 | HS2 client | **C/C++ thrift HS2 client** in-process—not JDBC/Java |
| D2 | Kudu client | **C++ `libkudu_client`** linked into the extension |
| D3 | Join pushdown to Impala | **Never in v1**—joins in Postgres |
| D4 | Sample algorithm | **First-N** scan (+ optional preds); random later |
| D5 | Multi-tenancy | **Out of scope**—single governance plane; use PG roles/RLS + Kerberos identity, not FDW tenants |
| D6 | Kerberos | **First-class** for Impala and Kudu; `nosasl` devenv/CI only; staged S0–S4 (§11.3) |
| D7 | RLS | **Understand and test** PG RLS + security-barrier views; no silent RLS→Kudu predicate push unless explicitly designed later |

## 19. Success metrics

- Governance PK lookup p99 latency: Kudu path ≪ HS2 path on warm local devenv  
- Equivalence: 0 row mismatches on probe table for §6 Kudu-eligible shapes  
- Partner/demo path: `impala_sql` executes multi-table Kudu SQL without using Kudu path  
- No Iceberg tables importable when `kudu_only=true`  
- Kerberos: HS2 path succeeds with `auth=kerberos` against signals KDC  
- RLS: documented deny/allow cases pass CI  

---

**Document history**

| Ver | Date | Note |
|-----|------|------|
| 0.1 | 2026-08-05 | Initial binding spec: dual path, governance catalog, Kudu-only |
| 0.2 | 2026-08-05 | C/C++ binding; single-tenant; Kerberos first-class; PG RLS patterns |
