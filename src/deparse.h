/* deparse.h — pushable quals and remote SQL for impala_fdw */
#ifndef IMPALA_FDW_DEPARSE_H
#define IMPALA_FDW_DEPARSE_H

#include "postgres.h"

#include "nodes/nodes.h"
#include "nodes/pathnodes.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "optimizer/optimizer.h"
#include "utils/rel.h"

/*
 * Classify scan_clauses into remote (pushable) vs local.
 * Pushable: OpExpr (= <> < <= > >=) with Var(foreign) + Const,
 *           ScalarArrayOpExpr (IN / = ANY) with Const array,
 *           NullTest, BoolExpr AND of pushable.
 */
extern void impala_classify_conditions(PlannerInfo *root,
									   RelOptInfo *baserel,
									   List *input_conds,
									   List **remote_conds,
									   List **local_conds);

/* True if expr is shippable to Impala (Const-side only in phase 1). */
extern bool impala_is_foreign_expr(PlannerInfo *root,
								   RelOptInfo *baserel,
								   Expr *expr);

/*
 * Build Impala SQL:
 *   SELECT <attrs> FROM `db`.`table` [WHERE ...] [LIMIT n]
 * retrieved_attrs: 1-based attnums for projected columns.
 * remote_conds: RestrictInfo or bare Expr list of pushable quals.
 * limit_count: -1 if none.
 */
extern char *impala_build_select_sql(Relation rel,
									 const char *database,
									 const char *table,
									 List *retrieved_attrs,
									 List *remote_conds,
									 int64 limit_count);

/* Impala-safe identifier: `name` with backticks doubled. */
/*
 * Fold PARAM_EXTERN nodes into the Consts the executor bound. Call from
 * BeginForeignScan before deparsing or building Kudu predicates.
 */
extern List *impala_resolve_extern_params(List *exprs, ParamListInfo pli);

extern void impala_append_ident(StringInfo buf, const char *ident);

/* Deparse a single pushable expr (Const-side only). */
extern bool impala_deparse_expr(StringInfo buf, Expr *expr, Relation rel);

#endif
