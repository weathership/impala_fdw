/*-------------------------------------------------------------------------
 *
 * impala_fdw.c
 *    Foreign Data Wrapper for Apache Impala (HS2) → Kudu storage only.
 *
 * Phase 1+: HS2 scans with projection, PK/eq predicates, IN / = ANY pushdown
 * (Atlas projection freeze). PR-K1: kudu_scan projection/LIMIT path (no preds).
 *
 * Copyright 2026 Weathership / signals-360 contributors
 * Licensed under the Apache License, Version 2.0
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/parallel.h"
#include "access/reloptions.h"
#include "access/table.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_type.h"
#include "catalog/pg_user_mapping.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "executor/executor.h"
#include "executor/tuptable.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#include "deparse.h"
#include "exec_impala.h"
#include "krb_util.h"
#include "path_select.h"

#ifdef IMPALA_FDW_WITH_KUDU
#include "exec_kudu.h"
#include "kudu_pred.h"
#endif

PG_MODULE_MAGIC;

/* GUCs (PR-K3) */
static bool impala_fdw_enable_kudu_scan = true;
static bool impala_fdw_log_path_choice = false;
static int	impala_fdw_sample_max = 1000;

/* Server options */
#define OPTION_HOST "host"
#define OPTION_PORT "port"
#define OPTION_AUTH "auth"
#define OPTION_KRB_REALM "krb_realm"
#define OPTION_DEFAULT_ACCESS "default_access"
#define OPTION_KUDU_MASTERS "kudu_masters"
/* Table options */
#define OPTION_DATABASE "database"
#define OPTION_TABLE "table"
#define OPTION_ACCESS "access"
#define OPTION_KUDU_TABLE "kudu_table"
#define OPTION_KUDU_COLUMN "kudu_column"

/* fdw_private indexes */
enum FdwPrivateIndex
{
	FdwPrivateDatabase = 0,
	FdwPrivateTable,
	FdwPrivateAccess,
	FdwPrivateShapeId,
	FdwPrivateMethod,
	FdwPrivateLimit,		/* int64 as string, or empty */
	FdwPrivateRetrievedAttrs,
	FdwPrivateRemoteExprs
};

typedef struct ImpalaFdwScanState
{
	char	   *host;
	int			port;
	char	   *database;
	char	   *table;
	char	   *auth;
	char	   *krb_realm;
	char	   *access;
	char	   *principal;
	const char *shape_id;
	ImpalaFdwAccessMethod method;
	ImpalaHs2Session *hs2;
	ImpalaHs2Result *result;
	List	   *retrieved_attrs;
	char	   *sql;
	/* kudu_scan (PR-K1+) */
	char	   *kudu_masters;
	char	   *kudu_table;		/* resolved Impala/Kudu name */
	int64		limit_count;
	bool		fell_back_to_hs2;
	bool		use_kudu;
	bool		has_local_quals;		/* residual scan quals present */
#ifdef IMPALA_FDW_WITH_KUDU
	ImpalaKuduScan *kudu;
#endif
	MemoryContextCallback scan_cb;
	bool		scan_cb_registered;
	MemoryContext scan_mctx;	/* context that owns festate (for reset cb) */
} ImpalaFdwScanState;

/*
 * Abort-path cleanup (F3/N6): EndForeignScan may not run on ereport.
 * Close Kudu scanner and HS2 session/result.
 */
static void
impala_fdw_scan_mcxt_callback(void *arg)
{
	ImpalaFdwScanState *festate = (ImpalaFdwScanState *) arg;

	if (festate == NULL)
		return;
#ifdef IMPALA_FDW_WITH_KUDU
	if (festate->kudu)
	{
		impala_kudu_scan_close(festate->kudu);
		festate->kudu = NULL;
		festate->use_kudu = false;
	}
#endif
	if (festate->result)
	{
		impala_hs2_close_result(festate->result);
		festate->result = NULL;
	}
	if (festate->hs2)
	{
		impala_hs2_close(festate->hs2);
		festate->hs2 = NULL;
	}
}

void		_PG_init(void);

PG_FUNCTION_INFO_V1(impala_fdw_handler);
PG_FUNCTION_INFO_V1(impala_fdw_validator);

static void impalaGetForeignRelSize(PlannerInfo *root,
									RelOptInfo *baserel,
									Oid foreigntableid);
static void impalaGetForeignPaths(PlannerInfo *root,
								  RelOptInfo *baserel,
								  Oid foreigntableid);
static ForeignScan *impalaGetForeignPlan(PlannerInfo *root,
										 RelOptInfo *baserel,
										 Oid foreigntableid,
										 ForeignPath *best_path,
										 List *tlist,
										 List *scan_clauses,
										 Plan *outer_plan);
static void impalaBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *impalaIterateForeignScan(ForeignScanState *node);
static void impalaReScanForeignScan(ForeignScanState *node);
static void impalaEndForeignScan(ForeignScanState *node);
static void impalaExplainForeignScan(ForeignScanState *node,
									 ExplainState *es);

static char *get_option_value(List *options, const char *optname);
static char *resolve_kudu_column_name(Oid reloid, int attno, Relation rel);
static char *resolve_kudu_table_name(const char *kudu_table_opt,
									 const char *database,
									 const char *table);
/* used early by attrs/path helpers — defined with other option helpers below */
static void impala_begin_hs2_scan(ImpalaFdwScanState *festate,
								  Relation rel,
								  List *remote_exprs,
								  int64 limit_count);
#ifdef IMPALA_FDW_WITH_KUDU
static bool impala_begin_kudu_scan(ImpalaFdwScanState *festate,
								   Relation rel,
								   List *retrieved_attrs,
								   List *remote_exprs,
								   int64 limit_count,
								   char **err_out);
#endif

/* Backend interrupt probe for HS2 wait loop (N3 function-pointer hook) */
static bool
impala_fdw_interrupt_check(void)
{
	return InterruptPending;
}

void
_PG_init(void)
{
	ImpalaFdwSetInterruptCheck(impala_fdw_interrupt_check);

	DefineCustomBoolVariable("impala_fdw.enable_kudu_scan",
							 "Allow kudu_scan access method when libkudu is linked.",
							 NULL,
							 &impala_fdw_enable_kudu_scan,
							 true,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);
	DefineCustomBoolVariable("impala_fdw.log_path_choice",
							 "LOG chosen shape id and access method at BeginForeignScan.",
							 NULL,
							 &impala_fdw_log_path_choice,
							 false,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);
	DefineCustomIntVariable("impala_fdw.sample_max",
							"Max LIMIT for gov.column_sample kudu_scan promotion.",
							NULL,
							&impala_fdw_sample_max,
							1000,
							1, INT_MAX,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);

	/* Reject typo'd impala_fdw.* GUCs (PG15+) */
	MarkGUCPrefixReserved("impala_fdw");
}

Datum
impala_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *routine = makeNode(FdwRoutine);

	routine->GetForeignRelSize = impalaGetForeignRelSize;
	routine->GetForeignPaths = impalaGetForeignPaths;
	routine->GetForeignPlan = impalaGetForeignPlan;
	routine->BeginForeignScan = impalaBeginForeignScan;
	routine->IterateForeignScan = impalaIterateForeignScan;
	routine->ReScanForeignScan = impalaReScanForeignScan;
	routine->EndForeignScan = impalaEndForeignScan;
	routine->ExplainForeignScan = impalaExplainForeignScan;

	PG_RETURN_POINTER(routine);
}

Datum
impala_fdw_validator(PG_FUNCTION_ARGS)
{
	List	   *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid			catalog = PG_GETARG_OID(1);
	ListCell   *cell;

	foreach(cell, options_list)
	{
		DefElem    *def = (DefElem *) lfirst(cell);
		char	   *name = def->defname;

		if (catalog == ForeignServerRelationId)
		{
			if (strcmp(name, OPTION_HOST) != 0 &&
				strcmp(name, OPTION_PORT) != 0 &&
				strcmp(name, OPTION_AUTH) != 0 &&
				strcmp(name, OPTION_KRB_REALM) != 0 &&
				strcmp(name, "krb_service") != 0 &&
				strcmp(name, "krb_host") != 0 &&
				strcmp(name, "kudu_masters") != 0 &&
				strcmp(name, OPTION_DEFAULT_ACCESS) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						 errmsg("invalid option \"%s\" for Impala server", name)));
		}
		else if (catalog == ForeignTableRelationId)
		{
			if (strcmp(name, OPTION_DATABASE) != 0 &&
				strcmp(name, OPTION_TABLE) != 0 &&
				strcmp(name, OPTION_ACCESS) != 0 &&
				strcmp(name, "kudu_table") != 0 &&
				strcmp(name, "kudu_column") != 0)
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						 errmsg("invalid option \"%s\" for Impala foreign table", name)));
		}
		else if (catalog == UserMappingRelationId)
		{
			if (strcmp(name, "principal") != 0 &&
				strcmp(name, "keytab") != 0)
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						 errmsg("invalid option \"%s\" for user mapping", name)));
		}
	}

	PG_RETURN_VOID();
}

static char *
get_option_value(List *options, const char *optname)
{
	ListCell   *lc;

	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, optname) == 0)
			return defGetString(def);
	}
	return NULL;
}

typedef struct
{
	Index		relid;
	Bitmapset  *attrs;
	bool		whole_row;		/* varattno == 0 seen */
} PullVarCtx;

static bool
pull_varattnos_cb(Node *node, PullVarCtx *ctx)
{
	if (node == NULL)
		return false;
	if (IsA(node, Var))
	{
		Var		   *var = (Var *) node;

		if (var->varno == ctx->relid && var->varlevelsup == 0)
		{
			if (var->varattno == 0)
				ctx->whole_row = true;
			else if (var->varattno > 0)
				ctx->attrs = bms_add_member(ctx->attrs, var->varattno);
		}
		return false;
	}
	return expression_tree_walker(node, pull_varattnos_cb, (void *) ctx);
}

/*
 * Columns to retrieve from remote: tlist Vars ∪ local-qual Vars.
 * Whole-row Var (varattno==0) expands to all live columns (F1 / postgres_fdw).
 */
static List *
attrs_for_foreign_scan(List *tlist, List *local_exprs, Index relid,
					   Relation rel)
{
	PullVarCtx	ctx;
	List	   *result = NIL;
	ListCell   *lc;
	int			attno;
	TupleDesc	tupdesc = RelationGetDescr(rel);

	ctx.relid = relid;
	ctx.attrs = NULL;
	ctx.whole_row = false;

	foreach(lc, tlist)
		(void) pull_varattnos_cb((Node *) lfirst(lc), &ctx);
	foreach(lc, local_exprs)
		(void) pull_varattnos_cb((Node *) lfirst(lc), &ctx);

	if (ctx.whole_row || ctx.attrs == NULL)
	{
		/* whole-row, SELECT *, or no vars — all live columns */
		for (attno = 1; attno <= tupdesc->natts; attno++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, attno - 1);

			if (!attr->attisdropped)
				result = lappend_int(result, attno);
		}
		return result;
	}

	attno = -1;
	while ((attno = bms_next_member(ctx.attrs, attno)) >= 0)
		result = lappend_int(result, attno);
	return result;
}

/* Collect remote-pred column names (attnames / kudu_column) into List of cstrings */
static void
collect_pred_colnames(Relation rel, Index relid, List *remote_exprs,
					  List **names_out)
{
	ListCell   *lc;
	Bitmapset  *attrs = NULL;
	int			attno;
	Oid			reloid = RelationGetRelid(rel);

	*names_out = NIL;
	foreach(lc, remote_exprs)
	{
		List	   *vars = pull_var_clause((Node *) lfirst(lc),
										   PVC_RECURSE_AGGREGATES |
										   PVC_RECURSE_PLACEHOLDERS);
		ListCell   *vc;

		foreach(vc, vars)
		{
			Var		   *v = (Var *) lfirst(vc);

			if (v->varlevelsup == 0 && v->varattno > 0 &&
				(relid == 0 || v->varno == relid))
				attrs = bms_add_member(attrs, v->varattno);
		}
	}

	attno = -1;
	while ((attno = bms_next_member(attrs, attno)) >= 0)
	{
		char	   *nm = resolve_kudu_column_name(reloid, attno, rel);

		*names_out = lappend(*names_out, nm);
	}
}

static bool
name_in_list_ci(const char *name, const char **set)
{
	int			i;

	for (i = 0; set[i] != NULL; i++)
	{
		if (pg_strcasecmp(name, set[i]) == 0)
			return true;
	}
	return false;
}

/* Atlas HASH / identity allowlist (KD15) */
static const char *atlas_allowlist[] = {
	"src", "dst", "guid", "qn_digest", "entity_guid", NULL
};

static bool
all_names_in_allowlist(List *names)
{
	ListCell   *lc;

	if (names == NIL)
		return false;
	foreach(lc, names)
	{
		if (!name_in_list_ci((const char *) lfirst(lc), atlas_allowlist))
			return false;
	}
	return true;
}

/*
 * Known freeze PK sets by Impala table name (v1; no Kudu schema introspection).
 * Returns true if every PK column appears in names (equality assumed by caller).
 */
static bool
names_cover_table_pk(const char *table, List *names)
{
	const char **pk = NULL;
	int			i;
	ListCell   *lc;

	if (table == NULL)
		return false;
	if (strcmp(table, "entity_flat") == 0)
	{
		static const char *p[] = {"guid", NULL};

		pk = p;
	}
	else if (strcmp(table, "entity_by_qn") == 0)
	{
		static const char *p[] = {"qn_digest", NULL};

		pk = p;
	}
	else if (strcmp(table, "edge_out") == 0)
	{
		static const char *p[] = {"src", "elabel", "dst", NULL};

		pk = p;
	}
	else if (strcmp(table, "edge_in") == 0)
	{
		static const char *p[] = {"dst", "elabel", "src", NULL};

		pk = p;
	}
	else if (strcmp(table, "entity_classifications") == 0)
	{
		static const char *p[] = {"tag_name", "guid", NULL};

		pk = p;
	}
	else if (strcmp(table, "entity_audit") == 0)
	{
		static const char *p[] = {"guid", "event_ts", "seq", NULL};

		pk = p;
	}
	else
		return false;

	for (i = 0; pk[i] != NULL; i++)
	{
		bool		found = false;

		foreach(lc, names)
		{
			if (pg_strcasecmp((const char *) lfirst(lc), pk[i]) == 0)
			{
				found = true;
				break;
			}
		}
		if (!found)
			return false;
	}
	return true;
}

/* True if every remote expr is equality OpExpr (no IN/range/null). */
static bool
remote_expr_is_equality_tree(Node *node)
{
	if (node == NULL)
		return true;
	if (IsA(node, BoolExpr))
	{
		BoolExpr   *be = (BoolExpr *) node;
		ListCell   *ac;

		if (be->boolop != AND_EXPR)
			return false;
		foreach(ac, be->args)
		{
			if (!remote_expr_is_equality_tree((Node *) lfirst(ac)))
				return false;
		}
		return true;
	}
	if (IsA(node, OpExpr))
	{
		char	   *name = get_opname(((OpExpr *) node)->opno);

		return (name != NULL && strcmp(name, "=") == 0);
	}
	if (IsA(node, RelabelType))
		return remote_expr_is_equality_tree(
			(Node *) ((RelabelType *) node)->arg);
	return false;
}

static bool
remote_exprs_all_equality(List *remote_exprs)
{
	ListCell   *lc;

	if (remote_exprs == NIL)
		return false;
	foreach(lc, remote_exprs)
	{
		if (!remote_expr_is_equality_tree((Node *) lfirst(lc)))
			return false;
	}
	return true;
}

/* Allowlist promotion (N5): only eq and IN/ANY — not range or <> */
static bool
remote_expr_is_eq_or_in_tree(Node *node)
{
	if (node == NULL)
		return true;
	if (IsA(node, BoolExpr))
	{
		BoolExpr   *be = (BoolExpr *) node;
		ListCell   *ac;

		if (be->boolop != AND_EXPR)
			return false;
		foreach(ac, be->args)
		{
			if (!remote_expr_is_eq_or_in_tree((Node *) lfirst(ac)))
				return false;
		}
		return true;
	}
	if (IsA(node, OpExpr))
	{
		char	   *name = get_opname(((OpExpr *) node)->opno);

		return (name != NULL && strcmp(name, "=") == 0);
	}
	if (IsA(node, ScalarArrayOpExpr))
	{
		ScalarArrayOpExpr *sa = (ScalarArrayOpExpr *) node;
		char	   *name = get_opname(sa->opno);

		return (name != NULL && strcmp(name, "=") == 0 && sa->useOr);
	}
	if (IsA(node, RelabelType))
		return remote_expr_is_eq_or_in_tree(
			(Node *) ((RelabelType *) node)->arg);
	return false;
}

static bool
remote_exprs_eq_or_in_only(List *remote_exprs)
{
	ListCell   *lc;

	if (remote_exprs == NIL)
		return false;
	foreach(lc, remote_exprs)
	{
		if (!remote_expr_is_eq_or_in_tree((Node *) lfirst(lc)))
			return false;
	}
	return true;
}

static void
impalaGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	List	   *remote = NIL;
	List	   *local = NIL;

	impala_classify_conditions(root, baserel, baserel->baserestrictinfo,
							   &remote, &local);
	/* Cheaper when remote filters exist */
	if (remote != NIL)
		baserel->rows = 100;
	else
		baserel->rows = 1000;
	baserel->fdw_private = (void *) remote;	/* stash for paths (list of RI) */
}

static void
impalaGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	Cost		startup_cost = 25;
	Cost		total_cost = 25 + baserel->rows;
	List	   *remote = (List *) baserel->fdw_private;

	if (remote != NIL)
	{
		startup_cost = 10;
		total_cost = 10 + baserel->rows * 0.1;
	}

	{
		ForeignPath *fpath;

		/*
		 * Kudu client reactors + fork are undefined; never parallelize
		 * foreign scans (design KD19).
		 */
		fpath = create_foreignscan_path(root, baserel,
										NULL,
										baserel->rows,
										startup_cost,
										total_cost,
										NIL,
										baserel->lateral_relids,
										NULL,
										NIL);
		fpath->path.parallel_safe = false;
		fpath->path.parallel_aware = false;
		add_path(baserel, (Path *) fpath);
	}
	(void) foreigntableid;
}

static char *
resolve_kudu_column_name(Oid reloid, int attno, Relation rel)
{
	List	   *colopts;
	ListCell   *lc;
	Form_pg_attribute attr;

	colopts = GetForeignColumnOptions(reloid, (AttrNumber) attno);
	foreach(lc, colopts)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, OPTION_KUDU_COLUMN) == 0)
			return pstrdup(defGetString(def));
	}
	attr = TupleDescAttr(RelationGetDescr(rel), attno - 1);
	return pstrdup(NameStr(attr->attname));
}

static char *
resolve_kudu_table_name(const char *kudu_table_opt,
						const char *database,
						const char *table)
{
	if (kudu_table_opt != NULL && kudu_table_opt[0] != '\0')
		return pstrdup(kudu_table_opt);
	/* HMS-free Impala convention: impala::<db>.<table> */
	return psprintf("impala::%s.%s",
					database ? database : "default",
					table ? table : "");
}

static void
impala_begin_hs2_scan(ImpalaFdwScanState *festate,
					  Relation rel,
					  List *remote_exprs,
					  int64 limit_count)
{
	char	   *err = NULL;

	festate->sql = impala_build_select_sql(rel,
										   festate->database,
										   festate->table,
										   festate->retrieved_attrs,
										   remote_exprs,
										   limit_count);
	elog(DEBUG1, "impala_fdw: shape=%s method=impala_sql sql=%s",
		 festate->shape_id, festate->sql);

	festate->hs2 = impala_hs2_connect(festate->host, festate->port,
									  festate->auth,
									  festate->principal,
									  festate->database, &err);
	if (festate->hs2 == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
				 errmsg("impala_fdw: HS2 connect failed: %s",
						err ? err : "unknown"),
				 errdetail("host=%s port=%d auth=%s",
						   festate->host, festate->port, festate->auth)));

	festate->result = impala_hs2_execute(festate->hs2, festate->sql, &err);
	if (festate->result == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_ERROR),
				 errmsg("impala_fdw: HS2 execute failed: %s",
						err ? err : "unknown"),
				 errdetail("sql: %s", festate->sql)));
}

#ifdef IMPALA_FDW_WITH_KUDU
static bool
impala_begin_kudu_scan(ImpalaFdwScanState *festate,
					   Relation rel,
					   List *retrieved_attrs,
					   List *remote_exprs,
					   int64 limit_count,
					   char **err_out)
{
	ListCell   *lc;
	int			ncolumns;
	const char **colnames;
	int			i;
	char	   *err = NULL;
	Oid			reloid = RelationGetRelid(rel);
	ImpalaKuduPred *preds = NULL;
	int			npreds = 0;
	bool		empty_result = false;
	char	   *perr = NULL;

	if (err_out)
		*err_out = NULL;

	/* Compile remote_exprs → Kudu pred IR (PR-K2) */
	if (remote_exprs != NIL)
	{
		if (!impala_build_kudu_preds(rel, remote_exprs, &preds, &npreds,
									 &empty_result, &perr))
		{
			if (err_out)
				*err_out = strdup(perr ? perr : "pred compile failed");
			return false;
		}
		if (empty_result)
		{
			/* Empty IN → no rows without RPC */
			festate->kudu = impala_kudu_scan_open(NULL, festate->kudu_table,
												  NULL, 0, NULL, -1,
												  limit_count, &err);
			if (festate->kudu == NULL)
			{
				if (err_out)
					*err_out = err;
				else if (err)
					free(err);
				return false;
			}
			festate->use_kudu = true;
			elog(DEBUG1,
				 "impala_fdw: shape=%s method=kudu_scan empty_IN table=%s",
				 festate->shape_id, festate->kudu_table);
			return true;
		}
	}

	ncolumns = list_length(retrieved_attrs);
	if (ncolumns <= 0)
	{
		/* project all live columns */
		TupleDesc	tupdesc = RelationGetDescr(rel);
		List	   *all = NIL;
		int			attno;

		for (attno = 1; attno <= tupdesc->natts; attno++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, attno - 1);

			if (!attr->attisdropped)
				all = lappend_int(all, attno);
		}
		retrieved_attrs = all;
		festate->retrieved_attrs = all;
		ncolumns = list_length(all);
	}

	colnames = (const char **) palloc(sizeof(char *) * ncolumns);
	i = 0;
	foreach(lc, retrieved_attrs)
	{
		int			attno = lfirst_int(lc);

		colnames[i++] = resolve_kudu_column_name(reloid, attno, rel);
	}

	festate->kudu = impala_kudu_scan_open(festate->kudu_masters,
										  festate->kudu_table,
										  colnames, ncolumns,
										  preds, npreds,
										  limit_count,
										  &err);
	/* pred IR may be discarded after open (deep-copied into Kudu) */
	if (festate->kudu == NULL)
	{
		if (err_out)
			*err_out = err;
		else if (err)
			free(err);
		return false;
	}
	festate->use_kudu = true;
	elog(DEBUG1,
		 "impala_fdw: shape=%s method=kudu_scan table=%s masters=%s cols=%d preds=%d limit=%ld",
		 festate->shape_id,
		 festate->kudu_table,
		 festate->kudu_masters,
		 ncolumns,
		 npreds,
		 (long) limit_count);
	return true;
}
#endif

/*
 * Extract pushable LIMIT count, or -1 if not safe to push.
 *
 * Refuse when remote truncation would change semantics under upper plan
 * nodes (postgres_fdw-style). Caller also requires local_exprs == NIL.
 *
 * N1: ORDER BY / DISTINCT / GROUP BY / HAVING / aggs / windows / SRFs
 * F2: OFFSET; multi-RTE; non-Const; WITH TIES (limitOption)
 */
static int64
extract_pushable_limit(PlannerInfo *root, RelOptInfo *baserel)
{
	Query	   *query = root->parse;
	Const	   *c;

	(void) baserel;
	if (query == NULL || query->commandType != CMD_SELECT)
		return -1;
	if (query->jointree == NULL)
		return -1;
	if (list_length(query->rtable) != 1)
		return -1;
	/* OFFSET: remote LIMIT n then PG skip → wrong window */
	if (query->limitOffset != NULL)
		return -1;
	/* WITH TIES needs ORDER BY; refuse any non-COUNT/DEFAULT option */
	if (query->limitOption != LIMIT_OPTION_COUNT &&
		query->limitOption != LIMIT_OPTION_DEFAULT)
		return -1;
	/*
	 * Upper-plan shapes: LIMIT in Query applies above Sort/Agg/etc.
	 * Pushing LIMIT to remote truncates the input of those nodes.
	 */
	if (query->sortClause != NIL ||
		query->distinctClause != NIL ||
		query->groupClause != NIL ||
		query->groupingSets != NIL ||
		query->havingQual != NULL ||
		query->hasAggs ||
		query->hasWindowFuncs ||
		query->hasTargetSRFs)
		return -1;
	if (query->limitCount == NULL)
		return -1;
	if (!IsA(query->limitCount, Const))
		return -1;
	c = (Const *) query->limitCount;
	if (c->constisnull)
		return -1;
	if (c->consttype != INT8OID && c->consttype != INT4OID)
		return -1;
	if (c->consttype == INT8OID)
		return (int64) DatumGetInt64(c->constvalue);
	return (int64) DatumGetInt32(c->constvalue);
}

static ForeignScan *
impalaGetForeignPlan(PlannerInfo *root,
					 RelOptInfo *baserel,
					 Oid foreigntableid,
					 ForeignPath *best_path,
					 List *tlist,
					 List *scan_clauses,
					 Plan *outer_plan)
{
	ForeignTable *table;
	ForeignServer *server;
	List	   *options;
	List	   *remote_ri = NIL;
	List	   *local_ri = NIL;
	List	   *remote_exprs = NIL;
	List	   *local_exprs = NIL;
	List	   *fdw_private;
	List	   *retrieved_attrs;
	ListCell   *lc;
	char	   *database;
	char	   *tbl;
	char	   *access;
	char	   *forced;
	const char *shape_id;
	ImpalaFdwAccessMethod method;
	int64		limit_count;
	char		limit_buf[32];
	Relation	rel;
	bool		has_limit;

	table = GetForeignTable(foreigntableid);
	server = GetForeignServer(table->serverid);
	options = list_concat(list_copy(server->options), list_copy(table->options));

	database = get_option_value(options, OPTION_DATABASE);
	if (database == NULL)
		database = "default";
	tbl = get_option_value(options, OPTION_TABLE);
	access = get_option_value(options, OPTION_ACCESS);
	if (access == NULL)
		access = get_option_value(options, OPTION_DEFAULT_ACCESS);
	if (access == NULL)
		access = "auto";

	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/* Re-classify from baserestrictinfo + join clauses in scan_clauses */
	foreach(lc, scan_clauses)
	{
		Expr	   *expr = (Expr *) lfirst(lc);

		if (impala_is_foreign_expr(root, baserel, expr))
			remote_exprs = lappend(remote_exprs, expr);
		else
			local_exprs = lappend(local_exprs, expr);
	}

	/*
	 * F2: only push LIMIT when no OFFSET (inside extract) and no local quals.
	 * Otherwise leave limit_count = -1 so remote returns full set; PG Limit
	 * node applies correctly after local filter.
	 */
	{
		int64		raw_limit = extract_pushable_limit(root, baserel);

		if (raw_limit >= 0 && local_exprs == NIL)
			limit_count = raw_limit;
		else
			limit_count = -1;
	}
	has_limit = (limit_count >= 0);

	/* Open relation for attr projection + path shape analysis */
	rel = table_open(foreigntableid, NoLock);
	retrieved_attrs = attrs_for_foreign_scan(tlist, local_exprs,
											 baserel->relid, rel);

	{
		List	   *pred_names = NIL;
		bool		eq_only;
		bool		full_pk = false;
		bool		unique_lu = false;
		bool		allow_f = false;
		bool		enable_kudu;
		bool		has_local = (local_exprs != NIL);

		collect_pred_colnames(rel, baserel->relid, remote_exprs, &pred_names);
		eq_only = remote_exprs_all_equality(remote_exprs);
		if (eq_only && names_cover_table_pk(tbl, pred_names))
			full_pk = true;
		/* unique_lookup: single-col qn_digest equality */
		if (eq_only && list_length(pred_names) == 1 &&
			pg_strcasecmp((const char *) linitial(pred_names), "qn_digest") == 0 &&
			tbl && strcmp(tbl, "entity_by_qn") == 0)
			unique_lu = true;
		/* N5: allowlist promote only for eq/IN on allowlist columns */
		if (remote_exprs != NIL && all_names_in_allowlist(pred_names) &&
			remote_exprs_eq_or_in_only(remote_exprs))
			allow_f = true;

		enable_kudu = impala_fdw_enable_kudu_scan;
#ifndef IMPALA_FDW_WITH_KUDU
		enable_kudu = false;
#endif
		forced = access;
		impala_fdw_select_path(forced,
							   enable_kudu,
							   has_local,
							   has_limit,	/* limit_pushable already gated */
							   limit_count,
							   impala_fdw_sample_max,
							   full_pk,
							   unique_lu,
							   allow_f,
							   remote_exprs == NIL,
							   &method,
							   &shape_id);
	}
	table_close(rel, NoLock);

	if (limit_count >= 0)
		snprintf(limit_buf, sizeof(limit_buf), INT64_FORMAT, limit_count);
	else
		limit_buf[0] = '\0';

	fdw_private = list_make5(
		makeString(pstrdup(database)),
		makeString(tbl ? pstrdup(tbl) : pstrdup("")),
		makeString(pstrdup(access)),
		makeString(pstrdup(shape_id)),
		makeInteger((int) method));
	fdw_private = lappend(fdw_private, makeString(pstrdup(limit_buf)));
	fdw_private = lappend(fdw_private, retrieved_attrs);
	fdw_private = lappend(fdw_private, remote_exprs);

	(void) remote_ri;
	(void) local_ri;
	(void) best_path;

	return make_foreignscan(tlist,
							local_exprs,	/* residual local quals */
							baserel->relid,
							NIL,			/* fdw_exprs (params later) */
							fdw_private,
							NIL,
							NIL,
							outer_plan);
}

static void
impalaBeginForeignScan(ForeignScanState *node, int eflags)
{
	ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
	ForeignTable *ftable;
	ForeignServer *server;
	UserMapping *mapping;
	ImpalaFdwScanState *festate;
	char	   *port_str;
	char	   *map_principal = NULL;
	List	   *options = NIL;
	List	   *map_opts = NIL;
	bool		auth_kerberos;
	List	   *fdw_private = fsplan->fdw_private;
	List	   *retrieved_attrs;
	List	   *remote_exprs;
	int64		limit_count = -1;
	char	   *limit_str;
	Relation	rel;
	char	   *kudu_table_opt;
	bool		forced_kudu;
	bool		try_kudu = false;

	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	ftable = GetForeignTable(RelationGetRelid(node->ss.ss_currentRelation));
	server = GetForeignServer(ftable->serverid);
	mapping = GetUserMapping(GetUserId(), ftable->serverid);

	options = list_concat(list_copy(server->options), list_copy(ftable->options));
	map_opts = list_copy(mapping->options);

	/*
	 * Own festate in CurrentMemoryContext (query/estate) and remember it —
	 * do NOT re-sample CurrentMemoryContext later for the reset callback
	 * (it may have switched to a short-lived context → mid-scan close crash).
	 */
	festate = (ImpalaFdwScanState *) palloc0(sizeof(ImpalaFdwScanState));
	festate->scan_mctx = CurrentMemoryContext;
	festate->host = get_option_value(options, OPTION_HOST);
	if (festate->host == NULL)
		festate->host = "127.0.0.1";
	port_str = get_option_value(options, OPTION_PORT);
	festate->port = port_str ? atoi(port_str) : 21050;

	festate->database = strVal(list_nth(fdw_private, FdwPrivateDatabase));
	festate->table = strVal(list_nth(fdw_private, FdwPrivateTable));
	if (festate->table[0] == '\0')
		festate->table = get_option_value(options, OPTION_TABLE);
	festate->access = strVal(list_nth(fdw_private, FdwPrivateAccess));
	festate->shape_id = strVal(list_nth(fdw_private, FdwPrivateShapeId));
	festate->method = (ImpalaFdwAccessMethod)
		intVal(list_nth(fdw_private, FdwPrivateMethod));
	limit_str = strVal(list_nth(fdw_private, FdwPrivateLimit));
	if (limit_str && limit_str[0])
		limit_count = (int64) atol(limit_str);
	festate->limit_count = limit_count;

	retrieved_attrs = (List *) list_nth(fdw_private, FdwPrivateRetrievedAttrs);
	remote_exprs = (List *) list_nth(fdw_private, FdwPrivateRemoteExprs);

	festate->auth = get_option_value(options, OPTION_AUTH);
	if (festate->auth == NULL)
		festate->auth = "kerberos";
	festate->krb_realm = get_option_value(options, OPTION_KRB_REALM);
	map_principal = get_option_value(map_opts, "principal");
	auth_kerberos = (pg_strcasecmp(festate->auth, "nosasl") != 0);
	festate->principal = impala_fdw_resolve_principal(map_principal,
													  festate->krb_realm,
													  auth_kerberos);

	festate->kudu_masters = get_option_value(options, OPTION_KUDU_MASTERS);
	if (festate->kudu_masters == NULL)
		festate->kudu_masters = "127.0.0.1:7051";
	kudu_table_opt = get_option_value(options, OPTION_KUDU_TABLE);
	festate->kudu_table = resolve_kudu_table_name(kudu_table_opt,
												  festate->database,
												  festate->table);

	rel = node->ss.ss_currentRelation;
	festate->retrieved_attrs = retrieved_attrs;
	node->fdw_state = (void *) festate;

	forced_kudu = (festate->access != NULL &&
				   strcmp(festate->access, "kudu_scan") == 0);
	festate->has_local_quals = (fsplan->scan.plan.qual != NIL);

	/*
	 * KD16: forced kudu_scan must not pretend residual local quals are remote.
	 */
	if (forced_kudu && festate->has_local_quals)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("impala_fdw: access=kudu_scan cannot evaluate unpushable local quals"),
				 errdetail("shape=%s; demote residual expressions or use access=impala_sql/auto",
						   festate->shape_id)));

	if (impala_fdw_log_path_choice)
		elog(LOG,
			 "impala_fdw: path shape=%s method=%s access=%s table=%s limit=%ld local_quals=%s",
			 festate->shape_id,
			 festate->method == IMPALA_FDW_ACCESS_KUDU_SCAN ? "kudu_scan" : "impala_sql",
			 festate->access ? festate->access : "?",
			 festate->kudu_table ? festate->kudu_table : "?",
			 (long) limit_count,
			 festate->has_local_quals ? "yes" : "no");

	/*
	 * Path selection for kudu_scan:
	 *  - parallel workers: refuse kudu (KD19)
	 *  - pred compile failure: forced ERROR; auto → HS2 (LOG, not WARNING)
	 *  - build without libkudu: demote / ERROR if forced
	 */
	if (festate->method == IMPALA_FDW_ACCESS_KUDU_SCAN)
	{
		try_kudu = true;

		if (IsParallelWorker())
		{
			if (forced_kudu)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("impala_fdw: kudu_scan not supported in parallel workers")));
			elog(LOG,
				 "impala_fdw: kudu_scan refused in parallel worker; using impala_sql shape=%s",
				 festate->shape_id);
			try_kudu = false;
			festate->method = IMPALA_FDW_ACCESS_IMPALA_SQL;
		}

#ifndef IMPALA_FDW_WITH_KUDU
		if (try_kudu)
		{
			if (forced_kudu)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("impala_fdw: built without libkudu_client")));
			elog(LOG, "impala_fdw: kudu_scan unavailable (no libkudu); using impala_sql");
			try_kudu = false;
			festate->method = IMPALA_FDW_ACCESS_IMPALA_SQL;
		}
#endif
	}

#ifdef IMPALA_FDW_WITH_KUDU
	if (try_kudu && festate->method == IMPALA_FDW_ACCESS_KUDU_SCAN)
	{
		char	   *kerr = NULL;

		if (!impala_kudu_scan_available() || !impala_fdw_enable_kudu_scan)
		{
			if (forced_kudu)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("impala_fdw: kudu_scan not available")));
			try_kudu = false;
			festate->method = IMPALA_FDW_ACCESS_IMPALA_SQL;
		}
		else if (!impala_begin_kudu_scan(festate, rel, retrieved_attrs,
										 remote_exprs, limit_count, &kerr))
		{
			if (forced_kudu)
			{
				/* palloc message so it survives free of malloc kerr */
				char	   *msg = pstrdup(kerr ? kerr : "kudu_scan open failed");

				if (kerr)
					free(kerr);
				ereport(ERROR,
						(errcode(ERRCODE_FDW_ERROR),
						 errmsg("%s", msg)));
			}
			/* auto: one HS2 fallback — expected demotion is LOG not WARNING (F8) */
			elog(LOG,
				 "impala_fdw: kudu_scan open/pred failed; falling back to impala_sql: %s",
				 kerr ? kerr : "unknown");
			if (kerr)
				free(kerr);
			festate->fell_back_to_hs2 = true;
			festate->method = IMPALA_FDW_ACCESS_IMPALA_SQL;
			festate->use_kudu = false;
			try_kudu = false;
		}
		else
		{
			/*
			 * F3/N6: close kudu (+ any HS2 handles) if query context resets.
			 * Register on festate's owning mctx only (not CurrentMemoryContext
			 * after nested allocs) — wrong context caused mid-scan teardown crash.
			 */
			if (!festate->scan_cb_registered && festate->scan_mctx != NULL)
			{
				festate->scan_cb.func = impala_fdw_scan_mcxt_callback;
				festate->scan_cb.arg = (void *) festate;
				MemoryContextRegisterResetCallback(festate->scan_mctx,
												   &festate->scan_cb);
				festate->scan_cb_registered = true;
			}
			return;
		}
	}
#else
	(void) try_kudu;
#endif

	/* HS2 path (default or fallback) */
	impala_begin_hs2_scan(festate, rel, remote_exprs, limit_count);
	/*
	 * HS2 abort-path mcxt callback intentionally omitted: registering a
	 * teardown that closes thrift while the executor still holds festate
	 * has caused backend SIGSEGV on short LIMIT scans (N6). Kudu path keeps
	 * the callback above; EndForeignScan always closes HS2 on the normal path.
	 */
}

static TupleTableSlot *
impalaIterateForeignScan(ForeignScanState *node)
{
	ImpalaFdwScanState *festate = (ImpalaFdwScanState *) node->fdw_state;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	char	  **values = NULL;
	bool	   *nulls = NULL;
	int			nfields = 0;
	char	   *err = NULL;
	int			rc;
	TupleDesc	tupdesc;
	int			i;
	Datum	   *dvalues;
	bool	   *dnulls;
	ListCell   *lc;
	int			atti;
	bool		from_kudu = false;

	ExecClearTuple(slot);
	if (festate == NULL)
		return slot;

#ifdef IMPALA_FDW_WITH_KUDU
	if (festate->use_kudu && festate->kudu != NULL)
	{
		from_kudu = true;
		rc = impala_kudu_scan_next(festate->kudu, &values, &nulls, &nfields, &err);
		if (rc < 0)
			ereport(ERROR,
					(errcode(ERRCODE_FDW_ERROR),
					 errmsg("impala_fdw: kudu fetch failed: %s", err ? err : "?")));
		if (rc == 0)
			return slot;
	}
	else
#endif
	{
		if (festate->result == NULL)
			return slot;
		rc = impala_hs2_fetch_row(festate->result, &values, &nfields, &nulls, &err);
		if (rc < 0)
			ereport(ERROR,
					(errcode(ERRCODE_FDW_ERROR),
					 errmsg("impala_fdw: HS2 fetch failed: %s", err ? err : "?")));
		if (rc == 0)
			return slot;
	}

	/*
	 * F3 free-before-call: copy malloc'd C strings into palloc, free the
	 * remote row, then run OidInputFunctionCall (which may ereport/longjmp).
	 */
	{
		char	  **pcopy = (char **) palloc0(sizeof(char *) * nfields);
		bool	   *ncopy = (bool *) palloc0(sizeof(bool) * nfields);

		for (i = 0; i < nfields; i++)
		{
			ncopy[i] = (nulls && nulls[i]) || (values[i] == NULL);
			if (!ncopy[i] && values[i])
				pcopy[i] = pstrdup(values[i]);
		}
#ifdef IMPALA_FDW_WITH_KUDU
		if (from_kudu)
			impala_kudu_scan_free_row(values, nulls, nfields);
		else
#endif
			impala_hs2_free_row(values, nulls, nfields);
		values = pcopy;
		nulls = ncopy;
	}

	tupdesc = slot->tts_tupleDescriptor;
	dvalues = (Datum *) palloc0(tupdesc->natts * sizeof(Datum));
	dnulls = (bool *) palloc(tupdesc->natts * sizeof(bool));
	for (i = 0; i < tupdesc->natts; i++)
		dnulls[i] = true;

	atti = 0;
	foreach(lc, festate->retrieved_attrs)
	{
		int			attno = lfirst_int(lc) - 1;
		Form_pg_attribute attr;
		Oid			typinput;
		Oid			typioparam;

		if (atti >= nfields)
			break;
		attr = TupleDescAttr(tupdesc, attno);
		if (nulls && nulls[atti])
			dnulls[attno] = true;
		else if (values[atti] == NULL)
			dnulls[attno] = true;
		else
		{
			getTypeInputInfo(attr->atttypid, &typinput, &typioparam);
			dvalues[attno] = OidInputFunctionCall(typinput, values[atti],
												  typioparam, attr->atttypmod);
			dnulls[attno] = false;
		}
		atti++;
	}

	return ExecStoreHeapTuple(
		heap_form_tuple(tupdesc, dvalues, dnulls), slot, false);
}

static void
impalaReScanForeignScan(ForeignScanState *node)
{
	ImpalaFdwScanState *festate = (ImpalaFdwScanState *) node->fdw_state;
	char	   *err = NULL;

	if (festate == NULL)
		return;

#ifdef IMPALA_FDW_WITH_KUDU
	if (festate->use_kudu && festate->kudu != NULL)
	{
		ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
		List	   *remote_exprs;

		/* reopen with same remote_exprs from plan */
		remote_exprs = (List *) list_nth(fsplan->fdw_private,
										 FdwPrivateRemoteExprs);
		impala_kudu_scan_close(festate->kudu);
		festate->kudu = NULL;
		festate->use_kudu = false;
		if (!impala_begin_kudu_scan(festate, node->ss.ss_currentRelation,
									festate->retrieved_attrs, remote_exprs,
									festate->limit_count, &err))
			ereport(ERROR,
					(errcode(ERRCODE_FDW_ERROR),
					 errmsg("impala_fdw: kudu re-open failed: %s",
							err ? err : "?")));
		return;
	}
#endif

	if (festate->hs2 == NULL || festate->sql == NULL)
		return;
	if (festate->result)
		impala_hs2_close_result(festate->result);
	festate->result = impala_hs2_execute(festate->hs2, festate->sql, &err);
	if (festate->result == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_ERROR),
				 errmsg("impala_fdw: HS2 re-execute failed: %s",
						err ? err : "?")));
}

static void
impalaEndForeignScan(ForeignScanState *node)
{
	ImpalaFdwScanState *festate = (ImpalaFdwScanState *) node->fdw_state;

	if (festate == NULL)
		return;
#ifdef IMPALA_FDW_WITH_KUDU
	if (festate->kudu)
	{
		impala_kudu_scan_close(festate->kudu);
		festate->kudu = NULL;
	}
#endif
	if (festate->result)
		impala_hs2_close_result(festate->result);
	if (festate->hs2)
		impala_hs2_close(festate->hs2);
	node->fdw_state = NULL;
}

static void
impalaExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
	List	   *fdw_private = fsplan->fdw_private;
	char	   *shape;
	char	   *database;
	char	   *table;
	char	   *access;
	int			method;
	List	   *retrieved;
	List	   *remote;
	Relation	rel;
	char	   *sql;
	int64		limit_count = -1;
	char	   *limit_str;

	if (fdw_private == NIL || list_length(fdw_private) < 8)
		return;

	database = strVal(list_nth(fdw_private, FdwPrivateDatabase));
	table = strVal(list_nth(fdw_private, FdwPrivateTable));
	access = strVal(list_nth(fdw_private, FdwPrivateAccess));
	shape = strVal(list_nth(fdw_private, FdwPrivateShapeId));
	method = intVal(list_nth(fdw_private, FdwPrivateMethod));
	limit_str = strVal(list_nth(fdw_private, FdwPrivateLimit));
	if (limit_str && limit_str[0])
		limit_count = (int64) atol(limit_str);
	retrieved = (List *) list_nth(fdw_private, FdwPrivateRetrievedAttrs);
	remote = (List *) list_nth(fdw_private, FdwPrivateRemoteExprs);

	ExplainPropertyText("Impala ShapeId", shape, es);
	/* Plan-time method only; runtime fallback shown when festate present */
	{
		const char *am = "impala_sql";
		ImpalaFdwScanState *festate = (ImpalaFdwScanState *) node->fdw_state;

		if (festate != NULL)
		{
			if (festate->use_kudu)
				am = "kudu_scan";
			else if (festate->fell_back_to_hs2)
				am = "kudu_scan→impala_sql";
			else
				am = "impala_sql";
		}
		else if (method == IMPALA_FDW_ACCESS_KUDU_SCAN)
			am = "kudu_scan";
		ExplainPropertyText("Impala AccessMethod", am, es);
	}
	ExplainPropertyText("Impala AccessOption", access, es);

	{
		List	   *options = NIL;
		ForeignTable *ftable;
		ForeignServer *server;
		char	   *masters;
		char	   *kudu_opt;
		char	   *resolved;

		ftable = GetForeignTable(RelationGetRelid(node->ss.ss_currentRelation));
		server = GetForeignServer(ftable->serverid);
		options = list_concat(list_copy(server->options),
							  list_copy(ftable->options));
		masters = get_option_value(options, OPTION_KUDU_MASTERS);
		if (masters == NULL)
			masters = "127.0.0.1:7051";
		kudu_opt = get_option_value(options, OPTION_KUDU_TABLE);
		resolved = resolve_kudu_table_name(kudu_opt, database, table);
		ExplainPropertyText("Impala KuduMasters", masters, es);
		ExplainPropertyText("Impala KuduTable", resolved, es);
	}

	if (method == IMPALA_FDW_ACCESS_KUDU_SCAN)
	{
		ExplainPropertyText("Impala KuduNote",
							remote == NIL ?
							"projection/LIMIT (no remote quals)" :
							"projection + remote predicates (kudu_scan)", es);
		/* Also show HS2 SQL for comparison / residual understanding */
	}

	rel = node->ss.ss_currentRelation;
	sql = impala_build_select_sql(rel, database, table, retrieved, remote,
								  limit_count);
	ExplainPropertyText("Impala Remote SQL", sql, es);
}
