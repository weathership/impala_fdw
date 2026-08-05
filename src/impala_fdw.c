/*-------------------------------------------------------------------------
 *
 * impala_fdw.c
 *    Foreign Data Wrapper for Apache Impala (HS2) → Kudu storage only.
 *
 * Scope: foreign tables map to Impala tables backed by Kudu. Iceberg and
 * other Impala formats are out of scope for v1.
 *
 * Scaffold: registers FDW routines; BeginForeignScan raises until HS2
 * client integration is implemented.
 *
 * Copyright 2026 Weathership / signals-360 contributors
 * Licensed under the Apache License, Version 2.0
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/reloptions.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "commands/defrem.h"
#include "executor/executor.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "nodes/makefuncs.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "utils/builtins.h"
#include "utils/rel.h"

PG_MODULE_MAGIC;

/* Server options */
#define OPTION_HOST "host"
#define OPTION_PORT "port"
#define OPTION_AUTH "auth"
/* Table options */
#define OPTION_DATABASE "database"
#define OPTION_TABLE "table"

/*
 * FDW-private state for a scan (placeholder).
 */
typedef struct ImpalaFdwScanState
{
	char	   *host;
	int			port;
	char	   *database;
	char	   *table;
} ImpalaFdwScanState;

/* Forward declarations */
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

static char *get_option_value(List *options, const char *optname);

void
_PG_init(void)
{
	/* reserved for GUC / library load */
}

/*
 * FDW handler: return FdwRoutine with callbacks.
 */
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

	PG_RETURN_POINTER(routine);
}

/*
 * Validate OPTIONS on SERVER / TABLE / USER MAPPING.
 */
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
				strcmp(name, OPTION_AUTH) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						 errmsg("invalid option \"%s\" for Impala server", name),
						 errhint("Valid options are: host, port, auth.")));
		}
		else if (catalog == ForeignTableRelationId)
		{
			if (strcmp(name, OPTION_DATABASE) != 0 &&
				strcmp(name, OPTION_TABLE) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						 errmsg("invalid option \"%s\" for Impala foreign table", name),
						 errhint("Valid options are: database, table.")));
		}
		/* UserMappingRelationId: no options yet */
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

static void
impalaGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	/* Unknown remote size until HS2 DESCRIBE / stats exist */
	baserel->rows = 1000;
}

static void
impalaGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	Cost		startup_cost = 100;
	Cost		total_cost = 100 + baserel->rows;

	add_path(baserel, (Path *)
			 create_foreignscan_path(root, baserel,
									 NULL,	/* default pathtarget */
									 baserel->rows,
									 startup_cost,
									 total_cost,
									 NIL,	/* no pathkeys */
									 baserel->lateral_relids,
									 NULL,	/* no extra plan */
									 NIL)); /* no fdw_private */
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
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	return make_foreignscan(tlist,
							scan_clauses,
							baserel->relid,
							NIL,	/* no expressions to evaluate */
							best_path->fdw_private,
							NIL,	/* no custom tlist */
							NIL,	/* no remote quals */
							outer_plan);
}

static void
impalaBeginForeignScan(ForeignScanState *node, int eflags)
{
	ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
	ForeignTable *table;
	ForeignServer *server;
	ImpalaFdwScanState *festate;
	char	   *port_str;
	List	   *options = NIL;

	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	table = GetForeignTable(RelationGetRelid(node->ss.ss_currentRelation));
	server = GetForeignServer(table->serverid);

	options = list_concat(options, list_copy(server->options));
	options = list_concat(options, list_copy(table->options));

	festate = (ImpalaFdwScanState *) palloc0(sizeof(ImpalaFdwScanState));
	festate->host = get_option_value(options, OPTION_HOST);
	if (festate->host == NULL)
		festate->host = "127.0.0.1";
	port_str = get_option_value(options, OPTION_PORT);
	festate->port = port_str ? atoi(port_str) : 21050;
	festate->database = get_option_value(options, OPTION_DATABASE);
	if (festate->database == NULL)
		festate->database = "default";
	festate->table = get_option_value(options, OPTION_TABLE);

	node->fdw_state = (void *) festate;

	/*
	 * HS2 client not yet wired: fail clearly on first real scan.
	 * EXPLAIN-only path returns above.
	 */
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("impala_fdw: Impala HS2 scan not implemented yet"),
			 errdetail("Would scan Kudu-backed table %s.%s via HS2 %s:%d",
					   festate->database,
					   festate->table ? festate->table : "(unset)",
					   festate->host,
					   festate->port),
			 errhint("Wire the HS2 client in IterateForeignScan; only Kudu storage is in scope. "
					 "Signals devenv HS2 is localhost:21050.")));

	(void) fsplan;
}

static TupleTableSlot *
impalaIterateForeignScan(ForeignScanState *node)
{
	/* Unreachable until BeginForeignScan succeeds */
	return ExecClearTuple(node->ss.ss_ScanTupleSlot);
}

static void
impalaReScanForeignScan(ForeignScanState *node)
{
	/* no-op scaffold */
}

static void
impalaEndForeignScan(ForeignScanState *node)
{
	node->fdw_state = NULL;
}
