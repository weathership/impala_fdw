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
#include "catalog/pg_user_mapping.h"
#include "commands/defrem.h"
#include "executor/executor.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/rel.h"

#include "exec_impala.h"
#include "krb_util.h"
#include "path_select.h"

#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "catalog/pg_type.h"
#include "executor/tuptable.h"
#include "access/htup_details.h"
#include "utils/memutils.h"
#include "utils/ruleutils.h"

PG_MODULE_MAGIC;

/* Server options */
#define OPTION_HOST "host"
#define OPTION_PORT "port"
#define OPTION_AUTH "auth"
#define OPTION_KRB_REALM "krb_realm"
#define OPTION_DEFAULT_ACCESS "default_access"
/* Table options */
#define OPTION_DATABASE "database"
#define OPTION_TABLE "table"
#define OPTION_ACCESS "access"

/*
 * FDW-private state for a scan (placeholder).
 */
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
	/* HS2 */
	ImpalaHs2Session *hs2;
	ImpalaHs2Result *result;
	List	   *retrieved_attrs;	/* 1-based attnums */
	char	   *sql;
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
				strcmp(name, OPTION_AUTH) != 0 &&
				strcmp(name, OPTION_KRB_REALM) != 0 &&
				strcmp(name, "krb_service") != 0 &&
				strcmp(name, "krb_host") != 0 &&
				strcmp(name, "kudu_masters") != 0 &&
				strcmp(name, OPTION_DEFAULT_ACCESS) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						 errmsg("invalid option \"%s\" for Impala server", name),
						 errhint("Valid options: host, port, auth, krb_realm, krb_service, krb_host, kudu_masters, default_access.")));
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
						 errmsg("invalid option \"%s\" for Impala foreign table", name),
						 errhint("Valid options: database, table, access, kudu_table, kudu_column.")));
		}
		else if (catalog == UserMappingRelationId)
		{
			if (strcmp(name, "principal") != 0 &&
				strcmp(name, "keytab") != 0)
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						 errmsg("invalid option \"%s\" for user mapping", name),
						 errhint("Valid options: principal, keytab.")));
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
	UserMapping *mapping;
	ImpalaFdwScanState *festate;
	char	   *port_str;
	char	   *forced;
	char	   *map_principal = NULL;
	List	   *options = NIL;
	List	   *map_opts = NIL;
	bool		auth_kerberos;

	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	table = GetForeignTable(RelationGetRelid(node->ss.ss_currentRelation));
	server = GetForeignServer(table->serverid);
	mapping = GetUserMapping(GetUserId(), table->serverid);

	options = list_concat(list_copy(server->options), list_copy(table->options));
	map_opts = list_copy(mapping->options);

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
	festate->auth = get_option_value(options, OPTION_AUTH);
	if (festate->auth == NULL)
		festate->auth = "kerberos";		/* signals default; nosasl for CI only */
	festate->krb_realm = get_option_value(options, OPTION_KRB_REALM);
	festate->access = get_option_value(options, OPTION_ACCESS);
	if (festate->access == NULL)
		festate->access = get_option_value(options, OPTION_DEFAULT_ACCESS);
	if (festate->access == NULL)
		festate->access = "auto";

	map_principal = get_option_value(map_opts, "principal");
	auth_kerberos = (pg_strcasecmp(festate->auth, "nosasl") != 0);
	festate->principal = impala_fdw_resolve_principal(map_principal,
													  festate->krb_realm,
													  auth_kerberos);

	forced = festate->access;
	impala_fdw_select_path(forced, false, false,
						   &festate->method, &festate->shape_id);

	node->fdw_state = (void *) festate;

	/* Kudu path not yet implemented — fall back to HS2 with notice */
	if (festate->method == IMPALA_FDW_ACCESS_KUDU_SCAN)
	{
		elog(DEBUG1,
			 "impala_fdw: kudu_scan not implemented; using impala_sql for %s.%s",
			 festate->database,
			 festate->table ? festate->table : "?");
		festate->method = IMPALA_FDW_ACCESS_IMPALA_SQL;
	}

	/* Build simple SELECT list from foreign table attrs */
	{
		Relation	rel = node->ss.ss_currentRelation;
		TupleDesc	tupdesc = RelationGetDescr(rel);
		StringInfoData buf;
		int			i;
		bool		first = true;

		initStringInfo(&buf);
		appendStringInfoString(&buf, "SELECT ");
		festate->retrieved_attrs = NIL;
		for (i = 0; i < tupdesc->natts; i++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

			if (attr->attisdropped)
				continue;
			if (!first)
				appendStringInfoString(&buf, ", ");
			first = false;
			appendStringInfo(&buf, "%s", quote_identifier(NameStr(attr->attname)));
			festate->retrieved_attrs =
				lappend_int(festate->retrieved_attrs, i + 1);
		}
		if (first)
			appendStringInfoString(&buf, "*");
		appendStringInfo(&buf, " FROM %s.%s",
						 quote_identifier(festate->database),
						 festate->table ? quote_identifier(festate->table) : "dual");
		festate->sql = buf.data;
	}

	{
		char	   *err = NULL;

		festate->hs2 = impala_hs2_connect(festate->host, festate->port,
										  festate->auth,
										  festate->principal,
										  festate->database, &err);
		if (festate->hs2 == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
					 errmsg("impala_fdw: HS2 connect failed: %s",
							err ? err : "unknown"),
					 errdetail("host=%s port=%d auth=%s principal=%s",
							   festate->host, festate->port, festate->auth,
							   festate->principal ? festate->principal : "(none)"),
					 errhint("For phase-1 dep validation use auth=nosasl. "
							 "Kerberos GSSAPI is next once HS2 thrift is proven.")));

		festate->result = impala_hs2_execute(festate->hs2, festate->sql, &err);
		if (festate->result == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_FDW_ERROR),
					 errmsg("impala_fdw: HS2 execute failed: %s",
							err ? err : "unknown"),
					 errdetail("sql: %s", festate->sql)));
	}

	(void) fsplan;
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

	ExecClearTuple(slot);
	if (festate == NULL || festate->result == NULL)
		return slot;

	rc = impala_hs2_fetch_row(festate->result, &values, &nfields, &nulls, &err);
	if (rc < 0)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_ERROR),
				 errmsg("impala_fdw: HS2 fetch failed: %s", err ? err : "?")));
	if (rc == 0)
		return slot;

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
		{
			dnulls[attno] = true;
		}
		else if (values[atti] == NULL)
		{
			dnulls[attno] = true;
		}
		else
		{
			getTypeInputInfo(attr->atttypid, &typinput, &typioparam);
			dvalues[attno] = OidInputFunctionCall(typinput, values[atti],
												  typioparam, attr->atttypmod);
			dnulls[attno] = false;
		}
		atti++;
	}

	impala_hs2_free_row(values, nulls, nfields);
	return ExecStoreHeapTuple(
		heap_form_tuple(tupdesc, dvalues, dnulls), slot, false);
}

static void
impalaReScanForeignScan(ForeignScanState *node)
{
	ImpalaFdwScanState *festate = (ImpalaFdwScanState *) node->fdw_state;
	char	   *err = NULL;

	if (festate == NULL || festate->hs2 == NULL || festate->sql == NULL)
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
	if (festate->result)
		impala_hs2_close_result(festate->result);
	if (festate->hs2)
		impala_hs2_close(festate->hs2);
	node->fdw_state = NULL;
}
