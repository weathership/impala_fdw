/* deparse.c — Impala SQL deparse for pushable predicates (SPEC + freeze) */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/params.h"
#include "optimizer/optimizer.h"
#include "optimizer/restrictinfo.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "mb/pg_wchar.h"

#include "deparse.h"

void
impala_append_ident(StringInfo buf, const char *ident)
{
	const char *p;

	appendStringInfoChar(buf, '`');
	for (p = ident; *p; p++)
	{
		if (*p == '`')
			appendStringInfoString(buf, "``");
		else
			appendStringInfoChar(buf, *p);
	}
	appendStringInfoChar(buf, '`');
}

static bool
is_foreign_var(Expr *expr, Index relid, Var **out_var)
{
	Var		   *var;

	if (!IsA(expr, Var))
		return false;
	var = (Var *) expr;
	if (var->varlevelsup != 0)
		return false;
	if (var->varno != relid)
		return false;
	if (var->varattno <= 0)
		return false;
	*out_var = var;
	return true;
}

static bool
is_simple_const(Expr *expr)
{
	if (IsA(expr, Const))
		return true;
	/* Param not shippable in phase 1 (needs execution-time bind) */
	return false;
}

static bool
op_is_pushable_cmp(Oid opno, char **out_op)
{
	char	   *name = get_opname(opno);

	if (name == NULL)
		return false;
	if (strcmp(name, "=") == 0 ||
		strcmp(name, "<>") == 0 ||
		strcmp(name, "<") == 0 ||
		strcmp(name, "<=") == 0 ||
		strcmp(name, ">") == 0 ||
		strcmp(name, ">=") == 0)
	{
		*out_op = name;
		return true;
	}
	return false;
}

static bool
foreign_expr_walker(Node *node, Index relid)
{
	if (node == NULL)
		return true;

	if (IsA(node, Var))
	{
		Var		   *var = (Var *) node;

		if (var->varlevelsup != 0)
			return false;
		if (var->varno != relid)
			return false;
		if (var->varattno <= 0)
			return false;
		return true;
	}
	if (IsA(node, Const))
		return true;

	/*
	 * External parameters are pushable. The remote SQL and the Kudu predicate
	 * IR are both built in BeginForeignScan, by which point the executor holds
	 * the bound values, so impala_resolve_extern_params() can fold these into
	 * Consts before anything is deparsed.
	 *
	 * Without this every parameterised qual was classified local and the scan
	 * degraded to a full fetch filtered in Postgres. Measured on
	 * gpu_metrics_tier0, same 66-row window, same connection:
	 *   literals  0.02s      $1/$2  12.92s
	 * Every asyncpg / PREPARE / JDBC caller paid that.
	 *
	 * PARAM_EXEC is deliberately excluded: those come from correlated
	 * subplans and nestloop inner scans, and are not reliably bound when the
	 * scan is opened.
	 */
	if (IsA(node, Param))
		return ((Param *) node)->paramkind == PARAM_EXTERN;

	if (IsA(node, OpExpr))
	{
		OpExpr	   *op = (OpExpr *) node;
		char	   *opname;

		if (!op_is_pushable_cmp(op->opno, &opname))
			return false;
		if (list_length(op->args) != 2)
			return false;
		return foreign_expr_walker(linitial(op->args), relid) &&
			foreign_expr_walker(lsecond(op->args), relid);
	}

	if (IsA(node, ScalarArrayOpExpr))
	{
		ScalarArrayOpExpr *sa = (ScalarArrayOpExpr *) node;
		char	   *opname = get_opname(sa->opno);

		/* Only = ANY / <> ALL style with useOr true for IN / = ANY */
		if (opname == NULL || strcmp(opname, "=") != 0)
			return false;
		if (!sa->useOr)
			return false;
		if (list_length(sa->args) != 2)
			return false;
		return foreign_expr_walker(linitial(sa->args), relid) &&
			foreign_expr_walker(lsecond(sa->args), relid);
	}

	if (IsA(node, NullTest))
	{
		NullTest   *nt = (NullTest *) node;

		return foreign_expr_walker((Node *) nt->arg, relid);
	}

	if (IsA(node, BoolExpr))
	{
		BoolExpr   *be = (BoolExpr *) node;
		ListCell   *lc;

		if (be->boolop != AND_EXPR)
			return false;
		foreach(lc, be->args)
		{
			if (!foreign_expr_walker(lfirst(lc), relid))
				return false;
		}
		return true;
	}

	if (IsA(node, RelabelType))
		return foreign_expr_walker((Node *) ((RelabelType *) node)->arg, relid);

	if (IsA(node, ArrayExpr))
	{
		ArrayExpr  *ae = (ArrayExpr *) node;
		ListCell   *lc;

		/* Only constant arrays (each element Const) */
		foreach(lc, ae->elements)
		{
			if (!IsA(lfirst(lc), Const))
				return false;
		}
		return true;
	}

	return false;
}

/*
 * Replace PARAM_EXTERN nodes with the Consts the executor has bound.
 *
 * Called from BeginForeignScan, before the remote SQL or the Kudu predicate
 * IR is built, so every downstream deparse path sees only Consts and needs no
 * param awareness of its own.
 */
static Node *
resolve_extern_params_mutator(Node *node, void *context)
{
	ParamListInfo pli = (ParamListInfo) context;

	if (node == NULL)
		return NULL;

	if (IsA(node, Param))
	{
		Param	   *p = (Param *) node;
		ParamExternData *prm;
		ParamExternData workspace;

		if (p->paramkind != PARAM_EXTERN || pli == NULL)
			return node;
		if (p->paramid < 1 || p->paramid > pli->numParams)
			return node;

		if (pli->paramFetch != NULL)
			prm = pli->paramFetch(pli, p->paramid, false, &workspace);
		else
			prm = &pli->params[p->paramid - 1];

		if (prm == NULL || !OidIsValid(prm->ptype) || prm->ptype != p->paramtype)
			return node;

		return (Node *) makeConst(p->paramtype,
								  p->paramtypmod,
								  p->paramcollid,
								  get_typlen(p->paramtype),
								  prm->isnull ? (Datum) 0 : prm->value,
								  prm->isnull,
								  get_typbyval(p->paramtype));
	}

	return expression_tree_mutator(node, resolve_extern_params_mutator, context);
}

List *
impala_resolve_extern_params(List *exprs, ParamListInfo pli)
{
	if (exprs == NIL || pli == NULL)
		return exprs;
	return (List *) resolve_extern_params_mutator((Node *) exprs, (void *) pli);
}

bool
impala_is_foreign_expr(PlannerInfo *root, RelOptInfo *baserel, Expr *expr)
{
	(void) root;
	return foreign_expr_walker((Node *) expr, baserel->relid);
}

void
impala_classify_conditions(PlannerInfo *root,
						   RelOptInfo *baserel,
						   List *input_conds,
						   List **remote_conds,
						   List **local_conds)
{
	ListCell   *lc;

	*remote_conds = NIL;
	*local_conds = NIL;

	foreach(lc, input_conds)
	{
		RestrictInfo *ri = (RestrictInfo *) lfirst(lc);
		Expr	   *clause = ri->clause;

		if (impala_is_foreign_expr(root, baserel, clause))
			*remote_conds = lappend(*remote_conds, ri);
		else
			*local_conds = lappend(*local_conds, ri);
	}
}

/* ---- constant deparse ---- */

static void
deparse_string_literal(StringInfo buf, const char *val, int len)
{
	const char *p;
	const char *end;

	appendStringInfoChar(buf, '\'');
	end = val + len;
	for (p = val; p < end; p++)
	{
		if (*p == '\'')
			appendStringInfoString(buf, "\\'");
		else if (*p == '\\')
			appendStringInfoString(buf, "\\\\");
		else if ((unsigned char) *p < 0x20)
			appendStringInfo(buf, "\\u%04x", (unsigned char) *p);
		else
			appendStringInfoChar(buf, *p);
	}
	appendStringInfoChar(buf, '\'');
}

static void
deparse_bytea_const(StringInfo buf, Datum d)
{
	bytea	   *b = DatumGetByteaPP(d);
	char	   *data = VARDATA_ANY(b);
	int			len = VARSIZE_ANY_EXHDR(b);
	int			i;

	/*
	 * Impala BINARY compares need CAST(unhex('…') AS BINARY). Bare X'…'
	 * is treated as a string literal and fails against BINARY columns.
	 */
	appendStringInfoString(buf, "CAST(unhex('");
	for (i = 0; i < len; i++)
		appendStringInfo(buf, "%02X", (unsigned char) data[i]);
	appendStringInfoString(buf, "') AS BINARY)");
}

static bool
deparse_const(StringInfo buf, Const *c)
{
	Oid			typoutput;
	bool		typIsVarlena;
	char	   *extval;

	if (c->constisnull)
	{
		appendStringInfoString(buf, "NULL");
		return true;
	}

	switch (c->consttype)
	{
		case BOOLOID:
			appendStringInfoString(buf,
								   DatumGetBool(c->constvalue) ? "TRUE" : "FALSE");
			return true;
		case INT2OID:
			appendStringInfo(buf, "%d", (int) DatumGetInt16(c->constvalue));
			return true;
		case INT4OID:
			appendStringInfo(buf, "%d", DatumGetInt32(c->constvalue));
			return true;
		case INT8OID:
			appendStringInfo(buf, INT64_FORMAT, (int64) DatumGetInt64(c->constvalue));
			return true;
		case FLOAT4OID:
		case FLOAT8OID:
		case NUMERICOID:
			getTypeOutputInfo(c->consttype, &typoutput, &typIsVarlena);
			extval = OidOutputFunctionCall(typoutput, c->constvalue);
			appendStringInfoString(buf, extval);
			pfree(extval);
			return true;
		case TEXTOID:
		case VARCHAROID:
		case BPCHAROID:
		case NAMEOID:
			getTypeOutputInfo(c->consttype, &typoutput, &typIsVarlena);
			extval = OidOutputFunctionCall(typoutput, c->constvalue);
			deparse_string_literal(buf, extval, strlen(extval));
			pfree(extval);
			return true;
		case BYTEAOID:
			deparse_bytea_const(buf, c->constvalue);
			return true;
		default:
			/* try generic text output quoted */
			getTypeOutputInfo(c->consttype, &typoutput, &typIsVarlena);
			extval = OidOutputFunctionCall(typoutput, c->constvalue);
			deparse_string_literal(buf, extval, strlen(extval));
			pfree(extval);
			return true;
	}
}

static bool
deparse_var(StringInfo buf, Var *var, Relation rel)
{
	TupleDesc	tupdesc = RelationGetDescr(rel);
	Form_pg_attribute attr;

	if (var->varattno <= 0 || var->varattno > tupdesc->natts)
		return false;
	attr = TupleDescAttr(tupdesc, var->varattno - 1);
	if (attr->attisdropped)
		return false;
	impala_append_ident(buf, NameStr(attr->attname));
	return true;
}

static bool deparse_node(StringInfo buf, Node *node, Relation rel);

static bool
deparse_array_elements(StringInfo buf, List *elements, Relation rel)
{
	ListCell   *lc;
	bool		first = true;

	appendStringInfoChar(buf, '(');
	foreach(lc, elements)
	{
		if (!first)
			appendStringInfoString(buf, ", ");
		first = false;
		if (!deparse_node(buf, lfirst(lc), rel))
			return false;
	}
	appendStringInfoChar(buf, ')');
	return true;
}

static bool
deparse_const_array(StringInfo buf, Const *c)
{
	ArrayType  *arr;
	Oid			elemtype;
	int16		typlen;
	bool		typbyval;
	char		typalign;
	Datum	   *elems;
	bool	   *nulls;
	int			nelems;
	int			i;

	if (c->constisnull)
		return false;

	arr = DatumGetArrayTypeP(c->constvalue);
	elemtype = ARR_ELEMTYPE(arr);
	get_typlenbyvalalign(elemtype, &typlen, &typbyval, &typalign);
	deconstruct_array(arr, elemtype, typlen, typbyval, typalign,
					  &elems, &nulls, &nelems);

	appendStringInfoChar(buf, '(');
	for (i = 0; i < nelems; i++)
	{
		Const		nconst;

		if (i > 0)
			appendStringInfoString(buf, ", ");
		memset(&nconst, 0, sizeof(nconst));
		nconst.xpr.type = T_Const;
		nconst.consttype = elemtype;
		nconst.consttypmod = -1;
		nconst.constcollid = InvalidOid;
		nconst.constlen = typlen;
		nconst.constvalue = elems[i];
		nconst.constisnull = nulls[i];
		nconst.constbyval = typbyval;
		if (!deparse_const(buf, &nconst))
			return false;
	}
	appendStringInfoChar(buf, ')');
	return true;
}

static bool
deparse_node(StringInfo buf, Node *node, Relation rel)
{
	if (node == NULL)
		return false;

	if (IsA(node, Var))
		return deparse_var(buf, (Var *) node, rel);

	if (IsA(node, Const))
	{
		/* Array const used as IN-list right side */
		if (type_is_array(((Const *) node)->consttype))
			return deparse_const_array(buf, (Const *) node);
		return deparse_const(buf, (Const *) node);
	}

	if (IsA(node, RelabelType))
		return deparse_node(buf, (Node *) ((RelabelType *) node)->arg, rel);

	if (IsA(node, OpExpr))
	{
		OpExpr	   *op = (OpExpr *) node;
		char	   *opname;
		Node	   *left = linitial(op->args);
		Node	   *right = lsecond(op->args);

		if (!op_is_pushable_cmp(op->opno, &opname))
			return false;
		/* Prefer column on left */
		if (IsA(right, Var) && is_simple_const((Expr *) left))
		{
			Node	   *tmp = left;

			left = right;
			right = tmp;
			if (strcmp(opname, "<") == 0)
				opname = ">";
			else if (strcmp(opname, "<=") == 0)
				opname = ">=";
			else if (strcmp(opname, ">") == 0)
				opname = "<";
			else if (strcmp(opname, ">=") == 0)
				opname = "<=";
		}
		if (!deparse_node(buf, left, rel))
			return false;
		appendStringInfo(buf, " %s ", opname);
		if (!deparse_node(buf, right, rel))
			return false;
		return true;
	}

	if (IsA(node, ScalarArrayOpExpr))
	{
		ScalarArrayOpExpr *sa = (ScalarArrayOpExpr *) node;
		Node	   *left = linitial(sa->args);
		Node	   *right = lsecond(sa->args);

		if (!deparse_node(buf, left, rel))
			return false;
		appendStringInfoString(buf, " IN ");
		if (IsA(right, Const))
			return deparse_const_array(buf, (Const *) right);
		if (IsA(right, ArrayExpr))
			return deparse_array_elements(buf, ((ArrayExpr *) right)->elements, rel);
		return false;
	}

	if (IsA(node, NullTest))
	{
		NullTest   *nt = (NullTest *) node;

		if (!deparse_node(buf, (Node *) nt->arg, rel))
			return false;
		if (nt->nulltesttype == IS_NULL)
			appendStringInfoString(buf, " IS NULL");
		else
			appendStringInfoString(buf, " IS NOT NULL");
		return true;
	}

	if (IsA(node, BoolExpr))
	{
		BoolExpr   *be = (BoolExpr *) node;
		ListCell   *lc;
		bool		first = true;

		if (be->boolop != AND_EXPR)
			return false;
		appendStringInfoChar(buf, '(');
		foreach(lc, be->args)
		{
			if (!first)
				appendStringInfoString(buf, " AND ");
			first = false;
			if (!deparse_node(buf, lfirst(lc), rel))
				return false;
		}
		appendStringInfoChar(buf, ')');
		return true;
	}

	if (IsA(node, ArrayExpr))
		return deparse_array_elements(buf, ((ArrayExpr *) node)->elements, rel);

	return false;
}

bool
impala_deparse_expr(StringInfo buf, Expr *expr, Relation rel)
{
	return deparse_node(buf, (Node *) expr, rel);
}

char *
impala_build_select_sql(Relation rel,
						const char *database,
						const char *table,
						List *retrieved_attrs,
						List *remote_conds,
						int64 limit_count)
{
	StringInfoData buf;
	TupleDesc	tupdesc = RelationGetDescr(rel);
	ListCell   *lc;
	bool		first = true;

	initStringInfo(&buf);
	appendStringInfoString(&buf, "SELECT ");

	if (retrieved_attrs == NIL)
	{
		appendStringInfoString(&buf, "*");
	}
	else
	{
		foreach(lc, retrieved_attrs)
		{
			int			attno = lfirst_int(lc);
			Form_pg_attribute attr;

			if (attno <= 0 || attno > tupdesc->natts)
				continue;
			attr = TupleDescAttr(tupdesc, attno - 1);
			if (attr->attisdropped)
				continue;
			if (!first)
				appendStringInfoString(&buf, ", ");
			first = false;
			impala_append_ident(&buf, NameStr(attr->attname));
		}
		if (first)
			appendStringInfoString(&buf, "*");
	}

	appendStringInfoString(&buf, " FROM ");
	impala_append_ident(&buf, database ? database : "default");
	appendStringInfoChar(&buf, '.');
	impala_append_ident(&buf, table ? table : "dual");

	if (remote_conds != NIL)
	{
		bool		wfirst = true;

		appendStringInfoString(&buf, " WHERE ");
		foreach(lc, remote_conds)
		{
			Node	   *n = lfirst(lc);
			Expr	   *expr;
			StringInfoData piece;

			if (IsA(n, RestrictInfo))
				expr = ((RestrictInfo *) n)->clause;
			else
				expr = (Expr *) n;

			initStringInfo(&piece);
			if (!impala_deparse_expr(&piece, expr, rel))
			{
				/* skip unshippable (should not happen if classified) */
				continue;
			}
			if (!wfirst)
				appendStringInfoString(&buf, " AND ");
			wfirst = false;
			appendStringInfoString(&buf, piece.data);
		}
		if (wfirst)
		{
			/* all failed — strip WHERE */
			buf.len -= 7;		/* " WHERE " */
			buf.data[buf.len] = '\0';
		}
	}

	if (limit_count >= 0)
		appendStringInfo(&buf, " LIMIT %lld", (long long) limit_count);

	return buf.data;
}
