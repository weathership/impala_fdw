/*-------------------------------------------------------------------------
 *
 * kudu_pred.c
 *    Compile shippable PG exprs → ImpalaKuduPred IR for libkudu_client.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "foreign/foreign.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#include "kudu_pred.h"

typedef struct PredBuildState
{
	Relation	rel;
	Oid			reloid;
	List	   *preds;			/* List of ImpalaKuduPred* */
	bool		empty_result;
	char	   *err;
} PredBuildState;

static char *
resolve_col(Relation rel, Oid reloid, AttrNumber attno)
{
	List	   *colopts;
	ListCell   *lc;
	Form_pg_attribute attr;

	colopts = GetForeignColumnOptions(reloid, attno);
	foreach(lc, colopts)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "kudu_column") == 0)
			return pstrdup(defGetString(def));
	}
	attr = TupleDescAttr(RelationGetDescr(rel), attno - 1);
	return pstrdup(NameStr(attr->attname));
}

static bool
is_foreign_var(Expr *expr, Relation rel, Var **out)
{
	Var		   *var;

	if (IsA(expr, RelabelType))
		return is_foreign_var(((RelabelType *) expr)->arg, rel, out);
	if (!IsA(expr, Var))
		return false;
	var = (Var *) expr;
	if (var->varlevelsup != 0 || var->varattno <= 0)
		return false;
	/* Foreign scan baserel: accept any Var on this relation's attnos */
	if (var->varattno > RelationGetDescr(rel)->natts)
		return false;
	*out = var;
	return true;
}

/* Flip comparison when Const is on the left (match deparse.c). */
static const char *
flip_op(const char *op)
{
	if (strcmp(op, "<") == 0)
		return ">";
	if (strcmp(op, "<=") == 0)
		return ">=";
	if (strcmp(op, ">") == 0)
		return "<";
	if (strcmp(op, ">=") == 0)
		return "<=";
	/* = and <> are symmetric */
	return op;
}

static ImpalaKuduPredOp
opname_to_pred(const char *op, bool *ok)
{
	*ok = true;
	if (strcmp(op, "=") == 0)
		return KUDU_PRED_EQ;
	if (strcmp(op, "<>") == 0)
		return KUDU_PRED_NE;
	if (strcmp(op, "<") == 0)
		return KUDU_PRED_LT;
	if (strcmp(op, "<=") == 0)
		return KUDU_PRED_LE;
	if (strcmp(op, ">") == 0)
		return KUDU_PRED_GT;
	if (strcmp(op, ">=") == 0)
		return KUDU_PRED_GE;
	*ok = false;
	return KUDU_PRED_EQ;
}

/*
 * Pack one Const into typed payload storage (palloc).
 * Returns false on null Const (use NullTest) or unsupported type.
 */
static bool
pack_const(Const *c, const void **ptr_out, int *len_out, Oid *type_out,
		   char **err)
{
	Oid			ty = c->consttype;

	if (c->constisnull)
	{
		if (err)
			*err = pstrdup("NULL Const in comparison (use IS NULL)");
		return false;
	}

	*type_out = ty;
	*len_out = 0;

	switch (ty)
	{
		case BOOLOID:
		{
			bool	   *p = (bool *) palloc(sizeof(bool));

			*p = DatumGetBool(c->constvalue);
			*ptr_out = p;
			return true;
		}
		case INT2OID:
		{
			int16	   *p = (int16 *) palloc(sizeof(int16));

			*p = DatumGetInt16(c->constvalue);
			*ptr_out = p;
			return true;
		}
		case INT4OID:
		{
			int32	   *p = (int32 *) palloc(sizeof(int32));

			*p = DatumGetInt32(c->constvalue);
			*ptr_out = p;
			return true;
		}
		case INT8OID:
		{
			int64	   *p = (int64 *) palloc(sizeof(int64));

			*p = DatumGetInt64(c->constvalue);
			*ptr_out = p;
			return true;
		}
		case FLOAT4OID:
		{
			float4	   *p = (float4 *) palloc(sizeof(float4));

			*p = DatumGetFloat4(c->constvalue);
			*ptr_out = p;
			return true;
		}
		case FLOAT8OID:
		{
			float8	   *p = (float8 *) palloc(sizeof(float8));

			*p = DatumGetFloat8(c->constvalue);
			*ptr_out = p;
			return true;
		}
		case TEXTOID:
		case VARCHAROID:
		case BPCHAROID:
		case NAMEOID:
		{
			char	   *s = TextDatumGetCString(c->constvalue);
			int			len = strlen(s);
			char	   *buf = (char *) palloc(len + 1);

			memcpy(buf, s, len + 1);
			pfree(s);
			*ptr_out = buf;
			*len_out = len;
			/* normalize to TEXTOID for encode */
			*type_out = TEXTOID;
			return true;
		}
		case BYTEAOID:
		{
			bytea	   *b = DatumGetByteaPP(c->constvalue);
			int			len = VARSIZE_ANY_EXHDR(b);
			char	   *buf = (char *) palloc(len > 0 ? len : 1);

			if (len > 0)
				memcpy(buf, VARDATA_ANY(b), len);
			*ptr_out = buf;
			*len_out = len;
			return true;
		}
		default:
			if (err)
				*err = psprintf("unsupported Const type OID %u for kudu_scan",
								ty);
			return false;
	}
}

static ImpalaKuduPred *
make_pred_base(const char *col, ImpalaKuduPredOp op, int nvalues, Oid vtype)
{
	ImpalaKuduPred *p = (ImpalaKuduPred *) palloc0(sizeof(ImpalaKuduPred));

	p->column = col;
	p->op = op;
	p->nvalues = nvalues;
	p->value_type = vtype;
	if (nvalues > 0)
	{
		p->value_ptrs = (const void **) palloc0(sizeof(void *) * nvalues);
		p->value_lens = (int *) palloc0(sizeof(int) * nvalues);
	}
	return p;
}

static bool
append_opexpr(PredBuildState *st, OpExpr *op)
{
	Expr	   *left;
	Expr	   *right;
	Var		   *var = NULL;
	Const	   *c = NULL;
	char	   *opname;
	const char *use_op;
	ImpalaKuduPredOp pop;
	bool		ok;
	const void *ptr;
	int			len;
	Oid			vtype;
	ImpalaKuduPred *pred;
	char	   *col;
	char	   *err = NULL;

	if (list_length(op->args) != 2)
	{
		st->err = pstrdup("OpExpr arity != 2");
		return false;
	}

	opname = get_opname(op->opno);
	if (opname == NULL)
	{
		st->err = pstrdup("unknown operator");
		return false;
	}

	left = (Expr *) linitial(op->args);
	right = (Expr *) lsecond(op->args);

	/* Normalize to (Var, Const) with op flip if needed */
	if (is_foreign_var(left, st->rel, &var) && IsA(right, Const))
	{
		c = (Const *) right;
		use_op = opname;
	}
	else if (IsA(left, Const) && is_foreign_var(right, st->rel, &var))
	{
		c = (Const *) left;
		use_op = flip_op(opname);
	}
	else
	{
		st->err = pstrdup("OpExpr not (Var, Const)");
		return false;
	}

	pop = opname_to_pred(use_op, &ok);
	if (!ok)
	{
		st->err = psprintf("unpushable operator %s", use_op);
		return false;
	}

	/* Kudu client has no NOT_EQUAL ComparisonOp — refuse NE for now */
	if (pop == KUDU_PRED_NE)
	{
		st->err = pstrdup("operator <> not supported on kudu_scan (use impala_sql)");
		return false;
	}

	if (!pack_const(c, &ptr, &len, &vtype, &err))
	{
		st->err = err ? err : pstrdup("pack_const failed");
		return false;
	}

	col = resolve_col(st->rel, st->reloid, var->varattno);
	pred = make_pred_base(col, pop, 1, vtype);
	pred->value_ptrs[0] = ptr;
	pred->value_lens[0] = len;
	st->preds = lappend(st->preds, pred);
	return true;
}

static bool
append_consts_from_array(PredBuildState *st, ArrayType *arr, Oid elemtype,
						 const char *col, List **vals, List **lens, Oid *out_type)
{
	Datum	   *elems;
	bool	   *nulls;
	int			nelems;
	int			i;
	int16		typlen;
	bool		typbyval;
	char		typalign;

	get_typlenbyvalalign(elemtype, &typlen, &typbyval, &typalign);
	deconstruct_array(arr, elemtype, typlen, typbyval, typalign,
					  &elems, &nulls, &nelems);

	if (nelems > IMPALA_KUDU_IN_LIST_MAX)
	{
		st->err = psprintf("IN list size %d exceeds max %d",
						   nelems, IMPALA_KUDU_IN_LIST_MAX);
		return false;
	}

	*out_type = InvalidOid;
	for (i = 0; i < nelems; i++)
	{
		Const		nconst;
		const void *ptr;
		int			len;
		Oid			vtype;
		char	   *err = NULL;

		if (nulls && nulls[i])
		{
			st->err = pstrdup("NULL element in IN list");
			return false;
		}

		MemSet(&nconst, 0, sizeof(nconst));
		nconst.xpr.type = T_Const;
		nconst.consttype = elemtype;
		nconst.consttypmod = -1;
		nconst.constcollid = InvalidOid;
		nconst.constlen = typlen;
		nconst.constvalue = elems[i];
		nconst.constisnull = false;
		nconst.constbyval = typbyval;

		if (!pack_const(&nconst, &ptr, &len, &vtype, &err))
		{
			st->err = err ? err : pstrdup("IN element pack failed");
			return false;
		}
		if (*out_type == InvalidOid)
			*out_type = vtype;
		else if (*out_type != vtype)
		{
			st->err = pstrdup("mixed types in IN list");
			return false;
		}
		*vals = lappend(*vals, (void *) ptr);
		*lens = lappend_int(*lens, len);
	}
	return true;
}

static bool
append_scalar_array(PredBuildState *st, ScalarArrayOpExpr *sa)
{
	Expr	   *left;
	Expr	   *right;
	Var		   *var = NULL;
	char	   *opname;
	char	   *col;
	List	   *val_list = NIL;
	List	   *len_list = NIL;
	Oid			vtype = InvalidOid;
	ImpalaKuduPred *pred;
	ListCell   *lc;
	int			i;
	int			n;

	opname = get_opname(sa->opno);
	if (opname == NULL || strcmp(opname, "=") != 0 || !sa->useOr)
	{
		st->err = pstrdup("ScalarArrayOpExpr must be = ANY");
		return false;
	}
	if (list_length(sa->args) != 2)
	{
		st->err = pstrdup("ScalarArrayOpExpr arity != 2");
		return false;
	}

	left = (Expr *) linitial(sa->args);
	right = (Expr *) lsecond(sa->args);

	if (!is_foreign_var(left, st->rel, &var))
	{
		st->err = pstrdup("IN/ANY left side not foreign Var");
		return false;
	}

	col = resolve_col(st->rel, st->reloid, var->varattno);

	if (IsA(right, Const))
	{
		Const	   *c = (Const *) right;
		ArrayType  *arr;
		Oid			elemtype;

		if (c->constisnull)
		{
			/* NULL array → unknown; treat as empty result */
			st->empty_result = true;
			return true;
		}
		arr = DatumGetArrayTypeP(c->constvalue);
		elemtype = ARR_ELEMTYPE(arr);
		if (ARR_NDIM(arr) == 0 || ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr)) == 0)
		{
			st->empty_result = true;
			return true;
		}
		if (!append_consts_from_array(st, arr, elemtype, col,
									  &val_list, &len_list, &vtype))
			return false;
	}
	else if (IsA(right, ArrayExpr))
	{
		ArrayExpr  *ae = (ArrayExpr *) right;
		ListCell   *elc;

		if (ae->elements == NIL)
		{
			st->empty_result = true;
			return true;
		}
		if (list_length(ae->elements) > IMPALA_KUDU_IN_LIST_MAX)
		{
			st->err = psprintf("IN list size exceeds max %d",
							   IMPALA_KUDU_IN_LIST_MAX);
			return false;
		}
		foreach(elc, ae->elements)
		{
			Const	   *ec;
			const void *ptr;
			int			len;
			Oid			vt;
			char	   *err = NULL;

			if (!IsA(lfirst(elc), Const))
			{
				st->err = pstrdup("non-Const in ArrayExpr IN list");
				return false;
			}
			ec = (Const *) lfirst(elc);
			if (!pack_const(ec, &ptr, &len, &vt, &err))
			{
				st->err = err ? err : pstrdup("ArrayExpr element pack failed");
				return false;
			}
			if (vtype == InvalidOid)
				vtype = vt;
			else if (vtype != vt)
			{
				st->err = pstrdup("mixed types in ArrayExpr IN");
				return false;
			}
			val_list = lappend(val_list, (void *) ptr);
			len_list = lappend_int(len_list, len);
		}
	}
	else
	{
		st->err = pstrdup("IN/ANY right side not Const array or ArrayExpr");
		return false;
	}

	n = list_length(val_list);
	if (n == 0)
	{
		st->empty_result = true;
		return true;
	}

	pred = make_pred_base(col, KUDU_PRED_IN, n, vtype);
	i = 0;
	foreach(lc, val_list)
	{
		pred->value_ptrs[i] = (const void *) lfirst(lc);
		pred->value_lens[i] = list_nth_int(len_list, i);
		i++;
	}
	st->preds = lappend(st->preds, pred);
	return true;
}

static bool
append_nulltest(PredBuildState *st, NullTest *nt)
{
	Var		   *var = NULL;
	char	   *col;
	ImpalaKuduPred *pred;
	ImpalaKuduPredOp pop;

	if (!is_foreign_var(nt->arg, st->rel, &var))
	{
		st->err = pstrdup("NullTest arg not foreign Var");
		return false;
	}
	if (nt->nulltesttype == IS_NULL)
		pop = KUDU_PRED_IS_NULL;
	else if (nt->nulltesttype == IS_NOT_NULL)
		pop = KUDU_PRED_IS_NOT_NULL;
	else
	{
		st->err = pstrdup("unknown NullTest type");
		return false;
	}

	col = resolve_col(st->rel, st->reloid, var->varattno);
	pred = make_pred_base(col, pop, 0, InvalidOid);
	st->preds = lappend(st->preds, pred);
	return true;
}

static bool flatten_node(PredBuildState *st, Node *node);

static bool
flatten_node(PredBuildState *st, Node *node)
{
	if (node == NULL)
		return true;

	if (IsA(node, BoolExpr))
	{
		BoolExpr   *be = (BoolExpr *) node;
		ListCell   *lc;

		if (be->boolop != AND_EXPR)
		{
			st->err = pstrdup("only AND BoolExpr supported on kudu_scan");
			return false;
		}
		foreach(lc, be->args)
		{
			if (!flatten_node(st, (Node *) lfirst(lc)))
				return false;
		}
		return true;
	}
	if (IsA(node, RelabelType))
		return flatten_node(st, (Node *) ((RelabelType *) node)->arg);
	if (IsA(node, OpExpr))
		return append_opexpr(st, (OpExpr *) node);
	if (IsA(node, ScalarArrayOpExpr))
		return append_scalar_array(st, (ScalarArrayOpExpr *) node);
	if (IsA(node, NullTest))
		return append_nulltest(st, (NullTest *) node);

	st->err = psprintf("unsupported node tag %d for kudu_scan pred",
					   (int) nodeTag(node));
	return false;
}

bool
impala_build_kudu_preds(Relation rel,
						List *remote_exprs,
						ImpalaKuduPred **preds_out,
						int *npreds_out,
						bool *empty_result,
						char **err_out)
{
	PredBuildState st;
	ListCell   *lc;
	int			n;
	int			i;
	ImpalaKuduPred *arr;

	if (preds_out)
		*preds_out = NULL;
	if (npreds_out)
		*npreds_out = 0;
	if (empty_result)
		*empty_result = false;
	if (err_out)
		*err_out = NULL;

	memset(&st, 0, sizeof(st));
	st.rel = rel;
	st.reloid = RelationGetRelid(rel);
	st.preds = NIL;
	st.empty_result = false;
	st.err = NULL;

	foreach(lc, remote_exprs)
	{
		if (!flatten_node(&st, (Node *) lfirst(lc)))
		{
			if (err_out)
				*err_out = st.err ? st.err : pstrdup("pred compile failed");
			return false;
		}
		if (st.empty_result)
		{
			if (empty_result)
				*empty_result = true;
			if (npreds_out)
				*npreds_out = 0;
			return true;
		}
	}

	n = list_length(st.preds);
	if (n == 0)
	{
		if (npreds_out)
			*npreds_out = 0;
		return true;
	}

	arr = (ImpalaKuduPred *) palloc(sizeof(ImpalaKuduPred) * n);
	i = 0;
	foreach(lc, st.preds)
	{
		ImpalaKuduPred *p = (ImpalaKuduPred *) lfirst(lc);

		arr[i++] = *p;
	}

	if (preds_out)
		*preds_out = arr;
	if (npreds_out)
		*npreds_out = n;
	return true;
}
