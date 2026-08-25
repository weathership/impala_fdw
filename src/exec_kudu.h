/*-------------------------------------------------------------------------
 *
 * exec_kudu.h
 *    C API for direct Kudu scans (libkudu_client) — dual-path FDW executor.
 *    See docs/kudu_scan.md (PR-K0–K5).
 *
 *-------------------------------------------------------------------------
 */
#ifndef IMPALA_FDW_EXEC_KUDU_H
#define IMPALA_FDW_EXEC_KUDU_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef struct ImpalaKuduScan ImpalaKuduScan;

typedef enum
{
	KUDU_PRED_EQ = 0,
	KUDU_PRED_NE,
	KUDU_PRED_LT,
	KUDU_PRED_LE,
	KUDU_PRED_GT,
	KUDU_PRED_GE,
	KUDU_PRED_IN,
	KUDU_PRED_IS_NULL,
	KUDU_PRED_IS_NOT_NULL
} ImpalaKuduPredOp;

typedef struct ImpalaKuduPred
{
	const char	   *column;		/* Kudu column name (after kudu_column resolve) */
	ImpalaKuduPredOp op;
	int				nvalues;	/* 0 for null-tests; empty IN handled above open */
	unsigned int	value_type; /* PG Oid */
	const void	  **value_ptrs; /* typed payloads; see kudu_scan.md encode table */
	int			   *value_lens; /* BYTEA / string byte length */
} ImpalaKuduPred;

/*
 * PR-K5b: outbound auth for libkudu_client.
 * mode: "nosasl" (default) or "kerberos".
 * principal: optional; used for cache key + logging (e.g. signals@REALM).
 * ccache: optional KRB5CCNAME (FILE:/path or DIR:…); if NULL, process env used.
 * keytab: optional; if set with principal, kinit into ccache before Build.
 * sasl_protocol: Kudu SPN primary; NULL → "kudu".
 */
typedef struct ImpalaKuduAuth
{
	const char *mode;
	const char *principal;
	const char *ccache;
	const char *keytab;
	const char *sasl_protocol;
} ImpalaKuduAuth;

/*
 * Open a Kudu scan.
 *
 * masters: comma-separated "host:port" list.
 * kudu_table: resolved Kudu table name (e.g. impala::atlas.edge_out).
 * columns / ncolumns: projected column names (Kudu names).
 * preds / npreds: borrowed for this call only; deep-copied into Kudu objects.
 * npreds < 0: open an empty-result scan (empty IN; no RPC).
 * auth: NULL or mode nosasl → unauthenticated client (lab).
 * err: on failure, malloc'd message (caller free).
 *
 * Returns NULL on error.
 */
ImpalaKuduScan *impala_kudu_scan_open(const char *masters,
									  const char *kudu_table,
									  const char **columns, int ncolumns,
									  const ImpalaKuduPred *preds, int npreds,
									  int64_t limit,
									  const ImpalaKuduAuth *auth,
									  char **err);

/*
 * Fetch next row. values/nulls are malloc'd; free with impala_kudu_scan_free_row.
 * Returns 1 if row, 0 if exhausted, -1 on error.
 */
int				impala_kudu_scan_next(ImpalaKuduScan *s,
									  char ***values, bool **nulls, int *nfields,
									  char **err);

void			impala_kudu_scan_free_row(char **values, bool *nulls, int nfields);
void			impala_kudu_scan_close(ImpalaKuduScan *s);

/* True if this build was linked with libkudu_client (IMPALA_FDW_WITH_KUDU). */
bool			impala_kudu_scan_available(void);

/*
 * INSERT/UPSERT (SPEC v0.5). Typed cells from PG Datums.
 * type_oid: INT2/4/8, FLOAT4/8, BOOLOID, TEXTOID, BYTEAOID.
 */
typedef struct ImpalaKuduCell
{
	unsigned int	type_oid;
	int				isnull;
	int64_t			i64;
	double			f8;
	const char	   *ptr;		/* NUMERIC: exact decimal text (numeric_out) */
	int				len;
} ImpalaKuduCell;

typedef struct ImpalaKuduModify ImpalaKuduModify;

ImpalaKuduModify *impala_kudu_modify_open(const char *masters,
										  const char *kudu_table,
										  const char **columns, int ncolumns,
										  const ImpalaKuduAuth *auth,
										  char **err);
int				impala_kudu_modify_upsert(ImpalaKuduModify *m,
										  const ImpalaKuduCell *cells,
										  int nfields,
										  char **err);
int				impala_kudu_modify_flush(ImpalaKuduModify *m, char **err);
void			impala_kudu_modify_close(ImpalaKuduModify *m);

#ifdef __cplusplus
}
#endif

#endif							/* IMPALA_FDW_EXEC_KUDU_H */
