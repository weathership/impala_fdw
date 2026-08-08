/*-------------------------------------------------------------------------
 *
 * kudu_pred.h
 *    Build ImpalaKuduPred[] from PG remote_exprs (PR-K2).
 *
 *-------------------------------------------------------------------------
 */
#ifndef IMPALA_FDW_KUDU_PRED_H
#define IMPALA_FDW_KUDU_PRED_H

#include "postgres.h"

#include "nodes/pg_list.h"
#include "utils/rel.h"

#include "exec_kudu.h"

/* Soft cap for IN-list size (frontier B=256 safe; design 4096). */
#define IMPALA_KUDU_IN_LIST_MAX 4096

/*
 * Compile remote_exprs into palloc'd ImpalaKuduPred array.
 *
 * Returns true on success.
 *   *preds_out / *npreds_out — may be NULL / 0 if no preds (projection-only).
 *   *empty_result — true if an empty IN/ANY was found (no rows; skip Kudu RPC).
 * On false: *err_out set to palloc'd message when non-NULL (caller pfree).
 *
 * Pred IR is valid only for the duration of BeginForeignScan → open; open
 * deep-copies into Kudu objects.
 */
bool		impala_build_kudu_preds(Relation rel,
									List *remote_exprs,
									ImpalaKuduPred **preds_out,
									int *npreds_out,
									bool *empty_result,
									char **err_out);

#endif							/* IMPALA_FDW_KUDU_PRED_H */
