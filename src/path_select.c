/* path_select.c — dual-path selector (SPEC §6–§7, PR-K3 strict) */
#include "postgres.h"

#include <string.h>

#include "path_select.h"

void
impala_fdw_select_path(const char *forced_access,
					   bool enable_kudu,
					   bool has_local_quals,
					   bool limit_pushable,
					   int64_t limit_count,
					   int sample_max,
					   bool full_pk_eq,
					   bool unique_lookup,
					   bool allowlist_filtered,
					   bool remote_empty,
					   ImpalaFdwAccessMethod *out_method,
					   const char **out_shape_id)
{
	ImpalaFdwAccessMethod method = IMPALA_FDW_ACCESS_IMPALA_SQL;
	const char *shape = IMPALA_FDW_SHAPE_SQL_GENERAL;
	bool		auto_mode;
	bool		force_kudu;
	bool		force_hs2;

	if (forced_access == NULL)
		forced_access = "auto";

	auto_mode = (strcmp(forced_access, "auto") == 0);
	force_kudu = (strcmp(forced_access, "kudu_scan") == 0);
	force_hs2 = (strcmp(forced_access, "impala_sql") == 0);

	/* ── Shape recognition (independent of force) ─────────────────────── */
	if (full_pk_eq)
		shape = IMPALA_FDW_SHAPE_GOV_PK_LOOKUP;
	else if (unique_lookup)
		shape = IMPALA_FDW_SHAPE_GOV_UNIQUE_LOOKUP;
	else if (allowlist_filtered && !remote_empty)
		shape = IMPALA_FDW_SHAPE_GOV_FILTERED_SCAN;
	/* column_sample: pushable LIMIT only, no remote quals (N5 dead disjuncts removed) */
	else if (limit_pushable && limit_count >= 0 &&
			 limit_count <= (int64_t) sample_max &&
			 remote_empty)
		shape = IMPALA_FDW_SHAPE_GOV_COLUMN_SAMPLE;
	else if (remote_empty && !limit_pushable)
		shape = IMPALA_FDW_SHAPE_GOV_PROJECTION;
	else if (!has_local_quals && !remote_empty)
		/* pushable but not allowlist/PK — still filtered_scan shape; may stay HS2 */
		shape = IMPALA_FDW_SHAPE_GOV_FILTERED_SCAN;
	else
		shape = IMPALA_FDW_SHAPE_SQL_GENERAL;

	/* ── Method selection ─────────────────────────────────────────────── */
	if (force_hs2)
	{
		method = IMPALA_FDW_ACCESS_IMPALA_SQL;
	}
	else if (force_kudu)
	{
		/* Forced: attempt kudu; Begin enforces KD16 on residuals / compile */
		method = IMPALA_FDW_ACCESS_KUDU_SCAN;
	}
	else if (auto_mode && enable_kudu && !has_local_quals)
	{
		/*
		 * Auto promote only closed shapes with full remote residual NIL.
		 * Incomplete PK is never labeled pk_lookup (handled in recognition).
		 */
		if (shape == IMPALA_FDW_SHAPE_GOV_PK_LOOKUP ||
			shape == IMPALA_FDW_SHAPE_GOV_UNIQUE_LOOKUP ||
			shape == IMPALA_FDW_SHAPE_GOV_COLUMN_SAMPLE ||
			(shape == IMPALA_FDW_SHAPE_GOV_FILTERED_SCAN && allowlist_filtered))
			method = IMPALA_FDW_ACCESS_KUDU_SCAN;
		else
			method = IMPALA_FDW_ACCESS_IMPALA_SQL;
	}
	else
	{
		method = IMPALA_FDW_ACCESS_IMPALA_SQL;
		if (auto_mode && has_local_quals)
			shape = IMPALA_FDW_SHAPE_SQL_GENERAL;
	}

	*out_method = method;
	*out_shape_id = shape;
}
