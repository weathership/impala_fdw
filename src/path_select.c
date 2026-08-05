/* path_select.c — dual-path selector stub (SPEC §6–§7) */
#include "postgres.h"

#include <string.h>

#include "path_select.h"

void
impala_fdw_select_path(const char *forced_access,
					   bool has_limit,
					   bool quals_are_pk_eq,
					   ImpalaFdwAccessMethod *out_method,
					   const char **out_shape_id)
{
	ImpalaFdwAccessMethod method = IMPALA_FDW_ACCESS_IMPALA_SQL;
	const char *shape = IMPALA_FDW_SHAPE_SQL_GENERAL;

	if (forced_access != NULL)
	{
		if (strcmp(forced_access, "kudu_scan") == 0)
			method = IMPALA_FDW_ACCESS_KUDU_SCAN;
		else if (strcmp(forced_access, "impala_sql") == 0)
			method = IMPALA_FDW_ACCESS_IMPALA_SQL;
		/* auto or unknown → fall through */
	}

	/* Minimal promotion until full classifier (phase 3–4) */
	if (method == IMPALA_FDW_ACCESS_IMPALA_SQL ||
		(forced_access != NULL && strcmp(forced_access, "auto") == 0) ||
		forced_access == NULL)
	{
		if (quals_are_pk_eq)
		{
			shape = IMPALA_FDW_SHAPE_GOV_PK_LOOKUP;
			if (forced_access == NULL || strcmp(forced_access, "auto") == 0)
				method = IMPALA_FDW_ACCESS_KUDU_SCAN;
		}
		else if (has_limit)
		{
			shape = IMPALA_FDW_SHAPE_GOV_COLUMN_SAMPLE;
			if (forced_access == NULL || strcmp(forced_access, "auto") == 0)
				method = IMPALA_FDW_ACCESS_KUDU_SCAN;
		}
	}

	if (forced_access != NULL && strcmp(forced_access, "kudu_scan") == 0)
		method = IMPALA_FDW_ACCESS_KUDU_SCAN;
	if (forced_access != NULL && strcmp(forced_access, "impala_sql") == 0)
		method = IMPALA_FDW_ACCESS_IMPALA_SQL;

	*out_method = method;
	*out_shape_id = shape;
}
