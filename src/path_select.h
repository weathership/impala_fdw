/* path_select.h — shape id + access method for impala_fdw (SPEC §6–§7) */
#ifndef IMPALA_FDW_PATH_SELECT_H
#define IMPALA_FDW_PATH_SELECT_H

typedef enum ImpalaFdwAccessMethod
{
	IMPALA_FDW_ACCESS_AUTO = 0,
	IMPALA_FDW_ACCESS_IMPALA_SQL,
	IMPALA_FDW_ACCESS_KUDU_SCAN
} ImpalaFdwAccessMethod;

/* Stable shape ids (SPEC §6.1) */
#define IMPALA_FDW_SHAPE_GOV_PK_LOOKUP		"gov.pk_lookup"
#define IMPALA_FDW_SHAPE_GOV_UNIQUE_LOOKUP	"gov.unique_lookup"
#define IMPALA_FDW_SHAPE_GOV_FILTERED_SCAN	"gov.filtered_scan"
#define IMPALA_FDW_SHAPE_GOV_COLUMN_SAMPLE	"gov.column_sample"
#define IMPALA_FDW_SHAPE_GOV_PROJECTION		"gov.projection_only"
#define IMPALA_FDW_SHAPE_GOV_SCHEMA		"gov.schema"
#define IMPALA_FDW_SHAPE_SQL_GENERAL		"sql.general"

/*
 * Classify a foreign scan. Implementation filled in phase 2–4.
 * Returns shape id string (constant) and chosen access method.
 */
extern void impala_fdw_select_path(const char *forced_access,
								   bool has_limit,
								   bool quals_are_pk_eq,
								   ImpalaFdwAccessMethod *out_method,
								   const char **out_shape_id);

#endif
