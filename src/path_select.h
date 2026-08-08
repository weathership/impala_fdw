/* path_select.h — shape id + access method for impala_fdw (SPEC §6–§7, PR-K3) */
#ifndef IMPALA_FDW_PATH_SELECT_H
#define IMPALA_FDW_PATH_SELECT_H

#include <stdbool.h>
#include <stdint.h>

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
#define IMPALA_FDW_SHAPE_GOV_SCHEMA			"gov.schema"
#define IMPALA_FDW_SHAPE_SQL_GENERAL		"sql.general"

/*
 * PR-K3 strict selector.
 *
 * forced_access: "auto" | "impala_sql" | "kudu_scan" | NULL (=auto)
 * enable_kudu: GUC + build WITH_KUDU
 * has_local_quals: residual unpushable quals (blocks auto promote; forced
 *   kudu still selected — Begin ERRORs per KD16)
 * limit_pushable: Const LIMIT, no OFFSET, COUNT option, no local quals
 * limit_count: -1 if none / not pushable
 * sample_max: GUC ceiling for column_sample
 * full_pk_eq: remote quals are equality on complete known PK set
 * unique_lookup: equality on unique key (qn_digest)
 * allowlist_filtered: every remote pred column ∈ Atlas allowlist (HASH/IN hops)
 * remote_empty: no remote quals
 */
extern void impala_fdw_select_path(const char *forced_access,
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
								   const char **out_shape_id);

#endif
