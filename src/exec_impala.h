/*-------------------------------------------------------------------------
 *
 * exec_impala.h
 *    C API for Impala HS2 (TCLIService) — phase 1 NOSASL; Kerberos later.
 *
 *-------------------------------------------------------------------------
 */
#ifndef IMPALA_FDW_EXEC_IMPALA_H
#define IMPALA_FDW_EXEC_IMPALA_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include <stddef.h>

typedef struct ImpalaHs2Session ImpalaHs2Session;
typedef struct ImpalaHs2Result ImpalaHs2Result;

/*
 * Connect and OpenSession.
 * auth: "nosasl" | "kerberos" (kerberos returns error until SASL wired).
 * errbuf: optional; if non-NULL and fail, malloc'd message (caller free).
 */
ImpalaHs2Session *impala_hs2_connect(const char *host, int port,
									 const char *auth,
									 const char *principal,
									 const char *database,
									 char **errbuf);

/* Execute SQL; returns result handle or NULL. Session remains open. */
ImpalaHs2Result *impala_hs2_execute(ImpalaHs2Session *session,
									const char *sql,
									char **errbuf);

/*
 * Fetch next row into values[] as C strings (malloc'd; free with
 * impala_hs2_free_row). nfields set to column count.
 * Returns 1 if row, 0 if exhausted, -1 on error.
 */
int impala_hs2_fetch_row(ImpalaHs2Result *result,
						 char ***values, int *nfields,
						 bool **nulls,
						 char **errbuf);

void impala_hs2_free_row(char **values, bool *nulls, int nfields);

void impala_hs2_close_result(ImpalaHs2Result *result);
void impala_hs2_close(ImpalaHs2Session *session);

#ifdef __cplusplus
}
#endif

#endif
