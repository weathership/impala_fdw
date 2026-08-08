/* Minimal HS2 smoke: OpenSession + SELECT 1 + fetch (NOSASL). */
#include "exec_impala.h"

#include <cstdio>
#include <cstdlib>

int
main(int argc, char **argv)
{
	const char *host = argc > 1 ? argv[1] : "127.0.0.1";
	int port = argc > 2 ? atoi(argv[2]) : 21050;
	char *err = NULL;

	ImpalaHs2Session *s = impala_hs2_connect(host, port, "nosasl", "signals",
											 "default", &err);
	if (!s)
	{
		fprintf(stderr, "connect failed: %s\n", err ? err : "?");
		free(err);
		return 1;
	}

	/* Impala treats bare `one` as reserved/odd; use a plain integer projection. */
	ImpalaHs2Result *r = impala_hs2_execute(s, "SELECT 1", &err);
	if (!r)
	{
		fprintf(stderr, "execute failed: %s\n", err ? err : "?");
		free(err);
		impala_hs2_close(s);
		return 1;
	}

	char **vals = NULL;
	bool *nulls = NULL;
	int nf = 0;
	int rc = impala_hs2_fetch_row(r, &vals, &nf, &nulls, &err);
	if (rc == 1)
	{
		printf("ok columns=%d value0=%s\n", nf,
			   (nulls && nulls[0]) ? "NULL" : (vals[0] ? vals[0] : ""));
		impala_hs2_free_row(vals, nulls, nf);
	}
	else
	{
		fprintf(stderr, "fetch rc=%d err=%s\n", rc, err ? err : "");
		free(err);
	}

	impala_hs2_close_result(r);
	impala_hs2_close(s);
	return rc == 1 ? 0 : 2;
}
