/* Standalone HS2 GSSAPI connect (no Postgres). */
#include "exec_impala.h"
#include <cstdio>
#include <cstdlib>

int main() {
  const char *host = getenv("SIGNALS_KRB_HOST");
  if (!host || !host[0])
    host = "tinybox.dev.vista.zndx.org";
  char *err = NULL;
  ImpalaHs2Session *s = impala_hs2_connect_ex(
      host, 21050, "kerberos", "signals@DEV.VISTA.ZNDX.ORG",
      "signals_dataproducts",
      getenv("SIGNALS_KRB_USER_KEYTAB"),
      getenv("KRB5CCNAME"), &err);
  if (!s) {
    std::fprintf(stderr, "FAIL %s\n", err ? err : "unknown");
    return 1;
  }
  ImpalaHs2Result *r = impala_hs2_execute(
      s, "SELECT COUNT(*) FROM gpu_metrics_tier1", &err);
  if (!r) {
    std::fprintf(stderr, "EXEC FAIL %s\n", err ? err : "unknown");
    impala_hs2_close(s);
    return 1;
  }
  char **vals = NULL;
  bool *nulls = NULL;
  int n = 0;
  int rc = impala_hs2_fetch_row(r, &vals, &n, &nulls, &err);
  if (rc == 1 && n > 0 && vals[0])
    std::printf("OK count=%s\n", vals[0]);
  else
    std::printf("fetch rc=%d err=%s\n", rc, err ? err : "");
  impala_hs2_free_row(vals, nulls, n);
  impala_hs2_close_result(r);
  impala_hs2_close(s);
  return rc == 1 ? 0 : 1;
}
