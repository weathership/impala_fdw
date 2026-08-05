/* krb_util.h — principal resolution for end-to-end Kerberos (SPEC §11.3) */
#ifndef IMPALA_FDW_KRB_UTIL_H
#define IMPALA_FDW_KRB_UTIL_H

/*
 * Resolve outbound Kerberos principal for Impala/Kudu.
 * Prefer GSS session identity; then role@realm; then explicit mapping.
 * Returns palloc'd string or NULL if auth=nosasl / undetermined.
 *
 * Signals posture: Kerberos is expected for real users; nosasl is CI-only.
 */
extern char *impala_fdw_resolve_principal(const char *mapping_principal,
										  const char *krb_realm,
										  bool auth_is_kerberos);

/* Optional keytab path from SecretSpec env SIGNALS_KRB_USER_KEYTAB */
extern const char *impala_fdw_user_keytab_from_env(void);

#endif
