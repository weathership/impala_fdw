/* krb_util.c — Kerberos principal helpers (SPEC §11.3) */
#include "postgres.h"

#include "miscadmin.h"
#include "utils/acl.h"
#include "utils/builtins.h"

#include "krb_util.h"

#include <stdlib.h>
#include <string.h>

const char *
impala_fdw_user_keytab_from_env(void)
{
	const char *p = getenv("SIGNALS_KRB_USER_KEYTAB");

	if (p == NULL || p[0] == '\0')
		return NULL;
	return p;
}

char *
impala_fdw_resolve_principal(const char *mapping_principal,
							 const char *krb_realm,
							 bool auth_is_kerberos)
{
	const char *realm;
	char	   *username;

	if (!auth_is_kerberos)
		return NULL;

	if (mapping_principal != NULL && mapping_principal[0] != '\0')
		return pstrdup(mapping_principal);

	realm = krb_realm;
	if (realm == NULL || realm[0] == '\0')
	{
		realm = getenv("KRB5_REALM");
		if (realm == NULL || realm[0] == '\0')
			realm = "DEV.VISTA.ZNDX.ORG";
	}

	/*
	 * Prefer session user name as Kerberos primary (pg_ident already stripped
	 * realm on GSS login). Full GSS display-name plumbing is phase 1c/2b.
	 */
	username = GetUserNameFromId(GetUserId(), true);
	if (username != NULL && username[0] != '\0')
		return psprintf("%s@%s", username, realm);

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
			 errmsg("impala_fdw: cannot determine Kerberos principal for outbound Impala/Kudu"),
			 errhint("Authenticate to Postgres with GSSAPI, set USER MAPPING principal, "
					 "or use auth=nosasl only for CI.")));

	return NULL;				/* keep compiler calm */
}
