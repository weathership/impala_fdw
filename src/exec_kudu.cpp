/*-------------------------------------------------------------------------
 *
 * exec_kudu.cpp
 *    Direct Kudu client executor for impala_fdw (libkudu_client).
 *
 *    PR-K0: link + stub
 *    PR-K1: OpenTable, projection, LIMIT, client/table cache, READ_LATEST
 *    PR-K2: predicates
 *    PR-K5b: Kerberos SASL (builder + principal/ccache/keytab cache key)
 *
 *-------------------------------------------------------------------------
 */
#include "exec_kudu.h"

#include <kudu/client/client.h>
#include <kudu/client/scan_batch.h>
#include <kudu/client/scan_predicate.h>
#include <kudu/client/schema.h>
#include <kudu/client/shared_ptr.h>
#include <kudu/client/value.h>
#include <kudu/util/monotime.h>
#include <kudu/util/status.h>

#include <krb5.h>

#include <algorithm>
#include <cctype>
#include <climits>
#include <cstdlib>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <strings.h>
#include <unistd.h>
#include <vector>

using kudu::MonoDelta;
using kudu::Slice;
using kudu::Status;
using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduPredicate;
using kudu::client::KuduScanBatch;
using kudu::client::KuduScanner;
using kudu::client::KuduSchema;
using kudu::client::KuduTable;
using kudu::client::KuduValue;
using kudu::client::sp::shared_ptr;

/* ── process-global client + OpenTable cache (per PG backend) ─────────── */

struct KuduClientEntry
{
	shared_ptr<KuduClient> client;
	int open_scans;
	std::map<std::string, shared_ptr<KuduTable>> tables;
};

static std::mutex g_kudu_mu;
static std::map<std::string, KuduClientEntry> g_kudu_clients;

struct ImpalaKuduScan
{
	std::string cache_key;	/* masters|mode|principal|ccache|keytab */
	std::string table_name;
	shared_ptr<KuduClient> client;
	shared_ptr<KuduTable> table;
	std::unique_ptr<KuduScanner> scanner;
	KuduScanBatch batch;
	int batch_idx;
	bool opened;
	bool done;
	bool empty_result;		/* empty IN — no RPC */
	int64_t limit;			/* -1 = none */
	int64_t rows_returned;
	int ncolumns;
	std::vector<KuduColumnSchema::DataType> col_types;
	std::vector<std::string> col_names;
};

static char *
dup_err(const std::string &s)
{
	return strdup(s.c_str());
}

static std::string
trim_copy(const std::string &s)
{
	size_t b = 0;
	while (b < s.size() && std::isspace(static_cast<unsigned char>(s[b])))
		b++;
	size_t e = s.size();
	while (e > b && std::isspace(static_cast<unsigned char>(s[e - 1])))
		e--;
	return s.substr(b, e - b);
}

/* split masters on ',', trim, sort, rejoin — stable cache key */
static std::string
canonical_masters(const char *masters)
{
	std::vector<std::string> parts;
	if (masters == NULL || masters[0] == '\0')
		return "127.0.0.1:7051";

	std::string s(masters);
	size_t start = 0;
	while (start < s.size())
	{
		size_t comma = s.find(',', start);
		if (comma == std::string::npos)
			comma = s.size();
		std::string p = trim_copy(s.substr(start, comma - start));
		if (!p.empty())
			parts.push_back(p);
		start = comma + 1;
	}
	if (parts.empty())
		return "127.0.0.1:7051";
	std::sort(parts.begin(), parts.end());
	std::ostringstream o;
	for (size_t i = 0; i < parts.size(); i++)
	{
		if (i)
			o << ',';
		o << parts[i];
	}
	return o.str();
}

static std::vector<std::string>
split_masters_list(const std::string &canon)
{
	std::vector<std::string> parts;
	size_t start = 0;
	while (start < canon.size())
	{
		size_t comma = canon.find(',', start);
		if (comma == std::string::npos)
			comma = canon.size();
		parts.push_back(canon.substr(start, comma - start));
		start = comma + 1;
	}
	return parts;
}

static std::string
bytes_to_pg_hex(const uint8_t *data, size_t len)
{
	static const char *H = "0123456789abcdef";
	std::string out;
	out.reserve(2 + len * 2);
	out.push_back('\\');
	out.push_back('x');
	for (size_t i = 0; i < len; i++)
	{
		unsigned char b = data[i];
		out.push_back(H[b >> 4]);
		out.push_back(H[b & 0x0f]);
	}
	return out;
}

/* v1 Atlas freeze only (F4): BOOL, INT8, INT64, STRING, BINARY */
static bool
type_supported(KuduColumnSchema::DataType t)
{
	switch (t)
	{
		case KuduColumnSchema::BOOL:
		case KuduColumnSchema::INT8:
		case KuduColumnSchema::INT64:
		case KuduColumnSchema::STRING:
		case KuduColumnSchema::BINARY:
			return true;
		default:
			return false;
	}
}

static Status
cell_to_pg_string(const KuduScanBatch::RowPtr &row, int col_idx,
				  KuduColumnSchema::DataType dtype, std::string *out)
{
	out->clear();
	if (row.IsNull(col_idx))
		return Status::OK(); /* caller treats empty + null flag */

	switch (dtype)
	{
		case KuduColumnSchema::BOOL:
		{
			bool v = false;
			Status st = row.GetBool(col_idx, &v);
			if (!st.ok())
				return st;
			*out = v ? "t" : "f";
			return Status::OK();
		}
		case KuduColumnSchema::INT8:
		{
			int8_t v = 0;
			Status st = row.GetInt8(col_idx, &v);
			if (!st.ok())
				return st;
			*out = std::to_string(static_cast<int>(v));
			return Status::OK();
		}
		case KuduColumnSchema::INT64:
		{
			int64_t v = 0;
			Status st = row.GetInt64(col_idx, &v);
			if (!st.ok())
				return st;
			*out = std::to_string(v);
			return Status::OK();
		}
		case KuduColumnSchema::STRING:
		{
			Slice sl;
			Status st = row.GetString(col_idx, &sl);
			if (!st.ok())
				return st;
			out->assign(reinterpret_cast<const char *>(sl.data()), sl.size());
			return Status::OK();
		}
		case KuduColumnSchema::BINARY:
		{
			Slice sl;
			Status st = row.GetBinary(col_idx, &sl);
			if (!st.ok())
				return st;
			*out = bytes_to_pg_hex(sl.data(), sl.size());
			return Status::OK();
		}
		default:
			return Status::NotSupported("unsupported Kudu column type for v1");
	}
}

/* ── PR-K5b: auth mode + client cache key ─────────────────────────────── */

static bool
auth_is_kerberos(const ImpalaKuduAuth *auth)
{
	if (auth == NULL || auth->mode == NULL || auth->mode[0] == '\0')
		return false;
	/* treat anything other than nosasl as kerberos (matches FDW option) */
	return strcasecmp(auth->mode, "nosasl") != 0;
}

static std::string
nz(const char *s)
{
	return (s && s[0]) ? std::string(s) : std::string();
}

/*
 * Cache key: masters|mode|principal|ccache|keytab
 * Prevents cross-principal client reuse in a multi-mapping backend.
 */
static std::string
make_client_cache_key(const std::string &canon_masters,
					  const ImpalaKuduAuth *auth)
{
	std::string mode = auth_is_kerberos(auth) ? "kerberos" : "nosasl";
	std::string principal;
	std::string ccache;
	std::string keytab;
	if (auth != NULL)
	{
		principal = nz(auth->principal);
		ccache = nz(auth->ccache);
		keytab = nz(auth->keytab);
	}
	/* If no explicit ccache, fold process KRB5CCNAME into key for kerberos */
	if (auth_is_kerberos(auth) && ccache.empty())
	{
		const char *env = getenv("KRB5CCNAME");
		if (env && env[0])
			ccache = env;
	}
	std::ostringstream o;
	o << canon_masters << '|' << mode << '|' << principal << '|' << ccache
	  << '|' << keytab;
	return o.str();
}

/*
 * Optional keytab → ccache kinit (mechanism D). Uses libkrb5.
 * If ccache_out is empty, uses FILE:/tmp/impala_fdw_krb5cc_<pid>.
 * On success, *ccache_used is set to the KRB5CCNAME value to apply for Build.
 */
static Status
kinit_from_keytab(const std::string &principal, const std::string &keytab,
				  const std::string &ccache_in, std::string *ccache_used)
{
	if (principal.empty() || keytab.empty())
		return Status::InvalidArgument("keytab kinit requires principal and keytab");

	std::string ccache = ccache_in;
	if (ccache.empty())
	{
		std::ostringstream o;
		o << "FILE:/tmp/impala_fdw_krb5cc_" << static_cast<long>(getpid());
		ccache = o.str();
	}

	krb5_context ctx = NULL;
	krb5_error_code code = krb5_init_context(&ctx);
	if (code)
		return Status::RuntimeError("krb5_init_context failed");

	krb5_principal princ = NULL;
	krb5_keytab kt = NULL;
	krb5_ccache cc = NULL;
	krb5_creds creds;
	memset(&creds, 0, sizeof(creds));
	bool creds_valid = false;

	auto cleanup = [&]() {
		if (creds_valid)
			krb5_free_cred_contents(ctx, &creds);
		if (cc)
			krb5_cc_close(ctx, cc);
		if (kt)
			krb5_kt_close(ctx, kt);
		if (princ)
			krb5_free_principal(ctx, princ);
		if (ctx)
			krb5_free_context(ctx);
	};

	code = krb5_parse_name(ctx, principal.c_str(), &princ);
	if (code)
	{
		const char *msg = krb5_get_error_message(ctx, code);
		std::string err = std::string("krb5_parse_name: ") + (msg ? msg : "?");
		krb5_free_error_message(ctx, msg);
		cleanup();
		return Status::InvalidArgument(err);
	}

	code = krb5_kt_resolve(ctx, keytab.c_str(), &kt);
	if (code)
	{
		const char *msg = krb5_get_error_message(ctx, code);
		std::string err = std::string("krb5_kt_resolve: ") + (msg ? msg : "?") +
						  " path=" + keytab;
		krb5_free_error_message(ctx, msg);
		cleanup();
		return Status::IOError(err);
	}

	code = krb5_get_init_creds_keytab(ctx, &creds, princ, kt,
									  /*start_time=*/0, /*in_tkt_service=*/NULL,
									  /*options=*/NULL);
	if (code)
	{
		const char *msg = krb5_get_error_message(ctx, code);
		std::string err = std::string("krb5_get_init_creds_keytab: ") +
						  (msg ? msg : "?");
		krb5_free_error_message(ctx, msg);
		cleanup();
		return Status::NotAuthorized(err);
	}
	creds_valid = true;

	code = krb5_cc_resolve(ctx, ccache.c_str(), &cc);
	if (code)
	{
		const char *msg = krb5_get_error_message(ctx, code);
		std::string err = std::string("krb5_cc_resolve: ") + (msg ? msg : "?");
		krb5_free_error_message(ctx, msg);
		cleanup();
		return Status::IOError(err);
	}

	code = krb5_cc_initialize(ctx, cc, princ);
	if (code)
	{
		const char *msg = krb5_get_error_message(ctx, code);
		std::string err = std::string("krb5_cc_initialize: ") + (msg ? msg : "?");
		krb5_free_error_message(ctx, msg);
		cleanup();
		return Status::IOError(err);
	}

	code = krb5_cc_store_cred(ctx, cc, &creds);
	if (code)
	{
		const char *msg = krb5_get_error_message(ctx, code);
		std::string err = std::string("krb5_cc_store_cred: ") + (msg ? msg : "?");
		krb5_free_error_message(ctx, msg);
		cleanup();
		return Status::IOError(err);
	}

	cleanup();
	*ccache_used = ccache;
	return Status::OK();
}

/* RAII restore of KRB5CCNAME around client Build. */
struct Krb5CcacheEnvGuard
{
	bool active;
	bool had_prev;
	std::string prev;

	Krb5CcacheEnvGuard() : active(false), had_prev(false) {}

	void apply(const std::string &ccache)
	{
		if (ccache.empty())
			return;
		const char *cur = getenv("KRB5CCNAME");
		if (cur)
		{
			had_prev = true;
			prev = cur;
		}
		else
			had_prev = false;
		setenv("KRB5CCNAME", ccache.c_str(), 1);
		active = true;
	}

	~Krb5CcacheEnvGuard()
	{
		if (!active)
			return;
		if (had_prev)
			setenv("KRB5CCNAME", prev.c_str(), 1);
		else
			unsetenv("KRB5CCNAME");
	}
};

static Status
get_or_create_client(const std::string &cache_key,
					 const std::string &canon_masters,
					 const ImpalaKuduAuth *auth,
					 shared_ptr<KuduClient> *out_client,
					 KuduClientEntry **out_entry)
{
	std::lock_guard<std::mutex> lock(g_kudu_mu);
	auto it = g_kudu_clients.find(cache_key);
	if (it != g_kudu_clients.end() && it->second.client)
	{
		*out_client = it->second.client;
		if (out_entry)
			*out_entry = &it->second;
		return Status::OK();
	}

	bool kerberos = auth_is_kerberos(auth);
	std::string ccache_for_build;
	std::string principal = auth ? nz(auth->principal) : std::string();
	std::string keytab = auth ? nz(auth->keytab) : std::string();
	std::string ccache_opt = auth ? nz(auth->ccache) : std::string();

	if (kerberos && !keytab.empty())
	{
		if (principal.empty())
			return Status::InvalidArgument(
				"auth=kerberos with keytab requires principal");
		Status st = kinit_from_keytab(principal, keytab, ccache_opt,
									  &ccache_for_build);
		if (!st.ok())
			return st.CloneAndPrepend("keytab kinit");
	}
	else if (kerberos && !ccache_opt.empty())
		ccache_for_build = ccache_opt;
	/* else: rely on ambient KRB5CCNAME / default ccache for kerberos */

	Krb5CcacheEnvGuard env_guard;
	if (!ccache_for_build.empty())
		env_guard.apply(ccache_for_build);

	KuduClientBuilder builder;
	builder.master_server_addrs(split_masters_list(canon_masters));
	builder.default_admin_operation_timeout(MonoDelta::FromSeconds(60));
	builder.default_rpc_timeout(MonoDelta::FromSeconds(30));

	if (kerberos)
	{
		std::string proto = (auth && auth->sasl_protocol && auth->sasl_protocol[0])
								? auth->sasl_protocol
								: "kudu";
		builder.sasl_protocol_name(proto);
		builder.require_authentication(true);
	}

	shared_ptr<KuduClient> client;
	Status st = builder.Build(&client);
	if (!st.ok())
	{
		if (kerberos)
			return st.CloneAndPrepend(
				"Kudu client Build (kerberos SASL; check KRB5CCNAME/keytab/SPN)");
		return st;
	}

	KuduClientEntry entry;
	entry.client = client;
	entry.open_scans = 0;
	auto ins = g_kudu_clients.emplace(cache_key, std::move(entry));
	*out_client = client;
	if (out_entry)
		*out_entry = &ins.first->second;
	return Status::OK();
}

static Status
open_table_cached(const std::string &cache_key, const std::string &table_name,
				  shared_ptr<KuduClient> client,
				  shared_ptr<KuduTable> *out_table)
{
	std::lock_guard<std::mutex> lock(g_kudu_mu);
	auto it = g_kudu_clients.find(cache_key);
	if (it == g_kudu_clients.end())
		return Status::IllegalState("client entry missing");

	KuduClientEntry &entry = it->second;
	auto tit = entry.tables.find(table_name);
	if (tit != entry.tables.end() && tit->second)
	{
		*out_table = tit->second;
		return Status::OK();
	}

	shared_ptr<KuduTable> table;
	Status st = client->OpenTable(table_name, &table);
	if (!st.ok())
	{
		/* drop any stale positive entry; no permanent negative cache */
		entry.tables.erase(table_name);
		return st;
	}
	entry.tables[table_name] = table;
	*out_table = table;
	return Status::OK();
}

static void
inc_open_scans(const std::string &cache_key)
{
	std::lock_guard<std::mutex> lock(g_kudu_mu);
	auto it = g_kudu_clients.find(cache_key);
	if (it != g_kudu_clients.end())
		it->second.open_scans++;
}

static void
dec_open_scans(const std::string &cache_key)
{
	std::lock_guard<std::mutex> lock(g_kudu_mu);
	auto it = g_kudu_clients.find(cache_key);
	if (it != g_kudu_clients.end() && it->second.open_scans > 0)
		it->second.open_scans--;
}

/* Find column type by name; false if missing. */
static bool
lookup_col_type(const KuduSchema &schema, const std::string &name,
				KuduColumnSchema::DataType *out)
{
	for (size_t ci = 0; ci < schema.num_columns(); ci++)
	{
		if (schema.Column(ci).name() == name)
		{
			*out = schema.Column(ci).type();
			return true;
		}
	}
	return false;
}

/*
 * Build KuduValue from typed pred payload + target Kudu column type.
 * Ownership of returned pointer is transferred to caller (then to predicate).
 */
static Status
make_kudu_value(const ImpalaKuduPred *pred, int vidx,
				KuduColumnSchema::DataType dtype, KuduValue **out)
{
	if (pred->value_ptrs == NULL || vidx < 0 || vidx >= pred->nvalues)
		return Status::InvalidArgument("missing pred value");

	const void *ptr = pred->value_ptrs[vidx];
	int len = pred->value_lens ? pred->value_lens[vidx] : 0;
	unsigned int pg_ty = pred->value_type;

	switch (dtype)
	{
		case KuduColumnSchema::BOOL:
		{
			if (pg_ty != 16 /* BOOLOID */)
				return Status::InvalidArgument("BOOL column needs boolean Const");
			*out = KuduValue::FromBool(*static_cast<const bool *>(ptr));
			return Status::OK();
		}
		case KuduColumnSchema::INT8:
		case KuduColumnSchema::INT64:
		{
			int64_t v = 0;
			if (pg_ty == 21) /* INT2OID — state smallint vs TINYINT */
				v = *static_cast<const int16_t *>(ptr);
			else if (pg_ty == 23) /* INT4OID */
				v = *static_cast<const int32_t *>(ptr);
			else if (pg_ty == 20) /* INT8OID */
				v = *static_cast<const int64_t *>(ptr);
			else
				return Status::InvalidArgument("integer column needs int Const");

			if (dtype == KuduColumnSchema::INT8 &&
				(v < -128 || v > 127))
				return Status::InvalidArgument("value out of range for INT8/TINYINT");

			*out = KuduValue::FromInt(v);
			return Status::OK();
		}
		case KuduColumnSchema::STRING:
		{
			/* pack_const normalizes all text types to TEXTOID (25) */
			if (pg_ty != 25)
				return Status::InvalidArgument(
					"STRING column needs text Const (TEXTOID payload)");
			*out = KuduValue::CopyString(
				Slice(static_cast<const char *>(ptr), static_cast<size_t>(len)));
			return Status::OK();
		}
		case KuduColumnSchema::BINARY:
		{
			if (pg_ty != 17 /* BYTEAOID */)
				return Status::InvalidArgument(
					"BINARY column needs bytea Const (raw bytes, not hex text)");
			*out = KuduValue::CopyString(
				Slice(static_cast<const uint8_t *>(ptr), static_cast<size_t>(len)));
			return Status::OK();
		}
		default:
			return Status::NotSupported("unsupported Kudu type for predicate");
	}
}

static Status
apply_predicates(KuduTable *table, KuduScanner *scanner,
				 const ImpalaKuduPred *preds, int npreds)
{
	if (npreds <= 0 || preds == NULL)
		return Status::OK();

	const KuduSchema &schema = table->schema();

	for (int i = 0; i < npreds; i++)
	{
		const ImpalaKuduPred &p = preds[i];
		if (p.column == NULL || p.column[0] == '\0')
			return Status::InvalidArgument("empty predicate column");

		std::string col(p.column);
		KuduColumnSchema::DataType dtype;
		if (!lookup_col_type(schema, col, &dtype))
			return Status::NotFound(std::string("column not found: ") + col);

		KuduPredicate *kpred = NULL;

		switch (p.op)
		{
			case KUDU_PRED_IS_NULL:
				kpred = table->NewIsNullPredicate(col);
				break;
			case KUDU_PRED_IS_NOT_NULL:
				kpred = table->NewIsNotNullPredicate(col);
				break;
			case KUDU_PRED_IN:
			{
				if (p.nvalues <= 0)
					return Status::InvalidArgument("empty IN list at apply");
				std::vector<KuduValue *> values;
				values.reserve(static_cast<size_t>(p.nvalues));
				for (int v = 0; v < p.nvalues; v++)
				{
					KuduValue *kv = NULL;
					Status st = make_kudu_value(&p, v, dtype, &kv);
					if (!st.ok())
					{
						for (auto *x : values)
							delete x;
						return st;
					}
					values.push_back(kv);
				}
				kpred = table->NewInListPredicate(col, &values);
				/* NewInListPredicate takes ownership of values */
				break;
			}
			case KUDU_PRED_EQ:
			case KUDU_PRED_LT:
			case KUDU_PRED_LE:
			case KUDU_PRED_GT:
			case KUDU_PRED_GE:
			{
				KuduPredicate::ComparisonOp cop;
				switch (p.op)
				{
					case KUDU_PRED_EQ:
						cop = KuduPredicate::EQUAL;
						break;
					case KUDU_PRED_LT:
						cop = KuduPredicate::LESS;
						break;
					case KUDU_PRED_LE:
						cop = KuduPredicate::LESS_EQUAL;
						break;
					case KUDU_PRED_GT:
						cop = KuduPredicate::GREATER;
						break;
					case KUDU_PRED_GE:
						cop = KuduPredicate::GREATER_EQUAL;
						break;
					default:
						return Status::InvalidArgument("bad comparison op");
				}
				KuduValue *kv = NULL;
				Status st = make_kudu_value(&p, 0, dtype, &kv);
				if (!st.ok())
					return st;
				kpred = table->NewComparisonPredicate(col, cop, kv);
				/* ownership of kv transferred to predicate */
				break;
			}
			case KUDU_PRED_NE:
				return Status::NotSupported(
					"<> / NOT_EQUAL not supported by Kudu ComparisonOp");
			default:
				return Status::InvalidArgument("unknown pred op");
		}

		Status st = scanner->AddConjunctPredicate(kpred);
		if (!st.ok())
			return st.CloneAndPrepend("AddConjunctPredicate");
	}
	return Status::OK();
}

extern "C" {

bool
impala_kudu_scan_available(void)
{
	return true;
}

/* Empty-result handle: no client/scanner (empty IN). */
static ImpalaKuduScan *
open_empty_scan(void)
{
	ImpalaKuduScan *scan = new ImpalaKuduScan();
	scan->batch_idx = 0;
	scan->opened = false;
	scan->done = true;
	scan->empty_result = true;
	scan->limit = -1;
	scan->rows_returned = 0;
	scan->ncolumns = 0;
	return scan;
}

static ImpalaKuduScan *
impala_kudu_scan_open_inner(const char *masters,
							const char *kudu_table,
							const char **columns, int ncolumns,
							const ImpalaKuduPred *preds, int npreds,
							int64_t limit,
							const ImpalaKuduAuth *auth,
							char **err);

ImpalaKuduScan *
impala_kudu_scan_open(const char *masters,
					  const char *kudu_table,
					  const char **columns, int ncolumns,
					  const ImpalaKuduPred *preds, int npreds,
					  int64_t limit,
					  const ImpalaKuduAuth *auth,
					  char **err)
{
	try
	{
		return impala_kudu_scan_open_inner(masters, kudu_table, columns,
										   ncolumns, preds, npreds, limit,
										   auth, err);
	}
	catch (const std::exception &ex)
	{
		if (err)
			*err = dup_err(std::string("impala_fdw: kudu_scan_open exception: ") +
						   ex.what());
		return NULL;
	}
	catch (...)
	{
		if (err)
			*err = dup_err("impala_fdw: kudu_scan_open: unknown C++ exception");
		return NULL;
	}
}

static ImpalaKuduScan *
impala_kudu_scan_open_inner(const char *masters,
							const char *kudu_table,
							const char **columns, int ncolumns,
							const ImpalaKuduPred *preds, int npreds,
							int64_t limit,
							const ImpalaKuduAuth *auth,
							char **err)
{
	if (err)
		*err = NULL;

	/*
	 * Special: npreds == -1 signals empty-result scan (empty IN) from Begin.
	 */
	if (npreds < 0)
		return open_empty_scan();

	if (kudu_table == NULL || kudu_table[0] == '\0')
	{
		if (err)
			*err = dup_err("impala_fdw: kudu_scan_open: empty kudu_table");
		return NULL;
	}

	std::string canon = canonical_masters(masters);
	std::string cache_key = make_client_cache_key(canon, auth);
	std::string tname(kudu_table);

	shared_ptr<KuduClient> client;
	Status st = get_or_create_client(cache_key, canon, auth, &client, NULL);
	if (!st.ok())
	{
		if (err)
		{
			std::ostringstream o;
			o << "impala_fdw: Kudu client Build failed for masters \"" << canon
			  << "\"";
			if (auth_is_kerberos(auth))
			{
				o << " auth=kerberos";
				if (auth && auth->principal && auth->principal[0])
					o << " principal=" << auth->principal;
			}
			o << ": " << st.ToString();
			*err = dup_err(o.str());
		}
		return NULL;
	}

	shared_ptr<KuduTable> table;
	st = open_table_cached(cache_key, tname, client, &table);
	if (!st.ok())
	{
		if (err)
		{
			std::ostringstream o;
			o << "impala_fdw: Kudu OpenTable failed for \"" << tname
			  << "\" on masters \"" << canon << "\": " << st.ToString()
			  << "\nHINT: set foreign table option kudu_table to the exact "
				 "Kudu name; verify with Impala SHOW CREATE TABLE";
			*err = dup_err(o.str());
		}
		return NULL;
	}

	std::unique_ptr<ImpalaKuduScan> scan(new ImpalaKuduScan());
	scan->cache_key = cache_key;
	scan->table_name = tname;
	scan->client = client;
	scan->table = table;
	scan->batch_idx = 0;
	scan->opened = false;
	scan->done = false;
	scan->empty_result = false;
	scan->limit = limit;
	scan->rows_returned = 0;
	scan->ncolumns = 0;

	std::vector<std::string> proj;
	if (ncolumns > 0 && columns != NULL)
	{
		for (int i = 0; i < ncolumns; i++)
		{
			if (columns[i] == NULL || columns[i][0] == '\0')
			{
				if (err)
					*err = dup_err("impala_fdw: empty projected column name");
				return NULL;
			}
			proj.emplace_back(columns[i]);
		}
	}
	else
	{
		/* project all columns */
		const KuduSchema &schema = table->schema();
		for (size_t i = 0; i < schema.num_columns(); i++)
			proj.push_back(schema.Column(i).name());
	}

	/* resolve types from table schema; reject unsupported */
	const KuduSchema &schema = table->schema();
	for (const auto &cname : proj)
	{
		int found = -1;
		for (size_t ci = 0; ci < schema.num_columns(); ci++)
		{
			if (schema.Column(ci).name() == cname)
			{
				found = static_cast<int>(ci);
				break;
			}
		}
		if (found < 0)
		{
			if (err)
			{
				std::ostringstream o;
				o << "impala_fdw: Kudu column \"" << cname
				  << "\" not found in table \"" << tname << "\"";
				*err = dup_err(o.str());
			}
			return NULL;
		}
		KuduColumnSchema col = schema.Column(static_cast<size_t>(found));
		if (!type_supported(col.type()))
		{
			if (err)
			{
				std::ostringstream o;
				o << "impala_fdw: unsupported Kudu type "
				  << KuduColumnSchema::DataTypeToString(col.type())
				  << " for column \"" << cname
				  << "\" (v1 Atlas types only)";
				*err = dup_err(o.str());
			}
			return NULL;
		}
		scan->col_types.push_back(col.type());
		scan->col_names.push_back(cname);
	}
	scan->ncolumns = static_cast<int>(proj.size());

	scan->scanner.reset(new KuduScanner(table.get()));
	st = scan->scanner->SetProjectedColumnNames(proj);
	if (!st.ok())
	{
		if (err)
			*err = dup_err("SetProjectedColumnNames: " + st.ToString());
		return NULL;
	}
	st = scan->scanner->SetReadMode(KuduScanner::READ_LATEST);
	if (!st.ok())
	{
		if (err)
			*err = dup_err("SetReadMode: " + st.ToString());
		return NULL;
	}
	st = scan->scanner->SetTimeoutMillis(60000);
	if (!st.ok())
	{
		if (err)
			*err = dup_err("SetTimeoutMillis: " + st.ToString());
		return NULL;
	}
	if (limit >= 0)
	{
		st = scan->scanner->SetLimit(limit);
		if (!st.ok())
		{
			if (err)
				*err = dup_err("SetLimit: " + st.ToString());
			return NULL;
		}
	}

	/* Predicates (PR-K2) — deep-copy values into Kudu objects */
	st = apply_predicates(table.get(), scan->scanner.get(), preds, npreds);
	if (!st.ok())
	{
		if (err)
		{
			std::ostringstream o;
			o << "impala_fdw: predicate apply failed for \"" << tname
			  << "\": " << st.ToString();
			*err = dup_err(o.str());
		}
		return NULL;
	}

	st = scan->scanner->Open();
	if (!st.ok())
	{
		if (err)
		{
			std::ostringstream o;
			o << "impala_fdw: Kudu Scanner::Open failed for \"" << tname
			  << "\" on masters \"" << canon << "\": " << st.ToString();
			*err = dup_err(o.str());
		}
		return NULL;
	}

	scan->opened = true;
	inc_open_scans(cache_key);
	return scan.release();
}

static int
impala_kudu_scan_next_inner(ImpalaKuduScan *s,
							char ***values, bool **nulls, int *nfields,
							char **err);

int
impala_kudu_scan_next(ImpalaKuduScan *s,
					  char ***values, bool **nulls, int *nfields,
					  char **err)
{
	try
	{
		return impala_kudu_scan_next_inner(s, values, nulls, nfields, err);
	}
	catch (const std::exception &ex)
	{
		if (err)
			*err = dup_err(std::string("impala_fdw: kudu_scan_next exception: ") +
						   ex.what());
		return -1;
	}
	catch (...)
	{
		if (err)
			*err = dup_err("impala_fdw: kudu_scan_next: unknown C++ exception");
		return -1;
	}
}

static int
impala_kudu_scan_next_inner(ImpalaKuduScan *s,
							char ***values, bool **nulls, int *nfields,
							char **err)
{
	if (err)
		*err = NULL;
	if (values)
		*values = NULL;
	if (nulls)
		*nulls = NULL;
	if (nfields)
		*nfields = 0;

	if (s == NULL)
	{
		if (err)
			*err = dup_err("impala_fdw: kudu_scan_next: null scan");
		return -1;
	}
	if (s->empty_result || s->done)
		return 0;
	if (!s->opened)
	{
		if (err)
			*err = dup_err("impala_fdw: kudu_scan_next: scan not open");
		return -1;
	}

	if (s->limit >= 0 && s->rows_returned >= s->limit)
	{
		s->done = true;
		return 0;
	}

	/* refill batch when exhausted */
	while (s->batch_idx >= s->batch.NumRows())
	{
		if (!s->scanner->HasMoreRows())
		{
			s->done = true;
			return 0;
		}
		Status st = s->scanner->NextBatch(&s->batch);
		if (!st.ok())
		{
			if (err)
				*err = dup_err("impala_fdw: Kudu NextBatch: " + st.ToString());
			return -1;
		}
		s->batch_idx = 0;
		if (s->batch.NumRows() == 0 && !s->scanner->HasMoreRows())
		{
			s->done = true;
			return 0;
		}
	}

	KuduScanBatch::RowPtr row = s->batch.Row(s->batch_idx);
	s->batch_idx++;

	int nf = s->ncolumns;
	char **vals = (char **)calloc(static_cast<size_t>(nf), sizeof(char *));
	bool *nls = (bool *)calloc(static_cast<size_t>(nf), sizeof(bool));
	if (vals == NULL || nls == NULL)
	{
		free(vals);
		free(nls);
		if (err)
			*err = dup_err("impala_fdw: out of memory materializing row");
		return -1;
	}

	for (int i = 0; i < nf; i++)
	{
		if (row.IsNull(i))
		{
			nls[i] = true;
			vals[i] = NULL;
			continue;
		}
		std::string cell;
		Status st = cell_to_pg_string(row, i, s->col_types[i], &cell);
		if (!st.ok())
		{
			for (int j = 0; j < i; j++)
				free(vals[j]);
			free(vals);
			free(nls);
			if (err)
				*err = dup_err("impala_fdw: cell decode: " + st.ToString());
			return -1;
		}
		nls[i] = false;
		vals[i] = strdup(cell.c_str());
		if (vals[i] == NULL)
		{
			for (int j = 0; j < i; j++)
				free(vals[j]);
			free(vals);
			free(nls);
			if (err)
				*err = dup_err("impala_fdw: out of memory for cell");
			return -1;
		}
	}

	*values = vals;
	*nulls = nls;
	*nfields = nf;
	s->rows_returned++;
	return 1;
}

void
impala_kudu_scan_free_row(char **values, bool *nulls, int nfields)
{
	if (values)
	{
		for (int i = 0; i < nfields; i++)
			free(values[i]);
		free(values);
	}
	free(nulls);
}

void
impala_kudu_scan_close(ImpalaKuduScan *s)
{
	/* N6: catch on abort path (mcxt reset callback) — no std::terminate */
	try
	{
		if (s == NULL)
			return;
		if (s->opened && s->scanner)
		{
			(void)s->scanner->Close();
			s->scanner.reset();
			dec_open_scans(s->cache_key);
			s->opened = false;
		}
		delete s;
	}
	catch (...)
	{
		/* best-effort teardown only */
	}
}

} /* extern "C" */
