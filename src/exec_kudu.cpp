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
#include <kudu/client/write_op.h>
#include <kudu/util/int128.h>
#include <kudu/util/monotime.h>
#include <kudu/util/status.h>

#include <krb5.h>

#include <algorithm>
#include <cctype>
#include <climits>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
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
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::KuduUpsert;
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
	std::vector<KuduColumnSchema> col_schemas;
	std::vector<std::string> col_names;
};

struct ImpalaKuduModify
{
	std::string cache_key;
	shared_ptr<KuduClient> client;
	shared_ptr<KuduTable> table;
	shared_ptr<KuduSession> session;
	int ncolumns;
	std::vector<std::string> col_names;
	std::vector<KuduColumnSchema::DataType> col_types;
	std::vector<int8_t> col_scales;	/* DECIMAL columns only; 0 otherwise */
	int pending;
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

static bool
scalar_type_supported(KuduColumnSchema::DataType t)
{
	switch (t)
	{
		case KuduColumnSchema::BOOL:
		case KuduColumnSchema::INT8:
		case KuduColumnSchema::INT16:
		case KuduColumnSchema::INT32:
		case KuduColumnSchema::INT64:
		case KuduColumnSchema::FLOAT:
		case KuduColumnSchema::DOUBLE:
		case KuduColumnSchema::STRING:
		case KuduColumnSchema::BINARY:
		case KuduColumnSchema::UNIXTIME_MICROS:
		case KuduColumnSchema::DECIMAL:
		case KuduColumnSchema::VARCHAR:
		case KuduColumnSchema::DATE:
		case KuduColumnSchema::SERIAL:
			return true;
		default:
			return false;
	}
}

/* All Kudu scalars plus 1D ARRAY (NESTED). Nested-of-nested is rejected. */
static bool
type_supported(const KuduColumnSchema &col)
{
	if (col.type() != KuduColumnSchema::NESTED)
		return scalar_type_supported(col.type());
	const KuduColumnSchema::KuduNestedTypeDescriptor *nt = col.nested_type();
	if (nt == NULL || !nt->is_array() || nt->array() == NULL)
		return false;
	if (nt->array()->nested_type() != NULL)
		return false;
	/* SERIAL is UINT64; there is no public GetArrayUInt64. */
	if (nt->array()->type() == KuduColumnSchema::SERIAL)
		return false;
	/* Kudu does not store DECIMAL128 (precision > 18) as 1D arrays. */
	if (nt->array()->type() == KuduColumnSchema::DECIMAL &&
		col.type_attributes().precision() > 18)
		return false;
	return scalar_type_supported(nt->array()->type());
}

/* PostgreSQL array-element quoting (backslash-escape, not SQL ident). */
static std::string
pg_array_quote_string(const std::string &s)
{
	std::string o = "\"";
	for (char c : s)
	{
		if (c == '"' || c == '\\')
			o += '\\';
		o += c;
	}
	o += '"';
	return o;
}

static std::string
format_unscaled_decimal(kudu::int128_t unscaled, int scale)
{
	if (scale < 0)
		scale = 0;
	const bool neg = unscaled < 0;
	unsigned __int128 mag;
	if (!neg)
		mag = static_cast<unsigned __int128>(unscaled);
	else if (unscaled == kudu::INT128_MIN)
		mag = static_cast<unsigned __int128>(1) << 127;
	else
		mag = static_cast<unsigned __int128>(-unscaled);

	std::string digits;
	if (mag == 0)
		digits = "0";
	else
	{
		while (mag > 0)
		{
			digits.push_back(static_cast<char>('0' + static_cast<int>(mag % 10)));
			mag /= 10;
		}
		std::reverse(digits.begin(), digits.end());
	}
	if (scale == 0)
		return (neg ? "-" : "") + digits;
	if (static_cast<int>(digits.size()) <= scale)
		digits.insert(0, static_cast<size_t>(scale - static_cast<int>(digits.size()) + 1),
					  '0');
	const size_t split = digits.size() - static_cast<size_t>(scale);
	return (neg ? "-" : "") + digits.substr(0, split) + "." + digits.substr(split);
}

static std::string
pg_array_literal(const std::vector<std::string> &elems,
				 const std::vector<bool> &validity)
{
	std::string o = "{";
	for (size_t i = 0; i < elems.size(); i++)
	{
		if (i > 0)
			o += ",";
		/* Empty validity bitmap means every element is valid (Kudu client). */
		if (!validity.empty() && (i >= validity.size() || !validity[i]))
			o += "NULL";
		else
			o += elems[i];
	}
	o += "}";
	return o;
}

static std::string
format_unix_micros(int64_t us)
{
	time_t sec = static_cast<time_t>(us / 1000000LL);
	long usec = static_cast<long>(us % 1000000LL);
	if (usec < 0)
	{
		sec -= 1;
		usec += 1000000L;
	}
	struct tm tm;
	memset(&tm, 0, sizeof(tm));
	gmtime_r(&sec, &tm);
	char buf[40];
	snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d.%06ld+00",
			 tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour,
			 tm.tm_min, tm.tm_sec, usec);
	return buf;
}

static std::string
format_unix_days(int32_t days)
{
	time_t sec = static_cast<time_t>(days) * 86400;
	struct tm tm;
	memset(&tm, 0, sizeof(tm));
	gmtime_r(&sec, &tm);
	char buf[16];
	snprintf(buf, sizeof(buf), "%04d-%02d-%02d", tm.tm_year + 1900,
			 tm.tm_mon + 1, tm.tm_mday);
	return buf;
}

static Status
array_cell_to_pg(const KuduScanBatch::RowPtr &row, int col_idx,
				 const KuduColumnSchema &col, std::string *out)
{
	const KuduColumnSchema::KuduNestedTypeDescriptor *nt = col.nested_type();
	if (nt == NULL || !nt->is_array() || nt->array() == NULL)
		return Status::NotSupported("NESTED column is not a 1D array");
	const KuduColumnSchema::DataType elem = nt->array()->type();
	std::vector<bool> validity;
	std::vector<std::string> elems;
	Status st;
	switch (elem)
	{
		case KuduColumnSchema::BOOL:
		{
			std::vector<bool> data;
			st = row.GetArrayBool(col_idx, &data, &validity);
			if (!st.ok())
				return st;
			elems.reserve(data.size());
			for (size_t i = 0; i < data.size(); i++)
				elems.push_back(data[i] ? "t" : "f");
			break;
		}
		case KuduColumnSchema::INT8:
		{
			std::vector<int8_t> data;
			st = row.GetArrayInt8(col_idx, &data, &validity);
			if (!st.ok())
				return st;
			for (int8_t v : data)
				elems.push_back(std::to_string(static_cast<int>(v)));
			break;
		}
		case KuduColumnSchema::INT16:
		{
			std::vector<int16_t> data;
			st = row.GetArrayInt16(col_idx, &data, &validity);
			if (!st.ok())
				return st;
			for (int16_t v : data)
				elems.push_back(std::to_string(static_cast<int>(v)));
			break;
		}
		case KuduColumnSchema::INT32:
		{
			std::vector<int32_t> data;
			st = row.GetArrayInt32(col_idx, &data, &validity);
			if (!st.ok())
				return st;
			for (int32_t v : data)
				elems.push_back(std::to_string(v));
			break;
		}
		case KuduColumnSchema::INT64:
		{
			std::vector<int64_t> data;
			st = row.GetArrayInt64(col_idx, &data, &validity);
			if (!st.ok())
				return st;
			for (int64_t v : data)
				elems.push_back(std::to_string(v));
			break;
		}
		case KuduColumnSchema::FLOAT:
		{
			std::vector<float> data;
			st = row.GetArrayFloat(col_idx, &data, &validity);
			if (!st.ok())
				return st;
			for (float v : data)
				elems.push_back(std::to_string(v));
			break;
		}
		case KuduColumnSchema::DOUBLE:
		{
			std::vector<double> data;
			st = row.GetArrayDouble(col_idx, &data, &validity);
			if (!st.ok())
				return st;
			for (double v : data)
				elems.push_back(std::to_string(v));
			break;
		}
		case KuduColumnSchema::UNIXTIME_MICROS:
		{
			std::vector<int64_t> data;
			st = row.GetArrayUnixTimeMicros(col_idx, &data, &validity);
			if (!st.ok())
				return st;
			for (int64_t v : data)
				elems.push_back(pg_array_quote_string(format_unix_micros(v)));
			break;
		}
		case KuduColumnSchema::DATE:
		{
			std::vector<int32_t> data;
			st = row.GetArrayDate(col_idx, &data, &validity);
			if (!st.ok())
				return st;
			for (int32_t v : data)
				elems.push_back(pg_array_quote_string(format_unix_days(v)));
			break;
		}
		case KuduColumnSchema::STRING:
		case KuduColumnSchema::VARCHAR:
		{
			std::vector<Slice> data;
			if (elem == KuduColumnSchema::VARCHAR)
				st = row.GetArrayVarchar(col_idx, &data, &validity);
			else
				st = row.GetArrayString(col_idx, &data, &validity);
			if (!st.ok())
				return st;
			for (const Slice &sl : data)
				elems.push_back(pg_array_quote_string(std::string(
					reinterpret_cast<const char *>(sl.data()), sl.size())));
			break;
		}
		case KuduColumnSchema::BINARY:
		{
			std::vector<Slice> data;
			st = row.GetArrayBinary(col_idx, &data, &validity);
			if (!st.ok())
				return st;
			for (const Slice &sl : data)
				elems.push_back(pg_array_quote_string(
					bytes_to_pg_hex(sl.data(), sl.size())));
			break;
		}
		case KuduColumnSchema::DECIMAL:
		{
			const int8_t prec = col.type_attributes().precision();
			const int8_t scale = col.type_attributes().scale();
			if (prec <= 9)
			{
				std::vector<int32_t> data;
				st = row.GetArrayUnscaledDecimal(col_idx, &data, &validity);
				if (!st.ok())
					return st;
				for (int32_t v : data)
					elems.push_back(format_unscaled_decimal(v, scale));
			}
			else if (prec <= 18)
			{
				std::vector<int64_t> data;
				st = row.GetArrayUnscaledDecimal(col_idx, &data, &validity);
				if (!st.ok())
					return st;
				for (int64_t v : data)
					elems.push_back(format_unscaled_decimal(v, scale));
			}
			else
				return Status::NotSupported(
					"DECIMAL128 1D arrays are not supported by Kudu");
			break;
		}
		default:
			return Status::NotSupported(
				"unsupported Kudu 1D array element type");
	}
	*out = pg_array_literal(elems, validity);
	return Status::OK();
}

static Status
cell_to_pg_string(const KuduScanBatch::RowPtr &row, int col_idx,
				  const KuduColumnSchema &col, std::string *out)
{
	out->clear();
	if (row.IsNull(col_idx))
		return Status::OK(); /* caller treats empty + null flag */

	const KuduColumnSchema::DataType dtype = col.type();
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
		case KuduColumnSchema::INT16:
		{
			int16_t v = 0;
			Status st = row.GetInt16(col_idx, &v);
			if (!st.ok())
				return st;
			*out = std::to_string(static_cast<int>(v));
			return Status::OK();
		}
		case KuduColumnSchema::INT32:
		{
			int32_t v = 0;
			Status st = row.GetInt32(col_idx, &v);
			if (!st.ok())
				return st;
			*out = std::to_string(v);
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
		case KuduColumnSchema::FLOAT:
		{
			float v = 0;
			Status st = row.GetFloat(col_idx, &v);
			if (!st.ok())
				return st;
			*out = std::to_string(v);
			return Status::OK();
		}
		case KuduColumnSchema::DOUBLE:
		{
			double v = 0;
			Status st = row.GetDouble(col_idx, &v);
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
		case KuduColumnSchema::VARCHAR:
		{
			Slice sl;
			Status st = row.GetVarchar(col_idx, &sl);
			if (!st.ok())
				return st;
			out->assign(reinterpret_cast<const char *>(sl.data()), sl.size());
			return Status::OK();
		}
		case KuduColumnSchema::UNIXTIME_MICROS:
		{
			int64_t v = 0;
			Status st = row.GetUnixTimeMicros(col_idx, &v);
			if (!st.ok())
				return st;
			*out = format_unix_micros(v);
			return Status::OK();
		}
		case KuduColumnSchema::DATE:
		{
			int32_t v = 0;
			Status st = row.GetDate(col_idx, &v);
			if (!st.ok())
				return st;
			*out = format_unix_days(v);
			return Status::OK();
		}
		case KuduColumnSchema::DECIMAL:
		{
#if KUDU_INT128_SUPPORTED
			kudu::int128_t v = 0;
			Status st = row.GetUnscaledDecimal(col_idx, &v);
			if (!st.ok())
				return st;
			*out = format_unscaled_decimal(v, col.type_attributes().scale());
			return Status::OK();
#else
			return Status::NotSupported("DECIMAL requires int128");
#endif
		}
		case KuduColumnSchema::SERIAL:
		{
			/* Stored as UINT64; no public GetUInt64 on RowPtr. */
			const uint64_t v =
				*reinterpret_cast<const uint64_t *>(row.cell(col_idx));
			*out = std::to_string(v);
			return Status::OK();
		}
		case KuduColumnSchema::NESTED:
			return array_cell_to_pg(row, col_idx, col, out);
		default:
			return Status::NotSupported(
				std::string("unsupported Kudu column type ") +
				KuduColumnSchema::DataTypeToString(dtype));
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
				KuduColumnSchema::DataType *out,
				int8_t *precision, int8_t *scale)
{
	for (size_t ci = 0; ci < schema.num_columns(); ci++)
	{
		if (schema.Column(ci).name() == name)
		{
			const KuduColumnSchema &c = schema.Column(ci);

			*out = c.type();
			/*
			 * DECIMAL is an unscaled integer plus (precision, scale) held on
			 * the column, not the value -- the predicate path needs both to
			 * rescale a constant, so surface them here rather than reopening
			 * the schema at each call site.
			 */
			if (precision != NULL && scale != NULL &&
				c.type() == KuduColumnSchema::DECIMAL)
			{
				*precision = c.type_attributes().precision();
				*scale = c.type_attributes().scale();
			}
			return true;
		}
	}
	return false;
}


/*
 * ---------------------------------------------------------------------------
 * DECIMAL predicate support.
 *
 * Kudu stores DECIMAL as an unscaled integer at a scale fixed by the column.
 * kudu_pred.c ships the constant as exact decimal TEXT because the PG side
 * cannot see the Kudu scale; the rescale happens here against the schema.
 *
 * Correctness note: impala_fdw does NOT recheck remote quals locally
 * (GetForeignPlan puts an expr in remote_exprs XOR local_exprs), so a bound
 * that is off by one ulp yields WRONG ROWS, not merely slow ones. When the
 * constant is not representable at the column scale, the bound must therefore
 * be rounded in the direction that preserves the predicate's meaning:
 *
 *   with F = floor(c * 10^scale), C = ceil(c * 10^scale)
 *
 *     col >  c   ->   col >  F        col >= c   ->   col >= C
 *     col <  c   ->   col <  C        col <= c   ->   col <= F
 *     col =  c   ->   col =  F  only when F == C, else matches nothing
 *
 * Both > and <= take F; both >= and < take C. That holds whether or not c is
 * representable, which is why no separate representable/not branch is needed
 * for the inequalities.
 * ---------------------------------------------------------------------------
 */

/* Largest magnitude an int128 can hold; decimal precision caps at 38 digits. */
static const int kDecMaxDigits = 38;

static bool
dec_pow10(int e, __int128 *out)
{
	__int128 r = 1;

	if (e < 0 || e > kDecMaxDigits)
		return false;
	for (int i = 0; i < e; i++)
		r *= 10;
	*out = r;
	return true;
}

/* 10^precision - 1: the largest unscaled magnitude the column can store. */
static bool
dec_col_max(int8_t precision, __int128 *out)
{
	__int128 p;

	if (!dec_pow10(static_cast<int>(precision), &p))
		return false;
	*out = p - 1;
	return true;
}

/*
 * Parse exact decimal text into (unscaled, scale) with value = unscaled*10^-scale.
 * Accepts optional sign, digits, one '.', and an optional exponent. Rejects
 * anything else -- NaN/Inf are already refused in kudu_pred.c.
 */
static bool
dec_parse(const char *s, int len, __int128 *unscaled, int *scale,
		  std::string *err)
{
	int			i = 0;
	bool		neg = false;
	bool		seen_dot = false;
	bool		seen_digit = false;
	int			frac = 0;
	int			digits = 0;
	int			exp = 0;
	__int128	mant = 0;

	if (s == NULL || len <= 0)
	{
		*err = "empty decimal constant";
		return false;
	}
	while (i < len && (s[i] == ' ' || s[i] == '\t'))
		i++;
	if (i < len && (s[i] == '+' || s[i] == '-'))
	{
		neg = (s[i] == '-');
		i++;
	}
	for (; i < len; i++)
	{
		char		ch = s[i];

		if (ch == '.')
		{
			if (seen_dot)
			{
				*err = "malformed decimal constant";
				return false;
			}
			seen_dot = true;
			continue;
		}
		if (ch == 'e' || ch == 'E')
			break;
		if (ch < '0' || ch > '9')
		{
			*err = std::string("malformed decimal constant: ") +
				std::string(s, static_cast<size_t>(len));
			return false;
		}
		seen_digit = true;
		/*
		 * Leading zeros carry no significance and must not consume the digit
		 * budget, else '0.000000001' would be refused at 38 digits.
		 */
		if (mant == 0 && ch == '0')
		{
			if (seen_dot)
				frac++;
			continue;
		}
		if (digits >= kDecMaxDigits)
		{
			*err = "decimal constant exceeds 38 significant digits";
			return false;
		}
		mant = mant * 10 + (ch - '0');
		digits++;
		if (seen_dot)
			frac++;
	}
	if (!seen_digit)
	{
		*err = "malformed decimal constant";
		return false;
	}
	if (i < len && (s[i] == 'e' || s[i] == 'E'))
	{
		bool		eneg = false;
		bool		edig = false;

		i++;
		if (i < len && (s[i] == '+' || s[i] == '-'))
		{
			eneg = (s[i] == '-');
			i++;
		}
		for (; i < len; i++)
		{
			if (s[i] < '0' || s[i] > '9')
			{
				*err = "malformed decimal exponent";
				return false;
			}
			edig = true;
			exp = exp * 10 + (s[i] - '0');
			if (exp > 1000)
			{
				*err = "decimal exponent out of range";
				return false;
			}
		}
		if (!edig)
		{
			*err = "malformed decimal exponent";
			return false;
		}
		if (eneg)
			exp = -exp;
	}
	*unscaled = neg ? -mant : mant;
	*scale = frac - exp;
	return true;
}

/*
 * Rescale (unscaled, cscale) to the column scale, returning floor and ceil of
 * the exact value * 10^col_scale. F == C exactly when the constant is
 * representable at col_scale.
 */
static bool
dec_bounds(__int128 unscaled, int cscale, int8_t col_scale,
		   __int128 *F, __int128 *C, std::string *err)
{
	int			shift = static_cast<int>(col_scale) - cscale;

	if (shift >= 0)
	{
		__int128	m;
		__int128	lim;

		if (!dec_pow10(shift, &m))
		{
			*err = "decimal rescale out of range";
			return false;
		}
		/*
		 * Guard the widening multiply before it wraps. Shift as UNSIGNED:
		 * ~(__int128)0 is -1, and >> on a negative signed value is an
		 * arithmetic shift that stays -1, which would reject every constant.
		 */
		lim = static_cast<__int128>(
			(~static_cast<unsigned __int128>(0)) >> 1);	/* INT128_MAX */
		if (m != 0 && unscaled != 0)
		{
			__int128	mag = unscaled < 0 ? -unscaled : unscaled;

			if (mag > lim / m)
			{
				*err = "decimal constant too large to rescale";
				return false;
			}
		}
		*F = *C = unscaled * m;
		return true;
	}
	else
	{
		__int128	d;
		__int128	q;
		__int128	r;

		if (!dec_pow10(-shift, &d))
		{
			/*
			 * The constant has far more fractional digits than the column
			 * keeps; its magnitude rounds to zero at col_scale.
			 */
			*F = (unscaled < 0) ? -1 : 0;
			*C = (unscaled > 0) ? 1 : 0;
			return true;
		}
		q = unscaled / d;		/* C++ truncates toward zero */
		r = unscaled % d;
		if (r == 0)
			*F = *C = q;
		else if (unscaled > 0)
		{
			*F = q;
			*C = q + 1;
		}
		else
		{
			*F = q - 1;
			*C = q;
		}
		return true;
	}
}

/*
 * Build the (op, value) pair for a DECIMAL comparison, adjusting the operator
 * when the constant is not representable or falls outside the column range.
 *
 * Out-of-range and non-representable equality are expressed as predicates that
 * are trivially false or trivially true rather than as errors, because
 * `WHERE d = 1.2345678901` on DECIMAL(18,6) is a legitimate query that must
 * return zero rows -- and because silently dropping the predicate would return
 * a superset that nothing downstream rechecks.
 *
 *   always false : col >  MAX      (no stored value exceeds MAX)
 *   always true  : col >= -MAX     (matches every non-NULL, like any
 *                                   comparison, so NULL semantics are kept)
 */
static Status
make_decimal_value(const ImpalaKuduPred *pred, int vidx,
				   int8_t precision, int8_t scale,
				   KuduPredicate::ComparisonOp in_op,
				   KuduPredicate::ComparisonOp *out_op,
				   KuduValue **out)
{
	const char *txt = static_cast<const char *>(pred->value_ptrs[vidx]);
	int			len = pred->value_lens ? pred->value_lens[vidx] : 0;
	__int128	unscaled = 0;
	__int128	F = 0;
	__int128	C = 0;
	__int128	maxv = 0;
	int			cscale = 0;
	std::string err;

	if (pred->value_type != 1700 /* NUMERICOID */)
		return Status::InvalidArgument(
			"DECIMAL column needs a numeric constant");
	if (len <= 0)
		len = txt ? static_cast<int>(strlen(txt)) : 0;
	if (!dec_parse(txt, len, &unscaled, &cscale, &err))
		return Status::InvalidArgument(err);
	if (!dec_col_max(precision, &maxv))
		return Status::InvalidArgument("unsupported DECIMAL precision");
	if (!dec_bounds(unscaled, cscale, scale, &F, &C, &err))
		return Status::InvalidArgument(err);

	/* Constant sits entirely above or below everything the column can hold. */
	if (F > maxv)
	{
		if (in_op == KuduPredicate::LESS || in_op == KuduPredicate::LESS_EQUAL)
		{
			*out_op = KuduPredicate::GREATER_EQUAL;
			*out = KuduValue::FromDecimal(-maxv, scale);
		}
		else
		{
			*out_op = KuduPredicate::GREATER;
			*out = KuduValue::FromDecimal(maxv, scale);
		}
		return Status::OK();
	}
	if (C < -maxv)
	{
		if (in_op == KuduPredicate::GREATER ||
			in_op == KuduPredicate::GREATER_EQUAL)
		{
			*out_op = KuduPredicate::GREATER_EQUAL;
			*out = KuduValue::FromDecimal(-maxv, scale);
		}
		else
		{
			*out_op = KuduPredicate::GREATER;
			*out = KuduValue::FromDecimal(maxv, scale);
		}
		return Status::OK();
	}

	switch (in_op)
	{
		case KuduPredicate::EQUAL:
			if (F != C)
			{
				/* Not representable at this scale: nothing can equal it. */
				*out_op = KuduPredicate::GREATER;
				*out = KuduValue::FromDecimal(maxv, scale);
				return Status::OK();
			}
			*out_op = KuduPredicate::EQUAL;
			*out = KuduValue::FromDecimal(F, scale);
			return Status::OK();
		case KuduPredicate::GREATER:
			*out_op = KuduPredicate::GREATER;
			*out = KuduValue::FromDecimal(F, scale);
			return Status::OK();
		case KuduPredicate::GREATER_EQUAL:
			*out_op = KuduPredicate::GREATER_EQUAL;
			*out = KuduValue::FromDecimal(C, scale);
			return Status::OK();
		case KuduPredicate::LESS:
			*out_op = KuduPredicate::LESS;
			*out = KuduValue::FromDecimal(C, scale);
			return Status::OK();
		case KuduPredicate::LESS_EQUAL:
			*out_op = KuduPredicate::LESS_EQUAL;
			*out = KuduValue::FromDecimal(F, scale);
			return Status::OK();
		default:
			return Status::InvalidArgument("bad DECIMAL comparison op");
	}
}

/*
 * IN-list member. A value that is not representable at the column scale can
 * never equal a stored value, so it is reported non-representable and dropped
 * by the caller rather than rounded into a neighbour it does not mean.
 */
static Status
make_decimal_in_value(const ImpalaKuduPred *pred, int vidx,
					  int8_t precision, int8_t scale,
					  KuduValue **out, bool *representable)
{
	const char *txt = static_cast<const char *>(pred->value_ptrs[vidx]);
	int			len = pred->value_lens ? pred->value_lens[vidx] : 0;
	__int128	unscaled = 0;
	__int128	F = 0;
	__int128	C = 0;
	__int128	maxv = 0;
	int			cscale = 0;
	std::string err;

	*representable = false;
	*out = NULL;
	if (pred->value_type != 1700 /* NUMERICOID */)
		return Status::InvalidArgument(
			"DECIMAL column needs a numeric constant");
	if (len <= 0)
		len = txt ? static_cast<int>(strlen(txt)) : 0;
	if (!dec_parse(txt, len, &unscaled, &cscale, &err))
		return Status::InvalidArgument(err);
	if (!dec_col_max(precision, &maxv))
		return Status::InvalidArgument("unsupported DECIMAL precision");
	if (!dec_bounds(unscaled, cscale, scale, &F, &C, &err))
		return Status::InvalidArgument(err);
	if (F != C || F > maxv || F < -maxv)
		return Status::OK();	/* representable stays false */
	*representable = true;
	*out = KuduValue::FromDecimal(F, scale);
	return Status::OK();
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
		case KuduColumnSchema::INT16:
		case KuduColumnSchema::INT32:
		case KuduColumnSchema::INT64:
		case KuduColumnSchema::SERIAL:
		case KuduColumnSchema::UNIXTIME_MICROS:
		case KuduColumnSchema::DATE:
		{
			int64_t v = 0;
			if (pg_ty == 21) /* INT2OID */
				v = *static_cast<const int16_t *>(ptr);
			else if (pg_ty == 23 || pg_ty == 1082) /* INT4OID / DATEOID */
				v = *static_cast<const int32_t *>(ptr);
			else if (pg_ty == 20 || pg_ty == 1114 || pg_ty == 1184)
				/* INT8OID / TIMESTAMPOID / TIMESTAMPTZOID — already epoch units */
				v = *static_cast<const int64_t *>(ptr);
			else
				return Status::InvalidArgument("integer/time column needs int Const");

			if (dtype == KuduColumnSchema::INT8 && (v < -128 || v > 127))
				return Status::InvalidArgument("value out of range for INT8/TINYINT");
			if (dtype == KuduColumnSchema::INT16 && (v < -32768 || v > 32767))
				return Status::InvalidArgument("value out of range for INT16/SMALLINT");
			if (dtype == KuduColumnSchema::INT32 &&
				(v < static_cast<int64_t>(INT32_MIN) ||
				 v > static_cast<int64_t>(INT32_MAX)))
				return Status::InvalidArgument("value out of range for INT32");
			if (dtype == KuduColumnSchema::DATE &&
				(v < static_cast<int64_t>(INT32_MIN) ||
				 v > static_cast<int64_t>(INT32_MAX)))
				return Status::InvalidArgument("value out of range for DATE");

			*out = KuduValue::FromInt(v);
			return Status::OK();
		}
		case KuduColumnSchema::FLOAT:
		{
			float v = 0;
			if (pg_ty == 700) /* FLOAT4OID */
				v = *static_cast<const float *>(ptr);
			else if (pg_ty == 701) /* FLOAT8OID */
				v = static_cast<float>(*static_cast<const double *>(ptr));
			else
				return Status::InvalidArgument("FLOAT column needs float Const");
			*out = KuduValue::FromFloat(v);
			return Status::OK();
		}
		case KuduColumnSchema::DOUBLE:
		{
			double v = 0;
			if (pg_ty == 701) /* FLOAT8OID */
				v = *static_cast<const double *>(ptr);
			else if (pg_ty == 700) /* FLOAT4OID */
				v = *static_cast<const float *>(ptr);
			else
				return Status::InvalidArgument("DOUBLE column needs float Const");
			*out = KuduValue::FromDouble(v);
			return Status::OK();
		}
		case KuduColumnSchema::STRING:
		case KuduColumnSchema::VARCHAR:
		{
			/* pack_const normalizes all text types to TEXTOID (25) */
			if (pg_ty != 25)
				return Status::InvalidArgument(
					"STRING/VARCHAR column needs text Const (TEXTOID payload)");
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
		case KuduColumnSchema::NESTED:
			return Status::NotSupported(
				"Kudu 1D ARRAY columns cannot be used as kudu_scan predicates");
		case KuduColumnSchema::DECIMAL:
			/* Handled by make_decimal_value()/make_decimal_in_value(), which
			 * need the column scale and may rewrite the comparison op. */
			return Status::InvalidArgument(
				"DECIMAL must go through make_decimal_value");
		default:
			return Status::NotSupported(
				std::string("unsupported Kudu type for predicate: ") +
				KuduColumnSchema::DataTypeToString(dtype));
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
		int8_t		dec_precision = 0;
		int8_t		dec_scale = 0;
		if (!lookup_col_type(schema, col, &dtype, &dec_precision, &dec_scale))
			return Status::NotFound(std::string("column not found: ") + col);
		bool		is_dec = (dtype == KuduColumnSchema::DECIMAL);

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
					Status st;

					if (is_dec)
					{
						bool		ok = false;

						st = make_decimal_in_value(&p, v, dec_precision,
												   dec_scale, &kv, &ok);
						if (st.ok() && !ok)
							continue;	/* cannot equal any stored value */
					}
					else
						st = make_kudu_value(&p, v, dtype, &kv);
					if (!st.ok())
					{
						for (auto *x : values)
							delete x;
						return st;
					}
					values.push_back(kv);
				}
				if (values.empty())
				{
					/*
					 * Every member was unrepresentable at the column scale, so
					 * the IN matches nothing. Emit a trivially-false predicate
					 * rather than dropping it -- remote quals are not rechecked
					 * locally, so an absent predicate would return every row.
					 */
					__int128	maxv = 0;

					if (!dec_col_max(dec_precision, &maxv))
						return Status::InvalidArgument(
							"unsupported DECIMAL precision");
					kpred = table->NewComparisonPredicate(
						col, KuduPredicate::GREATER,
						KuduValue::FromDecimal(maxv, dec_scale));
					break;
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
				Status		st;

				if (is_dec)
					st = make_decimal_value(&p, 0, dec_precision, dec_scale,
											cop, &cop, &kv);
				else
					st = make_kudu_value(&p, 0, dtype, &kv);
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
		if (!type_supported(col))
		{
			if (err)
			{
				std::ostringstream o;
				o << "impala_fdw: unsupported Kudu type "
				  << KuduColumnSchema::DataTypeToString(col.type())
				  << " for column \"" << cname << "\"";
				*err = dup_err(o.str());
			}
			return NULL;
		}
		scan->col_types.push_back(col.type());
		scan->col_schemas.push_back(col);
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
		Status st = cell_to_pg_string(row, i, s->col_schemas[i], &cell);
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

static ImpalaKuduModify *
impala_kudu_modify_open_inner(const char *masters,
							  const char *kudu_table,
							  const char **columns, int ncolumns,
							  const ImpalaKuduAuth *auth,
							  char **err)
{
	if (err)
		*err = NULL;
	if (kudu_table == NULL || kudu_table[0] == '\0')
	{
		if (err)
			*err = dup_err("impala_fdw: kudu_modify_open: empty kudu_table");
		return NULL;
	}
	std::string canon = canonical_masters(masters);
	std::string cache_key = make_client_cache_key(canon, auth);
	shared_ptr<KuduClient> client;
	KuduClientEntry *entry = NULL;
	Status st = get_or_create_client(cache_key, canon, auth, &client, &entry);
	if (!st.ok())
	{
		if (err)
			*err = dup_err(std::string("impala_fdw: kudu_modify client: ") +
						   st.ToString());
		return NULL;
	}
	shared_ptr<KuduTable> table;
	st = open_table_cached(cache_key, kudu_table, client, &table);
	if (!st.ok())
	{
		if (err)
			*err = dup_err(std::string("impala_fdw: kudu_modify OpenTable: ") +
						   st.ToString());
		return NULL;
	}
	shared_ptr<KuduSession> session = client->NewSession();
	st = session->SetFlushMode(KuduSession::AUTO_FLUSH_BACKGROUND);
	if (st.ok())
		session->SetTimeoutMillis(30000);
	if (!st.ok())
	{
		if (err)
			*err = dup_err(std::string("impala_fdw: kudu_modify session: ") +
						   st.ToString());
		return NULL;
	}

	ImpalaKuduModify *m = new ImpalaKuduModify();
	m->cache_key = cache_key;
	m->client = client;
	m->table = table;
	m->session = session;
	m->ncolumns = ncolumns;
	m->pending = 0;
	const KuduSchema &schema = table->schema();
	if (ncolumns <= 0)
	{
		m->ncolumns = static_cast<int>(schema.num_columns());
		for (int i = 0; i < m->ncolumns; i++)
		{
			KuduColumnSchema col = schema.Column(static_cast<size_t>(i));
			m->col_names.push_back(col.name());
			m->col_types.push_back(col.type());
			m->col_scales.push_back(col.type() == KuduColumnSchema::DECIMAL
										? col.type_attributes().scale() : 0);
		}
	}
	else
	{
		for (int i = 0; i < ncolumns; i++)
		{
			const char *cname = columns[i] ? columns[i] : "";
			int found = -1;
			for (int j = 0; j < static_cast<int>(schema.num_columns()); j++)
			{
				if (schema.Column(static_cast<size_t>(j)).name() == cname)
				{
					found = j;
					break;
				}
			}
			if (found < 0)
			{
				delete m;
				if (err)
					*err = dup_err(std::string("impala_fdw: unknown Kudu column ") +
								   cname);
				return NULL;
			}
			KuduColumnSchema col = schema.Column(static_cast<size_t>(found));
			m->col_names.push_back(col.name());
			m->col_types.push_back(col.type());
			m->col_scales.push_back(col.type() == KuduColumnSchema::DECIMAL
										? col.type_attributes().scale() : 0);
		}
	}
	return m;
}

ImpalaKuduModify *
impala_kudu_modify_open(const char *masters,
						const char *kudu_table,
						const char **columns, int ncolumns,
						const ImpalaKuduAuth *auth,
						char **err)
{
	try
	{
		return impala_kudu_modify_open_inner(masters, kudu_table, columns,
											 ncolumns, auth, err);
	}
	catch (const std::exception &ex)
	{
		if (err)
			*err = dup_err(std::string("impala_fdw: kudu_modify_open: ") +
						   ex.what());
		return NULL;
	}
}

static Status
set_cell(kudu::KuduPartialRow *row, const std::string &name,
		 KuduColumnSchema::DataType dtype, int8_t scale,
		 const ImpalaKuduCell &cell)
{
	if (cell.isnull)
		return row->SetNull(name);
	switch (dtype)
	{
		case KuduColumnSchema::DECIMAL:
		{
			/*
			 * NUMERIC arrives as exact decimal text (numeric_out), same as the
			 * predicate path. A write must be exact: rescale to the column
			 * scale and refuse rather than round if the value does not land on
			 * the scale. dec_parse/dec_bounds are the predicate helpers; F==C
			 * iff the constant is representable at 'scale'.
			 */
			if (cell.type_oid != 1700 /* NUMERICOID */)
				return Status::InvalidArgument(
					"DECIMAL column needs a numeric value");
			__int128 unscaled = 0, F = 0, C = 0, maxv = 0;
			int cscale = 0;
			std::string derr;
			int len = cell.len > 0 ? cell.len
								   : (cell.ptr ? (int) strlen(cell.ptr) : 0);
			if (!dec_parse(cell.ptr, len, &unscaled, &cscale, &derr))
				return Status::InvalidArgument(derr);
			if (!dec_bounds(unscaled, cscale, scale, &F, &C, &derr))
				return Status::InvalidArgument(derr);
			if (F != C)
				return Status::InvalidArgument(
					std::string("value ") +
					std::string(cell.ptr, (size_t) len) +
					" is not exact at the column scale; normalise before INSERT");
			(void) maxv;
			return row->SetUnscaledDecimal(name, F);
		}
		case KuduColumnSchema::INT8:
			return row->SetInt8(name, static_cast<int8_t>(cell.i64));
		case KuduColumnSchema::INT16:
			return row->SetInt16(name, static_cast<int16_t>(cell.i64));
		case KuduColumnSchema::INT32:
		case KuduColumnSchema::DATE:
			return row->SetInt32(name, static_cast<int32_t>(cell.i64));
		case KuduColumnSchema::INT64:
		case KuduColumnSchema::UNIXTIME_MICROS:
		case KuduColumnSchema::SERIAL:
			return row->SetInt64(name, cell.i64);
		case KuduColumnSchema::FLOAT:
			return row->SetFloat(name, static_cast<float>(cell.f8));
		case KuduColumnSchema::DOUBLE:
			return row->SetDouble(name, cell.f8);
		case KuduColumnSchema::BOOL:
			return row->SetBool(name, cell.i64 != 0);
		case KuduColumnSchema::STRING:
		case KuduColumnSchema::VARCHAR:
			return row->SetString(name, Slice(cell.ptr ? cell.ptr : "",
											  static_cast<size_t>(cell.len)));
		case KuduColumnSchema::BINARY:
			return row->SetBinary(name, Slice(cell.ptr ? cell.ptr : "",
											  static_cast<size_t>(cell.len)));
		default:
			return Status::NotSupported(
				std::string("kudu_modify: cannot INSERT column type ") +
				KuduColumnSchema::DataTypeToString(dtype));
	}
}

int
impala_kudu_modify_upsert(ImpalaKuduModify *m,
						  const ImpalaKuduCell *cells,
						  int nfields,
						  char **err)
{
	if (err)
		*err = NULL;
	if (m == NULL || m->session == NULL || m->table == NULL)
	{
		if (err)
			*err = dup_err("impala_fdw: kudu_modify_upsert: not open");
		return -1;
	}
	if (nfields != m->ncolumns)
	{
		if (err)
			*err = dup_err("impala_fdw: kudu_modify_upsert: column count mismatch");
		return -1;
	}
	try
	{
		KuduUpsert *up = m->table->NewUpsert();
		kudu::KuduPartialRow *row = up->mutable_row();
		Status st;
		for (int i = 0; i < nfields; i++)
		{
			st = set_cell(row, m->col_names[static_cast<size_t>(i)],
						  m->col_types[static_cast<size_t>(i)],
						  m->col_scales[static_cast<size_t>(i)], cells[i]);
			if (!st.ok())
			{
				delete up;
				if (err)
					*err = dup_err(std::string("impala_fdw: set ") +
								   m->col_names[static_cast<size_t>(i)] +
								   ": " + st.ToString());
				return -1;
			}
		}
		st = m->session->Apply(up);
		if (!st.ok())
		{
			if (err)
				*err = dup_err(std::string("impala_fdw: Apply: ") + st.ToString());
			return -1;
		}
		m->pending++;
		if (m->pending >= 32)
			return impala_kudu_modify_flush(m, err);
		return 0;
	}
	catch (const std::exception &ex)
	{
		if (err)
			*err = dup_err(std::string("impala_fdw: kudu_modify_upsert: ") +
						   ex.what());
		return -1;
	}
}

int
impala_kudu_modify_flush(ImpalaKuduModify *m, char **err)
{
	if (err)
		*err = NULL;
	if (m == NULL || m->session == NULL)
		return 0;
	Status st = m->session->Flush();
	m->pending = 0;
	if (!st.ok())
	{
		if (err)
			*err = dup_err(std::string("impala_fdw: kudu_modify Flush: ") +
						   st.ToString());
		return -1;
	}
	return 0;
}

void
impala_kudu_modify_close(ImpalaKuduModify *m)
{
	try
	{
		if (m == NULL)
			return;
		if (m->session)
			(void)m->session->Flush();
		delete m;
	}
	catch (...)
	{
	}
}

} /* extern "C" */
