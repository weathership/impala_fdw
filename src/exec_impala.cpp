/*-------------------------------------------------------------------------
 *
 * exec_impala.cpp
 *    Impala HiveServer2 client via Apache Thrift TCLIService.
 *
 * Phase 1: NOSASL (TSocket + TBufferedTransport + TBinaryProtocol).
 * Kerberos (GSSAPI/SASL) is intentional follow-on once this path is solid.
 *
 *-------------------------------------------------------------------------
 */
#include "exec_impala.h"

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TSocket.h>

#include "TCLIService.h"

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::hive::service::cli::thrift;

struct ImpalaHs2Session
{
	std::shared_ptr<TTransport> transport;
	std::shared_ptr<TCLIServiceClient> client;
	TSessionHandle session;
	std::string database;
};

struct ImpalaHs2Result
{
	ImpalaHs2Session *session;
	TOperationHandle op;
	bool op_open;
	/* columnar batch buffer */
	std::vector<std::vector<std::string>> col_values;
	std::vector<std::string> col_nulls;		/* bitmaps */
	/* F5: per-column binary from GetResultSetMetadata (not content sniffing) */
	std::vector<bool> col_is_binary;
	size_t nrows;
	size_t next_row;
	size_t nfields;
	bool exhausted;
	bool has_result_set;
};

/* N3: function-pointer hook — default no interrupt (hs2-smoke safe). */
static ImpalaFdwInterruptCheckFn g_interrupt_check = NULL;

extern "C" void
ImpalaFdwSetInterruptCheck(ImpalaFdwInterruptCheckFn fn)
{
	g_interrupt_check = fn;
}

extern "C" bool
ImpalaFdwInterruptPending(void)
{
	if (g_interrupt_check)
		return g_interrupt_check();
	return false;
}

static char *
dup_err(const std::string &s)
{
	return strdup(s.c_str());
}

static bool
status_ok(const TStatus &st, std::string *err)
{
	if (st.statusCode == TStatusCode::SUCCESS_STATUS ||
		st.statusCode == TStatusCode::SUCCESS_WITH_INFO_STATUS ||
		st.statusCode == TStatusCode::STILL_EXECUTING_STATUS)
		return true;
	if (err)
	{
		std::ostringstream o;
		o << "HS2 status " << (int)st.statusCode;
		if (st.__isset.errorMessage)
			o << ": " << st.errorMessage;
		*err = o.str();
	}
	return false;
}

static bool
bit_is_null(const std::string &nulls, size_t i)
{
	if (nulls.empty())
		return false;
	size_t byte = i / 8;
	size_t bit = i % 8;
	if (byte >= nulls.size())
		return false;
	return (nulls[byte] & (1 << bit)) != 0;
}

static size_t
column_len(const TColumn &c)
{
	if (c.__isset.stringVal)
		return c.stringVal.values.size();
	if (c.__isset.boolVal)
		return c.boolVal.values.size();
	if (c.__isset.byteVal)
		return c.byteVal.values.size();
	if (c.__isset.i16Val)
		return c.i16Val.values.size();
	if (c.__isset.i32Val)
		return c.i32Val.values.size();
	if (c.__isset.i64Val)
		return c.i64Val.values.size();
	if (c.__isset.doubleVal)
		return c.doubleVal.values.size();
	if (c.__isset.binaryVal)
		return c.binaryVal.values.size();
	return 0;
}

/* Postgres bytea hex input: \xDEADBEEF (safe with embedded NULs). */
static std::string
bytes_to_pg_hex(const std::string &raw)
{
	static const char *H = "0123456789abcdef";
	std::string out;
	out.reserve(2 + raw.size() * 2);
	out.push_back('\\');
	out.push_back('x');
	for (unsigned char b : raw)
	{
		out.push_back(H[b >> 4]);
		out.push_back(H[b & 0x0f]);
	}
	return out;
}

static void
column_to_strings(const TColumn &c, std::vector<std::string> *out,
				  std::string *nulls, bool is_binary)
{
	out->clear();
	/*
	 * BINARY: prefer binaryVal thrift field; else stringVal with metadata
	 * is_binary (F5 — no content-sniffing batch hex of text columns).
	 */
	if (c.__isset.binaryVal)
	{
		*nulls = c.binaryVal.nulls;
		out->reserve(c.binaryVal.values.size());
		for (const auto &v : c.binaryVal.values)
			out->push_back(bytes_to_pg_hex(v));
		return;
	}
	if (c.__isset.stringVal)
	{
		*nulls = c.stringVal.nulls;
		if (is_binary)
		{
			out->reserve(c.stringVal.values.size());
			for (const auto &v : c.stringVal.values)
				out->push_back(bytes_to_pg_hex(v));
		}
		else
			*out = c.stringVal.values;
		return;
	}
	if (c.__isset.boolVal)
	{
		*nulls = c.boolVal.nulls;
		for (bool v : c.boolVal.values)
			out->push_back(v ? "t" : "f");
		return;
	}
	if (c.__isset.byteVal)
	{
		*nulls = c.byteVal.nulls;
		for (auto v : c.byteVal.values)
			out->push_back(std::to_string((int)v));
		return;
	}
	if (c.__isset.i16Val)
	{
		*nulls = c.i16Val.nulls;
		for (auto v : c.i16Val.values)
			out->push_back(std::to_string(v));
		return;
	}
	if (c.__isset.i32Val)
	{
		*nulls = c.i32Val.nulls;
		for (auto v : c.i32Val.values)
			out->push_back(std::to_string(v));
		return;
	}
	if (c.__isset.i64Val)
	{
		*nulls = c.i64Val.nulls;
		for (auto v : c.i64Val.values)
			out->push_back(std::to_string(v));
		return;
	}
	if (c.__isset.doubleVal)
	{
		*nulls = c.doubleVal.nulls;
		for (auto v : c.doubleVal.values)
			out->push_back(std::to_string(v));
		return;
	}
}

static int
load_batch(ImpalaHs2Result *r, std::string *err)
{
	TFetchResultsReq freq;
	freq.operationHandle = r->op;
	freq.orientation = TFetchOrientation::FETCH_NEXT;
	freq.maxRows = 1024;

	TFetchResultsResp fresp;
	try
	{
		r->session->client->FetchResults(fresp, freq);
	}
	catch (const TException &ex)
	{
		if (err)
			*err = std::string("FetchResults: ") + ex.what();
		return -1;
	}
	if (!status_ok(fresp.status, err))
		return -1;

	r->col_values.clear();
	r->col_nulls.clear();
	r->nrows = 0;
	r->next_row = 0;

	if (!fresp.__isset.results)
	{
		r->exhausted = true;
		return 0;
	}

	const TRowSet &rs = fresp.results;
	if (rs.__isset.columns && !rs.columns.empty())
	{
		r->nfields = rs.columns.size();
		r->col_values.resize(r->nfields);
		r->col_nulls.resize(r->nfields);
		size_t n = column_len(rs.columns[0]);
		for (size_t c = 0; c < r->nfields; c++)
		{
			bool is_bin = (c < r->col_is_binary.size() && r->col_is_binary[c]);

			column_to_strings(rs.columns[c], &r->col_values[c],
							  &r->col_nulls[c], is_bin);
			if (r->col_values[c].size() > n)
				n = r->col_values[c].size();
		}
		r->nrows = n;
	}
	else if (!rs.rows.empty())
	{
		/* row-oriented fallback */
		r->nfields = rs.rows[0].colVals.size();
		r->col_values.assign(r->nfields, {});
		r->col_nulls.assign(r->nfields, {});
		for (const TRow &row : rs.rows)
		{
			for (size_t c = 0; c < row.colVals.size() && c < r->nfields; c++)
			{
				const TColumnValue &cv = row.colVals[c];
				if (cv.__isset.stringVal)
				{
					r->col_values[c].push_back(cv.stringVal.value);
				}
				else if (cv.__isset.i32Val)
					r->col_values[c].push_back(std::to_string(cv.i32Val.value));
				else if (cv.__isset.i64Val)
					r->col_values[c].push_back(std::to_string(cv.i64Val.value));
				else if (cv.__isset.doubleVal)
					r->col_values[c].push_back(std::to_string(cv.doubleVal.value));
				else if (cv.__isset.boolVal)
					r->col_values[c].push_back(cv.boolVal.value ? "t" : "f");
				else
					r->col_values[c].push_back("");
			}
		}
		r->nrows = rs.rows.size();
	}
	else
	{
		if (!fresp.hasMoreRows)
			r->exhausted = true;
		return 0;
	}

	if (r->nrows == 0 && !fresp.hasMoreRows)
		r->exhausted = true;
	return 0;
}

extern "C" {

ImpalaHs2Session *
impala_hs2_connect(const char *host, int port, const char *auth,
				   const char *principal, const char *database, char **errbuf)
{
	if (errbuf)
		*errbuf = NULL;

	if (auth == NULL)
		auth = "nosasl";

	if (strcmp(auth, "kerberos") == 0)
	{
		if (errbuf)
			*errbuf = dup_err(
				"HS2 Kerberos/SASL not yet implemented; use auth=nosasl to "
				"validate the thrift path first, then enable GSSAPI");
		return NULL;
	}
	if (strcmp(auth, "nosasl") != 0)
	{
		if (errbuf)
			*errbuf = dup_err(std::string("unsupported auth: ") + auth);
		return NULL;
	}

	try
	{
		auto sock = std::make_shared<TSocket>(host ? host : "127.0.0.1", port);
		sock->setConnTimeout(10000);
		sock->setRecvTimeout(120000);
		sock->setSendTimeout(60000);
		auto transport = std::make_shared<TBufferedTransport>(sock);
		auto protocol = std::make_shared<TBinaryProtocol>(transport);
		auto client = std::make_shared<TCLIServiceClient>(protocol);
		transport->open();

		TOpenSessionReq oreq;
		oreq.client_protocol = TProtocolVersion::HIVE_CLI_SERVICE_PROTOCOL_V6;
		if (principal && principal[0])
			oreq.__set_username(principal);
		else
			oreq.__set_username("signals");
		if (database && database[0])
		{
			std::map<std::string, std::string> conf;
			conf["use:database"] = database;
			oreq.__set_configuration(conf);
		}

		TOpenSessionResp oresp;
		client->OpenSession(oresp, oreq);
		std::string err;
		if (!status_ok(oresp.status, &err))
		{
			transport->close();
			if (errbuf)
				*errbuf = dup_err(err);
			return NULL;
		}

		auto *s = new ImpalaHs2Session();
		s->transport = transport;
		s->client = client;
		s->session = oresp.sessionHandle;
		s->database = database ? database : "default";
		return s;
	}
	catch (const TException &ex)
	{
		if (errbuf)
			*errbuf = dup_err(std::string("HS2 connect: ") + ex.what());
		return NULL;
	}
}

/*
 * Wait until HS2 operation reaches a terminal state. Impala may return from
 * ExecuteStatement before DML/DDL fragments finish even with runAsync=false;
 * closing the handle early cancels the query (Query Status: Cancelled).
 */
static int
wait_operation_complete(ImpalaHs2Session *session, const TOperationHandle &op,
						std::string *err)
{
	const auto deadline =
		std::chrono::steady_clock::now() + std::chrono::seconds(120);
	for (;;)
	{
		/* F7: observe PG cancel without longjmp across C++ frames */
		if (ImpalaFdwInterruptPending())
		{
			if (err)
				*err = "GetOperationStatus: query cancel requested";
			return -1;
		}

		TGetOperationStatusReq sreq;
		sreq.operationHandle = op;
		TGetOperationStatusResp sresp;
		try
		{
			session->client->GetOperationStatus(sresp, sreq);
		}
		catch (const TException &ex)
		{
			if (err)
				*err = std::string("GetOperationStatus: ") + ex.what();
			return -1;
		}
		if (!status_ok(sresp.status, err))
			return -1;

		if (!sresp.__isset.operationState)
		{
			if (err)
				*err = "GetOperationStatus: missing operationState";
			return -1;
		}

		const TOperationState::type st = sresp.operationState;
		if (st == TOperationState::FINISHED_STATE)
			return 0;
		if (st == TOperationState::CANCELED_STATE ||
			st == TOperationState::CLOSED_STATE ||
			st == TOperationState::ERROR_STATE ||
			st == TOperationState::UKNOWN_STATE)
		{
			if (err)
			{
				std::ostringstream o;
				o << "operation failed, state=" << (int)st;
				if (sresp.__isset.errorMessage && !sresp.errorMessage.empty())
					o << ": " << sresp.errorMessage;
				*err = o.str();
			}
			return -1;
		}
		/* PENDING / RUNNING / INITIALIZED — keep polling */
		if (std::chrono::steady_clock::now() >= deadline)
		{
			if (err)
				*err = "GetOperationStatus: timed out waiting for completion";
			return -1;
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(20));
	}
}

/* Load column type flags from TGetResultSetMetadata (BINARY → hex). */
static void
load_result_metadata(ImpalaHs2Result *r)
{
	r->col_is_binary.clear();
	try
	{
		TGetResultSetMetadataReq mreq;
		mreq.operationHandle = r->op;
		TGetResultSetMetadataResp mresp;
		r->session->client->GetResultSetMetadata(mresp, mreq);
		std::string err;
		if (!status_ok(mresp.status, &err) || !mresp.__isset.schema)
			return;
		const TTableSchema &schema = mresp.schema;
		r->col_is_binary.resize(schema.columns.size(), false);
		for (size_t i = 0; i < schema.columns.size(); i++)
		{
			const TTypeDesc &td = schema.columns[i].typeDesc;
			if (td.types.empty())
				continue;
			const TTypeEntry &te = td.types[0];
			if (te.__isset.primitiveEntry &&
				te.primitiveEntry.type == TTypeId::BINARY_TYPE)
				r->col_is_binary[i] = true;
		}
	}
	catch (...)
	{
		/* leave col_is_binary empty → treat all as non-binary text */
	}
}

ImpalaHs2Result *
impala_hs2_execute(ImpalaHs2Session *session, const char *sql, char **errbuf)
{
	if (errbuf)
		*errbuf = NULL;
	if (!session || !sql)
	{
		if (errbuf)
			*errbuf = dup_err("null session or sql");
		return NULL;
	}

	try
	{
		TExecuteStatementReq ereq;
		ereq.sessionHandle = session->session;
		ereq.statement = sql;
		/* Async + explicit status poll so DML is not CloseOperation'd mid-flight. */
		ereq.__set_runAsync(true);

		TExecuteStatementResp eresp;
		session->client->ExecuteStatement(eresp, ereq);
		std::string err;
		if (!status_ok(eresp.status, &err))
		{
			if (errbuf)
				*errbuf = dup_err(err);
			return NULL;
		}

		auto *r = new ImpalaHs2Result();
		r->session = session;
		r->op = eresp.operationHandle;
		r->op_open = true;
		r->nrows = 0;
		r->next_row = 0;
		r->nfields = 0;
		r->exhausted = false;
		r->has_result_set = eresp.operationHandle.hasResultSet;

		if (wait_operation_complete(session, r->op, &err) < 0)
		{
			impala_hs2_close_result(r);
			if (errbuf)
				*errbuf = dup_err(err);
			return NULL;
		}

		if (!r->has_result_set)
		{
			r->exhausted = true;
			return r;
		}

		load_result_metadata(r);

		if (load_batch(r, &err) < 0)
		{
			impala_hs2_close_result(r);
			if (errbuf)
				*errbuf = dup_err(err);
			return NULL;
		}
		return r;
	}
	catch (const TException &ex)
	{
		if (errbuf)
			*errbuf = dup_err(std::string("ExecuteStatement: ") + ex.what());
		return NULL;
	}
}

int
impala_hs2_fetch_row(ImpalaHs2Result *result, char ***values, int *nfields,
					 bool **nulls, char **errbuf)
{
	if (errbuf)
		*errbuf = NULL;
	if (!result || !values || !nfields)
		return -1;

	if (result->exhausted && result->next_row >= result->nrows)
		return 0;

	if (result->next_row >= result->nrows)
	{
		std::string err;
		if (load_batch(result, &err) < 0)
		{
			if (errbuf)
				*errbuf = dup_err(err);
			return -1;
		}
		if (result->nrows == 0)
		{
			result->exhausted = true;
			return 0;
		}
	}

	size_t row = result->next_row++;
	size_t nf = result->nfields;
	*nfields = (int)nf;
	char **vals = (char **)calloc(nf, sizeof(char *));
	bool *nuls = (bool *)calloc(nf, sizeof(bool));
	if (!vals || !nuls)
	{
		free(vals);
		free(nuls);
		if (errbuf)
			*errbuf = dup_err("oom");
		return -1;
	}

	for (size_t c = 0; c < nf; c++)
	{
		if (bit_is_null(result->col_nulls[c], row) ||
			row >= result->col_values[c].size())
		{
			nuls[c] = true;
			vals[c] = NULL;
		}
		else
		{
			nuls[c] = false;
			vals[c] = strdup(result->col_values[c][row].c_str());
		}
	}

	*values = vals;
	if (nulls)
		*nulls = nuls;
	else
		free(nuls);
	return 1;
}

void
impala_hs2_free_row(char **values, bool *nulls, int nfields)
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
impala_hs2_close_result(ImpalaHs2Result *result)
{
	if (!result)
		return;
	if (result->op_open && result->session && result->session->client)
	{
		try
		{
			TCloseOperationReq creq;
			creq.operationHandle = result->op;
			TCloseOperationResp cresp;
			result->session->client->CloseOperation(cresp, creq);
		}
		catch (...)
		{
		}
		result->op_open = false;
	}
	delete result;
}

void
impala_hs2_close(ImpalaHs2Session *session)
{
	if (!session)
		return;
	if (session->client)
	{
		try
		{
			TCloseSessionReq creq;
			creq.sessionHandle = session->session;
			TCloseSessionResp cresp;
			session->client->CloseSession(cresp, creq);
		}
		catch (...)
		{
		}
	}
	if (session->transport && session->transport->isOpen())
	{
		try
		{
			session->transport->close();
		}
		catch (...)
		{
		}
	}
	delete session;
}

} /* extern "C" */
