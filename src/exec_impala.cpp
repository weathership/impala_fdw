/*-------------------------------------------------------------------------
 *
 * exec_impala.cpp
 *    Impala HiveServer2 client via Apache Thrift TCLIService.
 *
 * NOSASL: TSocket + TBufferedTransport + TBinaryProtocol.
 * Kerberos: GSSAPI SASL handshake on the socket fd, then TFramedTransport
 * (QOP=auth) or Hs2SaslDataTransport (SSF>0). Stubs and libthrift must share
 * a Thrift minor — generated TCLIService is 0.22.
 *
 *-------------------------------------------------------------------------
 */
#include "exec_impala.h"

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TFDTransport.h>
#include <thrift/transport/TSocket.h>

#include "TCLIService.h"

#include <sasl/sasl.h>
#include <krb5.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <cerrno>
#include <sys/socket.h>
#include <vector>

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::hive::service::cli::thrift;

struct ImpalaHs2Session
{
	std::shared_ptr<TSocket> socket;
	std::shared_ptr<TTransport> transport;
	std::shared_ptr<TCLIServiceClient> client;
	TSessionHandle session;
	std::string database;
	sasl_conn_t *sasl;
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

/* Thrift SASL framing (same as Impala TSaslTransport / Python thrift_sasl). */
enum Hs2SaslStatus
{
	HS2_SASL_START = 1,
	HS2_SASL_OK = 2,
	HS2_SASL_BAD = 3,
	HS2_SASL_ERROR = 4,
	HS2_SASL_COMPLETE = 5
};

static void
fd_write_all(int fd, const void *p, size_t n)
{
	const uint8_t *b = static_cast<const uint8_t *>(p);
	while (n > 0)
	{
		ssize_t w = send(fd, b, n, MSG_NOSIGNAL);
		if (w < 0)
		{
			if (errno == EINTR)
				continue;
			throw TTransportException(std::string("HS2 send: ") + strerror(errno));
		}
		if (w == 0)
			throw TTransportException("HS2 send: eof");
		b += static_cast<size_t>(w);
		n -= static_cast<size_t>(w);
	}
}

static void
fd_read_all(int fd, void *p, size_t n)
{
	uint8_t *b = static_cast<uint8_t *>(p);
	while (n > 0)
	{
		ssize_t r = recv(fd, b, n, 0);
		if (r < 0)
		{
			if (errno == EINTR)
				continue;
			throw TTransportException(std::string("HS2 recv: ") + strerror(errno));
		}
		if (r == 0)
			throw TTransportException("HS2 recv: eof");
		b += static_cast<size_t>(r);
		n -= static_cast<size_t>(r);
	}
}

static void
hs2_sasl_send_fd(int fd, Hs2SaslStatus st, const void *payload, uint32_t len)
{
	uint8_t hdr[5];
	hdr[0] = static_cast<uint8_t>(st);
	hdr[1] = static_cast<uint8_t>(len >> 24);
	hdr[2] = static_cast<uint8_t>(len >> 16);
	hdr[3] = static_cast<uint8_t>(len >> 8);
	hdr[4] = static_cast<uint8_t>(len);
	fd_write_all(fd, hdr, 5);
	if (len > 0 && payload != NULL)
		fd_write_all(fd, payload, len);
}

static void
hs2_sasl_recv_fd(int fd, Hs2SaslStatus *st, std::string *payload)
{
	uint8_t hdr[5];
	fd_read_all(fd, hdr, 5);
	*st = static_cast<Hs2SaslStatus>(hdr[0]);
	uint32_t len = (uint32_t(hdr[1]) << 24) | (uint32_t(hdr[2]) << 16) |
				   (uint32_t(hdr[3]) << 8) | uint32_t(hdr[4]);
	payload->assign(len, '\0');
	if (len > 0)
		fd_read_all(fd, &(*payload)[0], len);
}

static std::string
hs2_kinit_keytab(const std::string &principal, const std::string &keytab,
				 const std::string &ccache_in)
{
	std::string ccache = ccache_in;
	if (ccache.empty())
	{
		std::ostringstream o;
		o << "FILE:/tmp/impala_fdw_hs2_krb5cc_" << static_cast<long>(getpid());
		ccache = o.str();
	}
	krb5_context ctx = NULL;
	if (krb5_init_context(&ctx))
		throw std::runtime_error("krb5_init_context failed");
	krb5_principal princ = NULL;
	krb5_keytab kt = NULL;
	krb5_ccache cc = NULL;
	krb5_creds creds;
	memset(&creds, 0, sizeof(creds));
	krb5_error_code code = krb5_parse_name(ctx, principal.c_str(), &princ);
	if (!code)
		code = krb5_kt_resolve(ctx, keytab.c_str(), &kt);
	if (!code)
		code = krb5_cc_resolve(ctx, ccache.c_str(), &cc);
	if (!code)
		code = krb5_get_init_creds_keytab(ctx, &creds, princ, kt, 0, NULL, NULL);
	if (!code)
		code = krb5_cc_initialize(ctx, cc, princ);
	if (!code)
		code = krb5_cc_store_cred(ctx, cc, &creds);
	std::string err;
	if (code)
	{
		const char *msg = krb5_get_error_message(ctx, code);
		err = msg ? msg : "kinit failed";
		krb5_free_error_message(ctx, msg);
	}
	krb5_free_cred_contents(ctx, &creds);
	if (cc)
		krb5_cc_close(ctx, cc);
	if (kt)
		krb5_kt_close(ctx, kt);
	if (princ)
		krb5_free_principal(ctx, princ);
	krb5_free_context(ctx);
	if (code)
		throw std::runtime_error(std::string("HS2 kinit: ") + err);
	return ccache;
}

/*
 * After SASL handshake, HS2 frames every Thrift message as 4-byte BE length +
 * payload (Hive TSaslTransport). QOP=auth (SSF=0) is wire-identical to
 * TFramedTransport; auth-int/conf uses sasl_encode/decode around the payload.
 *
 * Inherit TTransport and override *_virt — do not use TVirtualTransport with a
 * private readAll (hides the CRTP default and produced a SIGSEGV on the first
 * OpenSession read). Keep the TSocket alive for the fd; I/O is POSIX send/recv
 * because TSocket::write after GSSAPI has faulted on this stack.
 */
class Hs2SaslDataTransport : public TTransport
{
 public:
	Hs2SaslDataTransport(std::shared_ptr<TSocket> sock, sasl_conn_t *sasl, bool wrap)
		: sock_(std::move(sock)), fd_(sock_->getSocketFD()), sasl_(sasl),
		  wrap_(wrap), rpos_(0) {}

	bool isOpen() const override { return sock_ && sock_->isOpen(); }
	bool peek() override { return isOpen(); }
	void open() override
	{
		if (sock_ && !sock_->isOpen())
			sock_->open();
		if (sock_)
			fd_ = sock_->getSocketFD();
	}
	void close() override
	{
		if (sock_)
			sock_->close();
	}
	void flush() override
	{
		if (wbuf_.empty())
			return;
		const uint8_t *payload = reinterpret_cast<const uint8_t *>(wbuf_.data());
		uint32_t plen = static_cast<uint32_t>(wbuf_.size());
		if (wrap_)
		{
			const char *out = NULL;
			unsigned outlen = 0;
			int rc = sasl_encode(sasl_, wbuf_.data(), plen, &out, &outlen);
			if (rc != SASL_OK)
				throw TTransportException(std::string("sasl_encode: ") +
										  sasl_errdetail(sasl_));
			payload = reinterpret_cast<const uint8_t *>(out);
			plen = outlen;
		}
		uint8_t hdr[4] = {
			static_cast<uint8_t>(plen >> 24), static_cast<uint8_t>(plen >> 16),
			static_cast<uint8_t>(plen >> 8), static_cast<uint8_t>(plen)
		};
		fd_write_all(fd_, hdr, 4);
		if (plen > 0)
			fd_write_all(fd_, payload, plen);
		wbuf_.clear();
	}

	uint32_t read_virt(uint8_t *buf, uint32_t len) override
	{
		if (rpos_ >= rbuf_.size())
			fill();
		uint32_t n = std::min(len, static_cast<uint32_t>(rbuf_.size() - rpos_));
		if (n > 0)
		{
			memcpy(buf, rbuf_.data() + rpos_, n);
			rpos_ += n;
		}
		return n;
	}

	void write_virt(const uint8_t *buf, uint32_t len) override
	{
		wbuf_.append(reinterpret_cast<const char *>(buf), len);
	}

 private:
	void fill()
	{
		rbuf_.clear();
		rpos_ = 0;
		uint8_t hdr[4];
		fd_read_all(fd_, hdr, 4);
		uint32_t plen = (uint32_t(hdr[0]) << 24) | (uint32_t(hdr[1]) << 16) |
						(uint32_t(hdr[2]) << 8) | uint32_t(hdr[3]);
		if (plen > 32 * 1024 * 1024)
			throw TTransportException("HS2 SASL frame too large");
		std::string raw(plen, '\0');
		if (plen > 0)
			fd_read_all(fd_, &raw[0], plen);
		if (wrap_)
		{
			const char *out = NULL;
			unsigned outlen = 0;
			int rc = sasl_decode(sasl_, raw.data(), plen, &out, &outlen);
			if (rc != SASL_OK)
				throw TTransportException(std::string("sasl_decode: ") +
										  sasl_errdetail(sasl_));
			rbuf_.assign(out, out + outlen);
		}
		else
			rbuf_.swap(raw);
	}

	std::shared_ptr<TSocket> sock_;
	int fd_;
	sasl_conn_t *sasl_;
	bool wrap_;
	std::string rbuf_;
	size_t rpos_;
	std::string wbuf_;
};

static void
hs2_sasl_gssapi(int fd, const std::string &host, sasl_conn_t **out_conn)
{
	*out_conn = NULL;
	static std::once_flag sasl_once;
	std::call_once(sasl_once, []() {
#ifdef SASL_PLUGINDIR
		setenv("SASL_PATH", SASL_PLUGINDIR, 0);
#endif
		int rc = sasl_client_init(NULL);
		if (rc != SASL_OK)
			throw std::runtime_error(std::string("sasl_client_init: ") +
									 sasl_errstring(rc, NULL, NULL));
	});

	static sasl_callback_t cbs[] = {
		{ SASL_CB_LIST_END, NULL, NULL }
	};
	sasl_conn_t *conn = NULL;
	int rc = sasl_client_new("impala", host.c_str(), NULL, NULL, cbs, 0, &conn);
	if (rc != SASL_OK)
		throw std::runtime_error(std::string("sasl_client_new: ") +
								 sasl_errstring(rc, NULL, NULL));

	/* Prefer QOP=auth so post-handshake Thrift is not SASL-wrapped. */
	sasl_security_properties_t sec;
	memset(&sec, 0, sizeof(sec));
	sec.min_ssf = 0;
	sec.max_ssf = 0;
	(void)sasl_setprop(conn, SASL_SEC_PROPS, &sec);

	const char *out = NULL;
	unsigned outlen = 0;
	const char *mech = NULL;
	rc = sasl_client_start(conn, "GSSAPI", NULL, &out, &outlen, &mech);
	if (rc != SASL_OK && rc != SASL_CONTINUE)
	{
		std::string e = sasl_errdetail(conn);
		sasl_dispose(&conn);
		throw std::runtime_error(std::string("sasl_client_start GSSAPI: ") + e);
	}

	std::string mech_name = mech ? mech : "GSSAPI";
	hs2_sasl_send_fd(fd, HS2_SASL_START, mech_name.data(),
					 static_cast<uint32_t>(mech_name.size()));
	hs2_sasl_send_fd(fd, HS2_SASL_OK, out, outlen);

	bool got_complete = false;
	while (rc == SASL_CONTINUE)
	{
		Hs2SaslStatus st;
		std::string payload;
		hs2_sasl_recv_fd(fd, &st, &payload);
		if (st == HS2_SASL_COMPLETE)
		{
			got_complete = true;
			if (!payload.empty())
			{
				rc = sasl_client_step(conn, payload.data(),
									  static_cast<unsigned>(payload.size()),
									  NULL, &out, &outlen);
			}
			else
				rc = SASL_OK;
			break;
		}
		if (st != HS2_SASL_OK)
		{
			sasl_dispose(&conn);
			throw std::runtime_error("HS2 SASL peer status " +
									 std::to_string(static_cast<int>(st)));
		}
		rc = sasl_client_step(conn, payload.data(),
							  static_cast<unsigned>(payload.size()),
							  NULL, &out, &outlen);
		if (rc != SASL_OK && rc != SASL_CONTINUE)
		{
			std::string e = sasl_errdetail(conn);
			sasl_dispose(&conn);
			throw std::runtime_error(std::string("sasl_client_step: ") + e);
		}
		if (rc == SASL_CONTINUE || outlen > 0)
			hs2_sasl_send_fd(fd, HS2_SASL_OK, out, outlen);
	}

	if (rc != SASL_OK)
	{
		sasl_dispose(&conn);
		throw std::runtime_error("HS2 SASL GSSAPI did not complete");
	}
	/*
	 * Impala TSaslTransport always sends a final COMPLETE (status=5, len=0)
	 * after Cyrus returns SASL_OK. Leaving that 5-byte frame on the wire makes
	 * TFramedTransport read 0x05000000 as a length. Guru: #SL.00000028.HS2GSSAPI
	 */
	if (!got_complete)
	{
		Hs2SaslStatus st;
		std::string payload;
		hs2_sasl_recv_fd(fd, &st, &payload);
		if (st != HS2_SASL_COMPLETE)
		{
			sasl_dispose(&conn);
			throw std::runtime_error("HS2 SASL expected COMPLETE, got " +
									 std::to_string(static_cast<int>(st)));
		}
	}
	*out_conn = conn;
}

extern "C" {

ImpalaHs2Session *
impala_hs2_connect(const char *host, int port, const char *auth,
				   const char *principal, const char *database, char **errbuf)
{
	return impala_hs2_connect_ex(host, port, auth, principal, database,
								 NULL, NULL, errbuf);
}

ImpalaHs2Session *
impala_hs2_connect_ex(const char *host, int port, const char *auth,
					  const char *principal, const char *database,
					  const char *keytab, const char *ccache, char **errbuf)
{
	if (errbuf)
		*errbuf = NULL;

	if (auth == NULL)
		auth = "nosasl";

	if (strcmp(auth, "nosasl") != 0 && strcmp(auth, "kerberos") != 0)
	{
		if (errbuf)
			*errbuf = dup_err(std::string("unsupported auth: ") + auth);
		return NULL;
	}

	try
	{
		std::string hs2_host = host && host[0] ? host : "127.0.0.1";
		const bool kerberos = (strcmp(auth, "kerberos") == 0);
		if (kerberos &&
			(hs2_host == "127.0.0.1" || hs2_host == "localhost" ||
			 hs2_host == "::1"))
		{
			const char *fqdn = getenv("SIGNALS_KRB_HOST");
			if (fqdn && fqdn[0])
				hs2_host = fqdn;
			else
				throw std::runtime_error(
					"HS2 GSSAPI requires FQDN host (not loopback). "
					"Set SERVER OPTIONS (host 'tinybox.dev.vista.zndx.org') "
					"or SIGNALS_KRB_HOST. Guru: #SL.00000028.HS2GSSAPI");
		}

		std::string ccache_used;
		if (kerberos && keytab && keytab[0] && principal && principal[0])
			ccache_used = hs2_kinit_keytab(principal, keytab,
										   ccache ? ccache : "");
		else if (kerberos && ccache && ccache[0])
			ccache_used = ccache;

		if (!ccache_used.empty())
			setenv("KRB5CCNAME", ccache_used.c_str(), 1);

		auto sock = std::make_shared<TSocket>(hs2_host, port);
		sock->setConnTimeout(10000);
		sock->setRecvTimeout(120000);
		sock->setSendTimeout(60000);
		sock->open();
		sasl_conn_t *sasl = NULL;
		std::shared_ptr<TTransport> transport;
		if (kerberos)
		{
			int fd = sock->getSocketFD();
			hs2_sasl_gssapi(fd, hs2_host, &sasl);
			const void *ssf_p = NULL;
			int ssf = 0;
			if (sasl_getprop(sasl, SASL_SSF, &ssf_p) == SASL_OK && ssf_p)
				ssf = *static_cast<const int *>(ssf_p);
			/*
			 * SSF=0 (QOP=auth): TFramedTransport matches Hive SASL data framing.
			 * SSF>0: wrap with sasl_encode/decode. Stubs and libthrift MUST be
			 * the same Thrift minor (generated TCLIService is 0.22; 0.16
			 * TBinaryProtocol vtables lack writeUUID and SIGSEGV on the first
			 * OpenSession read). Guru: #SL.00000028.HS2GSSAPI
			 */
			if (ssf > 0)
				transport = std::make_shared<Hs2SaslDataTransport>(sock, sasl, true);
			else
			{
				auto fdtrans = std::make_shared<TFDTransport>(
					fd, TFDTransport::NO_CLOSE_ON_DESTROY);
				transport = std::make_shared<TFramedTransport>(fdtrans);
			}
		}
		else
		{
			transport = std::make_shared<TBufferedTransport>(sock);
			if (!transport->isOpen())
				transport->open();
		}
		auto protocol = std::make_shared<TBinaryProtocol>(transport);
		auto client = std::make_shared<TCLIServiceClient>(protocol);

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
		s->socket = sock;
		s->transport = transport;
		s->client = client;
		s->session = oresp.sessionHandle;
		s->database = database ? database : "default";
		s->sasl = sasl;
		return s;
	}
	catch (const TException &ex)
	{
		if (errbuf)
			*errbuf = dup_err(std::string("HS2 connect: ") + ex.what());
		return NULL;
	}
	catch (const std::exception &ex)
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
	if (session->sasl)
		sasl_dispose(&session->sasl);
	delete session;
}

} /* extern "C" */
