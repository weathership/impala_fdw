/* contrib/impala_fdw/impala_fdw--0.1.1.sql */

\echo Use "CREATE EXTENSION impala_fdw" to load this file. \quit

CREATE FUNCTION impala_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION impala_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER impala_fdw
  HANDLER impala_fdw_handler
  VALIDATOR impala_fdw_validator;

CREATE FUNCTION impala_fdw_exec(server_name text, sql text)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;
