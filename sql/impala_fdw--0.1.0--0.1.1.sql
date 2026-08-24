/* Impala Kudu range-partition DDL through HS2. Guru: #SL.00000029.RANGEDDL */

CREATE FUNCTION impala_fdw_exec(server_name text, sql text)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;
