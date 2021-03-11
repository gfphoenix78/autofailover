/* contrib/autofailover/autofailover--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION autofailover" to load this file. \quit

create function collect_status(IN last_role text)
returns table(role text, sync text, lsn pg_lsn, walconn bool)
LANGUAGE C STRICT SECURITY DEFINER
AS 'MODULE_PATHNAME', $$collect_status$$;

create function execute_action(cmd text)
returns void
LANGUAGE C STRICT SECURITY DEFINER
AS 'MODULE_PATHNAME', $$execute_action$$;