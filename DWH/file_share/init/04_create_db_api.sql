-- database name must not be in uppercase
-- if BD exists, this skript does not work
CREATE DATABASE db_api;
GRANT ALL PRIVILEGES ON DATABASE db_api TO dwh_postgres;
-- change the database to db_api
\c db_api;
CREATE SCHEMA api;
CREATE TABLE api.day_gen_visits_config (
	id serial4 NOT NULL,
	launched boolean NOT NULL,
	source_schema varchar NOT NULL,
	gen_store_schema varchar NOT NULL,
	day_gen_store varchar NOT NULL,
	external_day_gen_store varchar NOT NULL,
	source_tables json NOT NULL,
	extra_columns_length_limit int2 NOT NULL,
	gen_records int2 NULL,
	dt timestamptz NOT NULL,
	CONSTRAINT day_gen_visits_config_pk PRIMARY KEY (id)
);