-- database name must not be in uppercase
-- if BD exists, this skript does not work
CREATE DATABASE db_api;
GRANT ALL PRIVILEGES ON DATABASE db_api TO dwh_postgres;
-- change the database to db_api
\c db_api;
CREATE SCHEMA api;