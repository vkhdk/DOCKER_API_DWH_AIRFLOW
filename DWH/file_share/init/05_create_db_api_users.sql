-- change the database to db_api
\c db_api;
CREATE TABLE api.users (
    id serial4 NOT NULL,
	"login" varchar NOT NULL,
	"password" varchar NOT NULL,
	CONSTRAINT users_pk PRIMARY KEY (login, password)
);
-- insert data
INSERT INTO api.users ("login","password") VALUES
	 ('ivan','ivan123'),
	 ('masha','masha321');