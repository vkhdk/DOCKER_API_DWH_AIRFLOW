# load data into the database from dump
psql -c 'SET search_path TO bd_shops;' -U dwh_postgres -d dwh_shops  -f /var/lib/postgresql/dumps/dump1.sql