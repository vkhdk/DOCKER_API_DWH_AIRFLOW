# load data into the database from dump
current_dump="dump1.sql"
psql -c 'SET search_path TO bd_shops;' -U dwh_postgres -d dwh_shops  -f /var/lib/postgresql/dumps/$current_dump