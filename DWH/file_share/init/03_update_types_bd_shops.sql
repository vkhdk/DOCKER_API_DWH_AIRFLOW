-- change the database to dwh_shops
\c dwh_shops;
-- change types to serial
CREATE SEQUENCE bd_shops.shops_id_seq OWNED BY bd_shops.shops.id;
ALTER TABLE bd_shops.shops ALTER COLUMN id SET DEFAULT nextval('bd_shops.shops_id_seq');
SELECT setval('bd_shops.shops_id_seq', COALESCE(MAX(id), 0) + 1) FROM bd_shops.shops;
--
CREATE SEQUENCE bd_shops.visits_id_seq OWNED BY bd_shops.visits.id;
ALTER TABLE bd_shops.visits ALTER COLUMN id SET DEFAULT nextval('bd_shops.visits_id_seq');
SELECT setval('bd_shops.visits_id_seq', COALESCE(MAX(id), 0) + 1) FROM bd_shops.visits;
--
CREATE SEQUENCE bd_shops.employers_id_seq OWNED BY bd_shops.employers.id;
ALTER TABLE bd_shops.employers ALTER COLUMN id SET DEFAULT nextval('bd_shops.employers_id_seq');
SELECT setval('bd_shops.employers_id_seq', COALESCE(MAX(id), 0) + 1) FROM bd_shops.employers;
--
CREATE SEQUENCE bd_shops.vendors_id_seq OWNED BY bd_shops.vendors.id;
ALTER TABLE bd_shops.vendors ALTER COLUMN id SET DEFAULT nextval('bd_shops.vendors_id_seq');
SELECT setval('bd_shops.vendors_id_seq', COALESCE(MAX(id), 0) + 1) FROM bd_shops.vendors;
--
CREATE SEQUENCE bd_shops.product_id_seq OWNED BY bd_shops.product.id;
ALTER TABLE bd_shops.product ALTER COLUMN id SET DEFAULT nextval('bd_shops.product_id_seq');
SELECT setval('bd_shops.product_id_seq', COALESCE(MAX(id), 0) + 1) FROM bd_shops.product;
