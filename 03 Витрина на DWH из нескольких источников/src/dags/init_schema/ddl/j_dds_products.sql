CREATE TABLE IF NOT EXISTS dds.dm_products (
	id int NOT NULL GENERATED ALWAYS AS IDENTITY,
	restaurant_id int NOT NULL,
	product_id varchar(255) NOT NULL,
	product_name varchar(100) NOT NULL,
	product_price numeric(14, 2) NOT NULL DEFAULT 0,
	active_from timestamp NOT NULL,
	active_to timestamp NOT NULL,
	CONSTRAINT dds_dm_products_id_name_price_unique UNIQUE (restaurant_id,product_id,product_name,product_price),
	CONSTRAINT dm_products_product_price_check CHECK (product_price >= 0),
	CONSTRAINT dm_products_pkey PRIMARY KEY (id),
	CONSTRAINT dm_products_restaurant_id_fkey FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants(id)
);

