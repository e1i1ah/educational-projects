CREATE TABLE IF NOT EXISTS dds.dm_orders (
	id int NOT NULL GENERATED ALWAYS AS IDENTITY,
	user_id int NOT NULL,
	restaurant_id int NOT NULL,
	timestamp_id int NOT NULL,
	order_key varchar(255) NOT NULL,
	order_status varchar(50) NOT NULL,
	CONSTRAINT dds_dm_orders_order_key_unique UNIQUE (order_key),
	CONSTRAINT dm_orders_pkey PRIMARY KEY (id),
	CONSTRAINT dm_orders_restaurant_id_fkey FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants(id),
	CONSTRAINT dm_orders_timestamp_id_fkey FOREIGN KEY (timestamp_id) REFERENCES dds.dm_timestamps(id),
	CONSTRAINT dm_orders_user_id_fkey FOREIGN KEY (user_id) REFERENCES dds.dm_users(id)
);

