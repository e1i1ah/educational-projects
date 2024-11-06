CREATE TABLE IF NOT EXISTS stg.ordersystem_restaurants (
	id int NOT NULL GENERATED ALWAYS AS IDENTITY,
	object_id varchar(255) NOT NULL,
	object_value text NOT NULL,
	update_ts timestamp NOT NULL,
	CONSTRAINT ordersystem_restaurants_object_id_uindex UNIQUE (object_id),
	CONSTRAINT ordersystem_restaurants_pkey PRIMARY KEY (id)
);
CREATE TABLE IF NOT EXISTS stg.ordersystem_orders (
	id int NOT NULL GENERATED ALWAYS AS IDENTITY,
	object_id varchar(255) NOT NULL,
	object_value text NOT NULL,
	update_ts timestamp NOT NULL,
	CONSTRAINT ordersystem_orders_object_id_uindex UNIQUE (object_id),
	CONSTRAINT ordersystem_orders_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS stg.ordersystem_users (
	id int NOT NULL GENERATED ALWAYS AS IDENTITY,
	object_id varchar(255) NOT NULL,
	object_value text NOT NULL,
	update_ts timestamp NOT NULL,
	CONSTRAINT ordersystem_users_object_id_uindex UNIQUE (object_id),
	CONSTRAINT ordersystem_users_pkey PRIMARY KEY (id)
);