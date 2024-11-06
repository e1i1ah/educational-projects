CREATE TABLE IF NOT EXISTS dds.dm_restaurants (
	id int NOT NULL GENERATED ALWAYS AS IDENTITY,
	restaurant_id varchar(255) NOT NULL,
	restaurant_name varchar(100) NOT NULL,
	active_from timestamp NOT NULL,
	active_to timestamp NOT NULL,
	CONSTRAINT dds_dm_restaurant_id_name_unique UNIQUE (restaurant_id,restaurant_name),
	CONSTRAINT dm_restaurants_pkey PRIMARY KEY (id)
);