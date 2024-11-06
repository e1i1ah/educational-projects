CREATE TABLE IF NOT EXISTS stg.api_couriers (
	id int NOT NULL GENERATED ALWAYS AS IDENTITY,
	object_id varchar(255) NOT NULL,
	object_value text NOT NULL,
	update_ts timestamp NOT NULL,
	CONSTRAINT stg_api_couriers_object_id_uindex UNIQUE (object_id),
	CONSTRAINT stg_api_couriers_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS stg.api_deliveries (
	id int NOT NULL GENERATED ALWAYS AS IDENTITY,
	object_id varchar(255) NOT NULL,
	object_value text NOT NULL,
	update_ts timestamp NOT NULL,
	CONSTRAINT stg_api_deliveries_object_id_uindex UNIQUE (object_id),
	CONSTRAINT stg_api_deliveries_pkey PRIMARY KEY (id)
);