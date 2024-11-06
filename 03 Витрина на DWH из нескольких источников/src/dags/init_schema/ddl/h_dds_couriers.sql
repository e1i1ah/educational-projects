CREATE TABLE IF NOT EXISTS dds.dm_couriers (
	id int NOT NULL GENERATED ALWAYS AS IDENTITY,
	courier_id varchar(255) NOT NULL,
	courier_name varchar(100) NOT NULL,
	CONSTRAINT courier_courier_id_unique_key UNIQUE (courier_id),
	CONSTRAINT courier_pkey PRIMARY KEY (id)
);