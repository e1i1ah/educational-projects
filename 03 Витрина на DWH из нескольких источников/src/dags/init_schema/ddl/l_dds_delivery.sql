CREATE TABLE IF NOT EXISTS dds.dm_delivery (
	id int NOT NULL GENERATED ALWAYS AS IDENTITY,
	delivery_id varchar(255) NOT NULL,
	order_id int NOT NULL,
	courier_id int NOT NULL,
	timestamp_id int NOT NULL,
	"address" varchar(100) NOT NULL,
	rate smallint NOT NULL,
	tip_sum numeric(14, 2) NOT NULL,
	CONSTRAINT dm_delivery_delivery_id_key UNIQUE (delivery_id),
	CONSTRAINT dm_delivery_pkey_id PRIMARY KEY (id),
	CONSTRAINT dm_delivery_courier_id_fkey FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers(id),
	CONSTRAINT dm_delivery_order_id_fkey FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id),
	CONSTRAINT dm_delivery_timestamp_id_fkey FOREIGN KEY (timestamp_id) REFERENCES dds.dm_timestamps(id)
);

