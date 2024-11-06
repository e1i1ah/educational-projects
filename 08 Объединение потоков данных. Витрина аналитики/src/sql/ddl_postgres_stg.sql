CREATE TABLE IF NOT EXISTS stg.transactions_currencies (
	id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	object_id varchar,
	object_type varchar,
	sent_dttm timestamp,
	payload varchar,
);