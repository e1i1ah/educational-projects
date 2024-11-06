CREATE TABLE IF NOT EXISTS stg.bonussystem_ranks (
	id int4 NOT NULL,
	"name" varchar NOT NULL,
	bonus_percent numeric(19, 5) NOT NULL DEFAULT 0,
	min_payment_threshold numeric(19, 5) NOT NULL DEFAULT 0,
	CONSTRAINT ranks_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS stg.bonussystem_events (
	id int4 NOT NULL,
	event_ts timestamp NOT NULL,
	event_type varchar NOT NULL,
	event_value text NOT NULL,
	CONSTRAINT outbox_pkey PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS idx_bonussystem_events__event_ts ON stg.bonussystem_events USING btree (event_ts);

CREATE TABLE IF NOT EXISTS stg.bonussystem_users (
	id int4 NOT NULL,
	order_user_id text NOT NULL,
	CONSTRAINT users_pkey PRIMARY KEY (id)
);