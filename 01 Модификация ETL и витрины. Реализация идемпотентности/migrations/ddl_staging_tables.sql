DROP TABLE IF EXISTS staging.user_order_log;
CREATE TABLE IF NOT EXISTS staging.user_order_log (
	uniq_id varchar(32) NOT NULL,
	date_time timestamp NOT NULL,
	city_id int4 NOT NULL,
	city_name varchar(100) NULL,
	customer_id int4 NOT NULL,
	first_name varchar(100) NULL,
	last_name varchar(100) NULL,
	item_id int4 NOT NULL,
	item_name varchar(100) NULL,
	quantity int8 NULL,
	payment_amount numeric(10, 2) NULL,
	status varchar(15) NOT NULL DEFAULT 'shipped',
	CONSTRAINT user_order_log_pk PRIMARY KEY (uniq_id)
);
CREATE INDEX IF NOT EXISTS uo1 ON staging.user_order_log USING btree (customer_id);
CREATE INDEX IF NOT EXISTS uo2 ON staging.user_order_log USING btree (item_id);

DROP TABLE IF EXISTS staging.customer_research;
CREATE TABLE IF NOT EXISTS staging.customer_research (
	id serial4 NOT NULL,
	date_id timestamp NULL,
	category_id int4 NULL,
	geo_id int4 NULL,
	sales_qty int4 NULL,
	sales_amt numeric(14, 2) NULL,
	CONSTRAINT customer_research_pkey PRIMARY KEY (id)
);

DROP TABLE IF EXISTS staging.user_activity_log;
CREATE TABLE IF NOT EXISTS staging.user_activity_log (
	uniq_id varchar(32) NOT NULL,
	date_time timestamp NULL,
	action_id int8 NULL,
	customer_id int8 NULL,
	quantity int8 NULL,
	CONSTRAINT user_activity_log_pkey PRIMARY KEY (uniq_id)
);

DROP TABLE IF EXISTS staging.price_log;
CREATE TABLE IF NOT EXISTS staging.price_log (
	id serial4 NOT NULL,
	item varchar(100) NULL,
	price int8 NULL,
	CONSTRAINT price_log_pkey PRIMARY KEY (id)
);