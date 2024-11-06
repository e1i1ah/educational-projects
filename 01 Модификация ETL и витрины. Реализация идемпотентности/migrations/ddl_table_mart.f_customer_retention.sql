create table if not exists mart.f_customer_retention (
	id serial4 not null,
	item_id int not null,
	period_id smallint not null,
	new_customers_count int not null,
	returning_customers_count int not null,
	refunded_customer_count int not null,
	period_name varchar(6) not null default 'weekly'::character varying,
	new_customers_revenue bigint not null,
	returning_customers_revenue bigint not null,
	customers_refunded int not null,
	CONSTRAINT f_customer_retention_pkey PRIMARY KEY (id),
	CONSTRAINT f_customer_retention_item_id_fkey FOREIGN KEY (item_id) REFERENCES mart.d_item(item_id)
);

CREATE INDEX f_cr1 ON mart.f_customer_retention USING btree (item_id);
