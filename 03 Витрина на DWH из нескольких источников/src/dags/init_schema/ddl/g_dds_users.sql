CREATE TABLE IF NOT EXISTS dds.dm_users (
	id int NOT NULL GENERATED ALWAYS AS IDENTITY,
	user_id varchar(255) NOT NULL,
	user_name varchar(100) NOT NULL,
	user_login varchar(100) NOT NULL,
	CONSTRAINT dds_dm_users_user_id_unique UNIQUE (user_id),
	CONSTRAINT dm_users_pkey PRIMARY KEY (id)
);