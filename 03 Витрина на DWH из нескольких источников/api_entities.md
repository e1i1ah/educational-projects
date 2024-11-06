Для новой витрины созданы и наполнены данными две таблицы - dm_delivery, dm_couriers
ddl этих таблиц - src\dags\init_schema\ddl\l_dds_delivery.sql  
                  src\dags\init_schema\ddl\h_dds_couriers.sql

На основе этих таблиц и уже существующих таблиц dm_timestamps, dm_orders, fct_product_sales реализовано заполнение витрины
ddl витрины - src\dags\init_schema\ddl\o_cdm_courier_ledger.sql
заполнение витрины - src\dags\dml_marts\courier_ledger.sql