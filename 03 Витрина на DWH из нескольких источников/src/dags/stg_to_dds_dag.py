import logging

import pendulum
from airflow.decorators import dag, task
from core.domain.stg_dds_loader import StgDdsLoader
from config import DependencyConfig
from airflow.providers.postgres.operators.postgres import PostgresOperator


log = logging.getLogger(__name__)


@dag(
    schedule_interval="0/15 * * * *",
    start_date=pendulum.datetime(2024, 6, 6, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
)
def stg_to_dds_load_dag():

    @task(task_id="restaurants_load")
    def load_restaurants():
        job = StgDdsLoader(
            DependencyConfig.Repository.stg_restaurants_ordersystem_repository(),
            DependencyConfig.Repository.dds_restaurants_repository(),
            DependencyConfig.dwh_pg_db_connection(),
            log,
        )
        job.load_table()

    @task(task_id="users_load")
    def load_users():
        job = StgDdsLoader(
            DependencyConfig.Repository.stg_users_ordersystem_repository(),
            DependencyConfig.Repository.dds_users_repository(),
            DependencyConfig.dwh_pg_db_connection(),
            log,
        )
        job.load_table()

    @task(task_id="timestamps_orders_load")
    def load_timestamps_orders():
        job = StgDdsLoader(
            DependencyConfig.Repository.stg_orders_ordersystem_repository(),
            DependencyConfig.Repository.dds_timestamps_repository(),
            DependencyConfig.dwh_pg_db_connection(),
            log,
        )
        job.load_table()

    @task(task_id="timestamps_deliveries_load")
    def load_timestamps_deliveries():
        job = StgDdsLoader(
            DependencyConfig.Repository.stg_deliveries_api_repository(),
            DependencyConfig.Repository.dds_timestamps_repository(),
            DependencyConfig.dwh_pg_db_connection(),
            log,
        )
        job.load_table()

    @task(task_id="couriers_load")
    def load_couriers():
        job = StgDdsLoader(
            DependencyConfig.Repository.stg_couriers_api_repository(),
            DependencyConfig.Repository.dds_courier_repository(),
            DependencyConfig.dwh_pg_db_connection(),
            log,
        )
        job.load_table()

    @task(task_id="products_load")
    def load_products():
        job = StgDdsLoader(
            DependencyConfig.Repository.stg_orders_ordersystem_repository(),
            DependencyConfig.Repository.dds_products_repository(),
            DependencyConfig.dwh_pg_db_connection(),
            log,
        )
        job.load_table()

    @task(task_id="orders_load")
    def load_orders():
        job = StgDdsLoader(
            DependencyConfig.Repository.stg_orders_ordersystem_repository(),
            DependencyConfig.Repository.dds_orders_repository(),
            DependencyConfig.dwh_pg_db_connection(),
            log,
        )
        job.load_table()

    @task(task_id="deliveries_load")
    def load_deliveries():
        job = StgDdsLoader(
            DependencyConfig.Repository.stg_deliveries_api_repository(),
            DependencyConfig.Repository.dds_delivery_repository(),
            DependencyConfig.dwh_pg_db_connection(),
            log,
        )
        job.load_table()

    @task(task_id="fct_product_sales_load")
    def load_fct_product_sales():
        job = StgDdsLoader(
            DependencyConfig.Repository.stg_events_bonussystem_repository(),
            DependencyConfig.Repository.dds_product_sales_repository(),
            DependencyConfig.dwh_pg_db_connection(),
            log,
        )
        job.load_table()

    load_mart_settlement_report = PostgresOperator(
        task_id="load_mart_settlement_report_task",
        postgres_conn_id="PG_WAREHOUSE_CONNECTION",
        sql="./dml_marts/settlement_report.sql",
    )

    load_mart_courier_ledger = PostgresOperator(
        task_id="load_mart_courier_ledger_task",
        postgres_conn_id="PG_WAREHOUSE_CONNECTION",
        sql="./dml_marts/courier_ledger.sql",
    )

    restaurants = load_restaurants()
    users = load_users()
    timestamps_orders = load_timestamps_orders()
    timestamps_deliveries = load_timestamps_deliveries()
    couriers = load_couriers()
    products = load_products()
    orders = load_orders()
    deliveries = load_deliveries()
    fct_product_sales = load_fct_product_sales()

    (
        restaurants
        >> users
        >> timestamps_orders
        >> timestamps_deliveries
        >> couriers
        >> products
        >> orders
        >> deliveries
        >> fct_product_sales
        >> [load_mart_settlement_report, load_mart_courier_ledger]
    )


stg_to_dds_dag = stg_to_dds_load_dag()
