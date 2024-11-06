import logging

import pendulum
from airflow.decorators import dag, task
from core.domain.origin_stg_loader import Loader
from config import DependencyConfig


log = logging.getLogger(__name__)


@dag(
    schedule_interval="0/15 * * * *",
    start_date=pendulum.datetime(2024, 6, 6, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=True,
)
def origin_mongo_stg_load_dag():

    @task(task_id="restaurants_load")
    def load_restaurants():
        job = Loader(
            DependencyConfig.Connectors.origin_mongo_connector("restaurants"),
            DependencyConfig.Repository.stg_restaurants_ordersystem_repository(),
            DependencyConfig.dwh_pg_db_connection(),
            log,
        )
        job.load_table()

    @task(task_id="users_load")
    def load_users():
        job = Loader(
            DependencyConfig.Connectors.origin_mongo_connector("users"),
            DependencyConfig.Repository.stg_users_ordersystem_repository(),
            DependencyConfig.dwh_pg_db_connection(),
            log,
        )
        job.load_table()

    @task(task_id="orders_load")
    def load_orders():
        job = Loader(
            DependencyConfig.Connectors.origin_mongo_connector("orders"),
            DependencyConfig.Repository.stg_orders_ordersystem_repository(),
            DependencyConfig.dwh_pg_db_connection(),
            log,
        )
        job.load_table()

    restaurants = load_restaurants()
    users = load_users()
    orders = load_orders()

    restaurants >> users >> orders


origin_mongo_stg_dag = origin_mongo_stg_load_dag()
