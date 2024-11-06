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
def origin_api_stg_load_dag():

    @task(task_id="couriers_load")
    def load_couriers():
        job = Loader(
            DependencyConfig.Connectors.origin_api_connector("couriers"),
            DependencyConfig.Repository.stg_couriers_api_repository(),
            DependencyConfig.dwh_pg_db_connection(),
            log,
        )
        job.load_table()

    @task(task_id="deliveries_load")
    def load_deliveries():
        job = Loader(
            DependencyConfig.Connectors.origin_api_connector("deliveries"),
            DependencyConfig.Repository.stg_deliveries_api_repository(),
            DependencyConfig.dwh_pg_db_connection(),
            log,
        )
        job.load_table()

    couriers = load_couriers()
    deliveries = load_deliveries()

    couriers, deliveries


origin_api_stg_dag = origin_api_stg_load_dag()
