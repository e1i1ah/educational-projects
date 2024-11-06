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
def origin_pg_stg_load_dag():

    @task(task_id="ranks_load")
    def load_ranks():
        job = Loader(
            DependencyConfig.Connectors.origin_pg_connector("ranks"),
            DependencyConfig.Repository.stg_ranks_bonussystem_repository(),
            DependencyConfig.dwh_pg_db_connection(),
            log,
        )
        job.load_table()

    @task(task_id="users_load")
    def load_users():
        job = Loader(
            DependencyConfig.Connectors.origin_pg_connector("users"),
            DependencyConfig.Repository.stg_users_bonussystem_repository(),
            DependencyConfig.dwh_pg_db_connection(),
            log,
        )
        job.load_table()

    @task(task_id="events_load")
    def load_events():
        job = Loader(
            DependencyConfig.Connectors.origin_pg_connector("events"),
            DependencyConfig.Repository.stg_events_bonussystem_repository(),
            DependencyConfig.dwh_pg_db_connection(),
            log,
        )
        job.load_table()

    ranks = load_ranks()
    users = load_users()
    events = load_events()

    ranks >> users >> events


origin_pg_stg_dag = origin_pg_stg_load_dag()
