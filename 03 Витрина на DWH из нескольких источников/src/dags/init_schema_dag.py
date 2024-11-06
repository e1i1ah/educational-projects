import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from init_schema.schema_init import SchemaDdl
from config import DependencyConfig


log = logging.getLogger(__name__)


@dag(
    schedule_interval="0/15 * * * *",
    start_date=pendulum.datetime(2024, 6, 6, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=True,
)
def sprint5_example_stg_init_schema_dag():

    ddl_path = Variable.get("EXAMPLE_STG_DDL_FILES_PATH")

    @task(task_id="schema_init")
    def schema_init():
        rest_loader = SchemaDdl(DependencyConfig.dwh_pg_db_connection(), log)
        rest_loader.init_schema(ddl_path)

    init_schema = schema_init()

    init_schema


stg_init_schema_dag = sprint5_example_stg_init_schema_dag()
