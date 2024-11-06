import logging
import pendulum
from airflow.decorators import dag, task
from domain.pg_vertica_loader import Loader
from config import DependencyConfig
from airflow.operators.dagrun_operator import TriggerDagRunOperator

log = logging.getLogger(__name__)


@dag(
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2022, 10, 1, tz="UTC"),
    end_date=pendulum.datetime(2022, 10, 31, tz="UTC"),
    catchup=True,
    is_paused_upon_creation=True,
    max_active_runs=1,
)
def origin_vertica_stg_load_dag():

    @task(task_id="transactions_load")
    def load_transactions(exec_date: str):
        execution_date_dt = pendulum.parse(exec_date) - pendulum.duration(days=1)
        job = Loader(
            DependencyConfig.Repository.get_origin_pg_transaction_repository(),
            DependencyConfig.Repository.get_dwh_vertica_transaction_repository(),
            DependencyConfig.pg_connection(),
            DependencyConfig.vertica_connection(),
            execution_date_dt,
            log,
        )
        job.load_table()

    @task(task_id="сurrencies_load")
    def load_currencies(exec_date: str):
        execution_date_dt = pendulum.parse(exec_date) - pendulum.duration(days=1)
        job = Loader(
            DependencyConfig.Repository.get_origin_pg_currency_repository(),
            DependencyConfig.Repository.get_dwh_vertica_currency_repository(),
            DependencyConfig.pg_connection(),
            DependencyConfig.vertica_connection(),
            execution_date_dt,
            log,
        )
        job.load_table()

    exec_date = "{{ ds }}"

    transactions = load_transactions(exec_date)
    currencies = load_currencies(exec_date)

    trigger_global_metrics_dag = TriggerDagRunOperator(
        task_id="trigger_global_metrics_dag",
        trigger_dag_id="update_global_metrics_dag",
        wait_for_completion=False,
    )

    transactions >> currencies >> trigger_global_metrics_dag


origin_vertica_stg_dag = origin_vertica_stg_load_dag()
