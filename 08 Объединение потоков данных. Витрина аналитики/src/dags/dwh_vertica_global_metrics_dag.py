import logging
import pendulum
from airflow.decorators import dag, task
from config import DependencyConfig

log = logging.getLogger(__name__)


@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 10, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
)
def update_global_metrics_dag():

    @task(task_id="delete_previous_day")
    def delete_previous_day_data(exec_date: str):
        execution_date_dt = pendulum.parse(exec_date) - pendulum.duration(days=1)
        job = DependencyConfig.Repository.get_dwh_vertica_global_metrics_repository()
        job.save_objects(
            "/opt/airflow/dags/sql/dml_delete_previous_day_global_metric.sql",
            DependencyConfig.vertica_connection(),
            execution_date_dt,
        )

    @task(task_id="update_global_metrics")
    def update_metrics(exec_date: str):
        execution_date_dt = pendulum.parse(exec_date) - pendulum.duration(days=1)
        job = DependencyConfig.Repository.get_dwh_vertica_global_metrics_repository()
        job.save_objects(
            "/opt/airflow/dags/sql/dml_global_metrics.sql",
            DependencyConfig.vertica_connection(),
            execution_date_dt,
        )

    exec_date = "{{ ds }}"

    delete = delete_previous_day_data(exec_date)
    update = update_metrics(exec_date)

    delete >> update


update_global_metrics_dag = update_global_metrics_dag()
