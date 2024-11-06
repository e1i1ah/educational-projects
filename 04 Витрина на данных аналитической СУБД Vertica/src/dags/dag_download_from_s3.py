from airflow.operators.bash import BashOperator
from airflow.decorators import dag, task

from config import DependencyConfig
import pendulum


@dag(schedule_interval=None, start_date=pendulum.parse("2022-07-13"))
def get_data_dag():

    @task(task_id=f"users_load")
    def download_users():
        job = DependencyConfig.Connectors.origin_s3_connector(
            bucket="sprint6", key="users.csv"
        )
        job.download_dataset()

    @task(task_id=f"groups_load")
    def download_groups():
        job = DependencyConfig.Connectors.origin_s3_connector(
            bucket="sprint6", key="groups.csv"
        )
        job.download_dataset()

    @task(task_id=f"dialogs_load")
    def download_dialogs():
        job = DependencyConfig.Connectors.origin_s3_connector(
            bucket="sprint6", key="dialogs.csv"
        )
        job.download_dataset()

    @task(task_id=f"group_log_load")
    def download_group_log():
        job = DependencyConfig.Connectors.origin_s3_connector(
            bucket="sprint6", key="group_log.csv"
        )
        job.download_dataset()

    print_10_lines_of_each = BashOperator(
        task_id="print_10_lines_of_each",
        bash_command="""head -10 {{ params.files }}""",
        params={
            "files": " ".join(
                [
                    f"/lessons/dags/data/{f}"
                    for f in ("users.csv", "groups.csv", "dialogs.csv", "group_log.csv")
                ]
            )
        },
    )
    users = download_users()
    groups = download_groups()
    dialogs = download_dialogs()
    group_log = download_group_log()

    [users, groups, dialogs, group_log] >> print_10_lines_of_each


sprint6_dag = get_data_dag()
