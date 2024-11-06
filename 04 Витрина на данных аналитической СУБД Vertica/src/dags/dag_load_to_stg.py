from airflow.decorators import dag, task

from config import DependencyConfig

import pendulum


@dag(schedule_interval=None, start_date=pendulum.parse("2022-07-13"))
def load_to_stg_dag():

    @task(task_id=f"users_load")
    def load_in_users_table():
        job = DependencyConfig.Repository.stg_vertica_repo("users")
        job.load_data_in_table()

    @task(task_id=f"groups_load")
    def load_in_groups_table():
        job = DependencyConfig.Repository.stg_vertica_repo("groups")
        job.load_data_in_table()

    @task(task_id=f"dialogs_load")
    def load_in_dialogs_table():
        job = DependencyConfig.Repository.stg_vertica_repo("dialogs")
        job.load_data_in_table()

    @task(task_id=f"group_log_load")
    def load_in_group_log_table():
        job = DependencyConfig.Repository.stg_vertica_repo("group_log")
        job.load_data_in_table()

    users = load_in_users_table()
    groups = load_in_groups_table()
    dialogs = load_in_dialogs_table()
    group_log = load_in_group_log_table()

    users >> groups >> [dialogs, group_log]


sprint6_dag = load_to_stg_dag()
