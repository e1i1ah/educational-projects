import time
import requests
import json
import pandas as pd
import os
import psycopg2

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.http_hook import HttpHook

http_conn_id = HttpHook.get_connection("http_conn_id")
api_key = http_conn_id.extra_dejson.get("api_key")
base_url = http_conn_id.host
storage_url = "https://storage.yandexcloud.netxxx"
postgres_conn_id = "postgresql_de"
dimension_tasks = list()
nickname = "xxx"
cohort = "xx"

headers = {
    "X-Nickname": nickname,
    "X-Cohort": cohort,
    "X-Project": "True",
    "X-API-KEY": api_key,
    "Content-Type": "application/x-www-form-urlencoded",
}


def generate_report(ti):
    print("Making request generate_report")

    response = requests.post(f"{base_url}/generate_report", headers=headers)
    response.raise_for_status()
    task_id = json.loads(response.content)["task_id"]
    ti.xcom_push(key="task_id", value=task_id)
    print(f"Response is {response.content}")


def get_report(ti):
    print("Making request get_report")
    task_id = ti.xcom_pull(key="task_id")

    report_id = None

    for i in range(20):
        response = requests.get(
            f"{base_url}/get_report?task_id={task_id}", headers=headers
        )
        response.raise_for_status()
        print(f"Response is {response.content}")
        status = json.loads(response.content)["status"]
        if status == "SUCCESS":
            report_id = json.loads(response.content)["data"]["report_id"]
            break
        else:
            time.sleep(10)

    if not report_id:
        raise TimeoutError()

    ti.xcom_push(key="report_id", value=report_id)
    print(f"Report_id={report_id}")


def get_increment(date, ti):
    print("Making request get_increment")
    report_id = ti.xcom_pull(key="report_id")
    response = requests.get(
        f"{base_url}/get_increment?report_id={report_id}&date={str(date)}T00:00:00",
        headers=headers,
    )
    response.raise_for_status()
    print(f"Response is {response.content}")

    increment_id = json.loads(response.content)["data"]["increment_id"]
    if not increment_id:
        raise ValueError(f"Increment is empty. Most probably due to error in API call.")

    ti.xcom_push(key="increment_id", value=increment_id)
    print(f"increment_id={increment_id}")


def upload_increment_data_to_staging(filenames, date, pg_schema, ti):
    increment_id = ti.xcom_pull(key="increment_id")
    psql_hook = PostgresHook(postgres_conn_id)
    conn = psql_hook.get_conn()
    cur = conn.cursor()

    for filename in filenames:
        local_filename = date.replace("-", "") + "_local_inc_" + filename
        local_file_path = os.getcwd() + local_filename
        table_name = filename[:-8]

        # save_file_to_local
        df = pd.read_csv(
            f"{storage_url}{cohort}/{nickname}/project/{increment_id}/{filename}"
        )
        if filename != "customer_research_inc.csv":
            df.drop("id", axis=1, inplace=True)
            df.drop_duplicates(subset=["uniq_id"], inplace=True)
        if filename == "user_order_log_inc.csv" and "status" not in df.columns:
            df["status"] = "shipped"
        df.to_csv(local_file_path, index=False)

        # upload
        columns = ",".join(tuple(df.columns))
        insert_stmt = f"INSERT INTO {pg_schema}.{table_name} ({columns}) VALUES %s"
        psycopg2.extras.execute_values(cur, insert_stmt, df.values)
        conn.commit()

    cur.close()
    conn.close()


args = {
    "owner": "xxx",
    "email": ["xxx@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

business_dt = "{{ ds }}"

with DAG(
    "upload_inc_data",
    default_args=args,
    catchup=True,
    max_active_runs=1,
    start_date=datetime.today() - timedelta(days=7),
    end_date=datetime.today() - timedelta(days=1),
) as dag:
    generate_report = PythonOperator(
        task_id="generate_report", python_callable=generate_report
    )

    get_report = PythonOperator(task_id="get_report", python_callable=get_report)

    get_increment = PythonOperator(
        task_id="get_increment",
        python_callable=get_increment,
        op_kwargs={"date": business_dt},
    )

    delete_from_staging = PostgresOperator(
        task_id="delete_from_staging",
        postgres_conn_id=postgres_conn_id,
        sql="sql/delete_from_staging.sql",
    )

    upload_increment_data_to_staging = PythonOperator(
        task_id="upload_increment_data_to_staging",
        python_callable=upload_increment_data_to_staging,
        op_kwargs={
            "date": business_dt,
            "filenames": [
                "customer_research_inc.csv",
                "user_order_log_inc.csv",
                "user_activity_log_inc.csv",
            ],
            "pg_schema": "staging",
        },
    )

    for i in ["d_city", "d_item", "d_customer"]:
        dimension_tasks.append(
            PostgresOperator(
                task_id=f"update_{i}",
                postgres_conn_id=postgres_conn_id,
                sql=f"sql/mart.{i}.sql",
                dag=dag,
            )
        )

    update_f_sales = PostgresOperator(
        task_id="update_f_sales",
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_sales_inc.sql",
        parameters={"date": {business_dt}},
    )

    update_f_customer_retention = PostgresOperator(
        task_id="update_f_customer_retention",
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_customer_retention_inc.sql",
    )
    (
        generate_report
        >> get_report
        >> get_increment
        >> delete_from_staging
        >> upload_increment_data_to_staging
        >> dimension_tasks
        >> update_f_sales
        >> update_f_customer_retention
    )
