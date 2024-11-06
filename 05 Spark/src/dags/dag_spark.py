import os
from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["YARN_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["JAVA_HOME"] = "/usr"
os.environ["SPARK_HOME"] = "/usr/lib/spark"
os.environ["PYTHONPATH"] = "/usr/local/lib/python3.8"

default_args = {
    "owner": "elljah",
    "start_date": datetime(2022, 6, 15),
    "catchup": False,
}


spark_conf = {
    "spark.submit.deployMode": "client",
    "spark.executor.instances": "4",
    "spark.executor.memory": "8g",
    "spark.executor.cores": "2",
    "spark.driver.memory": "1g",
    "spark.driver.cores": "1",
}

dag_spark = DAG(
    dag_id="datalake_etl",
    default_args=default_args,
    schedule_interval="0 0 * * *",
    max_active_runs=1,
)

load_to_ods_task = SparkSubmitOperator(
    task_id="load_to_ods",
    dag=dag_spark,
    application="/path/to/run.py",
    conn_id="yarn_spark",
    application_args=["load_to_ods", "{{ ds }}"],
    conf=spark_conf,
)

user_geo_mart_task = SparkSubmitOperator(
    task_id="UserGeoMart",
    dag=dag_spark,
    application="/path/to/run.py",
    conn_id="yarn_spark",
    application_args=["UserGeoMart", "{{ ds }}"],
    conf=spark_conf,
)

zone_geo_mart_task = SparkSubmitOperator(
    task_id="ZoneGeoMart",
    dag=dag_spark,
    application="/path/to/run.py",
    conn_id="yarn_spark",
    application_args=["ZoneGeoMart", "{{ ds }}"],
    conf=spark_conf,
)

friend_recommendation_mart_task = SparkSubmitOperator(
    task_id="FriendRecommendationMart",
    dag=dag_spark,
    application="/path/to/run.py",
    conn_id="yarn_spark",
    application_args=["FriendRecommendationMart", "{{ ds }}"],
    conf=spark_conf,
)

(
    load_to_ods_task
    >> user_geo_mart_task
    >> zone_geo_mart_task
    >> friend_recommendation_mart_task
)
