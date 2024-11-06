import os

import psycopg2
import requests
import json
import time
import pandas as pd

report_id = None
nickname = "xxx"
cohort = "xx"
filenames = [
    "customer_research.csv",
    "user_order_log.csv",
    "user_activity_log.csv",
    "price_log.csv",
]
sql_filenames = [
    "mart.d_item.sql",
    "mart.d_customer.sql",
    "mart.d_city.sql",
    "mart.f_sales_archive.sql",
    "mart.f_customer_retention_archive.sql",
]
api_key = "xxxx-xxxx-63c01fc31460"
base_url = "https://xxxxx.apigw.yandexcloud.net"
storage_url = "https://storage.yandexcloud.net/xxxxx"
headers = {
    "X-Nickname": nickname,
    "X-Cohort": cohort,
    "X-Project": "True",
    "X-API-KEY": api_key,
    "Content-Type": "application/x-www-form-urlencoded",
}

response = requests.post(f"{base_url}/generate_report", headers=headers)
response.raise_for_status()
task_id = json.loads(response.content)["task_id"]

for i in range(20):
    response = requests.get(f"{base_url}/get_report?task_id={task_id}", headers=headers)
    response.raise_for_status()
    status = json.loads(response.content)["status"]
    if status == "SUCCESS":
        report_id = json.loads(response.content)["data"]["report_id"]
        break
    else:
        time.sleep(10)

if not report_id:
    raise TimeoutError()

conn = psycopg2.connect(
    "host='localhost' port='5432' dbname='de' user='jovyan' password='jovyan'"
)
cur = conn.cursor()
for filename in filenames:
    local_filename = "local_" + filename
    local_file_path = os.getcwd() + "/" + local_filename
    table_name = filename[:-4]

    # save_file_to_local
    df = pd.read_csv(f"{storage_url}{cohort}/{nickname}/project/{report_id}/{filename}")
    if filename not in ("customer_research.csv", "price_log.csv"):
        df.drop("id", axis=1, inplace=True)
        df.drop_duplicates(subset=["uniq_id"], inplace=True)
        if filename == "user_order_log.csv" and "status" not in df.columns:
            df["status"] = "shipped"
    df.to_csv(local_file_path, index=False)

    # upload
    columns = ",".join(tuple(df.columns))
    if filename == "price_log.csv":
        columns = "item,price"
    with open(local_file_path) as file:
        next(file)
        cur.copy_expert(
            f"COPY staging.{table_name} ({columns}) FROM STDIN WITH (FORMAT CSV)", file
        )
        conn.commit()

for sql in sql_filenames:
    with open(f"{os.getcwd()}/{sql}", "r") as f:
        cur.execute(f.read())
        conn.commit()

cur.close()
conn.close()
