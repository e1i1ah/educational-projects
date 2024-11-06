from lib.vertica_connect import VerticaConnect
from datetime import datetime
from typing import List
from pandas import DataFrame
import pandas as pd


class StgVertica:
    TABLES = {
        "users": {
            "id": "Int64",
            "chat_name": object,
            "registration_dt": object,
            "country": object,
            "age": "Int32",
        },
        "groups": {
            "id": "Int64",
            "admin_id": "Int64",
            "group_name": object,
            "registration_dt": object,
            "is_private": "Int8",
        },
        "dialogs": {
            "message_id": "Int64",
            "message_ts": object,
            "message_from": "Int64",
            "message_to": "Int64",
            "message": object,
            "message_group": "Int64",
        },
        "group_log": {
            "group_id": "Int64",
            "user_id": "Int64",
            "user_id_from": "Int64",
            "event": object,
            "datetime": object,
        },
    }

    def __init__(self, conn: VerticaConnect, table_name: str) -> None:
        if table_name not in self.TABLES:
            raise ValueError("not valid table name")
        self.conn = conn
        self.table = table_name
        self.data_path = f"/lessons/dags/data/{self.table}.csv"

    def csv_cleaning(self) -> DataFrame:
        launch_datetime = "2020-09-03 00:00:00"
        current_datetime = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")

        df = pd.read_csv(self.data_path, index_col=False, dtype=self.TABLES[self.table])

        def df_drop_dupl(df: DataFrame, columns: List, for_all_columns=False) -> None:
            if for_all_columns:
                df.drop_duplicates(subset=columns, inplace=True, keep="first")
            else:
                for col in columns:
                    df.drop_duplicates(subset=col, inplace=True, keep="first")

        def filter_by_date(
            df: DataFrame, column: str, start: str, end: str
        ) -> DataFrame:
            df[column] = pd.to_datetime(df[column])
            return df[(df[column] >= start) & (df[column] <= end)]

        if self.table == "users":
            df.dropna(subset=self.TABLES[self.table].keys(), inplace=True)
            df_drop_dupl(df, ["id"])
            df = filter_by_date(
                df, "registration_dt", launch_datetime, current_datetime
            )
            df = df[(df.age >= 0) & (df.age <= 100)]

        elif self.table == "groups":
            df.dropna(subset=self.TABLES[self.table].keys(), inplace=True)
            df_drop_dupl(df, ["id", "group_name"])
            df = filter_by_date(
                df, "registration_dt", launch_datetime, current_datetime
            )

        elif self.table == "dialogs":
            df.dropna(subset=list(self.TABLES[self.table].keys())[:-1], inplace=True)
            df_drop_dupl(df, ["message_id"])
            df_drop_dupl(
                df, list(self.TABLES[self.table].keys())[1:], for_all_columns=True
            )
            df = filter_by_date(df, "message_ts", launch_datetime, current_datetime)

        elif self.table == "group_log":
            df.dropna(subset=["group_id", "user_id", "event", "datetime"], inplace=True)
            df_drop_dupl(df, self.TABLES[self.table].keys(), for_all_columns=True)
            df = filter_by_date(df, "datetime", launch_datetime, current_datetime)

        return df

    def load_data_in_table(self) -> None:
        tmp_data_path = "/lessons/dags/data/tmp.csv"
        df = self.csv_cleaning()
        columns = ", ".join(df.columns.to_list())
        df.to_csv(tmp_data_path, index=False)

        with open(tmp_data_path) as file:
            file.readline()
            load_dml = f"""COPY STV2024060713__STAGING.{self.table} ({columns})
                        FROM STDIN 
                        REJECTED DATA AS TABLE STV2024060713__STAGING.{self.table}_rej
                        DELIMITER ','
                        ENCLOSED BY '"'
                        ENFORCELENGTH;
                        """
            with self.conn.connection() as conn:
                with conn.cursor() as cur:
                    cur.copy(load_dml, file, buffer_size=65536)
