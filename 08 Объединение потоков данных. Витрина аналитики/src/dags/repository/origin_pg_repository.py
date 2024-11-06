import pendulum
import csv
from typing import List
from psycopg2.extensions import connection as PgConnection
from psycopg2.extras import DictCursor
from pydantic import parse_obj_as, BaseModel
from entity.currency_entity import Currency
from entity.transaction_entity import Transaction
from repository.repository_interface import Repository


class OriginPgRepository(Repository):
    def _fetch_data(self, query: str, params: dict, conn: PgConnection) -> List[dict]:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(query, params)
            return cur.fetchall()

    def _save_to_csv(self, rows: List[BaseModel], headers: List[str], file_path: str):
        with open(file_path, "w", newline="") as csvfile:
            csv_writer = csv.DictWriter(csvfile, fieldnames=headers)
            csv_writer.writeheader()
            for row in rows:
                record = row.dict()
                payload_record = record["payload"].dict()
                csv_writer.writerow(
                    {key: payload_record.get(key, record.get(key)) for key in headers}
                )

    def get_objects(self):
        return NotImplemented

    def save_objects(self):
        return NotImplemented


class OriginPgTransactionRepository(OriginPgRepository):
    FILE_NAME = "transactions"

    def get_objects(self, dttm: pendulum.DateTime, file_path: str, conn: PgConnection):
        query = """
            SELECT object_id, object_type, sent_dttm, payload
            FROM stg.transactions_currencies
            WHERE sent_dttm::date = %(target_date)s AND object_type = %(type)s
        """
        headers = [
            "operation_id",
            "account_number_from",
            "account_number_to",
            "currency_code",
            "country",
            "status",
            "transaction_type",
            "amount",
            "transaction_dt",
        ]
        params = {"target_date": dttm.to_date_string(), "type": "TRANSACTION"}
        rows = self._fetch_data(query, params, conn)
        if not rows:
            return
        transactions = parse_obj_as(List[Transaction], rows)
        self._save_to_csv(transactions, headers, f"{file_path}{self.FILE_NAME}.csv")
        return self.FILE_NAME


class OriginPgCurrencyRepository(OriginPgRepository):
    FILE_NAME = "currencies"

    def get_objects(self, dttm: pendulum.DateTime, file_path: str, conn: PgConnection):
        query = """
            SELECT object_id, object_type, sent_dttm, payload
            FROM stg.transactions_currencies
            WHERE sent_dttm::date = %(target_date)s AND object_type = %(type)s
        """
        headers = [
            "date_update",
            "currency_code",
            "currency_code_with",
            "currency_with_div",
        ]
        params = {"target_date": dttm.to_date_string(), "type": "CURRENCY"}
        rows = self._fetch_data(query, params, conn)
        if not rows:
            return
        currencies = parse_obj_as(List[Currency], rows)
        self._save_to_csv(currencies, headers, f"{file_path}{self.FILE_NAME}.csv")
        return self.FILE_NAME
