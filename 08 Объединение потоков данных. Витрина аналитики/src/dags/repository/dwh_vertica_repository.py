from typing import List

from lib.vertica_connect import VerticaConnect
from repository.repository_interface import Repository


class VerticaCurrencyRepository(Repository):
    FILE_NAME = "currencies"

    def _copy_from_csv_with_conflict_handling(
        self, file_path: str, conn: VerticaConnect
    ):
        try:
            with conn.connection() as conn:
                conn.begin()
                with conn.cursor() as cur:
                    create_temp_table = """
                        CREATE LOCAL TEMPORARY TABLE currencies_temp
                        ON COMMIT PRESERVE ROWS AS
                        SELECT *
                        FROM STV2024060713__STAGING.currencies
                        WHERE 1=0;
                    """
                    copy_query = f"""
                        COPY currencies_temp (
                            date_update,
                            currency_code,
                            currency_code_with,
                            currency_with_div
                        )
                        FROM LOCAL '{file_path}'
                        DELIMITER ','
                        ENCLOSED BY '"'
                        SKIP 1
                        REJECTED DATA AS TABLE
                        STV2024060713__STAGING.currencies_rejected;
                    """
                    insert_query = """
                        INSERT INTO STV2024060713__STAGING.currencies (
                            date_update,
                            currency_code,
                            currency_code_with,
                            currency_with_div
                        )
                        SELECT
                            date_update,
                            currency_code,
                            currency_code_with,
                            currency_with_div
                        FROM currencies_temp temp
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM STV2024060713__STAGING.currencies main
                            WHERE main.date_update = temp.date_update
                            AND main.currency_code = temp.currency_code
                            AND main.currency_code_with = temp.currency_code_with
                        );
                    """
                    cur.execute(create_temp_table)
                    cur.execute(copy_query)
                    cur.execute(insert_query)

                conn.commit()

        except Exception as e:
            conn.rollback()
            print(f"Error during copying currencies file to Vertica: {e}")
            raise

    def save_objects(self, file_path: str, conn: VerticaConnect):
        file_full_path = f"{file_path}currencies.csv"
        self._copy_from_csv_with_conflict_handling(file_full_path, conn)

    def get_objects(self):
        return NotImplemented


class VerticaTransactionRepository(Repository):
    FILE_NAME = "transactions"

    def _copy_from_csv_with_conflict_handling(
        self, file_path: str, conn: VerticaConnect
    ):
        try:
            with conn.connection() as conn:
                conn.begin()
                with conn.cursor() as cur:
                    create_temp_table = """
                        CREATE LOCAL TEMPORARY TABLE transactions_temp
                        ON COMMIT PRESERVE ROWS AS
                        SELECT *
                        FROM STV2024060713__STAGING.transactions
                        WHERE 1=0;
                    """
                    copy_query = f"""
                        COPY transactions_temp (
                            operation_id,
                            account_number_from,
                            account_number_to,
                            currency_code,
                            country,
                            status,
                            transaction_type,
                            amount,
                            transaction_dt
                        )
                        FROM LOCAL '{file_path}'
                        DELIMITER ','
                        ENCLOSED BY '"'
                        SKIP 1
                        REJECTED DATA AS TABLE
                        STV2024060713__STAGING.transactions_rejected;
                    """
                    insert_query = """
                        INSERT INTO STV2024060713__STAGING.transactions (
                            operation_id,
                            account_number_from,
                            account_number_to,
                            currency_code,
                            country,
                            status,
                            transaction_type,
                            amount,
                            transaction_dt
                        )
                        SELECT
                            operation_id,
                            account_number_from,
                            account_number_to,
                            currency_code,
                            country,
                            status,
                            transaction_type,
                            amount,
                            transaction_dt
                        FROM transactions_temp temp
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM STV2024060713__STAGING.transactions main
                            WHERE main.operation_id = temp.operation_id
                            AND main.account_number_from = temp.account_number_from
                            AND main.account_number_to = temp.account_number_to
                            AND main.currency_code = temp.currency_code
                            AND main.country = temp.country
                            AND main.status = temp.status
                            AND main.transaction_type = temp.transaction_type
                            AND main.amount = temp.amount
                            AND main.transaction_dt = temp.transaction_dt
                        );
                    """
                    cur.execute(create_temp_table)
                    cur.execute(copy_query)
                    cur.execute(insert_query)

                conn.commit()

        except Exception as e:
            conn.rollback()
            print(f"Error during copying transactions file to Vertica: {e}")
            raise

    def save_objects(self, file_path: str, conn: VerticaConnect):
        file_full_path = f"{file_path}transactions.csv"
        self._copy_from_csv_with_conflict_handling(file_full_path, conn)

    def get_objects(self):
        return NotImplemented


class VerticaGlobalMetricsRepository(Repository):
    def save_objects(
        self, file_path: str, vertica_conn: VerticaConnect, execution_date_dt
    ):
        try:
            with open(file_path, "r") as sql_file:
                sql_query = sql_file.read()

            sql_query = sql_query.replace(
                "{{ ds }}", execution_date_dt.to_date_string()
            )

            with vertica_conn.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql_query)
        except Exception as e:
            print(f"Error during executing SQL file {file_path}: {e}")
            raise

    def get_objects(self):
        NotImplemented
