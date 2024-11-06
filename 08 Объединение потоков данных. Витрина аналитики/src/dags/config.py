import os

from lib.vertica_connect import VerticaConnect
from lib.pg_connect import PgConnect
from repository.origin_pg_repository import (
    OriginPgTransactionRepository,
    OriginPgCurrencyRepository,
)
from repository.dwh_vertica_repository import (
    VerticaCurrencyRepository,
    VerticaTransactionRepository,
    VerticaGlobalMetricsRepository,
)


class EnvConfig:
    PG_CONNECTION_PARAMS = {
        "host": os.getenv("PG_WAREHOUSE_HOST", "localhost"),
        "port": int(os.getenv("PG_WAREHOUSE_PORT", 5432)),
        "db_name": os.getenv("PG_WAREHOUSE_DBNAME", "default_db_pg"),
        "user": os.getenv("PG_WAREHOUSE_USER", "default_user_pg"),
        "password": os.getenv("PG_WAREHOUSE_PASSWORD", "default_password_pg"),
    }

    VERTICA_CONNECTION_PARAMS = {
        "host": os.getenv("VERTICA_HOST", "vertica.tgcloudenv.ru"),
        "port": int(os.getenv("VERTICA_PORT", 5433)),
        "user": os.getenv("VERTICA_USER", "default_user_vertica"),
        "password": os.getenv("VERTICA_PASSWORD", "default_password_vertica"),
        "database": os.getenv("VERTICA_DATABASE", "default_db_vertica"),
        "autocommit": os.getenv("VERTICA_AUTOCOMMIT", "True"),
    }

    STG_DATA_FILE_PATH = os.getenv("STG_DATA_FILE_PATH")


class DependencyConfig:

    @staticmethod
    def vertica_connection() -> VerticaConnect:
        return VerticaConnect(**EnvConfig.VERTICA_CONNECTION_PARAMS)

    @staticmethod
    def pg_connection() -> PgConnect:
        return PgConnect(**EnvConfig.PG_CONNECTION_PARAMS)

    @staticmethod
    def get_file_path() -> str:
        return EnvConfig.STG_DATA_FILE_PATH

    class Repository:

        @staticmethod
        def get_origin_pg_transaction_repository() -> OriginPgTransactionRepository:
            return OriginPgTransactionRepository()

        @staticmethod
        def get_origin_pg_currency_repository() -> OriginPgCurrencyRepository:
            return OriginPgCurrencyRepository()

        @staticmethod
        def get_dwh_vertica_transaction_repository() -> VerticaTransactionRepository:
            return VerticaTransactionRepository()

        @staticmethod
        def get_dwh_vertica_currency_repository() -> VerticaCurrencyRepository:
            return VerticaCurrencyRepository()

        @staticmethod
        def get_dwh_vertica_global_metrics_repository() -> (
            VerticaGlobalMetricsRepository
        ):
            return VerticaGlobalMetricsRepository()
