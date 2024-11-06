from lib.s3_connect import S3Connect
from lib.vertica_connect import VerticaConnect

from connectors.s3_connector import S3Connector
from repository.stg_vertica_repository import StgVertica


class EnvConfig:
    S3_CONNECTION_PARAMS = {
        "aws_access_key_id": "xxxx-hlt2K",
        "aws_secret_access_key": "xxxxxx-xxxx",
        "endpoint_url": "https://storage.yandexcloud.net",
    }

    VERTICA_CONNECTION_PARAMS = {
        "host": "vertica.tgcloudenv.ru",
        "port": "xxxx",
        "user": "xxxx",
        "password": "xxxxx",
        "database": "dwh",
        "autocommit": True,
    }


class DependencyConfig:

    @staticmethod
    def s3_connection() -> S3Connect:
        return S3Connect(**EnvConfig.S3_CONNECTION_PARAMS)

    @staticmethod
    def vertica_connection() -> VerticaConnect:
        return VerticaConnect(**EnvConfig.VERTICA_CONNECTION_PARAMS)

    class Connectors:
        @staticmethod
        def origin_s3_connector(bucket: str, key: str) -> S3Connector:
            return S3Connector(DependencyConfig.s3_connection(), bucket, key)

    class Repository:
        @staticmethod
        def stg_vertica_repo(table_name: str):
            return StgVertica(DependencyConfig.vertica_connection(), table_name)
