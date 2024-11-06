from airflow.hooks.base import BaseHook
from airflow.models.variable import Variable
from airflow.hooks.http_hook import HttpHook

from core.lib.pg_connect import PgConnect
from core.lib.mongo_connect import MongoConnect
from core.connectors.origin_mongo_connector import OriginMongoConnector
from core.connectors.origin_pg_connector import OriginPgConnector
from core.connectors.origin_api_connector import OriginApiConnector
from core.repository.stg_repositories.stg_couriers_api_repository import (
    CourierStgRepository,
)
from core.repository.stg_repositories.stg_deliveries_api_repository import (
    DeliveriesStgRepository,
)
from core.repository.stg_repositories.stg_events_bonussystem_repository import (
    EventStgRepository,
)
from core.repository.stg_repositories.stg_orders_ordersystem_repository import (
    OrdersStgRepository,
)
from core.repository.stg_repositories.stg_ranks_bonussystem_repository import (
    RanksStgRepository,
)
from core.repository.stg_repositories.stg_restaurants_ordersystem_repository import (
    RestaurantsStgRepository,
)
from core.repository.stg_repositories.stg_users_bonussystem_repository import (
    UsersBonussystemStgRepository,
)
from core.repository.stg_repositories.stg_users_ordersystem_repository import (
    UsersOrdersystemStgRepository,
)
from core.repository.dds_repositories.dds_courier_repository import CourierDdsRepository
from core.repository.dds_repositories.dds_delivery_repository import (
    DeliveryDdsRepository,
)
from core.repository.dds_repositories.dds_fct_product_sales import (
    ProductSalesDdsRepository,
)
from core.repository.dds_repositories.dds_orders_repository import OrdersDdsRepository
from core.repository.dds_repositories.dds_products_repository import (
    ProductsDdsRepository,
)
from core.repository.dds_repositories.dds_restaurants_repository import (
    RestaurantsDdsRepository,
)
from core.repository.dds_repositories.dds_timestamps_repository import (
    TimestampsDdsRepository,
)
from core.repository.dds_repositories.dds_users_repository import UserDdsRepository


class EnvConfig:
    POSTGRES_DWH_DB = "PG_WAREHOUSE_CONNECTION"
    POSTGRES_ORIGIN_DB = "PG_ORIGIN_BONUS_SYSTEM_CONNECTION"
    MONGO_DB = {
        "cert_path": Variable.get("MONGO_DB_CERTIFICATE_PATH"),
        "user": Variable.get("MONGO_DB_USER"),
        "pw": Variable.get("MONGO_DB_PASSWORD"),
        "rs": Variable.get("MONGO_DB_REPLICA_SET"),
        "auth_db": Variable.get("MONGO_DB_DATABASE_NAME"),
        "main_db": Variable.get("MONGO_DB_DATABASE_NAME"),
        "host": Variable.get("MONGO_DB_HOST"),
    }
    API_HOST = HttpHook.get_connection("api_conn").host
    API_HEADERS = {
        "X-Nickname": "xxx",
        "X-Cohort": "xxx",
        "X-API-KEY": HttpHook.get_connection("api_conn").extra_dejson.get("api_key"),
    }


class DependencyConfig:

    default_owner = "airflow"

    @staticmethod
    def dwh_pg_db_connection() -> PgConnect:
        connection = BaseHook.get_connection(EnvConfig.POSTGRES_DWH_DB)
        return PgConnect(
            str(connection.host),
            str(connection.port),
            str(connection.schema),
            str(connection.login),
            str(connection.password),
        )

    @staticmethod
    def origin_pg_db_connection() -> PgConnect:
        connection = BaseHook.get_connection(EnvConfig.POSTGRES_ORIGIN_DB)
        return PgConnect(
            str(connection.host),
            str(connection.port),
            str(connection.schema),
            str(connection.login),
            str(connection.password),
        )

    @staticmethod
    def origin_api_connection() -> tuple:
        url_headers = EnvConfig.API_HOST, EnvConfig.API_HEADERS
        return url_headers

    @staticmethod
    def mongo_connection() -> MongoConnect:
        return MongoConnect(**EnvConfig.MONGO_DB)

    class Connectors:
        @staticmethod
        def origin_mongo_connector(collection: str) -> OriginMongoConnector:
            return OriginMongoConnector(DependencyConfig.mongo_connection(), collection)

        @staticmethod
        def origin_pg_connector(collection: str) -> OriginPgConnector:
            return OriginPgConnector(
                DependencyConfig.origin_pg_db_connection(), collection
            )

        @staticmethod
        def origin_api_connector(collection: str) -> OriginApiConnector:
            return OriginApiConnector(
                DependencyConfig.origin_api_connection(), collection
            )

    class Repository:
        @staticmethod
        def stg_couriers_api_repository() -> CourierStgRepository:
            return CourierStgRepository()

        @staticmethod
        def stg_deliveries_api_repository() -> DeliveriesStgRepository:
            return DeliveriesStgRepository()

        @staticmethod
        def stg_events_bonussystem_repository() -> EventStgRepository:
            return EventStgRepository()

        @staticmethod
        def stg_orders_ordersystem_repository() -> OrdersStgRepository:
            return OrdersStgRepository()

        @staticmethod
        def stg_ranks_bonussystem_repository() -> RanksStgRepository:
            return RanksStgRepository()

        @staticmethod
        def stg_restaurants_ordersystem_repository() -> RestaurantsStgRepository:
            return RestaurantsStgRepository()

        @staticmethod
        def stg_users_bonussystem_repository() -> UsersBonussystemStgRepository:
            return UsersBonussystemStgRepository()

        @staticmethod
        def stg_users_ordersystem_repository() -> UsersOrdersystemStgRepository:
            return UsersOrdersystemStgRepository()

        @staticmethod
        def dds_users_repository() -> UserDdsRepository:
            return UserDdsRepository()

        @staticmethod
        def dds_timestamps_repository() -> TimestampsDdsRepository:
            return TimestampsDdsRepository()

        @staticmethod
        def dds_restaurants_repository() -> RestaurantsDdsRepository:
            return RestaurantsDdsRepository()

        @staticmethod
        def dds_products_repository() -> ProductsDdsRepository:
            return ProductsDdsRepository()

        @staticmethod
        def dds_orders_repository() -> OrdersDdsRepository:
            return OrdersDdsRepository()

        @staticmethod
        def dds_product_sales_repository() -> ProductSalesDdsRepository:
            return ProductSalesDdsRepository()

        @staticmethod
        def dds_delivery_repository() -> DeliveryDdsRepository:
            return DeliveryDdsRepository()

        @staticmethod
        def dds_courier_repository() -> CourierDdsRepository:
            return CourierDdsRepository()
