import json
from typing import Dict
from datetime import datetime
from psycopg import Connection
from psycopg.rows import class_row

from core.domain.entities.origin_objects.origin_mongo_object import OriginMongoObj
from core.repository.dds_repositories.dds_users_repository import UserDdsRepository
from core.repository.dds_repositories.dds_restaurants_repository import (
    RestaurantsDdsRepository,
)
from core.repository.dds_repositories.dds_timestamps_repository import (
    TimestampsDdsRepository,
)
from core.domain.entities.dwh_objects.dds_order_object import OrderDds
from core.lib.interface_repository import Repository


from typing import List


class OrdersDdsRepository(Repository):
    WF_KEY = "dds_orders"

    def __init__(self) -> None:
        self.restaurants = None
        self.timestamps = None
        self.users = None

    @staticmethod
    def get_correct_restaurant_version(
        rest_id: str, order_date: str, restaurants: Dict
    ) -> int:
        order_date = datetime.fromisoformat(order_date)

        for rest in restaurants[rest_id]:
            if rest.active_to > order_date >= rest.active_from:
                return rest.id

        for rest in restaurants[rest_id]:
            if rest.active_to == datetime(2099, 12, 31):
                return rest.id

    def order_values(self, order_obj: OriginMongoObj, conn: Connection) -> Dict:
        if not self.restaurants:
            self.restaurants = RestaurantsDdsRepository().get_objects(conn)
        if not self.timestamps:
            self.timestamps = TimestampsDdsRepository().get_objects(conn)
        if not self.users:
            self.users = UserDdsRepository().get_objects(conn)

        order_value = json.loads(order_obj.object_value)
        order = {}
        order["user_id"] = self.users[order_value["user"]["id"]].id
        order["restaurant_id"] = self.get_correct_restaurant_version(
            order_value["restaurant"]["id"], order_value["date"], self.restaurants
        )
        order["timestamp_id"] = self.timestamps[
            str(datetime.strptime(order_value["date"], "%Y-%m-%d %H:%M:%S"))
        ].id
        order["order_key"] = order_value["_id"]
        order["order_status"] = order_value["final_status"]
        return order

    def save_objects(self, obj: OriginMongoObj, conn: Connection):
        obj = self.order_values(obj, conn)

        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_orders (user_id, restaurant_id, timestamp_id, order_key, order_status)
                    VALUES (%(user_id)s, %(restaurant_id)s, %(timestamp_id)s, %(order_key)s, %(order_status)s)
                    ON CONFLICT (order_key)
                    DO UPDATE
                    SET order_status = EXCLUDED.timestamp_id
                """,
                obj,
            )

    def get_objects(self, conn: Connection) -> Dict:
        with conn.cursor(row_factory=class_row(OrderDds)) as cur:
            cur.execute(
                """
                    SELECT id, user_id, restaurant_id, timestamp_id, order_key, order_status
                    FROM dds.dm_orders  
                """
            )
            objs = cur.fetchall()

        result_dict = {}

        for ord in objs:
            result_dict[ord.order_key] = ord

        return result_dict
