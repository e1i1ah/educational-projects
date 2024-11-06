import json
from datetime import datetime
from typing import Dict

from core.domain.entities.origin_objects.origin_mongo_object import OriginMongoObj
from core.domain.entities.dwh_objects.dds_restaurant_object import RestaurantDds
from core.lib.interface_repository import Repository
from psycopg.rows import class_row
from psycopg import Connection


class RestaurantsDdsRepository(Repository):
    WF_KEY = "dds_restaurants"

    @staticmethod
    def restaurant_values(restaurant_obj: OriginMongoObj) -> Dict:
        restaurant_value = json.loads(restaurant_obj.object_value)
        restaurant = {}
        restaurant["restaurant_id"] = restaurant_value["_id"]
        restaurant["restaurant_name"] = restaurant_value["name"]
        restaurant["active_from"] = restaurant_obj.update_ts
        restaurant["active_to"] = datetime(2099, 12, 31)
        return restaurant

    def save_objects(self, obj: OriginMongoObj, conn: Connection) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    UPDATE dds.dm_restaurants
                    SET active_to = %(active_from)s
                    WHERE restaurant_id = %(restaurant_id)s 
                          AND restaurant_name != %(restaurant_name)s 
                          AND active_to = %(active_to)s      
                """,
                self.restaurant_values(obj),
            )

        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_restaurants (restaurant_id, restaurant_name, active_from, active_to)
                    VALUES (%(restaurant_id)s, %(restaurant_name)s, %(active_from)s, %(active_to)s)
                    ON CONFLICT (restaurant_id, restaurant_name)
                    DO NOTHING
                """,
                self.restaurant_values(obj),
            )

    def get_objects(self, conn: Connection) -> Dict:
        with conn.cursor(row_factory=class_row(RestaurantDds)) as cur:
            cur.execute(
                """
                    SELECT id, restaurant_id, restaurant_name, active_from, active_to
                    FROM dds.dm_restaurants    
                """
            )
            objs = cur.fetchall()

        result_dict = {}

        for rest in objs:
            result_dict[rest.restaurant_id] = result_dict.get(
                rest.restaurant_id, []
            ) + [rest]

        return result_dict
