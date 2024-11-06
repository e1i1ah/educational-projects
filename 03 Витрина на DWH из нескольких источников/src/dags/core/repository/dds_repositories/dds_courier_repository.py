import json
from typing import Dict

from core.domain.entities.origin_objects.origin_api_object import OriginApiObj
from core.domain.entities.dwh_objects.dds_courier_object import CouriertDds
from core.lib.interface_repository import Repository
from psycopg.rows import class_row
from psycopg import Connection


class CourierDdsRepository(Repository):
    WF_KEY = "dds_courier"

    @staticmethod
    def couriers_values(courier_obj: OriginApiObj) -> Dict:
        couriers_value = json.loads(courier_obj.object_value)
        courier = {}
        courier["courier_id"] = couriers_value["_id"]
        courier["courier_name"] = couriers_value["name"]
        return courier

    def save_objects(self, obj: OriginApiObj, conn: Connection) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                        INSERT INTO dds.dm_couriers (courier_id, courier_name)
                        VALUES (%(courier_id)s, %(courier_name)s)
                        ON CONFLICT (courier_id)
                        DO UPDATE
                        SET courier_name = EXCLUDED.courier_name
                    """,
                self.couriers_values(obj),
            )

    def get_objects(self, conn: Connection) -> Dict:
        with conn.cursor(row_factory=class_row(CouriertDds)) as cur:
            cur.execute(
                """
                    SELECT id, courier_id, courier_name
                    FROM dds.dm_couriers 
                """
            )
            objs = cur.fetchall()

        result_dict = {}

        for c in objs:
            result_dict[c.courier_id] = c

        return result_dict
