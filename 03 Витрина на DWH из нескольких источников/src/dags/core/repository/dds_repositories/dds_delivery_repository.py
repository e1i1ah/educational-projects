import json
from typing import Dict
from datetime import datetime

from core.domain.entities.origin_objects.origin_api_object import OriginApiObj
from core.repository.dds_repositories.dds_courier_repository import CourierDdsRepository
from core.repository.dds_repositories.dds_orders_repository import OrdersDdsRepository
from core.repository.dds_repositories.dds_timestamps_repository import (
    TimestampsDdsRepository,
)
from core.lib.interface_repository import Repository
from psycopg import Connection


class DeliveryDdsRepository(Repository):
    WF_KEY = "dds_delivery"

    def __init__(self) -> None:
        self.couriers = None
        self.timestamps = None
        self.orders = None

    def delivery_values(self, delivery_obj: OriginApiObj, conn: Connection) -> Dict:
        if not self.couriers:
            self.couriers = CourierDdsRepository().get_objects(conn)
        if not self.timestamps:
            self.timestamps = TimestampsDdsRepository().get_objects(conn)
        if not self.orders:
            self.orders = OrdersDdsRepository().get_objects(conn)

        delivery_value = json.loads(delivery_obj.object_value)
        delivery = {}
        delivery["delivery_id"] = delivery_value["delivery_id"]
        if delivery_value["order_id"] in self.orders:
            delivery["order_id"] = self.orders[delivery_value["order_id"]].id
        else:
            return None
        delivery["courier_id"] = self.couriers[delivery_value["courier_id"]].id
        delivery["timestamp_id"] = self.timestamps[
            str(datetime.fromisoformat(delivery_value["delivery_ts"]))
        ].id
        delivery["address"] = delivery_value["address"]
        delivery["rate"] = delivery_value["rate"]
        delivery["tip_sum"] = delivery_value["tip_sum"]
        return delivery

    def save_objects(self, obj: OriginApiObj, conn: Connection) -> None:
        obj = self.delivery_values(obj, conn)
        if obj:
            with conn.cursor() as cur:
                cur.execute(
                    """
                            INSERT INTO dds.dm_delivery (delivery_id, order_id, courier_id, timestamp_id, address, rate, tip_sum)
                            VALUES (%(delivery_id)s, %(order_id)s, %(courier_id)s, %(timestamp_id)s, %(address)s, %(rate)s, %(tip_sum)s)
                            ON CONFLICT (delivery_id)
                            DO NOTHING
                        """,
                    obj,
                )

    def get_objects(self):
        raise NotImplementedError
