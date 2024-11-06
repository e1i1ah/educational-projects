from psycopg import Connection
from datetime import datetime
from psycopg.rows import class_row
from typing import List

from core.lib.interface_repository import Repository
from core.domain.entities.origin_objects.origin_mongo_object import OriginMongoObj


class OrdersStgRepository(Repository):
    WF_KEY = "stg_orders"
    WF_SETTINGS = {"last_loaded_date": datetime(2024, 1, 1).isoformat()}

    def save_objects(self, conn: Connection, obj: OriginMongoObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                        INSERT INTO stg.ordersystem_orders(object_id, object_value, update_ts)
                        VALUES (%(id)s, %(val)s, %(update_ts)s)
                        ON CONFLICT (object_id) DO UPDATE
                        SET
                            object_value = EXCLUDED.object_value,
                            update_ts = EXCLUDED.update_ts;
                    """,
                {
                    "id": obj.object_id,
                    "val": obj.object_value,
                    "update_ts": obj.update_ts,
                },
            )

    def get_objects(
        self, conn: Connection, last_update_ts: datetime
    ) -> List[OriginMongoObj]:

        with conn.cursor(row_factory=class_row(OriginMongoObj)) as cur:
            cur.execute(
                """
                        SELECT object_id, object_value, update_ts
                        FROM stg.ordersystem_orders 
                        WHERE %(last_update_ts)s < update_ts
                    """,
                {"last_update_ts": last_update_ts},
            )
            objs = cur.fetchall()
        return objs
