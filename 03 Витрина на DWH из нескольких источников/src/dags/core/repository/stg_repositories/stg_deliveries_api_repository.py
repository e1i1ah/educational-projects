from psycopg import Connection
from datetime import datetime
from psycopg.rows import class_row
from typing import List

from core.lib.interface_repository import Repository
from core.domain.entities.origin_objects.origin_api_object import OriginApiObj


class DeliveriesStgRepository(Repository):
    WF_KEY = "stg_deliveries"
    WF_SETTINGS = {
        "last_loaded_date": datetime(2024, 1, 1).strftime("%Y-%m-%d %H:%M:%S")
    }

    def save_objects(self, conn: Connection, delivery: OriginApiObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.api_deliveries(object_id, object_value, update_ts)
                    VALUES (%(object_id)s, %(object_value)s, %(update_ts)s)
                    ON CONFLICT (object_id) DO UPDATE
                    SET
                        object_id = EXCLUDED.object_id,
                        object_value = EXCLUDED.object_value,
                        update_ts = EXCLUDED.update_ts
                """,
                {
                    "object_id": delivery.object_id,
                    "object_value": delivery.object_value,
                    "update_ts": delivery.update_ts,
                },
            )

    def get_objects(
        self, conn: Connection, last_update_ts: datetime
    ) -> List[OriginApiObj]:

        with conn.cursor(row_factory=class_row(OriginApiObj)) as cur:
            cur.execute(
                """
                        SELECT object_id, object_value, update_ts
                        FROM stg.api_deliveries
                        WHERE %(last_update_ts)s < update_ts
                    """,
                {"last_update_ts": last_update_ts},
            )
            objs = cur.fetchall()
        return objs
