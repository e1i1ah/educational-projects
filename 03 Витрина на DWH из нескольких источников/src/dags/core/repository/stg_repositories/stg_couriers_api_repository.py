from psycopg import Connection
from datetime import datetime
from psycopg.rows import class_row
from typing import List

from core.lib.interface_repository import Repository
from core.domain.entities.origin_objects.origin_api_object import OriginApiObj


class CourierStgRepository(Repository):
    WF_KEY = "stg_courier"
    WF_SETTINGS = {"offset": 0}

    def save_objects(self, conn: Connection, courier: OriginApiObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.api_couriers(object_id, object_value, update_ts)
                    VALUES (%(object_id)s, %(object_value)s, %(update_ts)s)
                    ON CONFLICT (object_id) DO UPDATE
                    SET
                        object_id = EXCLUDED.object_id,
                        object_value = EXCLUDED.object_value,
                        update_ts = EXCLUDED.update_ts
                """,
                {
                    "object_id": courier.object_id,
                    "object_value": courier.object_value,
                    "update_ts": courier.update_ts,
                },
            )

    def get_offset(self, conn: Connection):
        with conn.cursor() as cur:
            cur.execute(
                """
                    SELECT count(object_id) 
                    FROM stg.api_couriers
                """
            )
            return cur.fetchone()

    def get_objects(
        self, conn: Connection, last_update_ts: datetime
    ) -> List[OriginApiObj]:

        with conn.cursor(row_factory=class_row(OriginApiObj)) as cur:
            cur.execute(
                """
                        SELECT object_id, object_value, update_ts
                        FROM stg.api_couriers
                        WHERE %(last_update_ts)s < update_ts
                    """,
                {"last_update_ts": last_update_ts},
            )
            objs = cur.fetchall()
        return objs
