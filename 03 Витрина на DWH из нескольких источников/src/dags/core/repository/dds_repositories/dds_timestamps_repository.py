import json
from datetime import datetime
from typing import Dict

from core.domain.entities.origin_objects.origin_mongo_object import OriginMongoObj
from core.domain.entities.origin_objects.origin_api_object import OriginApiObj
from core.domain.entities.dwh_objects.dds_timestamp_object import TimestampDds
from core.lib.interface_repository import Repository
from psycopg import Connection
from psycopg.rows import class_row


class TimestampsDdsRepository(Repository):
    WF_KEY = "dds_timestamps"

    @staticmethod
    def timestamps_values(timestamp_obj: OriginMongoObj) -> Dict:
        timestamp_values = json.loads(timestamp_obj.object_value)
        timestamp = {}
        if isinstance(timestamp_obj, OriginMongoObj):
            timestamp["ts"] = datetime.strptime(
                timestamp_values["date"], "%Y-%m-%d %H:%M:%S"
            )
        elif isinstance(timestamp_obj, OriginApiObj):
            timestamp["ts"] = datetime.fromisoformat(timestamp_values["delivery_ts"])
        timestamp["year"] = timestamp["ts"].year
        timestamp["month"] = timestamp["ts"].month
        timestamp["day"] = timestamp["ts"].day
        timestamp["date"] = timestamp["ts"].date()
        timestamp["time"] = timestamp["ts"].time()
        return timestamp

    def save_objects(self, obj: OriginMongoObj, conn: Connection) -> None:
        obj_val = json.loads(obj.object_value)

        if (
            "final_status" in obj_val
            and obj_val["final_status"] in ("CLOSED", "CANCELLED")
        ) or "final_status" not in obj_val:

            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.dm_timestamps (ts, year, month, day, date, time)
                        VALUES (%(ts)s, %(year)s, %(month)s, %(day)s, %(date)s, %(time)s)
                        ON CONFLICT (ts)
                        DO NOTHING
                    """,
                    self.timestamps_values(obj),
                )

    def get_objects(self, conn: Connection) -> Dict:
        with conn.cursor(row_factory=class_row(TimestampDds)) as cur:
            cur.execute(
                """
                    SELECT id, ts, year, month, day, time, date
                    FROM dds.dm_timestamps    
                """
            )
            objs = cur.fetchall()

        result_dict = {}

        for t in objs:
            result_dict[str(t.ts)] = t

        return result_dict
