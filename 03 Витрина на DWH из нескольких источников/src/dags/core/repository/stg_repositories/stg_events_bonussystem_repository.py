from psycopg import Connection
from psycopg.rows import class_row
from typing import List
from datetime import datetime

from core.lib.interface_repository import Repository
from core.domain.entities.origin_objects.origin_pg_event_object import OriginEventObj


class EventStgRepository(Repository):
    WF_KEY = "stg_events"
    WF_SETTINGS = {"last_loaded_id": 539657}

    def save_objects(self, conn: Connection, event: OriginEventObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.bonussystem_events(id, event_ts, event_type, event_value)
                    VALUES (%(id)s, %(event_ts)s, %(event_type)s, %(event_value)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        event_ts = EXCLUDED.event_ts,
                        event_type = EXCLUDED.event_type,
                        event_value = EXCLUDED.event_value;
                """,
                {
                    "id": event.id,
                    "event_ts": event.event_ts,
                    "event_type": event.event_type,
                    "event_value": event.event_value,
                },
            )

    def get_objects(
        self, conn: Connection, last_update_ts: datetime
    ) -> List[OriginEventObj]:
        bonus_transaction = "bonus_transaction"
        with conn.cursor(row_factory=class_row(OriginEventObj)) as cur:
            cur.execute(
                """
                        SELECT id, event_ts, event_type, event_value
                        FROM stg.bonussystem_events
                        WHERE event_type = %(bonus_transaction)s AND %(last_update_ts)s < event_ts
                    """,
                {
                    "last_update_ts": last_update_ts,
                    "bonus_transaction": bonus_transaction,
                },
            )
            objs = cur.fetchall()
        return objs
