from psycopg.rows import class_row
from typing import List, Any
from datetime import datetime

from core.lib.interface_connector import Connector
from core.domain.entities.origin_objects.origin_pg_event_object import OriginEventObj
from core.domain.entities.origin_objects.origin_pg_rank_object import OriginRankObj
from core.domain.entities.origin_objects.origin_pg_user_object import OriginUserObj
from core.lib.pg_connect import PgConnect


class OriginPgConnector(Connector):
    def __init__(self, pg: PgConnect, collection: str) -> None:
        self.dbs = pg
        self.valid_collections = ("ranks", "users", "events")
        if collection in self.valid_collections:
            self.collection = collection
        else:
            raise ValueError("not valid collection name")

    def download_dataset(
        self,
        limit: int = 5000,
        offset: int = None,
        last_loaded_date: datetime = None,
        last_loaded_id: int = None,
    ) -> List[Any]:

        if last_loaded_id is None:
            raise ValueError("not valid last_loaded_id")

        if self.collection == "events":
            with self.dbs.client().cursor(row_factory=class_row(OriginEventObj)) as cur:
                cur.execute(
                    """
                        SELECT id, event_ts, event_type, event_value
                        FROM outbox
                        WHERE id > %(load_threshold)s 
                        ORDER BY id ASC 
                        LIMIT %(limit)s; 
                    """,
                    {"load_threshold": last_loaded_id, "limit": limit},
                )
                objs = cur.fetchall()
            return objs

        elif self.collection == "ranks":
            with self.dbs.client().cursor(row_factory=class_row(OriginRankObj)) as cur:
                cur.execute(
                    """
                        SELECT id, name, bonus_percent, min_payment_threshold
                        FROM ranks
                        WHERE id > %(load_threshold)s
                        ORDER BY id ASC
                        LIMIT %(limit)s;  
                    """,
                    {"load_threshold": last_loaded_id, "limit": limit},
                )
                objs = cur.fetchall()
            return objs

        elif self.collection == "users":
            with self.dbs.client().cursor(row_factory=class_row(OriginUserObj)) as cur:
                cur.execute(
                    """
                        SELECT id, order_user_id
                        FROM users
                        WHERE id > %(load_threshold)s
                        ORDER BY id ASC
                        LIMIT %(limit)s;  
                    """,
                    {"load_threshold": last_loaded_id, "limit": limit},
                )
                objs = cur.fetchall()
            return objs
