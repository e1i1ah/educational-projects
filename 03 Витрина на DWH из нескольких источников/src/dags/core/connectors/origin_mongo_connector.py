from datetime import datetime
from typing import List, Any
from core.lib.dict_util import json2str

from core.lib.interface_connector import Connector
from core.domain.entities.origin_objects.origin_mongo_object import OriginMongoObj
from core.lib.mongo_connect import MongoConnect


class OriginMongoConnector(Connector):
    def __init__(self, mc: MongoConnect, collection: str) -> None:
        self.dbs = mc.client()
        self.valid_collections = ("restaurants", "users", "orders")
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

        if last_loaded_date is None:
            raise ValueError("not valid last_loaded_date")
        last_loaded_date = datetime.fromisoformat(last_loaded_date)
        filter = {"update_ts": {"$gt": last_loaded_date}}
        sort = [("update_ts", 1)]
        content_list = list(
            self.dbs.get_collection(f"{self.collection}").find(
                filter=filter, sort=sort, limit=limit
            )
        )

        return [
            OriginMongoObj(
                object_id=str(o["_id"]),
                object_value=json2str(o),
                update_ts=o["update_ts"],
            )
            for o in content_list
        ]
