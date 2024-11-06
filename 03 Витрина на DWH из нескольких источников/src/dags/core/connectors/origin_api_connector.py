import json
import requests
from datetime import datetime
from typing import List, Any

from core.lib.interface_connector import Connector
from core.domain.entities.origin_objects.origin_api_object import OriginApiObj
from core.lib.dict_util import json2str


class OriginApiConnector(Connector):
    def __init__(self, url_headers: str, collection: str) -> None:
        self.url, self.headers = url_headers
        self.active_to = datetime(2099, 1, 1).strftime("%Y-%m-%d %H:%M:%S")
        self.valid_collections = ("couriers", "deliveries")
        if collection in self.valid_collections:
            self.collection = collection
        else:
            raise ValueError("not valid collection name")

    def download_dataset(
        self,
        limit: int = 50,
        offset: int = None,
        last_loaded_date: datetime = None,
        last_loaded_id: int = None,
    ) -> List[Any]:

        if self.collection == "couriers":
            current_datetime = datetime.now()
            if offset is None:
                raise ValueError("not valid offset")
            url = f"https://{self.url}/couriers?sort_field=id&sort_direction=asc&limit={limit}&offset={offset}"

        elif self.collection == "deliveries":
            if last_loaded_date is None:
                raise ValueError("not valid load_threshold")
            last_loaded_date = datetime.fromisoformat(last_loaded_date)
            url = f"https://{self.url}/deliveries?from={last_loaded_date}&to={self.active_to}&sort_field=date&sort_direction=asc&limit={limit}"

        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        content_list = json.loads(response.content)

        if self.collection == "couriers":
            return [
                OriginApiObj(
                    object_id=o["_id"],
                    object_value=json2str(o),
                    update_ts=current_datetime,
                )
                for o in content_list
            ]

        elif self.collection == "deliveries":
            return [
                OriginApiObj(
                    object_id=o["delivery_id"],
                    object_value=json2str(o),
                    update_ts=o["delivery_ts"],
                )
                for o in content_list
            ]
