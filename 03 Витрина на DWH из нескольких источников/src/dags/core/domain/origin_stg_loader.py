from logging import Logger
from typing import List, Any
from psycopg import Connection

from core.lib.interface_connector import Connector
from core.lib.interface_repository import Repository
from core.domain.entities.etl_setting import EtlSetting
from core.repository.stg_settings_repository import StgEtlSettingsRepository
from core.lib.dict_util import json2str
from core.lib.pg_connect import PgConnect


class Loader:
    def __init__(
        self, origin: Connector, destination: Repository, pg: PgConnect, log: Logger
    ) -> None:
        self.origin = origin
        self.destination = destination
        self.pg = pg
        self.log = log
        self.etl_settings = StgEtlSettingsRepository()
        self.wf_key = self.destination.WF_KEY
        self.base_wf_settings = self.destination.WF_SETTINGS

    def get_workflow_settings(self) -> EtlSetting:
        with self.pg.connection() as conn:
            current_wf_settings = self.etl_settings.get_setting_stg(conn, self.wf_key)
            if not current_wf_settings:
                current_wf_settings = EtlSetting(
                    id=0,
                    workflow_key=self.wf_key,
                    workflow_settings=self.base_wf_settings,
                )
            return current_wf_settings

    def save_workflow_settings(
        self, wf_setting: EtlSetting, load_queue: List[Any], conn: Connection
    ) -> None:
        if wf_setting:
            key = [*wf_setting.workflow_settings][0]

            if key == "last_loaded_date":
                wf_setting.workflow_settings[key] = max(
                    [obj.update_ts for obj in load_queue]
                )

            elif key == "last_loaded_id":
                wf_setting.workflow_settings[key] = max([obj.id for obj in load_queue])

            elif key == "offset":
                wf_setting.workflow_settings[key] = self.destination.get_offset(conn)[0]

            self.etl_settings.save_setting_stg(
                conn, wf_setting.workflow_key, json2str(wf_setting.workflow_settings)
            )

            self.log.info(f"Load finished on {key} {wf_setting.workflow_settings[key]}")

    def load_table(self) -> None:
        with self.pg.connection() as conn:
            wf_setting = self.get_workflow_settings()
            load_queue = self.origin.download_dataset(**wf_setting.workflow_settings)

            self.log.info(f"Found {len(load_queue)} {self.origin.collection} to load.")

            if not load_queue:
                self.log.info("Quitting.")
                return

            for obj in load_queue:
                self.destination.save_objects(conn, obj)

            self.save_workflow_settings(wf_setting, load_queue, conn)
