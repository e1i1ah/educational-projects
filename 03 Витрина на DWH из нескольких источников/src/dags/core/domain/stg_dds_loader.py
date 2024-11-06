from logging import Logger
from typing import List, Any
from psycopg import Connection
from datetime import datetime

from core.lib.interface_repository import Repository
from core.domain.entities.etl_setting import EtlSetting
from core.repository.stg_settings_repository import StgEtlSettingsRepository
from core.repository.dds_repositories.dds_fct_product_sales import (
    ProductSalesDdsRepository,
)
from core.repository.dds_repositories.dds_delivery_repository import (
    DeliveryDdsRepository,
)
from core.lib.dict_util import json2str
from core.lib.pg_connect import PgConnect


class StgDdsLoader:

    def __init__(
        self, origin: Repository, destination: Repository, pg: PgConnect, log: Logger
    ) -> None:
        self.origin = origin
        self.destination = destination
        self.pg = pg
        self.log = log
        self.etl_settings = StgEtlSettingsRepository()
        self.wf_key = self.get_wf_key(origin, destination)
        self.base_wf_settings = {"last_update_ts": datetime(2024, 1, 1)}

    @staticmethod
    def get_wf_key(origin, dest):
        s = f"{type(origin).__name__[:-10]} --> {type(dest).__name__[:-10]}"
        return s

    def get_workflow_settings(self) -> EtlSetting:
        with self.pg.connection() as conn:
            current_wf_settings = self.etl_settings.get_setting_dds(conn, self.wf_key)
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
        key = [*wf_setting.workflow_settings][0]

        if isinstance(
            self.destination, (ProductSalesDdsRepository, DeliveryDdsRepository)
        ):
            wf_setting.workflow_settings = self.etl_settings.get_setting_dds(
                conn, "OrdersStg --> OrdersDds"
            ).workflow_settings

        else:
            wf_setting.workflow_settings[key] = max(
                [obj.update_ts for obj in load_queue]
            )

        self.etl_settings.save_setting_dds(
            conn, wf_setting.workflow_key, json2str(wf_setting.workflow_settings)
        )

    def load_table(self) -> None:
        with self.pg.connection() as conn:
            wf_setting = self.get_workflow_settings()
            load_queue = self.origin.get_objects(
                **wf_setting.workflow_settings, conn=conn
            )

            self.log.info(f"Found {len(load_queue)} {self.origin.WF_KEY[4:]} to load.")

            if not load_queue:
                self.log.info("Quitting.")
                return

            for obj in load_queue:
                self.destination.save_objects(obj, conn)

            self.save_workflow_settings(wf_setting, load_queue, conn)
