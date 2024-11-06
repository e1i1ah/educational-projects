import json
from typing import Dict
from datetime import datetime
from psycopg import Connection

from core.domain.entities.origin_objects.origin_pg_event_object import OriginEventObj
from core.repository.dds_repositories.dds_orders_repository import OrdersDdsRepository
from core.repository.dds_repositories.dds_products_repository import (
    ProductsDdsRepository,
)
from core.lib.interface_repository import Repository


from typing import List


class ProductSalesDdsRepository(Repository):
    WF_KEY = "dds_fct_product_sales"

    def __init__(self) -> None:
        self.products = None
        self.orders = None

    @staticmethod
    def get_correct_product_version(
        product_id: str, order_date: str, products: Dict
    ) -> int:
        order_date = datetime.fromisoformat(order_date)

        for prod in products[product_id]:
            if prod.active_to > order_date >= prod.active_from:
                return prod.id

        for prod in products[product_id]:
            if prod.active_to == datetime(2099, 12, 31):
                return prod.id

    def event_values(self, event_obj: OriginEventObj, conn: Connection) -> List:
        if not self.products:
            self.products = ProductsDdsRepository().get_objects(conn)
        if not self.orders:
            self.orders = OrdersDdsRepository().get_objects(conn)

        event_val = json.loads(event_obj.event_value)
        products_list_of_dict = event_val["product_payments"]

        result_list_of_product_sales = []

        for dct_products in products_list_of_dict:
            event = {}
            if event_val["order_id"] not in self.orders:
                return None
            elif dct_products["product_id"] not in self.products:
                continue
            else:
                event["product_id"] = self.get_correct_product_version(
                    dct_products["product_id"], event_val["order_date"], self.products
                )
                event["order_id"] = self.orders[event_val["order_id"]].id
                event["count"] = dct_products["quantity"]
                event["price"] = dct_products["price"]
                event["total_sum"] = dct_products["product_cost"]
                event["bonus_payment"] = dct_products["bonus_payment"]
                event["bonus_grant"] = dct_products["bonus_grant"]
                result_list_of_product_sales.append(event)

        return result_list_of_product_sales

    def save_objects(self, obj: OriginEventObj, conn: Connection):
        list_of_product_sales = self.event_values(obj, conn)

        if list_of_product_sales:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                        INSERT INTO dds.fct_product_sales (product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
                        VALUES (%(product_id)s, %(order_id)s, %(count)s, %(price)s, %(total_sum)s, %(bonus_payment)s, %(bonus_grant)s)
                        ON CONFLICT (product_id, order_id)
                        DO UPDATE
                        SET
                            count = EXCLUDED.count,
                            price = EXCLUDED.price,
                            total_sum = EXCLUDED.total_sum,
                            bonus_payment = EXCLUDED.bonus_payment,
                            bonus_grant = EXCLUDED.bonus_grant                    
                    """,
                    [p for p in list_of_product_sales],
                )

    def get_objects(self, last_update_ts: datetime, conn: Connection):
        pass
