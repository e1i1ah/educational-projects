from typing import List, Dict
import json
from datetime import datetime
from psycopg import Connection
from psycopg.rows import class_row

from core.domain.entities.origin_objects.origin_mongo_object import OriginMongoObj
from core.repository.dds_repositories.dds_restaurants_repository import (
    RestaurantsDdsRepository,
)
from core.repository.dds_repositories.dds_orders_repository import OrdersDdsRepository
from core.domain.entities.dwh_objects.dds_product_object import ProductDds
from core.lib.interface_repository import Repository


class ProductsDdsRepository(Repository):
    WF_KEY = "dds_products"

    def __init__(self) -> None:
        self.restaurants = None

    def product_values(self, order: OriginMongoObj, conn: Connection) -> List[Dict]:
        if not self.restaurants:
            self.restaurants = RestaurantsDdsRepository().get_objects(conn)

        order_value = json.loads(order.object_value)
        products_list_of_dict = order_value["order_items"]
        result_list_of_products = []
        for dct_product in products_list_of_dict:
            product = {}
            product["restaurant_id"] = (
                OrdersDdsRepository.get_correct_restaurant_version(
                    order_value["restaurant"]["id"],
                    order_value["date"],
                    self.restaurants,
                )
            )
            product["product_id"] = dct_product["id"]
            product["product_name"] = dct_product["name"]
            product["product_price"] = dct_product["price"]
            product["active_from"] = order.update_ts
            product["active_to"] = datetime(2099, 12, 31)
            result_list_of_products.append(product)
        return result_list_of_products

    def save_objects(self, obj: OriginMongoObj, conn: Connection):
        with conn.cursor() as cur:
            cur.executemany(
                """
                    UPDATE dds.dm_products
                    SET active_to = %(active_from)s
                    WHERE restaurant_id = %(restaurant_id)s
                          AND product_id = %(product_id)s 
                          AND (product_name != %(product_name)s 
                          OR product_price != %(product_price)s)      
                """,
                [p for p in self.product_values(obj, conn)],
            )

        with conn.cursor() as cur:
            cur.executemany(
                """
                    INSERT INTO dds.dm_products (restaurant_id, product_id, product_name, product_price, active_from, active_to)
                    VALUES (%(restaurant_id)s, %(product_id)s, %(product_name)s, %(product_price)s, %(active_from)s, %(active_to)s)
                    ON CONFLICT (restaurant_id, product_id, product_name, product_price)
                    DO NOTHING
                """,
                [p for p in self.product_values(obj, conn)],
            )

    def get_objects(self, conn: Connection) -> Dict:
        with conn.cursor(row_factory=class_row(ProductDds)) as cur:
            cur.execute(
                """
                    SELECT id, restaurant_id, product_id, product_name, product_price, active_from, active_to
                    FROM dds.dm_products   
                """
            )
            objs = cur.fetchall()

        result_dict = {}

        for prod in objs:
            result_dict[prod.product_id] = result_dict.get(prod.product_id, []) + [prod]

        return result_dict
