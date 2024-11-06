from lib.pg import PgConnect
from typing import Dict
from cdm_loader.entity.dds_order_message import DdsOrderMessage


class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def cdm_insert(self, msg: Dict) -> None:
        if msg["object_type"] == "order":
            msg = DdsOrderMessage(**msg)
            self._cdm_order_insert(msg)

    def _cdm_order_insert(self, dds_kafka_message: DdsOrderMessage) -> None:
        try:
            with self._db.connection() as conn:
                with conn.cursor() as cur:
                    for order_data in dds_kafka_message.payload:
                        query_params = {
                            "order_id": order_data.order_id,
                            "user_id": order_data.user_id,
                            "category_id": order_data.category_id,
                            "category_name": order_data.category_name,
                            "product_id": order_data.product_id,
                            "product_name": order_data.product_name,
                            "quantity": order_data.quantity,
                        }

                        cur.execute(
                            """
                            SELECT 1 
                            FROM cdm.service_processed_orders 
                            WHERE order_id = %(order_id)s;
                            """,
                            query_params,
                        )

                        order_exists = cur.fetchone()

                        if not order_exists:
                            cur.execute(
                                """
                                INSERT INTO cdm.user_category_counters (user_id, category_id, category_name, order_cnt)
                                VALUES (%(user_id)s, %(category_id)s, %(category_name)s, 1)
                                ON CONFLICT (user_id, category_id) DO UPDATE
                                SET 
                                    order_cnt = cdm.user_category_counters.order_cnt + 1,
                                    category_name = EXCLUDED.category_name;
                                """,
                                query_params,
                            )

                            cur.execute(
                                """
                                INSERT INTO cdm.user_product_counters (user_id, product_id, product_name, order_cnt)
                                VALUES (%(user_id)s, %(product_id)s, %(product_name)s, 1)
                                ON CONFLICT (user_id, product_id) DO UPDATE
                                SET 
                                    order_cnt = cdm.user_product_counters.order_cnt + %(quantity)s,
                                    product_name = EXCLUDED.product_name;
                                """,
                                query_params,
                            )

                            cur.execute(
                                """
                                INSERT INTO cdm.service_processed_orders (order_id)
                                VALUES (%(order_id)s);
                                """,
                                query_params,
                            )
        finally:
            conn.close()
