from datetime import datetime
from psycopg.rows import class_row
from lib.pg import PgConnect
from typing import Dict, List
from dds_loader.entity.stg_order_message import StgOrderMessage, PayloadOut


class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def get_out_message(self, msg: Dict) -> Dict:
        if msg["object_type"] == "order":
            return self._get_order_out_message(msg)

    def dds_insert(self, msg: Dict) -> None:
        if msg["object_type"] == "order":
            msg = StgOrderMessage(**msg)
            self._dds_order_insert(msg)

    def _get_order_out_message(self, msg: Dict) -> Dict:
        def get_payload(object_id: int) -> List[PayloadOut]:
            with self._db.connection() as conn:
                conn.row_factory = class_row(PayloadOut)
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT 
                            ho.h_order_pk as order_id,
                            hu.h_user_pk as user_id,
                            hc.h_category_pk as category_id,
                            hc.category_name,
                            hp.h_product_pk as product_id,
                            spn.name AS product_name,
                            sopq.quantity 
                        FROM dds.h_user hu
                        JOIN dds.l_order_user lou ON hu.h_user_pk = lou.h_user_pk
                        JOIN dds.h_order ho ON lou.h_order_pk = ho.h_order_pk
                        JOIN dds.l_order_product lop ON ho.h_order_pk = lop.h_order_pk
                        JOIN dds.h_product hp ON lop.h_product_pk = hp.h_product_pk
                        JOIN dds.l_product_category lpc ON hp.h_product_pk = lpc.h_product_pk
                        JOIN dds.h_category hc ON lpc.h_category_pk = hc.h_category_pk
                        JOIN dds.s_product_names spn ON hp.h_product_pk = spn.h_product_pk
                        JOIN dds.s_order_status sos on sos.h_order_pk = ho.h_order_pk
                        JOIN dds.s_order_product_quantity sopq on sopq.hk_order_product_pk = lop.hk_order_product_pk
                        WHERE ho.order_id = %s AND sos.status = 'CLOSED';
                        """,
                        (object_id,),
                    )
                    payload = cur.fetchall()

            return payload

        object_id = msg["object_id"]
        object_type = msg["object_type"]
        payload = [p.dict() for p in get_payload(object_id)]

        message_out = {
            "object_id": object_id,
            "object_type": object_type,
            "payload": payload,
        }

        return message_out

    def _dds_order_insert(self, stg_kafka_message: StgOrderMessage) -> None:
        load_dt = datetime.now()
        load_src = "stg_service_kafka"
        order_data = stg_kafka_message.payload
        params = {
            "load_dt": load_dt,
            "load_src": load_src,
            "order_id": order_data.id,
            "date": order_data.date,
            "cost": order_data.cost,
            "payment": order_data.payment,
            "status": order_data.status,
            "user_id": order_data.user.id,
            "user_name": order_data.user.name,
            "user_login": order_data.user.login,
            "restaurant_id": order_data.restaurant.id,
            "restaurant_name": order_data.restaurant.name,
        }

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                self._insert_h_order(cur, params)
                self._insert_h_user(cur, params)
                self._insert_h_restaurant(cur, params)
                self._insert_l_order_user(cur, params)

                for product in order_data.products:
                    product_params = params.copy()
                    product_params.update(
                        {
                            "product_id": product.id,
                            "product_name": product.name,
                            "product_quantity": product.quantity,
                            "category": product.category,
                        }
                    )
                    self._insert_h_product(cur, product_params)
                    self._insert_h_category(cur, product_params)
                    self._insert_l_order_product(cur, product_params)
                    self._insert_l_product_restaurant(cur, product_params)
                    self._insert_l_product_category(cur, product_params)
                    self._insert_s_product_names(cur, product_params)
                    self._insert_s_order_product_quantity(cur, product_params)

                self._insert_s_order_cost(cur, params)
                self._insert_s_order_status(cur, params)
                self._insert_s_user_names(cur, params)
                self._insert_s_restaurant_names(cur, params)

    def _insert_h_order(self, cur, params):
        cur.execute(
            """
            INSERT INTO dds.h_order (h_order_pk, order_id, order_dt, load_dt, load_src)
            VALUES (md5(%(order_id)s::text)::uuid, %(order_id)s, %(date)s, %(load_dt)s, %(load_src)s)
            ON CONFLICT (h_order_pk) DO NOTHING;
            """,
            params,
        )

    def _insert_h_user(self, cur, params):
        cur.execute(
            """
            INSERT INTO dds.h_user (h_user_pk, user_id, load_dt, load_src)
            VALUES (md5(%(user_id)s::text)::uuid, %(user_id)s, %(load_dt)s, %(load_src)s)
            ON CONFLICT (h_user_pk) DO NOTHING;
            """,
            params,
        )

    def _insert_h_restaurant(self, cur, params):
        cur.execute(
            """
            INSERT INTO dds.h_restaurant (h_restaurant_pk, restaurant_id, load_dt, load_src)
            VALUES (md5(%(restaurant_id)s::text)::uuid, %(restaurant_id)s, %(load_dt)s, %(load_src)s)
            ON CONFLICT (h_restaurant_pk) DO NOTHING;
            """,
            params,
        )

    def _insert_h_product(self, cur, params):
        cur.execute(
            """
            INSERT INTO dds.h_product (h_product_pk, product_id, load_dt, load_src)
            VALUES (md5(%(product_id)s::text)::uuid, %(product_id)s, %(load_dt)s, %(load_src)s)
            ON CONFLICT (h_product_pk) DO NOTHING;
            """,
            params,
        )

    def _insert_h_category(self, cur, params):
        cur.execute(
            """
            INSERT INTO dds.h_category (h_category_pk, category_name, load_dt, load_src)
            VALUES (md5(%(category)s::text)::uuid, %(category)s, %(load_dt)s, %(load_src)s)
            ON CONFLICT (h_category_pk) DO NOTHING;
            """,
            params,
        )

    def _insert_l_order_product(self, cur, params):
        cur.execute(
            """
            INSERT INTO dds.l_order_product (hk_order_product_pk, h_order_pk, h_product_pk, load_dt, load_src)
            VALUES (md5(%(order_id)s::text || %(product_id)s::text)::uuid,
                    md5(%(order_id)s::text)::uuid,
                    md5(%(product_id)s::text)::uuid,
                    %(load_dt)s, %(load_src)s)
            ON CONFLICT (hk_order_product_pk) DO NOTHING;
            """,
            params,
        )

    def _insert_l_product_restaurant(self, cur, params):
        cur.execute(
            """
            INSERT INTO dds.l_product_restaurant (hk_product_restaurant_pk, h_restaurant_pk, h_product_pk, load_dt, load_src)
            VALUES (md5(%(restaurant_id)s::text || %(product_id)s::text)::uuid,
                    md5(%(restaurant_id)s::text)::uuid,
                    md5(%(product_id)s::text)::uuid,
                    %(load_dt)s, %(load_src)s)
            ON CONFLICT (hk_product_restaurant_pk) DO NOTHING;
            """,
            params,
        )

    def _insert_l_product_category(self, cur, params):
        cur.execute(
            """
            INSERT INTO dds.l_product_category (hk_product_category_pk, h_category_pk, h_product_pk, load_dt, load_src)
            VALUES (md5(%(category)s::text || %(product_id)s::text)::uuid,
                    md5(%(category)s::text)::uuid,
                    md5(%(product_id)s::text)::uuid,
                    %(load_dt)s, %(load_src)s)
            ON CONFLICT (hk_product_category_pk) DO NOTHING;
            """,
            params,
        )

    def _insert_l_order_user(self, cur, params):
        cur.execute(
            """
            INSERT INTO dds.l_order_user (hk_order_user_pk, h_order_pk, h_user_pk, load_dt, load_src)
            VALUES (md5(%(order_id)s::text || %(user_id)s::text)::uuid,
                    md5(%(order_id)s::text)::uuid,
                    md5(%(user_id)s::text)::uuid,
                    %(load_dt)s, %(load_src)s)
            ON CONFLICT (hk_order_user_pk) DO NOTHING;
            """,
            params,
        )

    def _insert_s_order_cost(self, cur, params):
        cur.execute(
            """
            INSERT INTO dds.s_order_cost (h_order_pk, cost, payment, load_dt, load_src, hk_order_cost_hashdiff)
            SELECT md5(%(order_id)s::text)::uuid, %(cost)s, %(payment)s, %(load_dt)s, %(load_src)s,
                   md5(%(cost)s::text || %(payment)s::text)::uuid
            WHERE NOT EXISTS (
                SELECT 1 FROM dds.s_order_cost 
                WHERE h_order_pk = md5(%(order_id)s::text)::uuid
                AND hk_order_cost_hashdiff = md5(%(cost)s::text || %(payment)s::text)::uuid
            );
            """,
            params,
        )

    def _insert_s_order_status(self, cur, params):
        cur.execute(
            """
            INSERT INTO dds.s_order_status (h_order_pk, status, load_dt, load_src, hk_order_status_hashdiff)
            SELECT md5(%(order_id)s::text)::uuid, %(status)s::varchar, %(load_dt)s, %(load_src)s,
                md5(%(status)s::text)::uuid
            WHERE NOT EXISTS (
                SELECT 1 FROM dds.s_order_status 
                WHERE h_order_pk = md5(%(order_id)s::text)::uuid
                AND hk_order_status_hashdiff = md5(%(status)s::text)::uuid
            );
            """,
            params,
        )

    def _insert_s_user_names(self, cur, params):
        cur.execute(
            """
            INSERT INTO dds.s_user_names (h_user_pk, username, userlogin, load_dt, load_src, hk_user_names_hashdiff)
            SELECT md5(%(user_id)s::text)::uuid, %(user_name)s::varchar, %(user_login)s::varchar, %(load_dt)s, %(load_src)s,
                md5(%(user_name)s::text || %(user_login)s::text)::uuid
            WHERE NOT EXISTS (
                SELECT 1 FROM dds.s_user_names 
                WHERE h_user_pk = md5(%(user_id)s::text)::uuid
                AND hk_user_names_hashdiff = md5(%(user_name)s::text || %(user_login)s::text)::uuid
            );
            """,
            params,
        )

    def _insert_s_restaurant_names(self, cur, params):
        cur.execute(
            """
            INSERT INTO dds.s_restaurant_names (h_restaurant_pk, "name", load_dt, load_src, hk_restaurant_names_hashdiff)
            SELECT md5(%(restaurant_id)s::text)::uuid, %(restaurant_name)s::varchar, %(load_dt)s, %(load_src)s,
                md5(%(restaurant_name)s::text)::uuid
            WHERE NOT EXISTS (
                SELECT 1 FROM dds.s_restaurant_names 
                WHERE h_restaurant_pk = md5(%(restaurant_id)s::text)::uuid
                AND hk_restaurant_names_hashdiff = md5(%(restaurant_name)s::text)::uuid
            );
            """,
            params,
        )

    def _insert_s_product_names(self, cur, params):
        cur.execute(
            """
            INSERT INTO dds.s_product_names (h_product_pk, "name", load_dt, load_src, hk_product_names_hashdiff)
            SELECT md5(%(product_id)s::text)::uuid, %(product_name)s::varchar, %(load_dt)s, %(load_src)s,
                   md5(%(product_name)s::text)::uuid
            WHERE NOT EXISTS (
                SELECT 1 FROM dds.s_product_names 
                WHERE h_product_pk = md5(%(product_id)s::text)::uuid
                AND hk_product_names_hashdiff = md5(%(product_name)s::text)::uuid
            );
            """,
            params,
        )

    def _insert_s_order_product_quantity(self, cur, params):
        cur.execute(
            """
            INSERT INTO dds.s_order_product_quantity (hk_order_product_pk, quantity, load_dt, load_src, hk_order_product_hashdiff)
            SELECT md5(%(order_id)s::text || %(product_id)s::text)::uuid, %(product_quantity)s, %(load_dt)s, %(load_src)s,
                md5(%(product_quantity)s::text)::uuid
            WHERE NOT EXISTS (
                SELECT 1 FROM dds.s_order_product_quantity 
                WHERE hk_order_product_pk = md5(%(order_id)s::text || %(product_id)s::text)::uuid
                AND hk_order_product_hashdiff = md5(%(product_quantity)s::text)::uuid
            );
            """,
            params,
        )
