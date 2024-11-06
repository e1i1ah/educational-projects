from psycopg import Connection

from core.lib.interface_repository import Repository
from core.domain.entities.origin_objects.origin_pg_user_object import OriginUserObj


class UsersBonussystemStgRepository(Repository):
    WF_KEY = "stg_bonussystem_users"
    WF_SETTINGS = {"last_loaded_id": -1}

    def save_objects(self, conn: Connection, user: OriginUserObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.bonussystem_users(id, order_user_id)
                    VALUES (%(id)s, %(order_user_id)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        order_user_id = EXCLUDED.order_user_id;
                """,
                {"id": user.id, "order_user_id": user.order_user_id},
            )

    def get_objects(self):
        raise NotImplementedError
