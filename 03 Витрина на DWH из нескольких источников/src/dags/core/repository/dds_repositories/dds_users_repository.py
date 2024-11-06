import json
from typing import Dict

from core.domain.entities.origin_objects.origin_mongo_object import OriginMongoObj
from core.domain.entities.dwh_objects.dds_user_object import UserDds
from core.lib.interface_repository import Repository
from psycopg import Connection
from psycopg.rows import class_row


class UserDdsRepository(Repository):
    WF_KEY = "dds_users"

    @staticmethod
    def user_values(user_obj: OriginMongoObj) -> Dict:
        user_values = json.loads(user_obj.object_value)
        user = {}
        user["user_id"] = user_values["_id"]
        user["user_name"] = user_values["name"]
        user["user_login"] = user_values["login"]
        return user

    def save_objects(self, obj: OriginMongoObj, conn: Connection) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_users (user_id, user_name, user_login)
                    VALUES (%(user_id)s, %(user_name)s, %(user_login)s)
                    ON CONFLICT (user_id)
                    DO UPDATE
                    SET user_name = EXCLUDED.user_name, 
                        user_login = EXCLUDED.user_login
                """,
                self.user_values(obj),
            )

    def get_objects(self, conn: Connection) -> Dict:
        with conn.cursor(row_factory=class_row(UserDds)) as cur:
            cur.execute(
                """
                    SELECT id, user_id, user_name, user_login
                    FROM dds.dm_users   
                """
            )
            objs = cur.fetchall()

        result_dict = {}

        for u in objs:
            result_dict[u.user_id] = u

        return result_dict
