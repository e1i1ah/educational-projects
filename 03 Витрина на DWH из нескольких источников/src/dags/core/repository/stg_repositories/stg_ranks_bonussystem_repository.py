from psycopg import Connection

from core.lib.interface_repository import Repository
from core.domain.entities.origin_objects.origin_pg_rank_object import OriginRankObj


class RanksStgRepository(Repository):
    WF_KEY = "stg_bonussystem_ranks"
    WF_SETTINGS = {"last_loaded_id": -1}

    def save_objects(self, conn: Connection, rank: OriginRankObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.bonussystem_ranks(id, name, bonus_percent, min_payment_threshold)
                    VALUES (%(id)s, %(name)s, %(bonus_percent)s, %(min_payment_threshold)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        name = EXCLUDED.name,
                        bonus_percent = EXCLUDED.bonus_percent,
                        min_payment_threshold = EXCLUDED.min_payment_threshold;
                """,
                {
                    "id": rank.id,
                    "name": rank.name,
                    "bonus_percent": rank.bonus_percent,
                    "min_payment_threshold": rank.min_payment_threshold,
                },
            )

    def get_objects(self):
        raise NotImplementedError
