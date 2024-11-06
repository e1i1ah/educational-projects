from contextlib import contextmanager
from typing import Generator
from psycopg2.extensions import connection as PgConnection

import psycopg2


class PgConnect:
    def __init__(
        self,
        host: str,
        port: str,
        db_name: str,
        user: str,
        password: str,
        sslmode: str = "disable",
    ) -> None:
        self.host = host
        self.port = int(port)
        self.db_name = db_name
        self.user = user
        self.password = password
        self.sslmode = sslmode

    def url(self) -> str:
        return """
            host={host}
            port={port}
            dbname={db_name}
            user={user}
            password={password}
            target_session_attrs=read-write
            sslmode={sslmode}
        """.format(
            host=self.host,
            port=self.port,
            db_name=self.db_name,
            user=self.user,
            password=self.password,
            sslmode=self.sslmode,
        )

    def client(self):
        return psycopg2.connect(self.url())

    @contextmanager
    def connection(self) -> Generator[PgConnection, None, None]:
        conn = psycopg2.connect(self.url())
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
