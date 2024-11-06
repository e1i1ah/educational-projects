from contextlib import contextmanager
from typing import Generator
import vertica_python


class VerticaConnect:
    def __init__(
        self,
        host: str,
        port: str,
        user: str,
        password: str,
        database: str,
        autocommit: bool,
    ) -> None:
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.autocommit = autocommit

    @contextmanager
    def connection(self) -> Generator[vertica_python.Connection, None, None]:
        conn = vertica_python.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            autocommit=self.autocommit,
        )
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
