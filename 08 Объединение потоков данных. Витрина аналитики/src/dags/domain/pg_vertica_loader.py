import pendulum
from logging import Logger
from repository.repository_interface import Repository
from lib.pg_connect import PgConnect
from lib.vertica_connect import VerticaConnect
from config import DependencyConfig


class Loader:
    def __init__(
        self,
        origin: Repository,
        destination: Repository,
        origin_conn: PgConnect,
        dest_conn: VerticaConnect,
        execution_date: pendulum.DateTime,
        log: Logger,
    ) -> None:
        self.origin = origin
        self.destination = destination
        self.origin_conn = origin_conn
        self.dest_conn = dest_conn
        self.execution_date = execution_date
        self.log = log

    def load_table(self) -> None:
        self.log.debug("Starting load_table method.")

        file_path = DependencyConfig.get_file_path()
        self.log.debug(f"File path obtained: {file_path}")

        with self.origin_conn.connection() as pg_conn:
            with self.dest_conn.connection() as v_conn:

                object_name = self.origin.get_objects(
                    self.execution_date, file_path, pg_conn
                )

                if object_name:
                    self.log.debug(f"{object_name} exist, proceeding to save.")
                    self.destination.save_objects(file_path, v_conn)
                    self.log.info(f"{object_name} saved successfully.")
                else:
                    self.log.info(f"{object_name} do not exist. Quitting.")

        self.log.debug("Ending load_table method.")
