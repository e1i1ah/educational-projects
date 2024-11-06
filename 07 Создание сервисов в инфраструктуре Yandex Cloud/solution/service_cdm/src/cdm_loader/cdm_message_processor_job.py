from datetime import datetime
from logging import Logger

from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository.cdm_repository import CdmRepository


class CdmMessageProcessor:
    def __init__(
        self,
        consumer: KafkaConsumer,
        cdm_repository: CdmRepository,
        logger: Logger,
    ) -> None:
        self._consumer = consumer
        self._cdm_repository = cdm_repository
        self._logger = logger
        self._batch_size = 100

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()

            if not msg:
                self._logger.info(f"{datetime.utcnow()}: message is None")
                break

            self._cdm_repository.cdm_insert(msg)

        self._logger.info(f"{datetime.utcnow()}: FINISH")
