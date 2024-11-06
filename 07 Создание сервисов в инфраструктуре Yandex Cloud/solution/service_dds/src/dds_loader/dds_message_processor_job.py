from datetime import datetime
from logging import Logger
from lib.kafka_connect.kafka_connectors import KafkaConsumer, KafkaProducer
from dds_loader.repository.dds_repository import DdsRepository


class DdsMessageProcessor:
    def __init__(
        self,
        consumer: KafkaConsumer,
        producer: KafkaProducer,
        dds_repository: DdsRepository,
        logger: Logger,
    ) -> None:
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._logger = logger
        self._batch_size = 30

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()

            if not msg:
                self._logger.info(f"{datetime.utcnow()}: message is None")
                break

            self._dds_repository.dds_insert(msg)
            out_message = self._dds_repository.get_out_message(msg)
            self._producer.produce(out_message)

        self._logger.info(f"{datetime.utcnow()}: FINISH")
