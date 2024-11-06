import time
import json
from typing import Dict, List
from datetime import datetime
from logging import Logger
from lib.kafka_connect import KafkaConsumer, KafkaProducer
from lib.redis import RedisClient
from stg_loader.repository.stg_repository import StgRepository


class StgMessageProcessor:
    def __init__(
        self,
        consumer: KafkaConsumer,
        producer: KafkaProducer,
        redis: RedisClient,
        stg_repository: StgRepository,
        batch_size: int,
        logger: Logger,
    ) -> None:
        self._logger = logger
        self._consumer = consumer
        self._producer = producer
        self._redis = redis
        self._stg_repository = stg_repository
        self._batch_size = batch_size

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()

            if not msg:
                self._logger.info(f"{datetime.utcnow()}: message is None")
                break

            # Проверка, что сообщение содержит все необходимые ключи
            if {"object_id", "object_type", "sent_dttm", "payload"} <= msg.keys():
                self._stg_repository.order_events_insert(
                    msg["object_id"],
                    msg["object_type"],
                    msg["sent_dttm"],
                    json.dumps(msg["payload"]),
                )

                user_id = msg["payload"]["user"]["id"]
                rest_id = msg["payload"]["restaurant"]["id"]

                user_info = self._redis.get(user_id)
                rest_info = self._redis.get(rest_id)

                enriched_message = StgMessageProcessor.get_enriched_message(
                    msg, user_info, rest_info
                )
                self._producer.produce(enriched_message)

            else:
                # Обработка сообщения с недостаточными данными
                self._logger.warning(f"Message missing expected keys: {msg}")

        self._logger.info(f"{datetime.utcnow()}: FINISH")

    @staticmethod
    def get_enriched_products(
        products_in_menu: Dict, products_in_orders: List
    ) -> List[Dict]:
        products = []
        for product in products_in_orders:
            p = {
                "id": product["id"],
                "price": product["price"],
                "quantity": product["quantity"],
                "name": product["name"],
                "category": products_in_menu[product["id"]]["category"],
            }
            products.append(p)
        return products

    @staticmethod
    def get_enriched_restaurant(rest_info: Dict) -> Dict:
        restaurant = {"id": rest_info["_id"], "name": rest_info["name"]}
        return restaurant

    @staticmethod
    def get_enriched_user(user_info: Dict) -> Dict:
        user = {
            "id": user_info["_id"],
            "name": user_info["name"],
            "login": user_info["login"],
        }
        return user

    @staticmethod
    def get_enriched_message(msg: Dict, user_info: Dict, rest_info: Dict) -> Dict:

        products_in_menu = {item["_id"]: item for item in rest_info["menu"]}
        products_in_orders = msg["payload"]["order_items"]

        enriched_products = StgMessageProcessor.get_enriched_products(
            products_in_menu, products_in_orders
        )
        enriched_restaurant = StgMessageProcessor.get_enriched_restaurant(rest_info)
        enriched_user = StgMessageProcessor.get_enriched_user(user_info)
        payload = {
            "id": msg["object_id"],
            "date": msg["payload"]["date"],
            "cost": msg["payload"]["cost"],
            "payment": msg["payload"]["payment"],
            "status": msg["payload"]["final_status"],
            "restaurant": enriched_restaurant,
            "user": enriched_user,
            "products": enriched_products,
        }

        enriched_message = {
            "object_id": msg["object_id"],
            "object_type": msg["object_type"],
            "payload": payload,
        }

        return enriched_message
