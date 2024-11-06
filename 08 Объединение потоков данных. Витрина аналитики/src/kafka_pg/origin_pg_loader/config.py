import os


class AppConfig:

    def __init__(self) -> None:
        self.kafka_host = os.getenv("KAFKA_HOST", "default_host")
        self.kafka_port = int(os.getenv("KAFKA_PORT", 9092))
        self.kafka_consumer_username = os.getenv(
            "KAFKA_CONSUMER_USERNAME", "default_user"
        )
        self.kafka_consumer_password = os.getenv(
            "KAFKA_CONSUMER_PASSWORD", "default_password"
        )
        self.kafka_consumer_topic = os.getenv("KAFKA_SOURCE_TOPIC", "default_topic")

        self.pg_warehouse_host = os.getenv("PG_WAREHOUSE_HOST", "localhost")
        self.pg_warehouse_port = int(os.getenv("PG_WAREHOUSE_PORT", 5432))
        self.pg_warehouse_dbname = os.getenv("PG_WAREHOUSE_DBNAME", "default_db")
        self.pg_warehouse_user = os.getenv("PG_WAREHOUSE_USER", "default_user")
        self.pg_warehouse_password = os.getenv(
            "PG_WAREHOUSE_PASSWORD", "default_password"
        )
        self.pg_table_transactions_currencies = "stg.transactions_currencies"

    def get_kafka_params(self):
        return {
            "kafka.bootstrap.servers": f"{self.kafka_host}:{self.kafka_port}",
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.mechanism": "SCRAM-SHA-512",
            "kafka.sasl.jaas.config": (
                "org.apache.kafka.common.security.scram.ScramLoginModule required "
                f'username="{self.kafka_consumer_username}" '
                f'password="{self.kafka_consumer_password}";'
            ),
            "kafka.ssl.truststore.location": "/home/jovyan/work/yrr.jks",
            "kafka.ssl.truststore.password": "MMm3zaDd",
            "subscribe": self.kafka_consumer_topic,
        }

    def get_pg_properties(self):
        return {
            "user": self.pg_warehouse_user,
            "password": self.pg_warehouse_password,
            "driver": "org.postgresql.Driver",
        }

    def get_pg_url(self):
        return f"jdbc:postgresql://{self.pg_warehouse_host}:{self.pg_warehouse_port}/{self.pg_warehouse_dbname}"
