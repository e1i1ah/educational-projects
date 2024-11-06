import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, LongType

# Настройки Kafka
KAFKA_BOOTSTRAP_SERVERS = "rc1b-xxxxb35n4j4v869.mdb.yandexcloud.net:9091"
KAFKA_SECURITY_OPTIONS = {
    "kafka.bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "SCRAM-SHA-512",
    "kafka.sasl.jaas.config": (
        "org.apache.kafka.common.security.scram.ScramLoginModule "
        'required username="xxx" password="xxx";'
    ),
}
TOPIC_IN = "student.topic.cohortxx.xxx.in"
TOPIC_OUT = "student.topic.cohortxx.xxx.out"

# Настройки PostgreSQL
POSTGRES_URL = "jdbc:postgresql://localhost:5432/de"
POSTGRES_PROPERTIES = {
    "user": "xxx",
    "password": "xxx",
    "driver": "org.postgresql.Driver",
}
POSTGRES_TABLE_FEEDBACK = "public.subscribers_feedback"
POSTGRES_TABLE_SUBSCRIBERS = "subscribers_restaurants"


logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)


def create_spark_session():
    spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )
    return (
        SparkSession.builder.appName("RestaurantSubscribeStreamingService")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.packages", spark_jars_packages)
        .getOrCreate()
    )


def read_kafka_stream(spark):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .options(**KAFKA_SECURITY_OPTIONS)
        .option("subscribe", TOPIC_IN)
        .load()
    )


def filter_stream_data(df: DataFrame, current_timestamp_utc):
    incoming_message_schema = StructType(
        [
            StructField("restaurant_id", StringType(), True),
            StructField("adv_campaign_id", StringType(), True),
            StructField("adv_campaign_content", StringType(), True),
            StructField("adv_campaign_owner", StringType(), True),
            StructField("adv_campaign_owner_contact", StringType(), True),
            StructField("adv_campaign_datetime_start", LongType(), True),
            StructField("adv_campaign_datetime_end", LongType(), True),
            StructField("datetime_created", LongType(), True),
        ]
    )
    return (
        df.withColumn("value", f.col("value").cast("string"))
        .withColumn("data", f.from_json("value", incoming_message_schema))
        .select("data.*")
        .filter(
            (current_timestamp_utc >= f.col("adv_campaign_datetime_start"))
            & (current_timestamp_utc < f.col("adv_campaign_datetime_end"))
        )
        .withWatermark("datetime_created", "1 hour")
        .dropDuplicates(["restaurant_id", "adv_campaign_id", "datetime_created"])
    )


def read_subscribers_data(spark):
    return (
        spark.read.format("jdbc")
        .option("url", POSTGRES_URL)
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", POSTGRES_TABLE_SUBSCRIBERS)
        .option("user", "student")
        .option("password", "de-student")
        .load()
        .dropDuplicates(["client_id", "restaurant_id"])
    )


def join_and_transform_data(filtered_data, subscribers_data):
    return filtered_data.join(subscribers_data, "restaurant_id", "inner")


def write_to_postgresql(df: DataFrame):
    try:
        postgres_df = df.select(
            "restaurant_id",
            "adv_campaign_id",
            "adv_campaign_content",
            "adv_campaign_owner",
            "adv_campaign_owner_contact",
            "adv_campaign_datetime_start",
            "adv_campaign_datetime_end",
            "datetime_created",
            "client_id",
            "trigger_datetime_created",
        ).withColumn("feedback", f.lit(""))

        postgres_df.write.jdbc(
            url=POSTGRES_URL,
            table=POSTGRES_TABLE_FEEDBACK,
            mode="append",
            properties=POSTGRES_PROPERTIES,
        )
    except Exception as e:
        logger.error(f"Error writing to PostgreSQL: {str(e)}")


def write_to_kafka(df: DataFrame):
    try:
        kafka_df = df.select(
            f.to_json(
                f.struct(
                    "restaurant_id",
                    "adv_campaign_id",
                    "adv_campaign_content",
                    "adv_campaign_owner",
                    "adv_campaign_owner_contact",
                    "adv_campaign_datetime_start",
                    "adv_campaign_datetime_end",
                    "datetime_created",
                    "client_id",
                    "trigger_datetime_created",
                )
            ).alias("value")
        )

        kafka_df.write.format("kafka").option(
            "kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS
        ).options(**KAFKA_SECURITY_OPTIONS).option("topic", TOPIC_OUT).save()
    except Exception as e:
        logger.error(f"Error writing to Kafka: {str(e)}")


def get_current_timestamp_utc():
    return f.unix_timestamp(f.current_timestamp())


def save_to_postgresql_and_kafka(df: DataFrame):
    current_timestamp_utc = get_current_timestamp_utc()
    df = df.withColumn("trigger_datetime_created", current_timestamp_utc).persist()

    write_to_postgresql(df)
    write_to_kafka(df)

    df.unpersist()


def main():
    spark = create_spark_session()
    restaurant_read_stream_df = read_kafka_stream(spark)
    current_timestamp_utc = get_current_timestamp_utc()
    filtered_data = filter_stream_data(restaurant_read_stream_df, current_timestamp_utc)
    subscribers_data = read_subscribers_data(spark)
    result_df = join_and_transform_data(filtered_data, subscribers_data)

    result_df.writeStream.foreachBatch(
        lambda df, _: save_to_postgresql_and_kafka(df)
    ).start().awaitTermination()


if __name__ == "__main__":
    main()
