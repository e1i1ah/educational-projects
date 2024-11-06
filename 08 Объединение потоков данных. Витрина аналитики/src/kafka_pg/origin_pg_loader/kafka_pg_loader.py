import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

from config import AppConfig


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


config = AppConfig()


def create_spark_session():
    spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )
    logger.info("Creating Spark session")

    return (
        SparkSession.builder.appName("TransactionsCurrenciesStreamingService")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.packages", spark_jars_packages)
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession) -> DataFrame:
    logger.info("Reading Kafka stream")

    return (
        spark.readStream.format("kafka")
        .options(**config.get_kafka_params())
        .option("maxOffsetsPerTrigger", 1000)
        .load()
    )


def write_to_postgresql(df: DataFrame):
    incoming_message_schema = StructType(
        [
            StructField("object_id", StringType(), True),
            StructField("object_type", StringType(), True),
            StructField("sent_dttm", TimestampType(), True),
            StructField("payload", StringType(), True),
        ]
    )

    try:
        logger.info("Transforming DataFrame")

        postgres_df = (
            df.withColumn("value", f.col("value").cast("string"))
            .withColumn("data", f.from_json("value", incoming_message_schema))
            .select("data.*")
        )
        logger.info("Writing to PostgreSQL")

        postgres_df.write.jdbc(
            url=config.get_pg_url(),
            table=config.pg_table_transactions_currencies,
            mode="append",
            properties=config.get_pg_properties(),
        )
    except Exception as e:
        logger.error(f"Error writing to PostgreSQL: {str(e)}")


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    kafka_df = read_kafka_stream(spark)

    checkpoint_dir = "/home/jovyan/work/checkpoint"

    query = (
        kafka_df.writeStream.foreachBatch(lambda df, _: write_to_postgresql(df))
        .outputMode("append")
        .trigger(processingTime="3 seconds")
        .option("checkpointLocation", checkpoint_dir)
        .start()
    )

    logger.info("Streaming started")
    query.awaitTermination()


if __name__ == "__main__":
    main()
