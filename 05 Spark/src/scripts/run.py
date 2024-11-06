import os
import findspark
import sys
from pyspark.sql import SparkSession
from domain import *

os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["YARN_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"
os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf/"


def main():
    task_name = sys.argv[1]
    start_date = sys.argv[2]

    findspark.init()

    spark = (
        SparkSession.builder.master("yarn")
        .config("spark.executor.instances", 4)
        .config("spark.executor.memory", "8g")
        .config("spark.executor.cores", 2)
        .config("spark.driver.memory", "1g")
        .config("spark.driver.cores", 1)
        .config("spark.sql.shuffle.partitions", 200)
        .getOrCreate()
    )

    raw_file_path = "/user/master/data/geo/events"
    ods_file_path = "/user/elljah/data/ods/events"
    cities_file_path = "/user/elljah/data/geo.csv"
    base_file_path = "/user/elljah/data"

    if task_name == "load_to_ods":
        raw_event = RawEvent(spark, raw_file_path, ods_file_path, start_date)
        raw_event.load_to_ods()

    elif task_name == "UserGeoMart":
        user_geo_mart = UserGeoMart(
            spark, ods_file_path, cities_file_path, base_file_path
        )
        user_geo_mart_data = user_geo_mart.create_data_mart()
        user_geo_mart.write(user_geo_mart_data, "user_geo_mart", "analytics")
        user_geo_mart.write(user_geo_mart_data, "user_geo_mart", "dml")

    elif task_name == "ZoneGeoMart":
        zone_geo_mart = ZoneGeoMart(
            spark, ods_file_path, cities_file_path, base_file_path
        )
        zone_geo_mart_data = zone_geo_mart.create_data_mart()
        zone_geo_mart.write(zone_geo_mart_data, "zone_geo_mart", "analytics")
        zone_geo_mart.write(zone_geo_mart_data, "zone_geo_mart", "dml")

    elif task_name == "FriendRecommendationMart":
        friend_recommendation_mart = FriendRecommendationMart(
            spark, ods_file_path, user_geo_mart_data, cities_file_path, base_file_path
        )
        friend_recommendation_mart_data = friend_recommendation_mart.create_data_mart()
        friend_recommendation_mart.write(
            friend_recommendation_mart_data, "friend_recommendation_mart", "analytics"
        )
        friend_recommendation_mart.write(
            friend_recommendation_mart_data, "friend_recommendation_mart", "dml"
        )


if __name__ == "__main__":
    main()
