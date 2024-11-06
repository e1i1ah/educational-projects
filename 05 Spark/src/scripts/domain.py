from pyspark.sql import Window
import pyspark.sql.functions as F


class RawEvent:
    def __init__(self, spark, raw_file_path, ods_file_path, date):
        self.spark = spark
        self.raw_file_path = raw_file_path
        self.ods_file_path = ods_file_path
        self.date = date

    def load_to_ods(self):
        # Чтение данных событий из Parquet и сохранение их в ODS с разбивкой по 'event_type'
        events = self.spark.read.parquet(f"{self.raw_file_path}/date={self.date}")
        events.write.partitionBy("event_type").mode("overwrite").format("parquet").save(
            f"{self.ods_file_path}/date={self.date}"
        )


class OdsEvent:
    def __init__(self, spark, ods_file_path, cities_path, base_file_path):
        self.spark = spark
        self.ods_file_path = ods_file_path
        self.base_file_path = base_file_path
        self.cities = self._load_cities(cities_path)

    def _load_cities(self, path):
        # Чтение данных о городах из CSV и преобразование координат в числовой формат
        return (
            self.spark.read.csv(path, sep=";", header=True, inferSchema=True)
            .withColumn("geo_lat", F.expr("replace(geo_lat, ',', '.')").cast("float"))
            .withColumn("geo_lon", F.expr("replace(geo_lon, ',', '.')").cast("float"))
            .cache()
        )

    def read(self, type):
        # Чтение событий из ODS и фильтрация их по типу
        events = self.spark.read.parquet(self.ods_file_path)
        events = events.where(f"event_type = '{type}'").repartition("date").cache()
        return events

    def write(self, mart, mart_name, layer):
        save_path = f"{self.base_file_path}/{layer}/{mart_name}"

        if mart_name == "user_geo_mart":
            # Выбор определенных колонок для витрины user_geo_mart
            mart = mart.select(
                "user_id",
                "act_city",
                "home_city",
                "travel_count",
                "travel_array",
                "local_time",
            )

        if layer == "dml":
            mart.repartition(1).write.mode("overwrite").format("csv").option(
                "header", "true"
            ).save(save_path)
        else:
            mart.write.mode("overwrite").format("parquet").save(save_path)

    def calculate_distance(self, df):
        # Вычисление расстояния между двумя точками на земной поверхности
        R = 6371.0

        distance_expr = (
            R
            * 2
            * F.asin(
                F.sqrt(
                    F.sin(F.radians(F.col("lat_first") - F.col("lat_second")) / 2) ** 2
                    + F.cos(F.radians(F.col("lat_first")))
                    * F.cos(F.radians(F.col("lat_second")))
                    * F.sin(F.radians(F.col("lon_first") - F.col("lon_second")) / 2)
                    ** 2
                )
            )
        )

        df = df.withColumn("distance", distance_expr)
        return df

    def calculate_nearest_city(self, df):
        # Находит ближайший город для каждого события
        df = df.withColumnRenamed("lon", "lon_first").withColumnRenamed(
            "lat", "lat_first"
        )
        cities = self.cities.withColumnRenamed(
            "geo_lat", "lat_second"
        ).withColumnRenamed("geo_lon", "lon_second")

        df_cities = df.crossJoin(cities)
        df = self.calculate_distance(df_cities)

        # Группировка по событию и выбор ближайшего города
        df = (
            df.groupBy("event", "lat_first", "lon_first", "date", "event_type")
            .agg(F.min(F.struct("distance", "city")).alias("min"))
            .select(
                "event",
                "lat_first",
                "lon_first",
                "date",
                "event_type",
                F.col("min").getField("city").alias("city"),
            )
        )

        return df


class UserGeoMart(OdsEvent):
    def __init__(self, spark, ods_file_path, cities_path, dml_file_path):
        super().__init__(spark, ods_file_path, cities_path, dml_file_path)
        self.messages = self.read("message")

    def add_city(self):
        # Добавление информации о городе к сообщениям
        result = self.calculate_nearest_city(self.messages)
        return result.select(
            result.event.message_from.alias("user_id"),
            result.event.message_ts.alias("timestamp"),
            result.lon_first,
            result.lat_first,
            result.date,
            result.city,
        ).cache()

    def add_home_last_loc(self, mart):
        # Определение домашнего города и последнего местоположения пользователя
        unique_messages = mart.select("user_id", "date", "city").distinct()
        window_user_city = Window.partitionBy("user_id", "city").orderBy("date")

        groups = unique_messages.withColumn(
            "group",
            F.sum(
                F.when(
                    F.datediff("date", F.lag("date").over(window_user_city)) != 1, 1
                ).otherwise(0)
            ).over(window_user_city),
        )

        days_in_sequence = (
            groups.groupBy("user_id", "city", "group")
            .agg(
                F.countDistinct("date").alias("days"), F.max("date").alias("last_date")
            )
            .filter(F.col("days") >= 27)
        )

        user_home = (
            days_in_sequence.withColumn(
                "row_num",
                F.row_number().over(
                    Window.partitionBy("user_id").orderBy(F.desc("last_date"))
                ),
            )
            .filter(F.col("row_num") == 1)
            .select("user_id", F.col("city").alias("home_city"))
        )

        last_location = (
            mart.groupBy("user_id")
            .agg(F.max(F.struct("timestamp", "city")).alias("max"))
            .select("user_id", F.col("max.city").alias("act_city"))
        )

        result = mart.join(last_location, "user_id").join(user_home, "user_id", "left")
        return result

    def add_travel_info(self, mart):
        # Добавление информации о перемещениях пользователя
        window_user = Window.partitionBy("user_id").orderBy(
            F.coalesce(F.col("timestamp"), F.to_timestamp("date"))
        )

        travel_details = mart.withColumn(
            "city_change",
            F.coalesce(
                (F.col("city") != F.lag("city").over(window_user)).cast("int"), F.lit(1)
            ),
        )

        travel_count = travel_details.groupBy("user_id").agg(
            F.sum("city_change").alias("travel_count")
        )
        travel_array = (
            travel_details.where("city_change = 1")
            .orderBy("timestamp")
            .groupBy("user_id")
            .agg(F.collect_list("city").alias("travel_array"))
        )

        return mart.join(travel_count, "user_id").join(travel_array, "user_id")

    def add_local_time(self, mart):
        # Добавление местного времени в соответствии с часовым поясом города
        result = (
            mart.join(self.cities.select("city", "timezone"), "city")
            .withColumn(
                "local_time",
                F.from_utc_timestamp(F.col("timestamp"), F.col("timezone")),
            )
            .drop("timezone")
        )

        return result

    def create_data_mart(self):
        # Создание витрины данных
        user_message_ts_city_df = self.add_city()
        travel_metrics_df = self.add_home_last_loc(user_message_ts_city_df)
        enriched_travel_metrics_df = self.add_travel_info(travel_metrics_df)
        final_data_mart_df = self.add_local_time(enriched_travel_metrics_df)
        return final_data_mart_df


class ZoneGeoMart(OdsEvent):
    def __init__(self, spark, ods_file_path, cities_path, dml_file_path):
        super().__init__(spark, ods_file_path, cities_path, dml_file_path)
        self.messages = self.read("message")
        self.reactions = self.read("reaction")
        self.subscriptions = self.read("subscription")

    def get_user_registrations(self, messages):
        # Получение регистрации пользователей на основе первых сообщений
        return (
            messages.withColumn(
                "rank",
                F.row_number().over(
                    Window.partitionBy("event.message_from").orderBy("event.message_ts")
                ),
            )
            .filter(F.col("rank") == 1)
            .withColumn("event_type", F.lit("registration"))
        )

    def create_data_mart(self):
        # Создание витрины данных
        messages = self.calculate_nearest_city(self.messages)
        registrations = self.get_user_registrations(messages).select(
            "event_type", "city", "date"
        )
        messages = messages.select("event_type", "city", "date")
        reactions = self.calculate_nearest_city(self.reactions).select(
            "event_type", "city", "date"
        )
        subscriptions = self.calculate_nearest_city(self.subscriptions).select(
            "event_type", "city", "date"
        )

        combined_df = (
            messages.unionByName(reactions)
            .unionByName(subscriptions)
            .unionByName(registrations)
            .repartition("date")
        )

        # Вычисление недельного и месячного количества событий по каждому типу
        combined_df = combined_df.withColumn(
            "week", F.date_trunc("week", "date")
        ).withColumn("month", F.date_trunc("month", "date"))

        w = Window().partitionBy("month", "city")

        result = (
            combined_df.groupBy("month", "week", "city")
            .agg(
                F.sum(F.when(F.col("event_type") == "message", 1)).alias(
                    "week_message"
                ),
                F.sum(F.when(F.col("event_type") == "reaction", 1)).alias(
                    "week_reaction"
                ),
                F.sum(F.when(F.col("event_type") == "subscription", 1)).alias(
                    "week_subscription"
                ),
                F.sum(F.when(F.col("event_type") == "registration", 1)).alias(
                    "week_user"
                ),
            )
            .withColumn("month_message", F.sum("week_message").over(w))
            .withColumn("month_reaction", F.sum("week_reaction").over(w))
            .withColumn("month_subscription", F.sum("week_subscription").over(w))
            .withColumn("month_user", F.sum("week_user").over(w))
            .orderBy(F.col("month"), F.col("week"), F.col("city"))
            .withColumnRenamed("city", "zone_id")
        )

        return result


class FriendRecommendationMart(OdsEvent):
    def __init__(
        self, spark, ods_file_path, user_location_df, cities_path, dml_file_path
    ):
        super().__init__(spark, ods_file_path, cities_path, dml_file_path)
        self.subscriptions = self.read("subscription").sample(fraction=0.1)
        self.messages = self.read("message")
        self.user_last_location_df = self._get_user_last_location_df(user_location_df)

    def _get_user_last_location_df(self, user_location_df):
        # Получение последнего местоположения пользователей
        result = user_location_df.withColumn(
            "rank",
            F.row_number().over(
                Window.partitionBy("user_id").orderBy(
                    F.coalesce(F.col("timestamp"), F.to_timestamp("date")).desc()
                )
            ),
        ).filter(F.col("rank") == 1)
        return result

    def create_data_mart(self):
        # Создание витрины рекомендаций друзей на основе подписок и сообщений
        pair_subscriptions = (
            self.subscriptions.alias("t1")
            .join(
                self.subscriptions.alias("t2"),
                (
                    F.col("t1.event.subscription_channel")
                    == F.col("t2.event.subscription_channel")
                )
                & (F.col("t1.event.user") < F.col("t2.event.user")),
            )
            .select(
                F.col("t1.event.user").alias("user_left"),
                F.col("t2.event.user").alias("user_right"),
            )
            .distinct()
        )

        message_pairs = self.messages.select(
            F.least(F.col("event.message_from"), F.col("event.message_to")).alias(
                "user_left"
            ),
            F.greatest(F.col("event.message_from"), F.col("event.message_to")).alias(
                "user_right"
            ),
        ).distinct()

        # Оставить только пары, которые не обменивались сообщениями
        filtered_subscriptions = pair_subscriptions.join(
            message_pairs, on=["user_left", "user_right"], how="left_anti"
        )

        # Объединение с последним известным местоположением
        joined_with_last_location = (
            filtered_subscriptions.join(
                self.user_last_location_df.alias("ul1"),
                F.col("user_left") == F.col("ul1.user_id"),
            )
            .join(
                self.user_last_location_df.alias("ul2"),
                F.col("user_right") == F.col("ul2.user_id"),
            )
            .select(
                "user_left",
                "user_right",
                F.col("ul1.lat_first"),
                F.col("ul1.lon_first"),
                F.col("ul2.lat_first").alias("lat_second"),
                F.col("ul2.lon_first").alias("lon_second"),
                F.col("ul1.city").alias("city_first"),
                F.col("ul2.city").alias("city_second"),
                F.col("ul1.local_time"),
            )
        )

        # Фильтрация пользователей в одном и том же городе и на расстояние менее 1 км
        joined_with_last_location = joined_with_last_location.filter(
            F.col("city_first") == F.col("city_second")
        )
        joined_with_last_location = self.calculate_distance(joined_with_last_location)

        processed_dttm = F.current_timestamp()

        # Формирование рекомендаций
        recommendations = joined_with_last_location.filter(
            F.col("distance") <= 1
        ).select(
            "user_left",
            "user_right",
            processed_dttm.alias("processed_dttm"),
            F.col("city_first").alias("zone_id"),
            "local_time",
        )

        return recommendations
