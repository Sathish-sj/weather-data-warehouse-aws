import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BRONZE_BUCKET', 'SILVER_BUCKET'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

BRONZE_BUCKET = args['BRONZE_BUCKET']
SILVER_BUCKET = args['SILVER_BUCKET']

print("=== Starting Bronze to Silver Transformation ===")

def process_current_weather():
    print("Processing current weather data...")

    bronze_path = f"s3://{BRONZE_BUCKET}/raw/current/*/*/*/*/*.json"

    try:
        df = spark.read.option("multiLine", "true").json(bronze_path)
    except Exception as e:
        print(f"No current weather data found: {e}")
        return None

    wind_fields = []
    if "wind" in df.schema.fieldNames():
        wind_fields = df.schema["wind"].dataType.fieldNames()

    if "gust" in wind_fields:
        wind_gust_col = col("wind.gust")
    else:
        wind_gust_col = lit(0.0)

    silver_df = df.select(
        col("city_name").alias("location_name"),
        col("country").alias("country_code"),
        col("coord.lat").alias("latitude"),
        col("coord.lon").alias("longitude"),
        to_timestamp(col("extraction_timestamp")).alias("observation_time"),
        col("batch_id"),

        # Weather metrics
        col("main.temp").alias("temperature_celsius"),
        col("main.feels_like").alias("feels_like_celsius"),
        col("main.temp_min").alias("temp_min_celsius"),
        col("main.temp_max").alias("temp_max_celsius"),
        col("main.humidity").alias("humidity_percent"),
        col("main.pressure").alias("pressure_hpa"),

        # Wind
        col("wind.speed").alias("wind_speed_mps"),
        col("wind.deg").alias("wind_direction_deg"),
        wind_gust_col.alias("wind_gust_mps"),

        # Clouds & visibility
        col("clouds.all").alias("cloud_cover_percent"),
        coalesce(col("visibility"), lit(10000)).alias("visibility_meters"),

        # Weather condition
        col("weather").getItem(0).getField("main").alias("weather_condition"),
        col("weather").getItem(0).getField("description").alias("weather_description"),

        # Timezone & sun
        col("timezone").alias("timezone_offset_seconds"),
        from_unixtime(col("sys.sunrise")).cast("timestamp").alias("sunrise_time"),
        from_unixtime(col("sys.sunset")).cast("timestamp").alias("sunset_time"),

        current_timestamp().alias("processed_timestamp")
    )

    # -------- DATA QUALITY --------
    silver_df = (
        silver_df
        .filter(col("temperature_celsius").isNotNull())
        .filter(col("temperature_celsius").between(-50, 60))
        .filter(col("humidity_percent").between(0, 100))
        .dropDuplicates(["location_name", "observation_time"])
    )

    # -------- DERIVED FIELDS --------
    silver_df = (
        silver_df
        .withColumn("observation_date", to_date(col("observation_time")))
        .withColumn(
            "is_daytime",
            when(
                col("observation_time").between(col("sunrise_time"), col("sunset_time")),
                True
            ).otherwise(False)
        )
        .withColumn(
            "heat_index_category",
            when(col("feels_like_celsius") > 40, "Extreme Heat")
            .when(col("feels_like_celsius") > 32, "High Heat")
            .when(col("feels_like_celsius") > 26, "Moderate")
            .when(col("feels_like_celsius") > 10, "Comfortable")
            .otherwise("Cold")
        )
    )

    # -------- WRITE SILVER --------
    silver_path = f"s3://{SILVER_BUCKET}/current_weather/"

    silver_df.write \
        .partitionBy("observation_date") \
        .mode("append") \
        .parquet(silver_path)

    record_count = silver_df.count()
    print(f"Processed {record_count} current weather records")

    return record_count

def process_forecast_weather():
    print("Processing forecast weather data...")

    bronze_path = f"s3://{BRONZE_BUCKET}/raw/forecast/*/*/*/*/*.json"

    try:
        df = spark.read.option("multiLine", "true").json(bronze_path)
    except Exception as e:
        print(f"No forecast data found: {e}")
        return None

    forecast_exploded = df.select(
        col("city_name").alias("location_name"),
        col("country").alias("country_code"),
        to_timestamp(col("extraction_timestamp")).alias("forecast_created_time"),
        col("batch_id"),
        explode(col("list")).alias("forecast_point")
    )

    silver_forecast = forecast_exploded.select(
        col("location_name"),
        col("country_code"),
        col("forecast_created_time"),
        col("batch_id"),

        from_unixtime(col("forecast_point.dt")).cast("timestamp").alias("forecast_for_time"),

        col("forecast_point.main.temp").alias("temperature_celsius_forecast"),
        col("forecast_point.main.feels_like").alias("feels_like_celsius_forecast"),
        col("forecast_point.main.humidity").alias("humidity_percent_forecast"),
        col("forecast_point.main.pressure").alias("pressure_hpa_forecast"),

        col("forecast_point.wind.speed").alias("wind_speed_mps_forecast"),
        col("forecast_point.wind.deg").alias("wind_direction_deg_forecast"),

        col("forecast_point.clouds.all").alias("cloud_cover_percent_forecast"),
        coalesce(col("forecast_point.pop"), lit(0.0)).alias("precipitation_probability"),

        col("forecast_point.weather").getItem(0).getField("main").alias("weather_condition_forecast"),

        current_timestamp().alias("processed_timestamp")
    )

    silver_forecast = (
        silver_forecast
        .withColumn("forecast_for_date", to_date(col("forecast_for_time")))
        .withColumn("forecast_created_date", to_date(col("forecast_created_time")))
        .withColumn(
            "forecast_horizon_hours",
            round(
                (unix_timestamp(col("forecast_for_time")) -
                 unix_timestamp(col("forecast_created_time"))) / 3600.0,
                1
            )
        )
    )

    silver_path = f"s3://{SILVER_BUCKET}/forecast_weather/"

    silver_forecast.write \
        .partitionBy("forecast_created_date") \
        .mode("append") \
        .parquet(silver_path)

    record_count = silver_forecast.count()
    print(f"Processed {record_count} forecast records")

    return record_count

current_count = process_current_weather()
forecast_count = process_forecast_weather()

print(f"""
=== Bronze to Silver Transformation Complete ===
Current Weather Records: {current_count}
Forecast Records: {forecast_count}
""")

job.commit()
