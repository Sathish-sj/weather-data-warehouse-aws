import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'SILVER_BUCKET', 'GOLD_BUCKET'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

SILVER_BUCKET = args['SILVER_BUCKET']
GOLD_BUCKET = args['GOLD_BUCKET']

print("=" * 70)
print("Building Dimensions")
print("=" * 70)

print("\nBuilding dim_location")

current_weather = spark.read.parquet(f"s3://{SILVER_BUCKET}/current_weather/")

locations_silver = current_weather.select(
    col("location_name"),
    col("country_code"),
    col("latitude"),
    col("longitude"),
    col("timezone_offset_seconds")
).distinct()

locations_silver = locations_silver.withColumn(
    "timezone_name",
    when(col("timezone_offset_seconds") == -18000, "EST")
    .when(col("timezone_offset_seconds") == 0, "GMT")
    .when(col("timezone_offset_seconds") == 32400, "JST")
    .when(col("timezone_offset_seconds") == 36000, "AEST")
    .when(col("timezone_offset_seconds") == 19800, "IST")
    .when(col("timezone_offset_seconds") == 14400, "GST")
    .when(col("timezone_offset_seconds") == -10800, "BRT")
    .otherwise("UTC")
)

print(f"Found {locations_silver.count()} unique locations in Silver")

try:
    dim_location_existing = spark.read.parquet(f"s3://{GOLD_BUCKET}/dim_location/")
    print(f"Existing dimension has {dim_location_existing.count()} records")
    dimension_exists = True
except:
    print("Dimension doesn't exist yet - creating fresh")
    dimension_exists = False

current_timestamp = datetime.now()
current_date = current_timestamp.date()

if not dimension_exists:
    print("Creating fresh dim_location")

    from pyspark.sql.window import Window
    
    window_spec = Window.orderBy("location_name")
    
    dim_location = locations_silver.withColumn(
        "location_key",
        row_number().over(window_spec)
    ).withColumn(
        "effective_from_date",
        lit(current_date).cast(DateType())
    ).withColumn(
        "effective_to_date",
        lit("9999-12-31").cast(DateType())
    ).withColumn(
        "is_current",
        lit(True)
    ).withColumn(
        "version",
        lit(1)
    ).withColumn(
        "created_timestamp",
        lit(current_timestamp).cast(TimestampType())
    ).withColumn(
        "updated_timestamp",
        lit(current_timestamp).cast(TimestampType())
    )

    dim_location.write.mode("overwrite").parquet(f"s3://{GOLD_BUCKET}/dim_location/")
    print(f"Created {dim_location.count()} location records")

else:
    print("Dimension exists, no updates needed")

print("\n=== Building dim_date ===")

start_date = datetime(2024, 1, 1)
end_date = datetime(2026, 12, 31)
date_range = []

current = start_date
while current <= end_date:
    date_range.append((current.date(),))
    current += timedelta(days=1)

dates_df = spark.createDataFrame(date_range, ["full_date"])

dim_date = dates_df.withColumn(
    "date_key",
    date_format(col("full_date"), "yyyyMMdd").cast(IntegerType())
).withColumn(
    "year",
    year(col("full_date"))
).withColumn(
    "quarter",
    quarter(col("full_date"))
).withColumn(
    "month",
    month(col("full_date"))
).withColumn(
    "month_name",
    date_format(col("full_date"), "MMMM")
).withColumn(
    "day",
    dayofmonth(col("full_date"))
).withColumn(
    "day_of_week",
    dayofweek(col("full_date"))
).withColumn(
    "day_name",
    date_format(col("full_date"), "EEEE")
).withColumn(
    "week_of_year",
    weekofyear(col("full_date"))
).withColumn(
    "is_weekend",
    when(dayofweek(col("full_date")).isin(1, 7), True).otherwise(False)
).withColumn(
    "is_holiday",
    lit(False)
)

dim_date.write.mode("overwrite").parquet(f"s3://{GOLD_BUCKET}/dim_date/")
print(f"Created dim_date with {dim_date.count()} records")
print("\n" + "=" * 70)
print("DIMENSIONS COMPLETED!")
print("=" * 70)

final_location = spark.read.parquet(f"s3://{GOLD_BUCKET}/dim_location/")
final_date = spark.read.parquet(f"s3://{GOLD_BUCKET}/dim_date/")

active_locations = final_location.filter(col("is_current") == True).count()

print(f"dim_location: {final_location.count()} total ({active_locations} current)")
print(f"dim_date: {final_date.count()} records")
print("=" * 70)
job.commit()