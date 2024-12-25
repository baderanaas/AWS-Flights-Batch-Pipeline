import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from datetime import datetime, timedelta
import boto3
import time
from pyspark.sql.functions import date_format, to_timestamp, col, lower, trim, coalesce, lit, upper

# Initialize Glue Context and Job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

yesterday_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
paths = [
    f"s3://flights-data-storage/transformed/EDDL/{yesterday_date}*",
    f"s3://flights-data-storage/transformed/EDDH/{yesterday_date}*",
    f"s3://flights-data-storage/transformed/EDDF/{yesterday_date}*",
    f"s3://flights-data-storage/transformed/EDDB/{yesterday_date}*",
    f"s3://flights-data-storage/transformed/EDDM/{yesterday_date}*"
]
transformed_data_path = "s3://flights-data-storage/processed/all_data"

dfs = []
for path in paths:
    try:
        df = spark.read.parquet(path)
        print(f"Schema for path {path}:")
        df.printSchema()
        if df.count() > 0:
            dfs.append(df)
    except Exception as e:
        print(f"Error reading path {path}: {str(e)}")
        continue

if not dfs:
    print("No data found in any of the paths")
    sys.exit(0)

combined_df = dfs[0]
for df in dfs[1:]:
    combined_df = combined_df.union(df)

print("Available columns in combined DataFrame:")
print(combined_df.columns)

df_processed = combined_df\
    .withColumn("scheduled_departure_time", 
                date_format(col("scheduled_departure_time"), "yyyy-MM-dd'T'HH:mm"))\
    .withColumn("actual_departure_time", 
                date_format(col("actual_departure_time"), "yyyy-MM-dd'T'HH:mm"))\
    .withColumn("flight_status", lower(trim(col("flight_status"))))\
    .withColumn("departure_airport_name", trim(col("departure_airport_name")))\
    .withColumn("departure_airport_icao", trim(upper(col("departure_airport_icao"))))\
    .withColumn("departure_terminal", coalesce(trim(col("departure_terminal")), lit("N/A")))\
    .withColumn("departure_gate", coalesce(trim(col("departure_gate")), lit("N/A")))\
    .withColumn("departure_delay", coalesce(col("departure_delay").cast("integer"), lit(0)))\
    .withColumn("arrival_airport_name", trim(col("arrival_airport_name")))\
    .withColumn("arrival_airport_icao", trim(upper(col("arrival_airport_icao"))))\
    .withColumn("airline_name", trim(col("airline_name")))\
    .withColumn("airline_icao", trim(upper(col("airline_icao"))))\
    .withColumn("flight_icao", trim(upper(col("flight_icao"))))\
    .withColumn("aircraft_icao24", trim(lower(col("aircraft_icao24"))))\
    .dropDuplicates()\
    .dropna(subset=["flight_date", "flight_icao", "departure_airport_icao", "arrival_airport_icao"])

dynamic_frame_transformed = DynamicFrame.fromDF(df_processed, glueContext, "dynamic_frame_processed")
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame_transformed,
    connection_type="s3",
    connection_options={
        "path": transformed_data_path,
        "partitionKeys": ["flight_date"]
    },
    format="parquet"
)

time.sleep(10)

# Triggering crawler
glue_client = boto3.client('glue')
crawler_name = "AllDataCrawler"
glue_client.start_crawler(Name=crawler_name)
print(f"Crawler '{crawler_name}' triggered successfully.")

job.commit()