import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import *
from pyspark.sql.types import *
from awsglue.dynamicframe import DynamicFrame
from datetime import datetime
from pyspark.sql import SparkSession

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

todays_date = datetime.today().strftime("%Y-%m-%d")

df = glueContext.create_data_frame_from_catalog(
    database="motor_theft_vehicles",
    table_name="raw_motor_theft_vehicles",
    push_down_predicate=f"report_date = '{todays_date}'",
)
df.printSchema()

df = (
    df.withColumn("addressline", trim(col("addressline").cast(StringType())))
    .withColumn("city", trim(col("city").cast(StringType())))
    .withColumn("incident_id", trim(col("incident_id").cast(StringType())))
    .withColumn("incident_status", trim(col("incident_status").cast(StringType())))
    .withColumn("licenseplate", trim(col("licenseplate").cast(StringType())))
    .withColumn("method_of_entry", trim(col("method_of_entry").cast(StringType())))
    .withColumn("occurrence_datetime", col("occurrence_datetime").cast(TimestampType()))
    .withColumn("recovery_date", col("recovery_date").cast(TimestampType()))
    .withColumn("recovery_status", trim(col("recovery_status").cast(StringType())))
    .withColumn("report_datetime", col("report_datetime").cast(TimestampType()))
    .withColumn("report_number", col("report_number").cast(IntegerType()))
    .withColumn("state", trim(col("state").cast(StringType())))
    .withColumn("vehicle_color", trim(col("vehicle_color").cast(StringType())))
    .withColumn("vehicle_id", col("vehicle_id").cast(IntegerType()))
    .withColumn("vehicle_year", col("vehicle_year").cast(IntegerType()))
    .withColumn("vin", trim(col("vin").cast(StringType())))
    .withColumn("zipcode", col("zipcode").cast(IntegerType()))
    .withColumn("source_file_name", trim(col("source_file_name").cast(StringType())))
    .withColumn("data_source", trim(col("data_source").cast(StringType())))
    .withColumn("load_timestamp", col("load_timestamp").cast(TimestampType()))
    .withColumn("report_date", col("report_date").cast(DateType()))
)

df = df.filter(
    (year("report_date") >= 2000) & (year("report_date") < year(current_date()) + 1)
)

col_renamed_required = {
    "addressline": "address",
    "licenseplate": "vehicle_license_plate",
    "zipcode": "zip_code",
    "vin": "vehicle_identification_number",
}
df = df.withColumnsRenamed(col_renamed_required)

df = df.withColumn("incident_id", upper(col("incident_id")))
df = df.withColumn("incident_status", upper(col("incident_status")))
df = df.withColumn("report_number", concat(lit("LON"), col("report_number")))

df = df.withColumn(
    "occurrence_datetime",
    date_format(col("occurrence_datetime"), "dd-MM-yyyy HH:mm:ss"),
)
df = df.withColumn(
    "report_datetime", date_format(col("report_datetime"), "dd-MM-yyyy HH:mm:ss")
)
df = df.withColumn(
    "load_timestamp", date_format(col("load_timestamp"), "dd-MM-yyyy HH:mm:ss")
)

df = df.dropDuplicates(subset=["incident_id", "report_number", "report_datetime"])

df = df.fillna("Unknown", subset=["incident_status", "method_of_entry"])
df = df.replace({"NA": "Unknown"}, subset=["recovery_status"])

spark = (
    SparkSession.builder.config(
        "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config(
        "spark.delta.logStore.class",
        "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
    )
    .getOrCreate()
)

df.write.format("delta").mode("append").partitionBy("report_date").save(
    "s3://motor-theft-vehicles-bucket/staging/"
)
