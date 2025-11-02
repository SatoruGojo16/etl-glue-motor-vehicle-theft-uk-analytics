import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *
from awsglue import DynamicFrame
import json
from datetime import date
import boto3
from botocore.exceptions import ClientError

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

todays_date = date.today().strftime("%Y-%m-%d")
base_raw_path = "s3://motor-theft-vehicles-bucket/raw/"
file_name = f"london_incidents"


def get_secret():
    secret_name = "dev/motor_theft_vehicles/postgresql"
    region_name = "us-east-1"
    client = boto3.client(service_name="secretsmanager", region_name=region_name)
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e
    db_config = get_secret_value_response["SecretString"]
    return db_config


db_config = json.loads(get_secret())

jdbc_url = (
    f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
)
query = f"(SELECT * FROM tbl_motor_theft_vehicles WHERE to_date(report_datetime ,'YYYY-MM-DD')='{todays_date}') tbl"
df = (
    spark.read.format("jdbc")
    .options(
        url=jdbc_url,
        driver=db_config["driver"],
        dbtable=query,
        user=db_config["user"],
        password=db_config["password"],
    )
    .load()
)

df.show()

df = df.withColumn("source_file_name", lit(file_name))
df = df.withColumn("load_timestamp", lit(current_timestamp()))
df = df.withColumn("report_date", to_date("report_datetime"))

s3output = glueContext.getSink(
    path=base_raw_path + file_name,
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["report_date"],
    enableUpdateCatalog=True,
)
s3output.setCatalogInfo(
    catalogDatabase="motor_theft_vehicles", catalogTableName="raw_motor_theft_vehicles"
)
s3output.setFormat("csv")
s3output.writeFrame(DynamicFrame.fromDF(df, glueContext))
