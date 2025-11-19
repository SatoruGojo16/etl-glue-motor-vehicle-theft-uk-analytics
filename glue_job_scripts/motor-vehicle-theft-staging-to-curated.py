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
args = getResolvedOptions(sys.argv, ["is_init_load"])

spark = (
    SparkSession.Builder()
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

initial_load = args["is_init_load"]

df_incremental_load = glueContext.create_data_frame.from_catalog(
    database_name="motor_theft_vehicles", table_name="staging_motor_theft_vehicles"
)

dyf_staging_incidents = DynamicFrame.fromDF(
    df_incremental_load.select(
        "incident_id",
        "occurrence_datetime",
        "report_datetime",
        "report_number",
        "method_of_entry",
        "recovery_date",
        "recovery_status",
        "data_source",
        "source_file_name",
    ),
    glueContext,
)

dyf_staging_vehicle = DynamicFrame.fromDF(
    df_incremental_load.select(
        "vehicle_year",
        "vehicle_color",
        "vehicle_identification_number",
        "vehicle_license_plate",
    ),
    glueContext,
)

df_lookup_coordinates = glueContext.create_data_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": [
            "s3://motor-theft-vehicles-bucket/extra_support/uk_lat_lon_lookups.parquet"
        ]
    },
    format="parquet",
)

df_staging_location = df_incremental_load.select(
    "address", "borough", "city", "zip_code"
)
df_staging_location = df_staging_location.join(
    df_lookup_coordinates, on="borough", how="inner"
).select(
    "address",
    df_staging_location["borough"],
    "city",
    "zip_code",
    "latitude",
    "longitude",
)
dyf_staging_location = DynamicFrame.fromDF(df_staging_location, glueContext)


def get_secret():
    secret_name = "dev/motor_theft_vehicles/redshift_connection"
    region_name = "us-east-1"
    client = boto3.client(service_name="secretsmanager", region_name=region_name)
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e
    db_config = get_secret_value_response["SecretString"]
    return db_config


db_config = json.loads(get_secret())

my_conn_options = {
    "url": db_config["dev_url"],
    "user": db_config["dev_username"],
    "password": db_config["dev_password"],
    "redshiftTmpDir": db_config["dev_redshift_temp_directory"],
}

incidents_merge_query_post_action = """

BEGIN;

UPDATE dim_incidents 
SET is_current = 'N', effective_end_date = getdate()
FROM staging_incidents
WHERE staging_incidents.incident_id = dim_incidents.incident_id
and dim_incidents.is_current = 'Y'
and dim_incidents.method_of_entry = dim_incidents.method_of_entry
;

INSERT INTO dim_incidents(incident_id, occurrence_datetime,report_datetime,report_number,method_of_entry,recovery_date,recovery_status,data_source,source_file_name)
SELECT incident_id,occurrence_datetime,report_datetime,report_number,method_of_entry,recovery_date,recovery_status,data_source,source_file_name
FROM staging_incidents;

COMMIT;
"""

# loading Staging Incidents
my_conn_options["dbtable"] = "staging_incidents"
my_conn_options["preactions"] = "TRUNCATE " + my_conn_options["dbtable"]
my_conn_options["postactions"] = incidents_merge_query_post_action
glueContext.write_dynamic_frame.from_options(
    connection_type="redshift",
    connection_options=my_conn_options,
    frame=dyf_staging_incidents,
)

vehicle_insert_query_post_action = """

INSERT INTO dim_vehicle(vehicle_year,vehicle_color,vehicle_identification_number,vehicle_license_plate)
(SELECT source.vehicle_year,source.vehicle_color,source.vehicle_identification_number,source.vehicle_license_plate
FROM staging_vehicle as source 
LEFT JOIN dim_vehicle as target 
ON source.vehicle_identification_number = target.vehicle_identification_number
-- WHERE HASH(vehicle_year,vehicle_color,vehicle_license_plate) != HASH(vehicle_year,vehicle_color,vehicle_license_plate)
WHERE target.vehicle_identification_number is NULL
)
"""

# loading Staging Vehicle
my_conn_options["dbtable"] = "staging_vehicle"
my_conn_options["preactions"] = "TRUNCATE " + my_conn_options["dbtable"]
my_conn_options["postactions"] = vehicle_insert_query_post_action
glueContext.write_dynamic_frame.from_options(
    connection_type="redshift",
    connection_options=my_conn_options,
    frame=dyf_staging_vehicle,
    pre_actions="TRUNCATE " + my_conn_options["dbtable"],
    post_actions=vehicle_merge_query_post_action,
)

location_insert_query_post_action = """

INSERT INTO dim_location(address_line,borough,city,zip_code)
(SELECT staging_location.address_line,staging_location.borough,staging_location.city,staging_location.zip_code 
FROM staging_location
LEFT JOIN dim_location
ON staging_location.address_line = dim_location.address_line 
 and staging_location.borough = dim_location.borough
WHERE dim_location.location_id is NULL)

"""

# loading Staging Location
my_conn_options["dbtable"] = "staging_location"
my_conn_options["preactions"] = "TRUNCATE " + my_conn_options["dbtable"]
my_conn_options["postactions"] = location_insert_query_post_action
glueContext.write_dynamic_frame.from_options(
    connection_type="redshift",
    connection_options=my_conn_options,
    frame=dyf_staging_location,
    pre_actions="TRUNCATE " + my_conn_options["dbtable"],
    post_actions=location_merge_query_post_action,
)

if initial_load:
    dyf_make_model = glueContext.create_dynamic_frame_from_options(
        connection_type="s3",
        connection_options={
            "path": ["s3://motor-theft-vehicles-bucket/extra_support/make_details.csv"]
        },
        format="csv",
    )
    # loading Dim Make Model
    my_conn_options["dbtable"] = "dim_make_model"
    glueContext.write_dynamic_frame.from_options(
        connection_type="redshift",
        connection_options=my_conn_options,
        frame=dyf_make_model,
    )

if initial_load:
    dim_date = spark.sql(
        """
    SELECT explode(sequence(to_date('2000-01-01'), to_date('2030-01-01'), interval 1 months)) as date
    """
    )
    dim_date_cols = {
        "date_id": date_format(dim_date.date, "MMdd"),
        "month": date_format(dim_date.date, "M"),
        "month_short": date_format(dim_date.date, "LLL"),
        "month_long": date_format(dim_date.date, "LLLL"),
        "year_short": date_format(dim_date.date, "yy"),
        "year_long": date_format(dim_date.date, "yyyy"),
        "quarter": ceil(date_format(dim_date.date, "M") / 3),
    }
    dim_date = dim_date.withColumns(dim_date_cols)
    dim_date = dim_date.drop("date")
    dfy_dim_date = glueContext.fromDF(dim_date)
    # loading Curated Dim Date
    my_conn_options["dbtable"] = "dim_date"
    glueContext.write_dynamic_frame.from_options(
        connection_type="redshift",
        connection_options=my_conn_options,
        frame=dyf_make_model,
    )

fact_table_insert_query_post_action = """

INSERT INTO staging_thefts_load(address,borough,city,incident_id,incident_status,vehicle_license_plate,method_of_entry,occurrence_datetime,recovery_date,recovery_status,report_datetime,report_number,state,vehicle_color,vehicle_id,vehicle_year,vehicle_identification_number,zip_code,source_file_name,data_source,load_timestamp,report_date)
(
SELECT dim_theft.report_id , dim_vehicle.vehicle_id, dim_location.location_id, dim_theft.report_datetime, getdate() load_timestamp
FROM staging_load
JOIN dim_theft on staging_load.incident_id = dim_theft.incident_id and is_current = 'Y' and effective_end_date is NULL 
JOIN dim_vehicle on staging_load.vehicle_identification_number = dim_vehicle.vehicle_identification_number
JOIN dim_location on staging_load.address = dim_location.address_line
JOIN dim_make_model on staging_load.make_id = dim_make_model.make_id
)

"""

# loading Staging Thefts Incremental Load
dyf_staging_incidents = DynamicFrame.fromDF(staging_incidents, glueContext, "sm")
my_conn_options["dbtable"] = "staging_thefts_load"
my_conn_options["postactions"] = fact_table_insert_query_post_action
glueContext.write_dynamic_frame.from_options(
    connection_type="redshift",
    connection_options=my_conn_options,
    frame=dyf_staging_incidents,
)
