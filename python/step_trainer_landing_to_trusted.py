import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# Initialize
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Load data from Glue Data Catalog
step_trainer_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="stedi", table_name="step_trainer_landing"
)

accelerometer_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="stedi", table_name="accelerometer_trusted"
)

customers_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="stedi", table_name="customer_curated"
)

# Convert to DataFrames
step_df = step_trainer_dyf.toDF()
accel_df = accelerometer_dyf.toDF()
cust_df = customers_dyf.toDF()

# Join: customers ↔ accelerometer (email = user)
cust_accel_df = cust_df.join(accel_df, cust_df["email"] == accel_df["user"], "inner")

# Join: step_trainer ↔ above (serialNumber = serialnumber)
joined_df = step_df.join(cust_accel_df, step_df["serialNumber"] == cust_accel_df["serialnumber"], "inner")

# Filter: only where sensorReadingTime matches timeStamp
filtered_df = joined_df.filter(joined_df["sensorReadingTime"] == joined_df["timeStamp"])

# Select relevant columns for Trusted Zone
trusted_df = filtered_df.select("sensorReadingTime", "serialNumber", "distanceFromObject")

# Convert to DynamicFrame
trusted_dyf = DynamicFrame.fromDF(trusted_df, glueContext, "trusted_dyf")

# Write to S3 in JSON format
glueContext.write_dynamic_frame.from_options(
    frame=trusted_dyf,
    connection_type="s3",
    connection_options={
        "path": "s3://stedi-lakehouse-roblau/step_trainer/trusted/",
        "partitionKeys": []
    },
    format="json"
)

job.commit()
