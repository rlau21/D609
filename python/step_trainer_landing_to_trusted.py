import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# Init job
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Load data from Glue catalog
step_trainer_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_landing"
)
accelerometer_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted"
)
customers_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated"
)

# Convert to DataFrames with aliases
step_df = step_trainer_dyf.toDF().alias("step")
accel_df = accelerometer_dyf.toDF().alias("accel")
cust_df = customers_dyf.toDF().alias("cust")

# Join customers_curated to accelerometer_trusted on email = user
cust_accel_df = cust_df.join(
    accel_df,
    cust_df["email"] == accel_df["user"],
    "inner"
).select(
    cust_df["serialnumber"].alias("cust_serial"),
    accel_df["timeStamp"]
).alias("cust_accel")

# Join step_trainer_landing on serialNumber and timestamp match
filtered_df = step_df.join(
    cust_accel_df,
    (step_df["serialNumber"] == cust_accel_df["cust_serial"]) &
    (step_df["sensorReadingTime"] == cust_accel_df["timeStamp"]),
    "inner"
).select(
    step_df["sensorReadingTime"],
    step_df["serialNumber"],
    step_df["distanceFromObject"]
).dropDuplicates()

# Convert back to DynamicFrame
final_dyf = DynamicFrame.fromDF(filtered_df, glueContext, "final_dyf")

# Write to Trusted Zone in S3
glueContext.write_dynamic_frame.from_options(
    frame=final_dyf,
    connection_type="s3",
    connection_options={"path": "s3://stedi-lakehouse-roblau/step_trainer_trusted/"},
    format="json"
)

job.commit()
