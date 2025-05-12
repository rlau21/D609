import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load customer and accelerometer trusted tables
customer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted"
)

accelerometer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted"
)

# Convert to DataFrames
customer_df = customer_trusted.toDF()
accelerometer_df = accelerometer_trusted.toDF()

# Get distinct users from accelerometer data
distinct_users_df = accelerometer_df.select("user").distinct()

# Join with customer_df where email matches user
filtered_df = customer_df.join(distinct_users_df, customer_df["email"] == distinct_users_df["user"], "inner").drop("user")

# Convert back to DynamicFrame
customer_curated_dyf = DynamicFrame.fromDF(filtered_df, glueContext, "customer_curated")

# Write to S3
glueContext.write_dynamic_frame.from_options(
    frame=customer_curated_dyf,
    connection_type="s3",
    connection_options={"path": "s3://stedi-lakehouse-roblau/customer_curated/"},
    format="json"
)

job.commit()
