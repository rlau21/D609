import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load tables from the catalog
accelerometer_landing = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing"
)

customer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted"
)

# Join on user = email
joined_data = Join.apply(
    frame1=accelerometer_landing,
    frame2=customer_trusted,
    keys1=["user"],
    keys2=["email"]
)

# Keep required columns, including timestamp
columns_to_keep = ["user", "timestamp", "x", "y", "z"]
accelerometer_trusted = joined_data.select_fields(columns_to_keep)

# Write to Trusted Zone
glueContext.write_dynamic_frame.from_options(
    frame=accelerometer_trusted,
    connection_type="s3",
    connection_options={"path": "s3://stedi-lakehouse-roblau/accelerometer_trusted/"},
    format="json"
)

job.commit()
