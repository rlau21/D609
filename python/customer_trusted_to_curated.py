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

# Load trusted tables
customer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted"
)

accelerometer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted"
)

# Join to filter only customers who have accelerometer data
joined = Join.apply(
    frame1=customer_trusted,
    frame2=accelerometer_trusted,
    keys1=["email"],
    keys2=["user"]
)

# Drop accelerometer columns and remove duplicates
customer_curated = joined.drop_fields(["user", "timestamp", "x", "y", "z"]).drop_duplicates()

# Write to curated S3 path
glueContext.write_dynamic_frame.from_options(
    frame=customer_curated,
    connection_type="s3",
    connection_options={"path": "s3://stedi-lakehouse-roblau/customer_curated/"},
    format="json"
)

job.commit()
