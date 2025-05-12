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

# Load customer data from Glue Catalog
customer_landing = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_landing"
)

# Fix filter: correct casing AND ignore empty strings
def is_research_approved(row):
    return (
        "shareWithResearchAsOfDate" in row and
        row["shareWithResearchAsOfDate"] and
        str(row["shareWithResearchAsOfDate"]).strip() != ""
    )

customer_trusted = Filter.apply(
    frame=customer_landing,
    f=is_research_approved
)

# Write sanitized data to Trusted S3 Zone
glueContext.write_dynamic_frame.from_options(
    frame=customer_trusted,
    connection_type="s3",
    connection_options={"path": "s3://stedi-lakehouse-roblau/customer_trusted/"},
    format="json"
)

job.commit()
