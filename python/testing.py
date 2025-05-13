from pyspark.sql import SparkSession

# Start Spark Session
spark = SparkSession.builder \
    .appName("Local Glue Job") \
    .getOrCreate()

# Load local data (or mock data if you want to test logic only)
step_df = spark.read.json("data/step_trainer_trusted.json").alias("step")
accel_df = spark.read.json("data/accelerometer_trusted.json").alias("accel")
cust_df = spark.read.json("data/customer_curated.json").alias("cust")

# Join 1: accel to step (timestamp match)
step_accel = step_df.join(
    accel_df,
    step_df["sensorReadingTime"] == accel_df["timestamp"],
    "inner"
).alias("step_accel")

# Join 2: keep only customers who agreed to share data
final_join = step_accel.join(
    cust_df,
    step_accel["user"] == cust_df["email"],
    "inner"
)

# Select relevant fields for ML
ml_df = final_join.select(
    "user",
    "timestamp",
    "x", "y", "z",
    "step_accel.serialNumber",
    "distanceFromObject"
)

# Write locally to JSON for testing
ml_df.write.mode("overwrite").json("output/machine_learning_curated")
