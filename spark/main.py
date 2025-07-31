from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType
from dotenv import load_dotenv
import os

# Load correct env file
if os.environ.get("RUNNING_IN_DOCKER") == "1":
    load_dotenv(dotenv_path="/app/.env.docker")
else:
    load_dotenv(dotenv_path=".env.local")

# Env vars
KAFKA_BOOTSTRAP = os.getenv("BOOTSTRAP_SERVERS")
SPARK_MASTER = "spark://localhost:7077"    # host-accessible Spark master URL
HOST_IP = "192.168.56.1"                    # your PC IP, adjust as needed

spark = SparkSession.builder \
    .appName("RealTimeFraudDetection") \
    .master(SPARK_MASTER) \
    .config("spark.driver.host", HOST_IP) \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .getOrCreate()

schema = StructType() \
    .add("step", StringType()) \
    .add("type", StringType()) \
    .add("amount", StringType()) \
    .add("nameOrig", StringType()) \
    .add("oldbalanceOrg", StringType()) \
    .add("newbalanceOrig", StringType()) \
    .add("nameDest", StringType()) \
    .add("oldbalanceDest", StringType()) \
    .add("newbalanceDest", StringType()) \
    .add("isFlaggedFraud", StringType())

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", "streaming-data") \
    .load()

df = df.selectExpr("CAST(value AS STRING) as json") \
       .select(from_json(col("json"), schema).alias("data")) \
       .select("data.*")

print("Spark is listening ...")

query = df.writeStream \
    .format("console") \
    .start()

query.awaitTermination()
