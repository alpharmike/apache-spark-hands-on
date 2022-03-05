from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from src.utils.utils import get_project_root

ROOT_DIR = get_project_root()

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
])

# Create a SparkSession
spark = SparkSession.builder.appName("Popular Superheros").getOrCreate()

names_df = spark.read.option("sep", " ").schema(schema).csv(f"file://{ROOT_DIR}/data/marvel/names.txt")
graph = spark.read.text(f"file://{ROOT_DIR}/data/marvel/graph.txt")

id_conn_rel = graph.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))

id_conn_rel.show()

min_conn = id_conn_rel.agg(func.min("connections")).first()[0]
least_popular = id_conn_rel.filter(func.col("connections") == min_conn).join(names_df, "id")

least_popular.show()

spark.stop()
