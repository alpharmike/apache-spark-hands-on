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

most_popular = id_conn_rel.sort(func.col("connections").desc()).first()

most_popular_mapped = names_df.filter(func.col("id") == most_popular[0]).select("name").first()

print(
    most_popular_mapped[0] + " is the most popular superhero with " + str(most_popular[1]) + " co-appearances.")

spark.stop()
