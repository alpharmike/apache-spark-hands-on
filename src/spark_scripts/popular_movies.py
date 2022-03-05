import codecs

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
from src.utils.utils import get_project_root

ROOT_DIR = get_project_root()

schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("movie_id", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", LongType(), True)
])


def load_movies_names():
    movie_names = {}
    # CHANGE THIS TO THE PATH TO YOUR u.ITEM FILE:
    with codecs.open(f"{ROOT_DIR}/data/ml-100k/u.item", "r", encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movie_names[int(fields[0])] = fields[1]
    return movie_names


# Create a SparkSession
spark = SparkSession.builder.appName("Popular Movies").getOrCreate()
names_dict = spark.sparkContext.broadcast(load_movies_names())

movies_df = spark.read.option("sep", "\t").schema(schema).csv(f"file://{ROOT_DIR}/data/ml-100k/u.data")

movies_counts = movies_df.groupBy("movie_id").count()

movies_counts.show()


# Create a user-defined function to look up movie names from our broadcast dictionary
def lookup_name(movie_id):
    return names_dict.value[movie_id]


lookup_name_udf = func.udf(lookup_name)
mapped_movies_count = movies_counts.withColumn("movie_title", lookup_name_udf(func.col("movie_id")))

sorted_count = mapped_movies_count.orderBy(func.desc("count"))
sorted_count.show()

spark.stop()
