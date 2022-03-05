from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from src.utils.utils import get_project_root

ROOT_DIR = get_project_root()

schema = StructType([
    StructField("station_id", StringType(), True),
    StructField("date", IntegerType(), True),
    StructField("measure_type", StringType(), True),
    StructField("temperature", FloatType(), True)
])

# Create a SparkSession
spark = SparkSession.builder.appName("Temperatures").getOrCreate()

dataframe = spark.read.schema(schema).csv(f"file://{ROOT_DIR}/data/temperatures.csv")
dataframe.filter(dataframe.measure_type == "TMIN")
temps = dataframe.select("station_id", "temperature")
min_temps = temps.groupBy("station_id").min("temperature")

min_temps.show()

min_temps_fahrenheit = min_temps.withColumn("temperature", func.round(func.col("min(temperature)") * 0.1 * 1.8 + 32, 2))

min_temps_fahrenheit.show()

results = min_temps.collect()

for result in results:
    print(result)

spark.stop()
