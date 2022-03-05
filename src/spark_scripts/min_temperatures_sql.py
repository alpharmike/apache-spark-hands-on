from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as func
from src.utils.utils import get_project_root

ROOT_DIR = get_project_root()


def mapper(line):
    fields = line.split(',')
    return Row(station_id=str(fields[0]), time=str(fields[1].encode("utf-8")), measure_type=str(fields[2]),
               temp=int(fields[3]))


# Create a SparkSession
spark = SparkSession.builder.appName("Temperatures").getOrCreate()

lines = spark.sparkContext.textFile(f"file://{ROOT_DIR}/data/temperatures.csv")
temps = lines.map(mapper)
schema_temps = spark.createDataFrame(temps)
schema_temps.createOrReplaceTempView("temps")


min_temps = spark.sql(
    "SELECT station_id, measure_type, MIN(temp) AS min_temp FROM temps WHERE measure_type='TMIN' GROUP BY station_id, measure_type ORDER BY min_temp")

results = min_temps.collect()

for result in results:
    print(result)

spark.stop()
