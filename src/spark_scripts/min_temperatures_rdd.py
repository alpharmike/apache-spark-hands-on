from pyspark import SparkConf, SparkContext
from src.utils.utils import get_project_root

ROOT_DIR = get_project_root()

conf = SparkConf().setMaster("local").setAppName("Temperatures")
sc = SparkContext(conf=conf)


def parse_line(line):
    fields = line.split(',')
    station_id = fields[0]
    measure_type = fields[2]
    temperature = int(fields[3])
    return station_id, measure_type, temperature


lines = sc.textFile(f"file://{ROOT_DIR}/data/temperatures.csv")
rdd = lines.map(parse_line)
filtered_stations = rdd.filter(lambda x: str(x[1]).strip() == "TMIN").map(lambda x: (x[0], x[2]))
min_temps = filtered_stations.reduceByKey(lambda x, y: min(x, y))

results = min_temps.collect()
for result in results:
    print(result)
