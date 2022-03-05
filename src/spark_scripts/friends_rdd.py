from pyspark import SparkConf, SparkContext
from src.utils.utils import get_project_root

ROOT_DIR = get_project_root()

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf=conf)


def parse_line(line):
    fields = line.split(',')
    age = int(fields[2])
    num_friends = int(fields[3])
    return age, num_friends


lines = sc.textFile(f"file://{ROOT_DIR}/data/fakefriends.csv")
rdd = lines.map(parse_line)
totals_by_age = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averages_by_age = totals_by_age.mapValues(lambda x: x[0] / x[1])
results = averages_by_age.collect()
for result in results:
    print(result)
