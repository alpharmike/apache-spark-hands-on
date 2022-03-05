from pyspark import SparkConf, SparkContext
from src.utils.utils import get_project_root

ROOT_DIR = get_project_root()

conf = SparkConf().setMaster("local").setAppName("Orders")
sc = SparkContext(conf=conf)


def parse_line(line):
    fields = line.split(',')
    user_id = fields[0]
    amount = float(fields[2])
    return user_id, amount


lines = sc.textFile(f"file://{ROOT_DIR}/data/customer-orders.csv")
rdd = lines.map(parse_line)
sorted_orders = rdd.reduceByKey(lambda x, y: x + y).map(lambda x: (x[1], x[0])).sortByKey()

results = sorted_orders.collect()
for result in results:
    print(result)
