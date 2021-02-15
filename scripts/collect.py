from pyspark import SparkConf
from pyspark import SparkContext


if __name__ == '__main__':
    conf = SparkConf().setMaster('local[*]').setAppName('Collect APP')
    sc = SparkContext(conf=conf)

    data = ['spark', 'hadoop', 'pyspark', 'hive', 'pig', 'cassandra', 'clickhouse']
    rdd = sc.parallelize(data)

    words = rdd.collect()

    for word in words:
        print(word)
