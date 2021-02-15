from pyspark import SparkConf
from pyspark import SparkContext
from pyspark import StorageLevel


if __name__ == '__main__':
    conf = SparkConf().setMaster('local[*]').setAppName('Persist')
    sc = SparkContext(conf=conf)

    data = list(range(10))
    rdd = sc.parallelize(data)

    rdd.persist(StorageLevel.MEMORY_ONLY)  # same as .cache()

    rdd.reduce(lambda x, y: x * y)
    rdd.count()
