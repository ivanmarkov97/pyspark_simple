from pyspark import SparkConf
from pyspark import SparkContext


if __name__ == '__main__':
    conf = SparkConf().setMaster('local[*]').setAppName('Sum')
    sc = SparkContext(conf=conf)

    data = sc.textFile('in/prime_nums.text')
    numbers = data.flatMap(lambda v: v.split())
    sum_number = numbers.map(lambda v: int(v)).reduce(lambda x, y: x + y)
    print(f'Sum is: {sum_number}')
