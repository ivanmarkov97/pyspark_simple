from pyspark import SparkConf
from pyspark import SparkContext


if __name__ == '__main__':
    conf = SparkConf().setMaster('local[*]').setAppName('Union App')
    sc = SparkContext(conf=conf)
    july_logs = sc.textFile('in/nasa_19950701.tsv')
    august_logs = sc.textFile('in/nasa_19950801.tsv')

    union_logs = july_logs.union(august_logs)
    union_logs = union_logs.filter(lambda v: not v.startswith('host'))
    union_logs = union_logs.map(lambda v: v.split('\t'))

    sample = union_logs.sample(withReplacement=False, fraction=0.1)
    sample = sample.repartition(1)
    sample.saveAsTextFile('out/sample_nasa_logs.txt')
