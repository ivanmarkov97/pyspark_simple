from pyspark import SparkConf
from pyspark import SparkContext


def clean_token(token):
    token = token.replace('"', '')
    token = token.replace('\n', '')
    return token


def get_cities(row):
    items = row.split()
    return items[1], items[2]


if __name__ == '__main__':

    conf = SparkConf().setMaster('local[*]').setAppName('pysparkTransformations')
    sc = SparkContext(conf=conf)

    airports = sc.textFile('in/airports.text')
    airportsInUSA = airports.filter(lambda v: v.split(',')[3].lower() == '"united states"')

    airportsInUSAClean = airportsInUSA.map(lambda v: ' '.join([clean_token(token) for token in v.split(',')]))
    airportNamesAndCities = airportsInUSAClean.map(lambda v: get_cities(v))
    airportNamesAndCities = airportNamesAndCities.repartition(1)
    airportNamesAndCities.saveAsTextFile('out/usa_airport_names_and_cities.txt')
