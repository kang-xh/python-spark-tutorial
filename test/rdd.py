import sys, datetime

sys.path.insert(0,'.')
from commons.Utils import Utils
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    sparkConfig = SparkConf().setAppName('airports').setMaster('local[*]')
    sparkContext = SparkContext(conf = sparkConfig)

    airports_RDD = sparkContext.textFile('out/airport_exec_2019-12-25-07-54.text/part-*')

    count = airports_RDD.count()

    print('output part-000* contains {} lines'.format(count))
