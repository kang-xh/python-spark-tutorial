import sys, datetime
sys.path.insert(0,'.')
from commons.Utils import Utils
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    sparkConfig = SparkConf().setAppName('airports').setMaster('local[*]')
    sparkContext = SparkContext(conf = sparkConfig)

    airports_RDD = sparkContext.textFile('out/airports_exec_2019-12-25-07-54.text')

    count = airports_RDD.count()

    print(count)