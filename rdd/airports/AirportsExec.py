# exec job while watching Spark Tutorial. 

import sys, datetime
sys.path.insert(0,'.')
from commons.Utils import Utils
from pyspark import SparkContext, SparkConf

def splitComma(line: str):
    result = Utils.COMMA_DELIMITER.split(line)
    print(result)

    return "{},{}".format(result[1],result[6])

if __name__ == "__main__":

    sparkConfig = SparkConf().setAppName('airports').setMaster('local[*]')
    sparkContext = SparkContext(conf = sparkConfig)

    airports_RDD = sparkContext.textFile('in/airports.text')
    airportsUSA_RDD = airports_RDD.filter(lambda line: float(Utils.COMMA_DELIMITER.split(line)[6]) > 40)

    airportsNames_RDD = airportsUSA_RDD.map(splitComma)
    airportsNames_RDD.saveAsTextFile('out/airport_exec_' + f"{datetime.datetime.now():%Y-%m-%d-%H-%M}"+".text")



