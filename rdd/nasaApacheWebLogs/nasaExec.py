# exec job while watching Spark Tutorial. 

import sys, datetime
sys.path.insert(0,'.')
from commons.Utils import Utils
from pyspark import SparkContext, SparkConf

def remove_header(line: str):
    return not (line.startswith("host") and ("bytes" in line))

def map_func(line: str):
    splits = line.split("\t")
    return '{}'.format(splits[0])

if __name__ == "__main__":

    sparkConfig = SparkConf().setAppName('NASA').setMaster('local[*]')
    sparkContext = SparkContext(conf = sparkConfig)

    julylog_RDD = sparkContext.textFile('in/nasa_19950701.tsv')
    auglog_RDD = sparkContext.textFile('in/nasa_19950801.tsv')
    
    totallog_RDD = julylog_RDD.union(auglog_RDD)
    print('total lines: {}'.format(totallog_RDD.count()))
    
    distinct_host_RDD = totallog_RDD.filter(remove_header).map(map_func).distinct().sortBy(lambda x: x)
    print('distinct lines: {}'.format(distinct_host_RDD.count()))

    distinct_host_RDD.saveAsTextFile('out/nasa_exec_' + f"{datetime.datetime.now():%Y-%m-%d-%H-%M}"+'.text')
