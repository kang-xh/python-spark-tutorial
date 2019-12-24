import sys, datetime
sys.path.insert(0,'.')
from commons.Utils import Utils
from pyspark import SparkContext, SparkConf

num = 10

if __name__ == "__main__":

    sparkConfig = SparkConf().setAppName('Reduce').setMaster('local[*]')   
    sparkContext = SparkContext(conf = sparkConfig)

    prime_num_rdd = sparkContext.textFile('in/prime_nums.text')
    sample_num_rdd = prime_num_rdd.flatMap(lambda line: line.split('\t'))

    sample_collection = sample_num_rdd.take(10)
    for sample in sample_collection:
        print(sample)

    int_num_rdd = sample_num_rdd.map(lambda line: int(line))

    sum_num = int_num_rdd.reduce(lambda x , y: x + y)

    print('the sum is {}'.format(sum_num))
