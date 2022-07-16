from pyspark import SparkContext
from pyspark import SparkConf

sparkConf = SparkConf().setAppName("CountWord")

sc = SparkContext(conf = sparkConf)

text = sc.textFile("hdfs://master:9000/test/Youvegottofindwhatyoulove.txt")

wordcount = text.map(lambda x: x.replace(',', ' ').replace('.', ' ') \
            .replace('"', ' ').replace('“', ' ').replace('”', ' ').lower()) \
            .flatMap(lambda x: x.split()) \
            .map(lambda x: (x, 1)) \
            .reduceByKey(lambda x,y: x+y) \
            .sortBy(lambda x: x[1], ascending=False)
                
# print(wordcount.take(30))

wordcount.coalesce(1).saveAsTextFile("hdfs://master:9000/user/q1_output/")