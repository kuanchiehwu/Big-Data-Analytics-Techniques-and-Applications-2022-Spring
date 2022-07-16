import csv
from pyspark import SparkContext
from pyspark import SparkConf

sparkConf = SparkConf().setAppName("TaxiAvg")

sc = SparkContext(conf = sparkConf)

csvdata = sc.textFile("hdfs://master:9000/test/yellow_tripdata_2018-10.csv")

rawdata = csvdata.map(lambda x: x.split(','))

header = rawdata.first()
tdata = rawdata.filter(lambda x: x!=header).filter(lambda x: len(x)>1)

# x[3] for passemger count
# x[9] for payment type
# x[16] for totak amount
data = tdata.filter(lambda x: (int(x[3]) > 0) & (int(x[3]) < 5))

# Credit Card
creditData = data.filter(lambda x: x[9] == '1')

cntCredit = creditData.map(lambda x: int(x[9])).sum()
amountCredit = creditData.map(lambda x: float(x[16])).sum()

avgCredit = amountCredit / cntCredit

# Cash
cashData = data.filter(lambda x: x[9] == '2')

cntCash = cashData.map(lambda x: int(x[9])).sum()
amountCash = cashData.map(lambda x: float(x[16])).sum()

avgCash = amountCash / cntCash

result = [['Credit', avgCredit], ['Cash', avgCash]]
resultrdd = sc.parallelize(result)

resultrdd.coalesce(1).saveAsTextFile("hdfs://master:9000/user/q2_output/")