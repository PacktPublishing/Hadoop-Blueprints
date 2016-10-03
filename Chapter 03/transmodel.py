# Submit to spark using
# spark-submit /Users/anurag/hdproject/eclipse/chapt3/transmodel.py
# You need the full path of the python script

from pyspark.mllib.clustering import KMeans, KMeansModel
from pyspark import SparkContext
from pyspark import SparkConf

def paymentCode(code):
    if code == "BA":
        return 1
    elif code == "IC":
        return 2
    elif code == "GM":
        return 3
    elif code == "DV":
        return 4
    elif code == "OV":
        return 5
    elif code == "GT":
        return 6

# Create a local StreamingContext with two working thread and batch interval of 1 second
conf = SparkConf().setAppName("model-kmeans")
# conf = conf.setMaster("local[2]")

sc = SparkContext(conf=conf)
f1 = sc.textFile("hdfs://192.168.2.103:9000/hbp/chapt3/INGB01.csv")
f2 = f1.map(lambda line : [ x for x in line.split(",")])
f3 = f2.map(lambda line : [ line[0], paymentCode(line[1]),line[3]])

#Create two clusters
clusters = KMeans.train(f3, 2)
print clusters.clusterCenters
clusters.save(sc, "kmeansmodel01")
