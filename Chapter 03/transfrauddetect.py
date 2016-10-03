# Submit to spark using
# spark-submit /Users/anurag/hdproject/eclipse/chapt3/transfrauddetect.py
# You need the full path of the python script

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.mllib.clustering import KMeans, KMeansModel
from pyspark.streaming import StreamingContext
from pyspark.mllib.linalg import Vectors

def detect(rdd):
    count = rdd.count()
    print "RDD -> ", count
    if count > 0:
        arrays = rdd.map(lambda line: [float(x) for x in line.split(" ")])
        print arrays.collect()
        indx = 0
        while indx < count:
            vec = Vectors.dense(arrays.collect()[indx])
            indx += 1
            clusternum = model.predict(vec)
            print "Cluster -> ",  clusternum, vec
    return

# Create a local StreamingContext with two working thread and batch interval of 1 second
conf = SparkConf().setAppName("Fraud Detector")
conf = conf.setMaster("local[2]")

sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 10)

# Create a DStream that will connect to hostname:port, like localhost:9999
lines = ssc.socketTextStream("localhost", 8999)
# Split each line into words

model = KMeansModel.load(sc, "kmeansmodel01")
print model.clusterCenters
print "************************** Loaded the model *********************"

words = lines.flatMap(lambda line: line.split(" "))

lines.foreachRDD(detect)
ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
