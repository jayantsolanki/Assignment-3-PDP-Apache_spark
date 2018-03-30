from pyspark import SparkConf, SparkContext
import sys
def inside(p):
    x, y = random.random(), random.random()
    return x*x + y*y < 1

count = SparkContext.parallelize(range(0, 20000)).filter(inside).count()
print ("Pi is roughly %f" % (4.0 * count / 20000))