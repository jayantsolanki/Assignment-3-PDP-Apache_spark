#!/usr/bin/python
# ex05.py
from pyspark import SparkConf, SparkContext
import sys
def vertex(line):
	l = line.split(" ")
	v = [ int(x) for x in l ]
	return [(v[0], 1), (v[1], 1)]
if __name__ == "__main__":
	conf = SparkConf().setAppName("RDDcreate")
	sc = SparkContext(conf = conf)
	lines = sc.textFile(sys.argv[1])
	# V = lines.flatMap(vertex).reduceByKey(lambda a, b: a + b)
	V = lines.map(vertex)

	n = V.count()
	print (n)
	V.saveAsTextFile(sys.argv[2])
	sc.stop()