#!/usr/bin/python
# ex0.py
#spark-submit --num-executors=8 --executor-cores 4 ex0.py
from pyspark import SparkContext
import time
if __name__ == "__main__":
	sc = SparkContext(appName="Spark App")
	time.sleep(5)
	sc.stop()