from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[1]').appName('shuffle and combiner').getOrCreate()
sc = spark.sparkContext

rdd = sc.parallelize([1,2,3])
rdd1= rdd.distinct()


# Calling the built in toDebugString method to show us the recursive dependencies and execution plan for an rdd
# so we can debug it, or find any shuffles happening
print(rdd1.toDebugString())

