



from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local').appName('Tester').getOrCreate()

print("Spark object created")



rdd = spark.sparkContext.parallelize([1,2,3])
print(rdd.first())