from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[1]').appName('tester_session').getOrCreate()

print('spark object is created')
print('Number of partitions', spark.conf.get('spark.sql.shuffle.partitions'))
print('env vars', spark.conf.get('spark.local.appMasterEnv.HDFS_PATH'))
#spark.stop()
