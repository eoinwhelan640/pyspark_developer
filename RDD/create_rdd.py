from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[1]').appName('Creating RDDs').getOrCreate()

# We need to use sparkContext for some of these, which is built in to sparkSession as
# a method / attribute

# Method one, loading from external/ local data sources via textFile
# rdd = spark.sparkContext.textFile('textfile.txt')
with open('data_files/textfile.txt') as readfile:
    other_list = readfile.read().splitlines()
rdd = spark.sparkContext.parallelize(other_list)

# method two - loading from a python list or some other python object and calling parallelize
# my_list = [1,2,3,4,5,6,7,8]
# rdd = spark.sparkContext.parallelize(my_list)

# requires an action to show it, try avoid using collect as its a big operation
print('take', rdd.take(3))

print('collect', rdd.collect())
print('numPartitions', rdd.getNumPartitions())
print('glom', rdd.glom())
# glom, shows us the number of records allocated to each partition
print('glom with map len', rdd.glom().map(len))
print('glom with map len collect',rdd.glom().map(len).collect())
# I have one partition only since i'm localhost, and all my objects go on that

# Method 3 - new rdd from an old rdd, either directly or by changing some elements eg by mapoing a function onto it
new_rdd = rdd
lambda_rdd = rdd.map(lambda x: x[0]*2)
print(id(rdd))
print(id(new_rdd))
print(id(lambda_rdd))


# Method 4 - a new rdd from a Dataframe
# DataFrames are just 1. an RDD and 2. A data schema. By combining an rdd with a valid schema, you can amke a df
df = spark.createDataFrame(
    data=[('eoin', 28), ('conor', 26)],
    schema= ['name', 'age'],
)
# Every dataframe has the .rdd method so just invoke that.
# We are essentially just removing the schema component from the DataFrame, so it goes back to just being an rdd
# the rows of the dataframe become elements of the rdd
rdd_from_df = df.rdd
print(type(rdd_from_df))
for row in rdd_from_df.take(2):
    print(row)

