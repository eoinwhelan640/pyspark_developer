from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[1]').appName('Low level rdd transformations').getOrCreate()
sc = spark.sparkContext


ord = sc.textFile('data_files/orders.txt')
print('original')
for i in ord.take(5):
    print(i)
print('\n')


# convert to pair rdd
ordPair = ord.map(lambda x: (int(x.split(',')[2]), x))
print('Pair rdd')
for i in ordPair.take(5):
    print(i)
print('\n')


# it must be pair RDD for sortByKey
sorted_rdd = ordPair.sortByKey(ascending=True)
print('Sorted ')
for i in sorted_rdd.take(5):
    print(i)
print('\n')

# Will also work the same if we have more keys
# ie in this case our key is the number + ord status
ordPair = ord.map(lambda x: ((int(x.split(',')[2]), x.split(',')[3]), x))
# works the exact same way as long as its something it can sort, string gets sorted alphabetically
sorted_rdd = ordPair.sortByKey(ascending=True)
print('two keys rdd')
for i in ordPair.take(5):
    print(i)

print('Sorted and two keys')
for i in sorted_rdd.take(5):
    print(i)