from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[1]').appName('Aggregations Transformations Actions').getOrCreate()
#spark = SparkSession.builder.master('local[1]').appName('Low level rdd transformations').getOrCreate()

sc = spark.sparkContext

ord = sc.textFile('data_files/orders.txt')
ordItems = sc.textFile('data_files/order_items.txt')



# Count the number of orders which are closed in our ord items
print(ordItems.filter(lambda x: int(x.split(',')[1]) < 11).take(3))

# Apply reduce to sum across these numbers in 4th row of whats left after the filter
# the reduce function basically works like "over" in kdb
print(ordItems.filter(lambda x: int(x.split(',')[1]) < 11).map(lambda x: int(x.split(',')[3]))\
    .reduce(lambda x,y: x+y))

# handy note - we have a built in library "operator" which has a bunch of these simplified, broadcast style functions
# so dont have to write lambdas
from operator import add
print(ordItems.filter(lambda x: int(x.split(',')[1]) < 11).map(lambda x: int(x.split(',')[3])) \
      .reduce(add))

# important to know how reduce can be flexible and is basically taking a sequence and taking any two elements at
# a time as input iterating over the sequence, exactly like kdbs over
# Syntax of this is a joke why make it so long as an example
print(ordItems.filter(lambda x: int(x.split(',')[1])==4)
      .reduce(lambda i,j: i if (float(i.split(',')[4])) > (float(j.split(',')[4])) else j))


