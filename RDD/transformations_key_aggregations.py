from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[1]').appName('Key Aggregations').getOrCreate()

sc = spark.sparkContext


ordItems = sc.textFile('data_files/order_items.txt')

###############################################################################################################
# Example of key aggregations - groupByKey

# try nopt to use groupByKey, its the only operation that doesnt use
# a combiner so its justa  shuffle which is slow. The others can do most of the same
# functionality but use the combiner

print(80*'#')
print('groupByKey')
# convert to a key value pair, ie pairRDD
ordGrp = ordItems.map(lambda x: (int(x.split(',')[2]), float(x.split(',')[4])))

print(ordGrp)
print(ordGrp.groupByKey())
for i in ordGrp.groupByKey().take(5):
    print(i)

# We can apply a function to this via mapValues, which works on the pairRDDs
print(ordGrp.groupByKey().mapValues(sum).collect())

###############################################################################################################
# Example of key aggregations - reduceByKey
from operator import add
print(80*'#')
print('reduceByKey')

# This one uses a combiner and a shuffle so is a bit faster, but using a shuffle is still a slow operation

# This also works on a pairRDD and basically applies the designated function
# to the V of the (K,V) values in the RDD, applying it across them like "over" in kdb

# it uses a concept called Associative Reduction, so for a sum function
# the combiner sums on its own partition and at the end it sums across partitions
# so the whole process is Associative Reduction

rdd = sc.parallelize((("a",1), ("b",1), ("a",1)))
# Can see a items cllected together and summed
print(rdd.reduceByKey(add).collect())


# total revenue sold for each order
# first convert to key value pair
ordGrp = ordItems.map(lambda x: (int(x.split(',')[1]), float(x.split(',')[4])))

print(ordGrp.reduceByKey(add).take(5))
# equivalent
print(ordGrp.reduceByKey(lambda x,y: x+y).take(5))


###############################################################################################################
# Example of key aggregations - reduceByKey
print(80*'#')
print('aggregateByKey')

# unlike reduceByKey aggregateByKey doesnt need to preserve the input/output format
# being the same. It can change to a non pair RDD if want

# Has 3 mandatory args. Can kinda think of this function as a super tedious and manual way
# to do the shuffle and combiner ops, except its like generic and can have any output format
# so you can really control the steps at each level. eg instead of calling sum
# and having one behaviour through it all, we could do sum then max. ALthough why we would do this who knows.
# ZeroValue -
# SeqOp -
# ComboOp -

# Sample
ordItems = sc.parallelize([
    (2, "Joseph", 200), (2, "Jimmy", 250), (2, "Tina", 130), (4, "Jimmy", 50), (4, "Tina", 300),
    (4, "Joseph", 150), (4, "Ram", 200), (7, "Tina", 200), (7, "Joseph", 300), (7, "Jimmy", 800),
],
    2,
)

# find the maximum revenue for each order (order id is the first of each tuple)

# first convert this to a pair rdd
ordPair = ordItems.map(lambda x: (x[0], (x[1],x[2]) ))

print(ordItems)
print(ordPair)# now in a pairwise format

# initialise our accumulator
zero_value = 0

# initialse our sequence operator, think of this as where you are doing
# the combiner part of the operation, ie what to do when combining on the same partition
# Here your element is some type of list or sequence
def seq_op(accumulator, element):
    # element is going to be the "value" part of the key value pairs
    if (accumulator > element[1]):
        return accumulator
    else:
        return element

# Think of this part then as then combining the results of all partitions in one place
# kinda like the shuffle step.
# function is the same as the above but now instead of having a list of values ina  seqeunce
# youre making a comparison on derived values for each partition and wanna keep the max in this case
def combiner(accumulator, derived_accumulator):
    if (accumulator > derived_accumulator):
        return accumulator
    else:
        return derived_accumulator

# now actually call these functions to find the max
print(ordPair.aggregateByKey(zero_value, seq_op, combiner))


# this failing for some reason ?
# for i in ordPair.aggregateByKey(zero_value, seq_op, combiner).collect():
#     print(i)


# Do the same as above but now print the customer name
zero_value = ('',0)

# Changes a bit this time around that our input is a tuple
def seq_op(accumulator, element):
    if accumulator[1] > element[1]:
        return accumulator
    else:
        # remember its a tuple we wanna retrun now
        return element

def combiner(acc_1, acc_2):
    if acc_1[1] > acc_2[1]:
        return acc_1
    else:
        return acc_2


aggr_ordItems = ordPair.aggregateByKey(zero_value, seq_op, combiner)
print(aggr_ordItems)
for i in aggr_ordItems.collect():
    print(i)


# last example is to count the sum And the number of records
zero_value = (0,0) # use second value as count

def seq_op(accumulator, element):
    # each time we execute this function, the "count" part of our accumulator increments by 1
    print('seq_op executing')
    return accumulator[0] + element[1], accumulator[1] + 1

# Important - think about these executions as kinda happening over the keys
# one at a time, with the values being what youre applying your function to. So
# you operate doing a kdb "over" where the x arg starts as accumulator,then is one of the values from a key
# value pair, then the next arg is the next value from k:v pair. The function executes each time
# so that func gets executed each time.

def combiner(acc_1, acc_2):
    print('combiner executing')
    return acc_1[0] + acc_2[0], acc_1[1] + acc_2[1]

aggr_ordItems = ordItems.aggregateByKey(zero_value, seq_op, combiner)
print(aggr_ordItems)
# print(aggr_ordItems.collect())

###############################################################################################################
# Example of key aggregations - countByKey
print(80*'#')
print('countByKey')

# Lastly countByKey is pretty easy conceptually, is literally just a count
# of the keys in the rdd. It does require a (K,V) pair Rdd cos it
# will be counting keys, and returns (K, n) with n being the count
# countByKey doesnt require a shuffle so is pretty light to call


ord = sc.textFile('data_files/orders.txt')
ordPair = ord.map(lambda x: (x.split(',')[3],1))

for i in ordPair.take(5):
    print(i)

# very important to know, countByKey returns a dict object
# not an rdd anymore.
# and its not even specifically a dict object, its a collection library defaultdict
# but that has ost of the same methods as pythons regular dict type
countByStatus = ordPair.countByKey()

print(type(countByStatus))
print(countByStatus)

for i in countByStatus.items():
    print(i)