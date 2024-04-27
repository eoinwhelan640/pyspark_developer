from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[1]').appName('Key Aggregations').getOrCreate()

sc = spark.sparkContext

###############################################################################################################
# Example of partitioning data
print(80*'#')
print('Partition sample')

# Create some dummy data for testing
df = spark.range(10000)
df = df.select(df.id, df.id*2, df.id*3)
df = df.union(df)
df = df.union(df)
df = df.union(df)
df = df.union(df)
df = df.union(df)




## Convert this DatFrame to an rdd
RDD = df.rdd.map(lambda x: str(x[0]) + ',' + str(x[1]) + ',' + str(x[2]))

# try saving it? might not work
# save the file at a HDFS path - I dont think i have this ? Cannot save it to a hadoop file path
# Coalesce merges all partitions into 1
# RDD.coalesce(1).saveAsTextFile('data_files/test_data')



###############################################################################################################
# Example of partitioning data - repartition
print(80*'#')
print('repartition sample')



ord = sc.textFile('data_files/orders.txt')
# print the number of partitions we're using by default

# On my local machine its basically always gonna be 1.
print(ord.getNumPartitions())

# glom converts the elements from each partition into a list
# so would ordinarily be able to see which elements are together
# in a partition.
# in my case I have one partition so list will be length 1, if i had 3 partitions the list would be length 3
print(ord.glom().map(len).collect())

# if wanted to could repartition then, changing it to 5 partitions
# Remember, repartition() retruns a new rdd, so we save the change in teh varaible
ord = ord.repartition(5)


##############################################################################################################
# Example of partitioning data - repartition and sort
print(80*'#')
print('repartition and sort example')

# For the repartition and sort function we repartition and then sort the values by key in each resulting partition
# So we require a key value pair to do this
rdd = sc.parallelize(((-9, ('a', 'z')), (-3, ('x', 'f')), (-6, ('j','b')), (4, ('a','b')), (8, ('s', 'b')), (1, ('a', 'b'))))

# always gonna be 1 for me here
print(rdd.getNumPartitions())


# calling repartition and sort function
# The partitionFunc arg is a function you apply which requires some kind of finite category output
# ideally low cardinality, that lets you assign elements to a partition. it takes the key as an input
# eg if you were repartitioning to 2 partitions, you could have a statement that returns a boolean. That way some
# one partition gets everything that returned 0, the other partition gets the 1s
# another example is what we're using here, ie have 3 partitions. Divide the key by modulo 3, meaning
# categories are 0,1,2 and relevant (k,v) pairs get assigned to a partition based on their key % 3
# partitionFunc creates the partition index

# this wont work on my local machine however
# rdd.repartitionAndSortWithinPartitions(
#     numPartitions=3,
#     partitionFunc= lambda x: x % 3,
#     ascending=True,
#     #keyfunc=None,
# )


# see if this works when I try to repartition to just one partition
new_rdd = rdd.repartitionAndSortWithinPartitions(
    numPartitions=1,
    partitionFunc= lambda x: True,
    ascending=True,
)

# We can use keyfunc to sort the keys in some way we want with a custom function,
# eg sort by absolute value. keyfunc is applied to the keys only and doesnt change them in any way
# even if the lambda itself does stuff to the keys
new_rdd_abs = rdd.repartitionAndSortWithinPartitions(
    numPartitions=1,
    partitionFunc= lambda x: True,
    ascending=True,
    keyfunc=lambda x: abs(x),
)

print(new_rdd.take(5))
print(new_rdd_abs.take(5))



##############################################################################################################
# Example of partitioning data - coalesce
print(80*'#')
print('coalesce example')


# Coalesce is another way to repartition, but is mainly used to reduce partitions used
# If we have applied some filter or something to reduce our data size and we now
# have 500 partitions and 400 arent used anymore, we could call coalesce
# to repartition to 100 partitions only and free up the others
# Coalesce doesnt do a shuffle so is a bit more optimised than repartition, it does have an optional arg to shuffle
# if you use the shuffle arg, coalesce and repartition are basically the same call.

# IMPORATNT - coalesce is reusing the old partitions, collapsing ino them, not creating any wholly new partitions.
# repartition creates fully new partitions

# In practice, we only want to use coalesce when reducing partitions and save repartition for increasing or decreasing optionally
# Important to know, with no shuffle its faster but also means you could have unbalanced partitions in terms of elements
# Like going from 4 partitions to 2 partitions, but since no shuffle happens maybe 1 partition has 200 elements but the
# other partition has just 5 elements

ord = sc.textFile('data_files/orders.txt')

# it returns a new rdd when you call coalesce
#ord = ord.coalesce(2)















