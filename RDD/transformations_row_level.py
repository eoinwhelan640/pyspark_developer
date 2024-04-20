from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[1]').appName('Low level rdd transformations').getOrCreate()
sc = spark.sparkContext

###############################################################################################################
# Example of row level transformations - map
print(80*'#')
print('map')

# It doesnt like parquet file format with actions ofr whatever reason
ord = sc.textFile('data_files/orders.txt')

# prints row by row
for i in ord.take(5):
    print(i)


# Using map function
# What if we wanted to print first element of each row. map is applied row wise so we can do
ordMap = ord.map(lambda x: x.split(',')[0])
print(ordMap.take(5))

# Extend this to getting the first element (id) and third element(status)
ordMap = ord.map(lambda x: (x.split(',')[0], x.split(',')[3] ))
print(ordMap.take(5))

# Extend to building a custom id
ordMap = ord.map(lambda x: x.split(',')[0] +'#'+ x.split(',')[3] )
print(ordMap.take(5))



# Create a udf (user defined function) that we will apply using map
def lowercase(str):
    return str.lower()

ordMap = ord.map(lambda x: lowercase(x.split(',')[3]))
print(ordMap.take(5))



###############################################################################################################
# Example of row level transformations - flatmap
print(80*'#')
print('flat')

# It doesnt like parquet file format with actions ofr whatever reason
ord = sc.textFile('data_files/orders.txt')

print(ord.map(lambda x: x.split(',')).take(5))
# flatmap flattens the list, so now each element is in output instead of the whole row being an element
print(ord.flatMap(lambda x: x.split(',')).take(5))

# Build the statement we'll use to do a word count for all elements in the file
# So not so much a word count as a "count the elements in the final and the number of times they occur"
ordMap = ord.flatMap(lambda x: x.split(',')).map(lambda w: (w,1))
print(ordMap.take(5))


# Looking at what the lambda for reduce by key does
print('What is actually in the reduceByKey lambda ??')
# Looks like the x and y in the reduce by key lambda are the 1s each time, so you're summing 1 + whatever
# the current running sum is so far, the actual key is not involved here
# kinda of like how dictionary operations work in kdb, the value elements of the key value pairs
# get used. That operation happens per key, kinda like a sum "over" each time
ordMap = ord.flatMap(lambda x: x.split(',')).map(lambda w: (w,1)).reduceByKey(lambda x,y: (x,y))
print(ordMap.take(5))

# Lastly use reduceByKey to try group by key and sum the values
print('\n', 'last step')
ordMap = ord.flatMap(lambda x: x.split(',')).map(lambda w: (w,1)).reduceByKey(lambda x,y: x+y)
print(ordMap.take(5))




###############################################################################################################
# Example of row level transformations - filter
print(80*'#')
print('filter')


# filter returns elelments of the rdd where whatever function we apply returns true.
# It applies a function to create boolean conditionals


# If we want to return all orders which are closed or complete and were ordered in 2013
ordMap = ord.filter(lambda x: x.split(',')[3] in ['CLOSED', 'COMPLETE'] \
                              and (x.split(',')[1].split('-')[0] == '2013')) # the split('-') cos of timestamp format
# This will return the full rdd row where the condition was true, so think of it as checking the row,
# and the function returns true or false, and if we get true its an indicator for the whole row and we get
# that row back. SO think about all rows being collapsed into the binary true or false for returning.
print(ordMap.take(5))

# can see we've filtered it vs original ord
print(ord.count(), ordMap.count())


###############################################################################################################
# Example of row level transformations - mapValues
print(80*'#')
print('mapValues')


rdd = sc.parallelize(([("a",[1,2,3]),("b", [3,4,5]),("a", [1,2,3,4,5])]))

# see what happens when applying len()
print(rdd.mapValues(len).collect())

# apply a custom function
def f(x):
    return len(x)
print(rdd.mapValues(f).collect())

# seeing what sum does
# So important to stress here that it treats each key value pair as unique in the pair RDD.
# Even when we have repeated keys like "a" here, each a is treated individually, and the function is
# applied to its elements ob its own, it doesnt collapse common keys in any way
print(rdd.mapValues(sum).collect())