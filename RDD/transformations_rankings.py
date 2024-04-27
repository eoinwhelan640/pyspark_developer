from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[1]').appName('rankings both global and within group').getOrCreate()
sc = spark.sparkContext


prod = sc.textFile('data_files/products.txt')

for i in prod.take(5):
    print(i)


# first convert to pairRDD
prodPair = prod.map(lambda x: (float(x.split(',')[4]),x))

for i in prodPair.take(5):
    print(i)




###############################################################################################################
# Example of GLOBAL RANKING
print(80*'#')
print('GLOBAL RANKING')

#  With ranking we dont actually have a built in ranking API. Instead we use the other utilities to get there
# Theres two types of ranking
# 1. Global ranking
# 2. Ranking per group - ie have some key and want to rank within that


# Starting with Global ranking

# Want to rank this and get top 5 highest priced products

# Use the SortBykey to get there. first you have to get rid of the one null record, which it was trying to convert
# into a string

# We have an issue with formatting of the txt file so we're getting a place where we have a null instead of something
# that can be a float, so get rid of that first
prodPair = prod.filter(lambda x: x.split(',')[4] != '').map(lambda x: (float(x.split(',')[4]),x))

print(prodPair.sortByKey())


###############################################################################################################
# Example of ranking transformation - takeOrdered
print(80*'#')
print('takeOrdered')

# take ordered lets us get some n elements of a list after it has been ordered in some way
# it ALWAYS gets it in ascending order

rdd = sc.parallelize([10,20,5,17,18,19,3,2,-10,5,4])

print(rdd.takeOrdered(2)) # get two elements

# neat trick to get it in descending order
print(rdd.takeOrdered(6, key=lambda x: -x))

# Do solve the above problem again we can do
print(prod.filter(lambda x: x.split(',')[4] != '').takeOrdered(5, lambda x: -float(x.split(',')[4])))


###############################################################################################################
# Example of RANKING PER GROUP
print(80*'#')
print('RANKING PER GROUP')


# Finding the most expensive products within a group
prod = prod.filter(lambda x: x.split(',')[4] != '')

# this just splitting it up into a smaller subset of categories of products and a few specific orders
prodF = prod.filter(lambda x: (int(x.split(',')[1]) in [2,3,4]) and (int(x.split(',')[0])) in [1,2,3,4,5,25,26,27,49,50])

# We can use groupBykey here to get (K, iterable(V)) based on the categories we group on
print(prodF.map(lambda x: (int((x.split(',')[1])),x) ).groupByKey().take(5))
groupByRDD = prodF.map(lambda x: (int((x.split(',')[1])),x) ).groupByKey()

# could sort using native python, use first to get at the first element, which is itself a tuple (K, iterable(V)) pair
first = groupByRDD.first()
print(sorted(first[1], key= lambda x: float(x.split(',')[4]), reverse=True))

# now that we have it in this format we can get at it by using flatmap
print(groupByRDD.flatMap(lambda x: sorted(x[1], key= lambda x: float(x.split(',')[4]), reverse=True)).take(5))

for row in groupByRDD.flatMap(lambda x: sorted(x[1], key= lambda x: float(x.split(',')[4]), reverse=True)).collect():
    # show the flattened rows
    print(row)















