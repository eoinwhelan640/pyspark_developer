from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[1]').appName('Key Aggregations').getOrCreate()

sc = spark.sparkContext

###############################################################################################################
# Example of set transformations - union
print(80*'#')
print('Union')


# Union DOES NOT apply any kind of distinct, we need to do that ourself to get the unique elements
ord = sc.textFile('data_files/orders.txt')


# Question is to get the customers who did an order in July or August

# get the individual months from the dates in the order table
julyOrd = ord.filter(lambda x: x.split(',')[1].split('-')[1] == '07').map(lambda x: x.split(',')[2])
augOrd = ord.filter(lambda x: x.split(',')[1].split('-')[1] == '08').map(lambda x: x.split(',')[2])

#print(julyOrd.take(5))

# Can then compare the two rdds using union, has both july and aug orders
print(julyOrd.union(augOrd).count())
print(julyOrd.union(augOrd).take(5))

print(julyOrd.union(augOrd).distinct().count())
print(julyOrd.union(augOrd).distinct().take(5))


###############################################################################################################
# Example of set transformations - intersection
print(80*'#')
print('intersection')

julyOrd.intersection(augOrd)


###############################################################################################################
# Example of set transformations -distinct()
print(80*'#')
print('distinct')

 julyOrd.distinct()


###############################################################################################################
# Example of set transformations - subtract
print(80*'#')
print('subtract')

# subtract is easy, is just the difference between left and right elelments

julyOrd.subtract(augOrd)

