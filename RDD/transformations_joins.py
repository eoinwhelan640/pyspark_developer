from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[1]').appName('Join examples').getOrCreate()

sc = spark.sparkContext

ord = sc.textFile('data_files/orders.txt')
ordItems = sc.textFile('data_files/order_items.txt')

print('Ord')
for row in ord.take(3):
    print(row)

print('OrdItems')
for row in ordItems.take(3):
    print(row)

# they both use an id that we can use as key col
# using the first index as key col apply a transfrom to both of them to reduce the rdd to just key col and value col
# for rdds it needs the specific format of rdd as as key value pair (two cols) ie (K, V) and (K, W)
ord_map = ord.map(lambda x: (x.split(',')[0], x.split(',')[2]))
orditems_map = ordItems.map(lambda x: (x.split(',')[1], x.split(',')[4]))

print('types - ', type(ord_map), type(orditems_map))
print('values - ', ord_map, orditems_map)

# now join the two rdds
res = ord_map.join(orditems_map)
for row in res.take(5):
    print(row)


# What happens if you try join the two original dfs ? - doesnt work cos they have to match exactly at the rdd level, ie
# they need to be in exact format of (K, V) and (K, W) to get (K, (V, W)).


###############################################################################################################
# Example of cogroup

# cogroup will return similar to join but with the values of the key as an iterable
# its a pyspark type iterable - pyspark.resultiterable.ResultIterable

print(80*'#')
print('Looking at cogroup')
x = sc.parallelize([("a",1), ("b", 4)])
y = sc.parallelize([("a",2)])

xy = x.cogroup(y)
print(xy.first())

# if you want to actually see these, use the inbuilt python map function
# here ind will be your key index and val is the actual tuple of iterators
# applying map here will convert each iterator to a list
for ind, val in xy.take(2):
    print(ind, map(list, val))


###############################################################################################################
# Example of cartesian cross join
print(80*'#')
print('Looking at cartesian cross join')

x = sc.parallelize([1,3,5])

# literally just joins every element of one to the other, is a dumb join and reusues parts
# so from above rdd you get (1,3) joined but then also get (3,1) -> thats what i mean by dumb, the sets are not unique
# or identified by elements
cartesian_x = x.cartesian(x)

print(cartesian_x.collect())