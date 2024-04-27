from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[1]').appName('Key Aggregations').getOrCreate()

sc = spark.sparkContext

###############################################################################################################
# Example of sample transformations - sample
print(80*'#')
print('TRANSFORMATION sample')


# Easy to take a sample, have options to do it with replacement or not, ie get same value multiple times or not,
# and fraction of the full sample size records we want. this arg is mandatory it has no default. Can also provide a seed

rdd = sc.parallelize(range(100), 4)

for i in rdd.take(5):
    print(i)

print(rdd.sample(withReplacement=True, fraction=0.1,
                 #seed=10
                 ).take(5))



###############################################################################################################
# Example of sample ACTION - takeSample

#### IMPORTANT #####
# this is an action not a transformation !!!
# Remember that transformations use lazy evaluation and  actions dont. want to call actions as little as possible
# transformations return an RDD but actions dont, so whatever is returned needs shifted into memory and forces evaluation
# so having actions in your execution DAG is inefficient


# it functions the exact same way but it takes a fixed number of elements to return

print(80*'#')
print('ACTION takeSample')

# Also notice how since its an action, we dont need to call take(), we're already getting objects returned to us in the
# output
print(rdd.takeSample(withReplacement=False, num=20, seed=2))
