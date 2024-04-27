from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local[1]').appName('Reading and Writing RDDs').getOrCreate()
sc = spark.sparkContext

###############################################################################################################
# Example of reading data - TextFile and SequenceFile
print(80*'#')
print('Reading TextFile and Sequence File')

# For RDDs there arent a huge amount of flexibility around saving and reading RDDs
# we have two options for the type of files to read & write, which are text files and sequence files

ord_text = sc.textFile('data_files/orders.txt')

# Wont work on my local machine
#ord_seq = sc.sequenceFile('data_files/orders.txt')

###############################################################################################################
# Example of writing data - TextFile and SequenceFile
print(80*'#')
print('Writing TextFile and Sequence File')

julyOrd = ord_text.filter(lambda x: x.split(',')[1].split('-')[1] == '07').map(lambda x: x.split(',')[2])
augOrd = ord_text.filter(lambda x: x.split(',')[1].split('-')[1] == '08').map(lambda x: x.split(',')[2])

july_and_aug = julyOrd.union(augOrd).distinct()


# When saving down an rdd, depending on the number of partitions currently
# being used, thats how the data will be partitioned when saving to HDFS / disk.
# so with 5 partitions here its partitioned in 5 parts kinda like a parquet file, eg part-0000, part-0001 files in folder
# Can also specificy a compression type at this step, if we want to compress the data


# This wont work on my local machine
#july_and_aug.repartition(5).saveAsTextFile('path')


# A sequence file is a type of binary file format specifically used with (k,v) format rdds
# Its a flat binary type that has really efficient serialisation and deserialisation when reading and writing and
# is really useful for mapReduce operations and distributed computing approaches

# So we need to convert this to a (k,v) format before trying to save
july_and_aug_kv = july_and_aug.map(lambda x: (x.split(',')[0], x))

# This wont work on my local machine
# use coalesce cos you want less splits for a sequence file to make it more efficient
# in general we want as little partitions on a persisted sequence as we can
july_and_aug_kv.coalesce(1).saveAsSequenceFile('path')

# Something we can do if we have no viable key is to save None as our key for our rdd
null_key_rdd = july_and_aug.map(lambda x: (None, x))

# can now save this down as sequence file
null_key_rdd.coalesce(1).saveAsSequenceFile('path')
