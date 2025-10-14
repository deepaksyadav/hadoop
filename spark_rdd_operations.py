from pyspark import SparkContext

# Initialize SparkContext
sc = SparkContext("local", "RDD_Operations_Example")

# Sample data
data = [1, 2, 3, 4, 5, 2, 3, 6, 7, 8, 9, 10]

# Create an RDD from a Python collection
rdd = sc.parallelize(data)

# -------------------------------
# 1. Transformations
# -------------------------------

# map: multiply each element by 2
rdd_map = rdd.map(lambda x: x * 2)

# flatMap: split each number into a list of digits
rdd_flatmap = rdd.flatMap(lambda x: [int(d) for d in str(x)])

# filter: keep only even numbers
rdd_filter = rdd.filter(lambda x: x % 2 == 0)

# distinct: remove duplicates
rdd_distinct = rdd.distinct()

# sample: random sample 50% of data without replacement
rdd_sample = rdd.sample(False, 0.5, seed=42)

# union: combine two RDDs
rdd_union = rdd.union(sc.parallelize([11, 12, 13]))

# intersection: common elements between two RDDs
rdd_intersection = rdd.intersection(sc.parallelize([2, 3, 10, 12]))

# subtract: elements in rdd but not in another RDD
rdd_subtract = rdd.subtract(sc.parallelize([1, 2, 3]))

# cartesian: Cartesian product
rdd_cartesian = rdd.cartesian(sc.parallelize([100, 200]))

# -------------------------------
# Pair RDD transformations
# -------------------------------

# Create a pair RDD
pair_rdd = sc.parallelize([("a", 1), ("b", 2), ("a", 3), ("b", 4), ("c", 5)])

# groupByKey
rdd_groupby = pair_rdd.groupByKey().mapValues(list)

# reduceByKey
rdd_reduceby = pair_rdd.reduceByKey(lambda x, y: x + y)

# sortByKey
rdd_sortbykey = pair_rdd.sortByKey()

# join
pair_rdd2 = sc.parallelize([("a", 100), ("b", 200), ("d", 300)])
rdd_join = pair_rdd.join(pair_rdd2)

# -------------------------------
# 2. Actions
# -------------------------------

print("Original RDD:", rdd.collect())
print("map:", rdd_map.collect())
print("flatMap:", rdd_flatmap.collect())
print("filter:", rdd_filter.collect())
print("distinct:", rdd_distinct.collect())
print("sample:", rdd_sample.collect())
print("union:", rdd_union.collect())
print("intersection:", rdd_intersection.collect())
print("subtract:", rdd_subtract.collect())
print("cartesian:", rdd_cartesian.collect())

print("groupByKey:", rdd_groupby.collect())
print("reduceByKey:", rdd_reduceby.collect())
print("sortByKey:", rdd_sortbykey.collect())
print("join:", rdd_join.collect())

print("count:", rdd.count())
print("first element:", rdd.first())
print("take(5):", rdd.take(5))
print("reduce(sum):", rdd.reduce(lambda x, y: x + y))
print("countByKey (pair RDD):", pair_rdd.countByKey())

# Save RDD to HDFS (example path)
# rdd.saveAsTextFile("hdfs:///user/maria_dev/spark/rdd_output")

# Stop SparkContext
sc.stop()
