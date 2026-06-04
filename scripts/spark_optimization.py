from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import multiprocessing
import time

spark = SparkSession.builder \
    .appName("SparkOptimization") \
    .master("local[*]") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

orders = spark.read.csv('seeds/raw/orders_raw.csv', header=True, inferSchema=True)
users = spark.read.csv('seeds/raw/users_raw.csv', header=True, inferSchema=True)

print("=== OPTIMIZATION DEMO ===\n")


print("1. FILTER EARLY")
filtered = orders.filter(F.col('amount') > 0).select('order_id', 'user_id', 'amount', 'status')
print(f"Rows after filter: {filtered.count()}")


print("\n2. CACHING")
enriched = filtered.join(
    users.select('user_id', 'acquisition_channel'),
    on='user_id',
    how='left'
)
enriched.cache()

start = time.time()
count = enriched.count()
print(f"First access (fills cache): {time.time()-start:.2f}s, rows: {count}")

start = time.time()
revenue = enriched.agg(F.sum('amount')).collect()[0][0]
print(f"Second access (from cache): {time.time()-start:.2f}s, revenue: {revenue}")

enriched.unpersist()

print("\n3. PARTITIONS")
cores = multiprocessing.cpu_count()
print(f"CPU cores available: {cores}")
print(f"Default partitions: {orders.rdd.getNumPartitions()}")
print(f"Recommended partitions: {cores * 2}")

print("\n4. DATA SKEW CHECK")
orders.groupBy('status').count().orderBy(F.desc('count')).show()

print("\n5. EXECUTION PLAN")
enriched_fresh = filtered.join(
    users.select('user_id', 'acquisition_channel'),
    on='user_id',
    how='left'
).groupBy('acquisition_channel').agg(F.sum('amount').alias('revenue'))
enriched_fresh.explain(mode='simple')

print("\n6. ADAPTIVE QUERY EXECUTION")
print("spark.sql.adaptive.enabled = true")
print("Spark automatically:")
print("- Adjusts partition count after shuffle")
print("- Converts sort-merge joins to broadcast joins")
print("- Handles data skew automatically")
print("Always enable this in production")

spark.stop()
print("\n=== DONE ===")