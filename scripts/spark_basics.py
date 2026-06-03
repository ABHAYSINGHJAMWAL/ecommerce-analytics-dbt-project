from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import broadcast

# =====================================================
# CREATE SPARK SESSION
# =====================================================

spark = SparkSession.builder \
    .appName("DataEngineeringFundamentals") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("=== SPARK SESSION CREATED ===")
print(f"Spark version: {spark.version}")

# =====================================================
# READ ORDERS DATA
# =====================================================

orders = spark.read.csv(
    "seeds/raw/orders_raw.csv",
    header=True,
    inferSchema=True
)

print("\n=== SCHEMA ===")
orders.printSchema()

print("\n=== FIRST 5 ROWS ===")
orders.show(5)

print(f"\nTotal rows: {orders.count()}")
print(f"Partitions: {orders.rdd.getNumPartitions()}")

# =====================================================
# CLEAN ORDERS
# =====================================================

cleaned = (
    orders
    .filter(F.col("amount") > 0)
    .withColumn("amount", F.col("amount").cast("double"))
    .withColumn("currency", F.upper(F.trim(F.col("currency"))))
    .dropDuplicates(["order_id"])
)

print("\n=== CLEANED DATA ===")
cleaned.show()

# =====================================================
# AGGREGATIONS
# =====================================================

print("\n=== REVENUE BY CURRENCY ===")

cleaned.groupBy("currency") \
    .agg(
        F.sum("amount").alias("total_revenue"),
        F.count("order_id").alias("order_count"),
        F.avg("amount").alias("avg_order_value"),
        F.max("amount").alias("max_order_value")
    ) \
    .orderBy(F.desc("total_revenue")) \
    .show()

# =====================================================
# WINDOW FUNCTIONS
# =====================================================

user_window = Window.partitionBy("user_id").orderBy("created_at")

with_rank = (
    cleaned
    .withColumn(
        "order_rank",
        F.row_number().over(user_window)
    )
    .withColumn(
        "prev_amount",
        F.lag("amount", 1).over(user_window)
    )
    .withColumn(
        "running_total",
        F.sum("amount").over(
            user_window.rowsBetween(
                Window.unboundedPreceding,
                Window.currentRow
            )
        )
    )
)

print("\n=== WITH WINDOW FUNCTIONS ===")

with_rank.select(
    "user_id",
    "order_id",
    "amount",
    "order_rank",
    "prev_amount",
    "running_total"
).show()

# =====================================================
# READ USERS
# =====================================================

users = spark.read.csv(
    "seeds/raw/users_raw.csv",
    header=True,
    inferSchema=True
)

# Remove duplicate users
users = users.dropDuplicates(["user_id"])

# Rename duplicate column
users = users.withColumnRenamed(
    "_loaded_at",
    "user_loaded_at"
)

print("\n=== USERS DATA ===")
users.show()

# =====================================================
# NORMAL JOIN
# =====================================================

enriched = cleaned.join(
    users,
    on="user_id",
    how="left"
)

# =====================================================
# BROADCAST JOIN
# =====================================================

enriched_broadcast = cleaned.join(
    broadcast(users),
    on="user_id",
    how="left"
)

print("\n=== ENRICHED ORDERS ===")

enriched_broadcast.select(
    "order_id",
    "user_id",
    "amount",
    "currency",
    "country",
    "acquisition_channel"
).show()

# =====================================================
# WRITE PARQUET
# =====================================================

output_path = "output/orders_enriched"

enriched_broadcast.write \
    .mode("overwrite") \
    .partitionBy("currency") \
    .parquet(output_path)

print("\n=== WRITTEN TO PARQUET ===")
print(output_path)

# =====================================================
# VERIFY OUTPUT
# =====================================================

verify = spark.read.parquet(output_path)

print("\n=== VERIFY DATA ===")
print(f"Rows written and read back: {verify.count()}")

verify.show(truncate=False)

# =====================================================
# STOP SPARK
# =====================================================

spark.stop()

print("\n=== DONE ===")