from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CSV to Hive Partitioned Table") \
    .enableHiveSupport() \
    .getOrCreate()

# Define input and output parameters
csv_path = "path/to/your/input.csv"
hive_database = "your_database"
hive_table = "your_table"

# Partition columns (adjust these based on your table structure)
partition_cols = ["year", "month", "day"]

# Read the CSV file
df = spark.read.csv(csv_path, header=True, inferSchema=True)

# Write the data to the Hive table with partitioning
df.write \
    .partitionBy(partition_cols) \
    .mode("append") \
    .format("hive") \
    .saveAsTable(f"{hive_database}.{hive_table}")

print(f"Data loaded successfully into {hive_database}.{hive_table}")

# Optional: Verify the data
result = spark.sql(f"SELECT * FROM {hive_database}.{hive_table} LIMIT 5")
result.show()

# Stop the Spark session
spark.stop()
