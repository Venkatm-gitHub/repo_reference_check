from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("CSV to Hive Partitioned Table") \
    .enableHiveSupport() \
    .getOrCreate()

# Read CSV file from HDFS
csv_path = "hdfs:///path/to/your/csv/file.csv"
df = spark.read.csv(csv_path, header=True, inferSchema=True)

# Define the target Hive table name and database
target_table = "your_database.your_target_table"

# Get the schema of the target Hive table
target_schema = spark.table(target_table).schema

# Add missing columns with null values
for field in target_schema.fields:
    if field.name not in df.columns:
        df = df.withColumn(field.name, lit(None).cast(field.dataType))

# Reorder columns to match the target table schema
df = df.select([col(c) for c in target_schema.names])

# Assuming the last two columns are partition columns
partition_cols = target_schema.names[-2:]

# Write the data to the Hive table
df.write \
    .partitionBy(partition_cols) \
    .mode("append") \
    .format("hive") \
    .saveAsTable(target_table)

print(f"Data successfully loaded into {target_table}")
