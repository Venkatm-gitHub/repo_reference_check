from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
from math import ceil

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CSV to Hive Partitioned Table") \
    .enableHiveSupport() \
    .getOrCreate()

# Set dynamic partitioning properties
spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
spark.sql("SET hive.exec.dynamic.partition=true")

# Define input and output parameters
input_csv = "path/to/your/massive_file.csv"
hive_table = "your_database.your_table"
partition_column = "your_partition_column"

# Read the CSV file
df = spark.read.csv(input_csv, header=True, inferSchema=True)

# Get total number of records
total_records = df.count()
batch_size = ceil(total_records / 10)

# Define the mapping between CSV columns and Hive table columns
column_mapping = {
    "csv_col1": "hive_col1",
    "csv_col2": "hive_col2",
    # Add more mappings as needed
}

# Function to transform the DataFrame to match Hive schema
def transform_dataframe(df):
    # Start with columns from CSV
    result = df.select([col(csv_col).alias(hive_col) for csv_col, hive_col in column_mapping.items()])
    
    # Add missing columns with null values
    hive_schema = spark.table(hive_table).schema
    for field in hive_schema.fields:
        if field.name not in result.columns:
            result = result.withColumn(field.name, lit(None).cast(field.dataType))
    
    # Reorder columns to match Hive schema
    return result.select([col(field.name) for field in hive_schema])

# Process data in batches
for offset in range(0, total_records, batch_size):
    batch_df = df.limit(batch_size).offset(offset)
    
    # Transform the batch DataFrame
    transformed_df = transform_dataframe(batch_df)
    
    # Write the batch to Hive table
    transformed_df.write \
        .mode("append") \
        .partitionBy(partition_column) \
        .format("hive") \
        .saveAsTable(hive_table)

    print(f"Processed batch: {offset} to {min(offset + batch_size, total_records)}")

print("Data import completed successfully!")

# Stop Spark session
spark.stop()
