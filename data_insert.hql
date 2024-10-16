-- Set dynamic partition properties
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=10000;
SET hive.exec.max.dynamic.partitions.pernode=1000;

-- Create an external table for the CSV file
CREATE EXTERNAL TABLE IF NOT EXISTS csv_source (
    column1 STRING,
    column2 INT,
    column3 DOUBLE,
    -- Add more columns as needed to match your CSV structure
    partition_column STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/path/to/csv/file/';

-- Create the target partitioned table
CREATE TABLE IF NOT EXISTS target_table (
    hive_column1 STRING,
    hive_column2 INT,
    hive_column3 DOUBLE,
    hive_column4 STRING,
    hive_column5 TIMESTAMP,
    -- Add more columns as needed to match your Hive table structure
    partition_column STRING
)
PARTITIONED BY (partition_column STRING)
STORED AS ORC;

-- Insert data from CSV into the partitioned Hive table
INSERT OVERWRITE TABLE target_table
PARTITION (partition_column)
SELECT
    csv_source.column1 AS hive_column1,
    csv_source.column2 AS hive_column2,
    csv_source.column3 AS hive_column3,
    NULL AS hive_column4,  -- Example of a column not present in CSV
    CURRENT_TIMESTAMP AS hive_column5,  -- Example of a generated column
    -- Map other columns as needed
    csv_source.partition_column
FROM csv_source;
