import pandas as pd
import pantab as pt
from tableauhyperapi import (
    HyperProcess,
    Connection,
    Telemetry,
    CreateMode,
    TableDefinition,
    SqlType,
    Inserter,
    TableName
)
import os
os.system('cls')

# Function to replace null values in specified columns
def replace_nulls(df, columns, default_value):
    for column in columns:
        df[column].fillna(default_value, inplace=True)
    return df

# Read data from the source Hyper file
source_hyper_file = r'D:\work\c\Tableau\community supported\test_file_prep\src\alter table\dim_geo.hyper'
table_name = TableName('public', 'dim_geo')
df = pt.frame_from_hyper(source_hyper_file, table=table_name)

# Specify the columns to check for null values and the default value
columns_to_replace = ['mngd_geo_chld']  # Replace with your actual column names
default_value = 0  # Replace with your desired default value

# Replace nulls in the specified columns
df_modified = replace_nulls(df, columns_to_replace, default_value)

# Write the modified DataFrame to a new Hyper file
destination_hyper_file = r'D:\work\c\Tableau\community supported\test_file_prep\src\alter table\dim_geo_2.hyper'
pt.frame_to_hyper(df_modified, destination_hyper_file, table="modified_table")
