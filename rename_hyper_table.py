from tableauhyperapi import HyperProcess, Connection, Telemetry, CreateMode, TableName

# Define your Hyper file path and the current and new table names
hyper_file_path = r'<urfile>\dim_geo_test.hyper'
current_table_name = 'dim_geo'
new_table_name = 'dim_geo_test'

# Start a Hyper process
with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:

    # Connect to the existing Hyper file
    with Connection(endpoint=hyper.endpoint, database=hyper_file_path, create_mode=CreateMode.NONE) as connection:
        
        # Execute the SQL command to rename the table
        rename_command = f'ALTER TABLE "{current_table_name}" RENAME TO "{new_table_name}";'
        connection.execute_command(rename_command)

        print(f'Table renamed from {current_table_name} to {new_table_name}')
