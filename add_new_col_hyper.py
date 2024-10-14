from tableauhyperapi import HyperProcess, Connection, Telemetry, CreateMode

# Define your Hyper file path and the new table name
hyper_file_path = r'<urfilepath>\dim_geo_test.hyper'
new_table_name = 'dim_geo_test'

# Start a Hyper process
with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:

    # Connect to the existing Hyper file
    with Connection(endpoint=hyper.endpoint, database=hyper_file_path, create_mode=CreateMode.NONE) as connection:
        
        # Define new columns to be added (example: adding a text column and an integer column with nullability)
        new_columns = [
            ('new_text_column6', 'text', True),   # True means nullable
            ('new_integer_column6', 'bigint', True)  # False means not nullable
        ]

        # Add new columns to the renamed table (initially as nullable)
        for column_name, data_type, is_nullable in new_columns:
            nullability = 'NULL' if is_nullable else 'NOT NULL'
            add_column_command = f'ALTER TABLE "{new_table_name}" ADD COLUMN "{column_name}" {data_type} {nullability};'
            connection.execute_command(add_column_command)
            print(f'Column "{column_name}" of type "{data_type}" and nullability "{nullability}" added to table "{new_table_name}".')

            # If the column is not nullable, set a default value for existing rows
            if not is_nullable:
                default_value = 0  # Set your desired default value here
                update_command = f'UPDATE "{new_table_name}" SET "{column_name}" = {default_value} WHERE "{column_name}" IS NULL;'
                connection.execute_command(update_command)
                print(f'Set default value of {default_value} for column "{column_name}" in existing rows.')

                # Alter the column to be non-nullable after updating existing NULLs
                alter_command = f'ALTER TABLE "{new_table_name}" ALTER COLUMN "{column_name}" SET NOT NULL;'
                connection.execute_command(alter_command)
                print(f'Column "{column_name}" is now set to NOT NULL.')
