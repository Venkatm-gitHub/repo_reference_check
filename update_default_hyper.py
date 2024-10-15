from tableauhyperapi import HyperProcess, Connection, Telemetry, CreateMode, TableName

# Specify your Hyper file path and the default value
HYPER_FILE = r'D:\work\citi\Tableau\community supported\test_file_prep\trg\defaulting\dim_geo.hyper'
DEFAULT_VALUE = '0'  # Change this to your desired default value

# Start the Hyper process
with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    # Connect to the existing Hyper file
    with Connection(endpoint=hyper.endpoint, database=HYPER_FILE, create_mode=CreateMode.NONE) as connection:
        # Update the column with null values
        sql_update = f"""
        UPDATE {TableName('public', 'dim_geo')} 
        SET mngd_geo_chld = '{DEFAULT_VALUE}' 
        WHERE mngd_geo_chld IS NULL;
        """
        
        # Execute the update command
        connection.execute_command(sql_update)

        print("Column updated successfully.")
