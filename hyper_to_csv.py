import pandas as pd
from tableauhyperapi import HyperProcess, Connection, TableName, Telemetry
from pathlib import Path

import pandas as pd
from tableauhyperapi import HyperProcess, Connection, SqlType, TableName, Telemetry

def hyper_to_csv(hyper_file_path, csv_file_path):
    # Start Hyper process
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        # Connect to the Hyper file
        with Connection(endpoint=hyper.endpoint, database=hyper_file_path) as connection:
            # Get the schema names in the Hyper file
            schema_names = connection.catalog.get_schema_names()

            if not schema_names:
                print("No schemas found in the Hyper file.")
                return

            # Assuming we want to use the first schema
            schema_name = schema_names[0]

            # Get the table names in the schema
            table_names = connection.catalog.get_table_names(schema=schema_name)

            if not table_names:
                print(f"No tables found in the schema '{schema_name}'.")
                return

            # Assuming we want to extract data from the first table
            table_name = table_names[0]

            # Execute a query to select all data from the table
            result = connection.execute_query(f"SELECT * FROM {table_name}")

            # Get column names
            column_names = [column.name for column in result.schema.columns]

            # Create a list to store the rows
            rows = list(result)

            # Convert the result to a pandas DataFrame
            df = pd.DataFrame(rows, columns=column_names)

            # Save the DataFrame to a CSV file
            df.to_csv(csv_file_path, index=False)

            print(f"Data extracted from {hyper_file_path} and saved to {csv_file_path}")


# Example usage
hyper_file_path = Path("D:\\workcct.hyper")
csv_file_path = Path("D:\\acct.csv")
hyper_to_csv(hyper_file_path, csv_file_path)
