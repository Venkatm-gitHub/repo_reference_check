import uuid
import os
from tableauhyperapi import Connection, Telemetry, CreateMode, TableDefinition, SqlType, Inserter, TableName, Telemetry, HyperProcess
import tableauserverclient as TSC
os.system('cls')

merged_hyper = "C:\\Users\\meiyy\\Documents\\My Tableau Repository\\Datasources\\My test\\source\\massive_File\\Individual_Incident_2022.hyper"
merged_hyper_name = merged_hyper.split('/')[-1].split('.')[0]

def split_data_into_chunks(data, chunk_size):
    """Split data into smaller chunks."""
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]

try:
    ENV = "SIT2"
    PUBLISH_CONFIGURATION = {'SIT2': {'site': 'DEV', 'project': 'Data Sources'}}[ENV]

    TABLEAU_SERVER = "https://XX.tableau.XX.net"
    server = TSC.Server(TABLEAU_SERVER, use_server_version=False)
    server.version = '3.13'
    server.add_http_options({'verify': False})
    site = PUBLISH_CONFIGURATION['site']
    tableau_auth = TSC.TableauAuth("user_name", "welcome", site_id=site)

    chunk_size = 1000  # Define your chunk size
     # Start Hyper Process with telemetry
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
            # Read data from the hyper file
            with Connection(endpoint=hyper.endpoint, database=merged_hyper) as connection: 
                # Get the table definition
                catalog = connection.catalog
                schemas = catalog.get_schema_names()
                print(f"{len(schemas)} schemas:{schemas}")
                tables = catalog.get_table_names(schema='Extract')
                print(f"{len(tables)} tables:{tables}")
                query_count = connection.execute_list_query("SELECT count(*) FROM \"Extract\".\"Extract\"")  # Adjust table name as necessary
                print(query_count)
                query_result = connection.execute_list_query("SELECT * FROM \"Extract\".\"Extract\"")  # Adjust table name as necessary
                print("Query * is executed")
                for chunk in split_data_into_chunks(query_result, chunk_size):
                    ##job = server.datasources.update_hyper_data(datasource_id, request_id=request_id, action=insert_actions, payload=chunk)    
                    print(f"iteration:"  + str(len(chunk)))
                    print(chunk)
                    #print(f"Insert - Update job posted (ID: {job.id})")
                    print("Insert Job finished successfully")


except Exception as e:
    print(f'ERROR: {e}')
    raise e




'''

import uuid
from tableauhyperapi import HyperProcess, Connection, Telemetry, CreateMode
import tableauserverclient as TSC

merged_hyper = "/tmp/test.hyper"
merged_hyper_name = merged_hyper.split('/')[-1].split('.')[0]

def split_data_into_chunks(data, chunk_size):
    """Split data into smaller chunks."""
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]

try:
    ENV = "SIT2"
    PUBLISH_CONFIGURATION = {
        'DEV': {'site': 'DEV', 'project': 'Data Sources'}
    }[ENV]

    TABLEAU_SERVER = "https://XX.tableau.XX.net"
    server = TSC.Server(TABLEAU_SERVER, use_server_version=False)
    server.version = '3.13'
    server.add_http_options({'verify': False})
    site = PUBLISH_CONFIGURATION['site']
    tableau_auth = TSC.TableauAuth("user_name", "welcome", site_id=site)

    with server.auth.sign_in(tableau_auth):
        request_id = str(uuid.uuid4())
        insert_actions = [{"action": "replace", "source-table": "Extract", "target-table": "Extract"}]
        datasources, = server.datasources.get()
        datasource_id = None  # Initialize datasource_id

        for ds in datasources:
            if ds.name == merged_hyper_name:
                datasource_id = ds.id
                break
        
        if datasource_id is None:
            raise ValueError(f"Datasource ({merged_hyper_name}) not found.")

        # Start Hyper Process with telemetry
        with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
            # Read data from the hyper file
            with Connection(endpoint=hyper.endpoint, database=merged_hyper, create_mode=CreateMode.NONE) as connection:
                query_result = connection.execute_list_query("SELECT * FROM \"Extract\"")  # Adjust table name as necessary

                # Split data into chunks and publish each chunk
                chunk_size = 1000  # Define your chunk size
                for chunk in split_data_into_chunks(query_result, chunk_size):
                    job = server.datasources.update_hyper_data(datasource_id, request_id=request_id, action=insert_actions, payload=chunk)
                    print(f"Insert - Update job posted (ID: {job.id})")
                    job = server.jobs.wait_for_job(job)
                    print("Insert Job finished successfully")

except Exception as e:
    print(f'ERROR: {e}')
    raise e

'''
