import uuid
import tableauserverclient as TSC
from tableauhyperapi import HyperProcess, Connection, Telemetry, TableDefinition, Inserter

merged_hyper = "/tmp/test.hyper"
merged_hyper_name = merged_hyper.split('/')[-1].split('.')[0]

def publish_hyper_chunks(merged_hyper, chunk_size):
    try:
        ENV = "SIT2"
        PUBLISH_CONFIGURATION = {
            'DEV': {'site': 'DEV', 'project': 'Data Sources'}
        }[ENV]

        TABLEAU_SERVER = "https:/XX.tableau.XX.net"
        server = TSC.Server(TABLEAU_SERVER, use_server_version=False)
        server.version = '3.13'
        server.add_http_options({'verity': False})
        site = PUBLISH_CONFIGURATION['site']
        tableau_auth = TSC.TableauAuth("user_name", "welcome", site_id=site)

        with server.auth.sign_in(tableau_auth):
            request_id = str(uuid.uuid4())
            insert_actions = [{"action": "replace", "source-table": " user acct dle", "target-table": " user acct dle"}]

            # Get the existing datasource
            datasources, = server.datasources.get()
            datasource_id = None
            for ds in datasources:
                if ds.name == merged_hyper_name:
                    datasource_id = ds.id

            if datasource_id is None:
                raise ValueError(f"Datasource {merged_hyper_name} not found.")

            # Open the Hyper file and split into chunks
            with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
                with Connection(endpoint=hyper.endpoint, database=merged_hyper) as connection:
                    # Get the rows to split them into chunks
                    table_def = TableDefinition(' user acct dle')  # Assuming this is the table name
                    row_count = connection.execute_scalar_query(f"SELECT COUNT(*) FROM {table_def.table_name}")
                    
                    # Determine how many chunks
                    num_chunks = (row_count // chunk_size) + 1
                    
                    # Use the same temporary hyper file name as the existing Tableau datasource
                    temp_hyper_file = f"/tmp/{merged_hyper_name}.hyper"

                    for chunk_index in range(num_chunks):
                        offset = chunk_index * chunk_size
                        chunk_query = f"SELECT * FROM {table_def.table_name} LIMIT {chunk_size} OFFSET {offset}"
                        chunk_data = connection.execute_list_query(chunk_query)

                        # Create/overwrite the temporary hyper file for each chunk
                        with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as chunk_hyper:
                            with Connection(endpoint=chunk_hyper.endpoint, database=temp_hyper_file) as chunk_connection:
                                chunk_connection.catalog.create_table(table_def)
                                with Inserter(chunk_connection, table_def) as inserter:
                                    inserter.add_rows(chunk_data)
                                    inserter.execute()

                        # Update the chunk to Tableau using the same datasource name
                        if insert_actions:
                            job = server.datasources.update_hyper_data(
                                datasource_id=datasource_id, 
                                request_id=request_id, 
                                actions=insert_actions, 
                                payload=temp_hyper_file
                            )
                            print(f"Insert - Update job posted for chunk {chunk_index} (ID: {job.id})")
                            job = server.jobs.wait_for_job(job)
                            print(f"Chunk {chunk_index} update finished successfully")

    except Exception as e:
        print(f'ERROR: {e}')
        raise e

# Call the function to publish chunks
chunk_size = 5000  # Adjust this value based on performance considerations
publish_hyper_chunks(merged_hyper, chunk_size)
