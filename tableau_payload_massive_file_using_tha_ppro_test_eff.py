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
            # Read data from the hyper file in batches
            batch_size = 1000  # Define your batch size
            offset = 0
            
            while True:
                with Connection(endpoint=hyper.endpoint, database=merged_hyper, create_mode=CreateMode.NONE) as connection:
                    query = f"SELECT * FROM \"EXTRACT\" LIMIT {batch_size} OFFSET {offset}"
                    query_result = connection.execute_list_query(query)  # Adjust table name as necessary

                    if not query_result:  # Break if no more records are returned
                        break

                    # Split data into chunks and publish each chunk
                    for chunk in split_data_into_chunks(query_result, batch_size):
                        job = server.datasources.update_hyper_data(datasource_id, request_id=request_id, action=insert_actions, payload=chunk)
                        print(f"Insert - Update job posted (ID: {job.id})")
                        job = server.jobs.wait_for_job(job)
                        print("Insert Job finished successfully")

                    offset += batch_size  # Move to the next batch

except Exception as e:
    print(f'ERROR: {e}')
    raise e