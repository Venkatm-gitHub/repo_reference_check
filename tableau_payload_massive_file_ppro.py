import os
import uuid
import tableauserverclient as TSC

# Configuration
ENV = "SIT2"
PUBLISH_CONFIGURATION = {
    'DEV': {'site': 'DEV', 'project': 'CitiFTP Data Sources'}
}[ENV]

TABLEAU_SERVER = "https:/XX.tableau.XX.net"
server = TSC.Server(TABLEAU_SERVER, use_server_version=False)
server.version = '3.13'
server.add_http_options({'verify': False})
site = PUBLISH_CONFIGURATION['site']
tableau_auth = TSC.TableauAuth("user_name", "welcome", site_id=site)

# File paths
merged_hyper = "/tmp/test.hyper"
merged_hyper_name = os.path.basename(merged_hyper).split('.')[0]

# Function to split the Hyper file into chunks
def split_hyper_file(file_path, chunk_size_mb=100):
    chunks = []
    with open(file_path, 'rb') as f:
        while True:
            chunk = f.read(chunk_size_mb * 1024 * 1024)  # Read in MB chunks
            if not chunk:
                break
            chunk_file_path = f"{file_path}.chunk{len(chunks)}.hyper"
            with open(chunk_file_path, 'wb') as chunk_file:
                chunk_file.write(chunk)
            chunks.append(chunk_file_path)
    return chunks

try:
    with server.auth.sign_in(tableau_auth):
        request_id = str(uuid.uuid4())
        insert_actions = [{"action": "insert", "source-table": "cftp user acct dle", "target-table": "cftp user acct dle"}]
        
        # Find the datasource ID
        datasources, _ = server.datasources.get()
        datasource_id = next((ds.id for ds in datasources if ds.name == merged_hyper_name), None)
        
        if datasource_id is None:
            raise ValueError(f"Datasource ({merged_hyper_name}) not found.")
        
        # Split the Hyper file into chunks
        hyper_chunks = split_hyper_file(merged_hyper)

        for chunk in hyper_chunks:
            # Initiate file upload session
            upload_session = server.datasources.initiate_file_upload(datasource_id)
            
            # Append each chunk to the upload session
            with open(chunk, 'rb') as f:
                while True:
                    data = f.read(5 * 1024 * 1024)  # Read in 5 MB increments
                    if not data:
                        break
                    server.datasources.append_to_file_upload(upload_session.upload_id, data)
            
            # Update the data source using the uploaded session
            job = server.datasources.update_hyper_data(datasource_id, request_id=request_id, action=insert_actions, upload_session_id=upload_session.upload_id)
            print(f"Insert - Update job posted (ID: {job.id})")
            job = server.jobs.wait_for_job(job)
            print("Insert Job finished successfully")
            
            # Clean up the chunk file after upload
            os.remove(chunk)

except Exception as e:
    print(f'ERROR: {e}')
    raise e