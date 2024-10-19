import os
import uuid
import tableauserverclient as TSC
from tableauserverclient.server.endpoint.fileuploads_endpoint import Fileuploads

# Set your merged hyper file path
merged_hyper = f"/tmp/fact_table.hyper"
merged_hyper_name = os.path.basename(merged_hyper).split('.')[0]

try:
    ENV = "SIT2"
    PUBLISH_CONFIGURATION = {
        'DEV': {'site': 'DEV', 'project': 'Data Sources'}
    }[ENV]

    TABLEAU_SERVER = "https://XX.tableau.XX.net"
    server = TSC.Server(TABLEAU_SERVER, use_server_version=False)
    server.version = '3.13'
    server.add_http_options({'verify': False})  # If SSL verification isn't needed
    site = PUBLISH_CONFIGURATION['site']
    
    tableau_auth = TSC.TableauAuth("user_name", "welcome", site_id=site)
    with server.auth.sign_in(tableau_auth):
        # Step 1: Initiate file upload
        file_upload_endpoint = Fileuploads(server)
        upload_session_id = file_upload_endpoint.initiate()
        print(f"Upload session initiated with ID: {upload_session_id}")
        
        # Step 2: Append to the file upload in chunks
        chunk_size = 1024 * 1024 * 5  # 5 MB chunk size
        with open(merged_hyper, 'rb') as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                file_upload_endpoint.append(upload_session_id, chunk)
        
        print(f"File upload completed for session: {upload_session_id}")
        
        # Step 3: Use the returned upload-id to update the data source
        request_id = str(uuid.uuid4())
        insert_actions = [{"action": "replace", "source-table": "user acct dle", "target-table": "user acct dle"}]
        
        # Fetch the existing data sources
        datasources, _ = server.datasources.get()
        datasource_id = None  # Initialize datasource_id
        
        for ds in datasources:
            if ds.name == merged_hyper_name:
                datasource_id = ds.id
                break
        
        if datasource_id is None:
            raise ValueError(f"Datasource '{merged_hyper_name}' not found.")
        
        if insert_actions:
            job = server.datasources.update_hyper_data(datasource_id, request_id=request_id, actions=insert_actions, payload=upload_session_id)
            print(f"Insert - Update job posted (ID: {job.id})")
            
            # Wait for the job to complete
            job = server.jobs.wait_for_job(job)
            print("Insert job finished successfully")
            
except Exception as e:
    print(f"ERROR: {e}")
    raise
