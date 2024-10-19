import uuid
import os
import tableauserverclient as TSC

def upload_file_in_chunks(server, file_path, chunk_size=1024*1024):
    file_size = os.path.getsize(file_path)
    file_name = os.path.basename(file_path)
    
    # Step 1: Initiate File Upload
    upload_session_id = server.fileuploads.initiate()
    
    # Step 2: Append to File Upload
    with open(file_path, 'rb') as file:
        for chunk_start in range(0, file_size, chunk_size):
            chunk = file.read(chunk_size)
            server.fileuploads.append(upload_session_id, chunk)
    
    return upload_session_id

try:
    merged_hyper = "/tmp/fact_table.hyper"
    merged_hyper_name = os.path.splitext(os.path.basename(merged_hyper))[0]
    
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
        insert_actions = [{"action": "replace", "source-table": "user_acct_dle", "target-table": "user_acct_dle"}]
        
        datasources, _ = server.datasources.get()
        datasource_id = next((ds.id for ds in datasources if ds.name == merged_hyper_name), None)
        
        if datasource_id is None:
            raise ValueError(f"Datasource {merged_hyper_name} not found.")
        
        if insert_actions:
            # Upload the file in chunks
            upload_session_id = upload_file_in_chunks(server, merged_hyper)
            
            # Step 3: Update Data Source
            job = server.datasources.update_hyper_data(
                datasource_id,
                upload_session_id,
                request_id=request_id,
                actions=insert_actions
            )
            
            print(f"Insert - Update job posted (ID: {job.id})")
            job = server.jobs.wait_for_job(job)
            print("Insert Job finished successfully")

except Exception as e:
    print(f'ERROR: {e}')
    raise e