import uuid
import os
import tableauserverclient as TSC

# Define the path to your Hyper file
merged_hyper = "/tmp/fact_table.hyper"
merged_hyper_name = os.path.basename(merged_hyper).split('.')[0]

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
        # Step 1: Initiate File Upload
        upload_id = server.fileuploads.initiate()
        print(f"Upload session initiated (ID: {upload_id})")

        # Step 2: Append the file in chunks
        with open(merged_hyper, "rb") as file:
            while True:
                chunk = file.read(TSC.config.CHUNK_SIZE_MB * TSC.config.BYTES_PER_MB)
                if not chunk:
                    break
                request, content_type = TSC.RequestFactory.Fileupload.chunk_req(chunk)
                server.fileuploads.append(upload_id, request, content_type)
                print(f"Uploaded a chunk to session (ID: {upload_id})")

        print(f"File upload finished (ID: {upload_id})")

        # Step 3: Update Data Source using the uploaded file
        request_id = str(uuid.uuid4())
        insert_actions = [{"action": "replace", "source-table": merged_hyper_name, "target-table": merged_hyper_name}]
        
        datasources, _ = server.datasources.get()
        datasource_id = None
        
        for ds in datasources:
            if ds.name == merged_hyper_name:
                datasource_id = ds.id
        
        if datasource_id is None:
            raise ValueError(f"Datasource ({merged_hyper_name}) not found.")
        
        job = server.datasources.update_hyper_data(datasource_id, request_id=request_id, action=insert_actions, payload=upload_id)
        print(f"Insert - Update job posted (ID: {job.id})")
        
        job = server.jobs.wait_for_job(job)
        print("Insert Job finished successfully")

except Exception as e:
    print(f'ERROR: {e}')