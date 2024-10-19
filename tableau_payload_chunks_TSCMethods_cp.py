import os
import uuid
import tableauserverclient as TSC
import requests

try:
    ENV = "SIT2"
    PUBLISH_CONFIGURATION = {
        'DEV': {'site': 'DEV', 'project': 'Data Sources'}
    }[ENV]
    TABLEAU_SERVER = "https://XX.tableau.XX.net"
    CHUNK_SIZE_MB = 64  # Adjust based on your needs
    server = TSC.Server(TABLEAU_SERVER, use_server_version=False)
    server.version = '3.13'
    server.add_http_options({'verify': False})
    site = PUBLISH_CONFIGURATION['site']
    tableau_auth = TSC.TableauAuth("user_name", "welcome", site_id=site)

    # Create an upload session
    def initiate_file_upload():
        response = server.fileuploads.initiate()
        return response

    # Append to file upload in chunks
    def upload_chunk(file_path, chunk_size_mb, upload_session_id):
        file_size = os.path.getsize(file_path)
        num_chunks = (file_size // (chunk_size_mb * 1024 * 1024)) + 1

        with open(file_path, 'rb') as f:
            for chunk_index in range(num_chunks):
                chunk_data = f.read(chunk_size_mb * 1024 * 1024)
                server.fileuploads.append(upload_session_id, chunk_data)

    # Update data source with the uploaded file
    def update_data_source(datasource_id, upload_session_id, request_id, insert_actions):
        server.datasources.update_hyper_data(datasource_id, request_id=request_id, action=insert_actions, payload=upload_session_id)

    with server.auth.sign_in(tableau_auth):
        request_id = str(uuid.uuid4())
        insert_actions = [{"action": "replace", "source-table": "cftp user acct dle", "target-table": "cftp user acct dle"}]
        datasources, _ = server.datasources.get()
        datasource_id = next((ds.id for ds in datasources if ds.name == merged_hyper_name), None)

        if datasource_id is None:
            raise ValueError(f"Datasource '{merged_hyper_name}' not found.")

        if insert_actions:
            # Initiate the file upload session
            upload_session_id = initiate_file_upload()
            # Upload the file in chunks
            upload_chunk(merged_hyper, CHUNK_SIZE_MB, upload_session_id)
            # Update the data source with the uploaded file
            update_data_source(datasource_id, upload_session_id, request_id, insert_actions)

            print(f"Insert - Update job posted (ID: {request_id})")
            job = server.jobs.wait_for_job(request_id)
            print("Insert Job finished successfully")

except Exception as e:
    print(f'ERROR: {e}')
    raise e
