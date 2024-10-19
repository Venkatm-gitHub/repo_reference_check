import uuid
import os

from tableauserverclient import Server, TableauAuth

# Replace with your Tableau Server details
TABLEAU_SERVER_URL = "https://your-tableau-server.com"
USER_NAME = "your_username"
PASSWORD = "your_password"
SITE_NAME = "your_site_name"

# Path to the massive Tableau Hyper file
MERGED_HYPER_PATH = "/tmp/fact_table.hyper"

def upload_and_update_hyper_file(tableau_server_url, user_name, password, site_name, hyper_file_path):
    """
    Uploads a massive Tableau Hyper file to Tableau Server using the payload insert action.

    Args:
        tableau_server_url (str): The URL of your Tableau Server.
        user_name (str): Your Tableau Server username.
        password (str): Your Tableau Server password.
        site_name (str): The name of the site on Tableau Server.
        hyper_file_path (str): The path to the massive Hyper file on your local machine.

    Raises:
        Exception: If any errors occur during the upload or update process.
    """

    server = Server(tableau_server_url, use_server_version=False)
    server.version = '3.13'
    server.add_http_options({'verify': False})  # Disable SSL verification (optional)

    tableau_auth = TableauAuth(user_name, password, site_id=site_name)

    try:
        with server.auth.sign_in(tableau_auth):
            # Get file size and create a unique request ID
            file_size = os.path.getsize(hyper_file_path)
            request_id = str(uuid.uuid4())

            # Initiate file upload
            upload_session = server.files.upload_initiate(file_size=file_size)
            upload_id = upload_session.upload_session_id

            # Open the Hyper file in binary mode
            with open(hyper_file_path, 'rb') as hyper_file:
                # Read the file in chunks (adjust chunk_size as needed for performance)
                chunk_size = 1024 * 1024  # 1 MB chunks
                while True:
                    chunk = hyper_file.read(chunk_size)
                    if not chunk:
                        break

                    # Append chunk to the upload session
                    server.files.upload_append(upload_id, chunk)

            # Find the data source by name (modify if needed)
            datasources, _ = server.datasources.get()
            datasource_name = os.path.splitext(os.path.basename(hyper_file_path))[0]
            datasource_id = None
            for ds in datasources:
                if ds.name == datasource_name:
                    datasource_id = ds.id
                    break

            if datasource_id is None:
                raise ValueError(f"Datasource '{datasource_name}' not found on Tableau Server.")

            # Update the data source with the uploaded Hyper file using payload insert
            insert_actions = [{"action": "replace", "source-table": "user acct dle", "target-table": "user acct dle"}]
            job = server.datasources.update_hyper_data(
                datasource_id, request_id=request_id, actions=insert_actions, payload=upload_id)

            print(f"Insert - Update job posted (ID: {job.id})")
            server.jobs.wait_for_job(job)
            print("Insert Job finished successfully")

    except Exception as e:
        print(f"Error: {e}")
        raise

# Call the function with your credentials and file path
upload_and_update_hyper_file(
    TABLEAU_SERVER_URL, USER_NAME, PASSWORD, SITE_NAME, MERGED_HYPER_PATH)