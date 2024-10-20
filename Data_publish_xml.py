import os
import uuid
import tableauserverclient as TSC
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import requests

# Suppress only the single warning from urllib3 needed.
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

VERSION = "3.13"
FILESIZE_LIMIT = 1024 * 1024 * 64  # 64MB

def sign_in(server, username, password, site):
    tableau_auth = TSC.TableauAuth(username, password, site_id=site)
    with server.auth.sign_in(tableau_auth):
        return server

def get_project_id(server, project_name):
    all_projects, _ = server.projects.get()
    for project in all_projects:
        if project.name == project_name:
            return project.id
    raise LookupError(f"Project named '{project_name}' was not found on server")

def publish_datasource(server, project_id, datasource_path):
    file_path = os.path.abspath(datasource_path)
    file_name = os.path.basename(file_path)
    file_extension = os.path.splitext(file_name)[1][1:]

    # Publish the datasource
    datasource = TSC.DatasourceItem(project_id)
    datasource.name = os.path.splitext(file_name)[0]

    if os.path.getsize(file_path) >= FILESIZE_LIMIT:
        print(f"Publishing '{file_name}' in chunks (file over 64MB)")
        with open(file_path, 'rb') as file:
            publish_job = server.datasources.publish(datasource, file, file_extension, chunked=True)
    else:
        print(f"Publishing '{file_name}' using the all-in-one method")
        with open(file_path, 'rb') as file:
            publish_job = server.datasources.publish(datasource, file, file_extension)
    
    return publish_job

def main():
    ENV = "SIT2"
    PUBLISH_CONFIGURATION = {
        'DEV': {'site': 'DEV', 'project': 'Data Sources'},
        'SIT2': {'site': 'kevin', 'project': 'default'}  # Added SIT2 configuration
    }[ENV]

    TABLEAU_SERVER = "https://la-tab01.dev.wkelms.com"
    server = TSC.Server(TABLEAU_SERVER, use_server_version=False)
    server.version = VERSION
    server.add_http_options({'verify': False})
    
    username = "admin"
    password = "p@ssw0rd!"
    site = PUBLISH_CONFIGURATION['site']
    project_name = PUBLISH_CONFIGURATION['project']

    merged_hyper = "/tmp/fact_table.hyper"
    merged_hyper_name = os.path.splitext(os.path.basename(merged_hyper))[0]

    try:
        # Sign in
        print(f"\n1. Signing in as {username}")
        server = sign_in(server, username, password, site)

        # Get project ID
        print(f"\n2. Finding the '{project_name}' project to publish to")
        project_id = get_project_id(server, project_name)

        # Publish datasource
        print(f"\n3. Publishing '{merged_hyper_name}'")
        publish_job = publish_datasource(server, project_id, merged_hyper)
        print(f"Publish job completed with status: {publish_job.status}")

        # Update hyper data if needed
        datasources, _ = server.datasources.get()
        datasource_id = next((ds.id for ds in datasources if ds.name == merged_hyper_name), None)

        if datasource_id:
            request_id = str(uuid.uuid4())
            insert_actions = [{"action": "replace", "source-table": "user_acct_dle", "target-table": "user_acct_dle"}]
            
            if insert_actions:
                job = server.datasources.update_hyper_data(datasource_id, request_id=request_id, actions=insert_actions, payload=merged_hyper)
                print(f"Insert - Update job posted (ID: {job.id})")
                job = server.jobs.wait_for_job(job)
                print("Insert Job finished successfully")
        else:
            print(f"Warning: Datasource '{merged_hyper_name}' not found for update.")

        # Sign out
        print("\n4. Signing out and invalidating the authentication token")
        server.auth.sign_out()

    except Exception as e:
        print(f'ERROR: {e}')
        raise e

if __name__ == '__main__':
    main()
