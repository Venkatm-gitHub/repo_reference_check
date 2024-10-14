import requests
import json

# Define Tableau Server credentials and site ID
server_url = "https://<your_tableau_server_url>"
site_id = "<your_site_id>"
username = "<your_username>"
password = "<your_password>"

# Replace with paths to your merged hyper file and source hyper file
merged_hyper_path = "/path/to/merged.hyper"
source_hyper_path = "/path/to/source.hyper"

# Function to upload a Hyper file
def upload_hyper_file(file_path):
    url = f"{server_url}/api/<api_version>/sites/{site_id}/workspaces/default/datasources/upload"
    headers = {
        "Authorization": f"Basic {base64.b64encode(f'{username}:{password}'.encode()).decode()}",
        "Accept": "application/json",
        "Content-Type": "multipart/form-data",
    }
    files = {"file": open(file_path, "rb")}
    response = requests.post(url, headers=headers, files=files)
    response.raise_for_status()
    return response.json()["uploadSessionId"]

# Function to perform multi-table append with selective deletion
def update_data_source(upload_session_id):
    url = f"{server_url}/api/<api_version>/sites/{site_id}/datasources/<data_source_id>/data"
    headers = {
        "Authorization": f"Basic {base64.b64encode(f'{username}:{password}'.encode()).decode()}",
        "Content-Type": "application/json",
    }

    # Define actions for dimension tables (full refresh)
    dimension_actions = []
    for table in ["dimension_table1", "dimension_table2", "dimension_table3", "dimension_table4"]:
        dimension_actions.append(
            {
                "action": "replace",
                "target-table": table,
                "target-schema": "public",
                "source-table": table,
                "source-schema": "public",
                "sourceUploadSessionId": upload_session_id,
            }
        )

    # Define action for fact table (append with deletion)
    fact_action = {
        "action": "upsert",
        "target-table": "fact_table",
        "target-schema": "public",
        "source-table": "fact_table",
        "source-schema": "public",
        "sourceUploadSessionId": upload_session_id,
        "conditions": [{"condition": f"Date < DATEADD(d, -60, GETDATE())"}],  # Delete rows older than 60 days
    }

    # Combine actions for all tables
    actions = dimension_actions + [fact_action]

    # Send PATCH request with JSON payload
    payload = json.dumps({"actions": actions})
    response = requests.patch(url, headers=headers, data=payload)
    response.raise_for_status()
    print("Data source updated successfully!")

# Upload merged hyper file
upload_session_id = upload_hyper_file(merged_hyper_path)

# Update data source with multi-table append and deletion
update_data_source(upload_session_id)
