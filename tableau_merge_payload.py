from tableauserverclient import Server
from datetime import datetime, timedelta

# Replace with your Tableau Server details
server = Server('your_server_address')
user = 'your_username'
password = 'your_password'
site_id = 'your_site_id'

# Define the path to your merged hyper file
merged_hyper_path = 'path/to/merged.hyper'

# Function to connect to Tableau Server
def connect_to_server():
  try:
    with server.auth(user, password):
      return server.get_site(site_id)
  except Exception as e:
    print(f"Error connecting to Tableau Server: {e}")
    exit(1)

# Function to update data in the Hyper file
def update_hyper_data(data_source_id, table_name, actions):
  with server.auth(user, password):
    # Get the data source
    data_source = server.get_datasource(data_source_id)
    
    # Update the data using update_hyper_data
    server.update_hyper_data(data_source.id, merged_hyper_path, actions)

# Connect to Tableau Server
tableau_site = connect_to_server()

# Get the data source for the Hyper file
data_source_id = 'your_data_source_id'  # Replace with the actual data source ID

# Define the actions to perform for each table
actions = [
    {
        "actionType": "delete",
        "filter": "Date < '2024-01-01'"  # Example filter to delete old data
    },
    {
        "actionType": "append",
        "tableName": "Table1"
    },
    {
        "actionType": "replace",
        "tableName": "Table2"
    },
    # ... other actions for remaining tables ...
]

# Update the data in the Hyper file
update_hyper_data(data_source_id, None, actions)

print("Data update completed successfully!")
