import uuid
import tableauserverclient as TSC
from pathlib import Path
from tableauhyperapi import HyperProcess, Connection, SqlType, TableDefinition, Inserter, CreateMode, TableName

CHUNK_SIZE = 100000  # Adjust this value based on your specific performance needs

def get_table_names(hyper_file_path):
    with HyperProcess() as hyper:
        with Connection(hyper.endpoint, hyper_file_path) as connection:
            return [table.table_name for table in connection.catalog.get_table_names()]

def chunk_hyper_table(file_path, table_name, chunk_size):
    with HyperProcess() as hyper:
        with Connection(hyper.endpoint, file_path) as connection:
            table_def = connection.catalog.get_table_definition(table_name)
            column_names = [col.name for col in table_def.columns]
            
            query = f"SELECT * FROM {table_name}"
            with connection.execute_query(query) as result:
                chunk = []
                for row in result:
                    chunk.append(row)
                    if len(chunk) == chunk_size:
                        yield chunk, table_def
                        chunk = []
                if chunk:
                    yield chunk, table_def

def create_temp_hyper_file(temp_path, table_def, chunk):
    with HyperProcess() as hyper:
        with Connection(hyper.endpoint, temp_path, CreateMode.CREATE_AND_REPLACE) as connection:
            connection.catalog.create_table(table_def)
            with Inserter(connection, table_def) as inserter:
                inserter.add_rows(chunk)
                inserter.execute()

def main():
    merged_hyper = Path("/var/app/ctp/sit2/tbfw/dlepoc/merged/output/tpmt dsnt herged perf test.hyper")
    merged_hyper_name = merged_hyper.stem

    ENV = "SIT2"
    PUBLISH_CONFIGURATION = {
        'DEV': {'site': 'DEV', 'project': 'Data Sources'}
    }[ENV]
    TABLEAU_SERVER = "https://XX.tableau.XX.net"

    try:
        server = TSC.Server(TABLEAU_SERVER, use_server_version=False)
        server.version = '3.13'
        server.add_http_options({'verify': False})
        site = PUBLISH_CONFIGURATION['site']
        tableau_auth = TSC.TableauAuth("user_name", "welcome", site_id=site)

        with server.auth.sign_in(tableau_auth):
            datasources, _ = server.datasources.get()
            datasource_id = next((ds.id for ds in datasources if ds.name == merged_hyper_name), None)

            if datasource_id is None:
                raise ValueError(f"Datasource {merged_hyper_name} not found.")

            table_names = get_table_names(merged_hyper)

            for table_name in table_names:
                print(f"Processing table: {table_name}")
                for chunk_index, (chunk, table_def) in enumerate(chunk_hyper_table(merged_hyper, table_name, CHUNK_SIZE)):
                    request_id = str(uuid.uuid4())
                    temp_hyper_path = Path(f"/tmp/chunk_{table_name}_{chunk_index}.hyper")

                    # Create a temporary Hyper file with the chunk data
                    create_temp_hyper_file(temp_hyper_path, table_def, chunk)

                    insert_actions = [{
                        "action": "insert" if chunk_index > 0 else "replace",
                        "source-table": table_name,
                        "target-table": table_name
                    }]

                    job = server.datasources.update_hyper_data(
                        datasource_id,
                        request_id=request_id,
                        actions=insert_actions,
                        payload=str(temp_hyper_path)
                    )
                    print(f"Table {table_name}, Chunk {chunk_index + 1} - Update job posted (ID: {job.id})")
                    job = server.jobs.wait_for_job(job)
                    print(f"Table {table_name}, Chunk {chunk_index + 1} - Job finished successfully")

                    # Clean up the temporary file
                    temp_hyper_path.unlink()

                print(f"Finished processing table: {table_name}")

        print("All tables and chunks processed successfully")

    except Exception as e:
        print(f'ERROR: {e}')
        raise e

if __name__ == "__main__":
    main()