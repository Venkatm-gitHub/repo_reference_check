'''
I have two dictionaries representing database schemas: one for the source and one for the target. Each dictionary contains schemas, which in turn contain tables and their respective columns with data types. I want to compare these two dictionaries to identify discrepancies between them.

Requirements:

1) Extract Schemas: Write a function to extract the schemas from both the source and target dictionaries.
2) Compare Tables: Create a function that:
Iterates through each schema in the source.
For each table in the source:
Initializes entries for missing_columns, different_data_types, and source_only_content.
Checks if the table exists in the target:
If it exists, compare each column:
If a column is missing in the target, store it along with its data type.
If a column exists but has a different data type, store this information.
If the table does not exist in the target at all, store its content under source_only_content.
3) Output Structure: The output should be a dictionary with three top-level keys:
missing_columns: A dictionary where each table name maps to a list of tuples containing missing column names and their data types.
different_data_types: A dictionary where each table name maps to another dictionary of columns that have differing data types, with their respective types from both source and target.
source_only_content: A dictionary where each table name maps to its content if it exists only in the source.

Example Input



curated_source_table_info = {
    '"HR"': [
        {'"HR.EMP"': {'"NAME"': 'TEXT', '"ID"': 'INT', '"ADDRESS"': 'VARCHAR'}},
        {'"HR.DEPT"': {'"NAME"': 'TEXT', '"ID"': 'INT'}}
    ]
}

curated_target_table_info = {
    '"HR"': [
        {'"HR.EMP"': {'"NAME"': 'TEXT', '"ID"': 'INT'}},
        {'"HR.DEPT"': {'"NAME"': 'TEXT', '"ID"': 'INT'}}
    ]
}

Expected Output
The output should be structured as follows:
python
{
    'missing_columns': {
        '"HR.EMP"': [('"ADDRESS"', 'VARCHAR')],
        '"HR.DEPT"': []
    },
    'different_data_types': {
        '"HR.EMP"': {},
        '"HR.DEPT"': {}
    },
    'source_only_content': {
        '"HR.EMP"': {},
        '"HR.DEPT"': {}
    }
}
'''
def extract_schemas(source, target):
    source_schemas = source.keys()
    target_schemas = target.keys()
    return list(source_schemas), list(target_schemas)

def compare_tables(source, target):
    results = {
        'missing_columns': {},
        'different_data_types': {},
        'source_only_content': {}
    }

    source_schemas = source.keys()
    
    for schema in source_schemas:
        source_tables = source[schema]
        target_tables = target.get(schema, [])
        target_table_names = {list(table.keys())[0]: table[list(table.keys())[0]] for table in target_tables}

        for table in source_tables:
            table_name = list(table.keys())[0]
            columns = table[table_name]
            
            # Initialize entries for each table
            results['missing_columns'][table_name] = []
            results['different_data_types'][table_name] = {}
            results['source_only_content'][table_name] = {}

            if table_name in target_table_names:
                target_columns = target_table_names[table_name]
                
                # Check for missing columns and different data types
                for column_name, data_type in columns.items():
                    if column_name not in target_columns:
                        # Store missing columns with their data types
                        results['missing_columns'][table_name].append((column_name, data_type))
                    elif target_columns[column_name] != data_type:
                        results['different_data_types'][table_name][column_name] = (data_type, target_columns[column_name])
            else:
                # Collect content of tables that are only in the source
                results['source_only_content'][table_name] = columns

    return results

# Sample data
curated_source_table_info = {
    '"HR"': [
        {'"HR.EMP"': {'"NAME"': 'TEXT', '"ID"': 'INT', '"ADDRESS"': 'VARCHAR'}},
        {'"HR.DEPT"': {'"NAME"': 'TEXT', '"ID"': 'INT'}}
    ]
}

curated_target_table_info = {
    '"HR"': [
        {'"HR.EMP"': {'"NAME"': 'TEXT', '"ID"': 'INT'}},
        {'"HR.DEPT"': {'"NAME"': 'TEXT', '"ID"': 'INT'}}
    ]
}

# Extract schemas
source_schemas, target_schemas = extract_schemas(curated_source_table_info, curated_target_table_info)

# Compare tables
results = compare_tables(curated_source_table_info, curated_target_table_info)

# Output results
print(results)