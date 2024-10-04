from cassandra import cluster as scylla_cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.metadata import Murmur3Token
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import threading
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

# Source cluster connection details
source_contact_points = os.getenv('SOURCE_CONTACT_POINTS').split(',')
source_port = int(os.getenv('SOURCE_PORT'))
source_username = os.getenv('SOURCE_USERNAME')
source_password = os.getenv('SOURCE_PASSWORD')
source_auth_provider = PlainTextAuthProvider(username=source_username, password=source_password)

# Target cluster connection details
target_contact_points = os.getenv('TARGET_CONTACT_POINTS').split(',')
target_port = int(os.getenv('TARGET_PORT'))
target_username = os.getenv('TARGET_USERNAME')
target_password = os.getenv('TARGET_PASSWORD')
target_auth_provider = PlainTextAuthProvider(username=target_username, password=target_password)

# Migration settings
batch_size = int(os.getenv('BATCH_SIZE'))
parallel_threads = int(os.getenv('PARALLEL_THREADS'))
keyspace = os.getenv('KEYSPACE')
table_name = os.getenv('TABLE_NAME')

# Connect to the source cluster
source_cluster = scylla_cluster.Cluster(
    contact_points=source_contact_points,
    port=source_port,
    auth_provider=source_auth_provider
)
source_session = source_cluster.connect()
source_cluster.refresh_schema_metadata()

# Connect to the target cluster
target_cluster = scylla_cluster.Cluster(
    contact_points=target_contact_points,
    port=target_port,
    auth_provider=target_auth_provider
)
target_session = target_cluster.connect()

# Refresh the schema metadata
source_cluster.refresh_schema_metadata()

# Get the keyspace metadata
keyspace_metadata = source_cluster.metadata.keyspaces.get(keyspace)
if not keyspace_metadata:
    raise ValueError(f"Keyspace '{keyspace}' does not exist in the source cluster.")

# Get the table metadata
table_metadata = keyspace_metadata.tables.get(table_name)
if not table_metadata:
    raise ValueError(f"Table '{table_name}' does not exist in the keyspace '{keyspace}' in the source cluster.")

# Recreate the keyspace in the target cluster if it doesn't exist
target_keyspace_metadata = target_cluster.metadata.keyspaces.get(keyspace)
if not target_keyspace_metadata:
    print(f"Creating target keyspace {keyspace}")
    replication_settings = keyspace_metadata.replication_strategy.export_for_schema()
    create_keyspace_cql = f"CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH replication = {replication_settings}"
    target_session.execute(create_keyspace_cql)

# Set keyspace for target session
target_session.set_keyspace(keyspace)

# Refresh target cluster schema metadata after creating keyspace
target_cluster.refresh_schema_metadata()

# Recreate the table in the target cluster if it doesn't exist
target_table_metadata = target_cluster.metadata.keyspaces[keyspace].tables.get(table_name)
if not target_table_metadata:
    print(f"Creating target table {table_name}")
    create_table_cql = table_metadata.as_cql_query(formatted=False)
    target_session.execute(create_table_cql)

# Recreate indexes on the table in the target cluster
for index_metadata in table_metadata.indexes.values():
    create_index_cql = index_metadata.as_cql_query()
    target_session.execute(create_index_cql)

# Recreate materialized views in the target cluster
for mv_name, mv_metadata in keyspace_metadata.views.items():
    if mv_metadata.base_table.metadata.name == table_name:
        create_mv_cql = mv_metadata.as_cql_query(formatted=False)
        target_session.execute(create_mv_cql)

# Generate column names and placeholders
column_names = list(table_metadata.columns.keys())
column_placeholders = ', '.join(['?' for _ in column_names])
columns_str = ', '.join(column_names)

# Prepare SELECT statement
select_cql = f"SELECT {columns_str} FROM {keyspace}.{table_name}"
select_statement = source_session.prepare(select_cql)

# Prepare INSERT statement
insert_cql = f"INSERT INTO {keyspace}.{table_name} ({columns_str}) VALUES ({column_placeholders})"
insert_statement = target_session.prepare(insert_cql)

# Fetch data from the source table with pagination
page_size = batch_size  # Adjust as needed
statement = select_statement.bind(())
statement.fetch_size = page_size

# Initialize tqdm progress bar with total rows
progress_bar = tqdm(desc="Data Migration", unit="rows")

# Lock for thread-safe updates to the progress bar
progress_lock = threading.Lock()

# Initialize the ResultSet
rows = source_session.execute(statement)

# Process rows in batches with concurrency
batch_size = batch_size   # Adjust as needed
max_workers = parallel_threads    # Number of concurrent threads (adjust based on your environment)

def insert_batch(batch):
    try:
        execute_concurrent_with_args(target_session, insert_statement, batch)
        # Update the progress bar
        with progress_lock:
            progress_bar.update(len(batch))
    except Exception as e:
        print(f"Error inserting batch: {e}")

batch = []
with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
    for row in rows:
        if row is not None:
            batch.append(tuple(row))
        if len(batch) >= batch_size:
            # Submit the batch to the executor
            executor.submit(insert_batch, batch.copy())
            batch.clear()
    # Insert any remaining rows
    if batch:
        executor.submit(insert_batch, batch)

# Wait for all futures to complete
executor.shutdown(wait=True)

# Close the progress bar
progress_bar.close()

# Close connections
source_session.shutdown()
source_cluster.shutdown()
target_session.shutdown()
target_cluster.shutdown()