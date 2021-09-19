from google.cloud import bigquery
from google.cloud.exceptions import NotFound


PROJECT_ID = 'datapipeline-325719'
filename = 'get_txn_by_address.csv'
dataset_id = 'etherscan'
table_id ='datapipeline-325719.etherscan.txn_by_address'
table_name='txn_by_address'

client = bigquery.Client(project=PROJECT_ID, location="US")

try:
    client.get_table(table_id)  # Make an API request.
    print("Table {} already exists.".format(table_id))
except:
    pass
    print("Table {} is not found.".format(table_id))
    schema = [
        bigquery.SchemaField("address", "STRING"),
        bigquery.SchemaField("blockNumber", "INTEGER"),
        bigquery.SchemaField("timestamp", "INTEGER"),
        bigquery.SchemaField("hash", "STRING"),
        bigquery.SchemaField("txn_from", "STRING"),
        bigquery.SchemaField("txn_to", "STRING"),
        bigquery.SchemaField("eth_value", "FLOAT"),
        bigquery.SchemaField("gas_price", "FLOAT"),
        bigquery.SchemaField("gas_fee_eth", "FLOAT"),
        bigquery.SchemaField("send", "INTEGER"),
        bigquery.SchemaField("receive", "INTEGER"),
        bigquery.SchemaField("confirmations", "INTEGER"),
        bigquery.SchemaField("confirmed", "INTEGER")

    ]



    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)
    print("Created table {}".format(table_id))

# tell the client everything it needs to know to upload our csv
dataset_ref = client.dataset(dataset_id)
table_ref = dataset_ref.table(table_name)
job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.CSV
job_config.skip_leading_rows = 1

# load the csv into bigquery
with open(filename, "rb") as source_file:
    job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

job.result()

# looks like everything worked :)
print("Loaded {} rows into {}.".format(job.output_rows, table_id))
