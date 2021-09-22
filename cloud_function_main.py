import base64

from google.cloud import storage
from google.cloud import bigquery
from datetime import datetime
import requests
import pandas as pd
from decimal import Decimal

address_list = ["0x7813C776cD8EAb537028b4499d467b1f1B86b14C",
"0xedDF0Df82006a477C87C2E16FfdEF0d97631E941",
"0x9f966149B7Dd6AB61440Ec4D4B853f4605739E73",
"0x630163B84674B2B404fB6036A510574F259c5Cb7",
"0x724D08F4688Cda05d8e3243db9Db1B20C90f3a05",
"0x5C3d1309D8b6e37EFF9FD6C258e1544549b39D22",
"0x4bE991B4d560BBa8308110Ed1E0D7F8dA60ACf6A",
"0x6a1Ef9bF93048533c49a1Eed984c080608f7DB6A",
"0xdC182F6E7461Fe1fE442665615aA44F70f978Db3",
"0xe4E345594375B54F395B54C41bD8b370A302bf69",
"0x06812A2035BDa4707107539725902e065622CEE7",
"0x8093150EC164753994A1F65616E04Ae92a9Ef8c5",
"0x29236dFcae0aEE2D6da157F3B6835830c75875Ad",
"0x33CCa8E7420114dB103d61bd39A72ff65E46352D",
"0x7009033C0d6702fd2dfAD3478d2AE4e3b6aCB966",
"0x5259AA3b262B3390EEef0A6E7b89C08d60c94622",
"0x989B836D68700DA948B5c04A65b3bBA39F400ad7"]

apikey = "Q7YRMBZ4RY8DWQG56F6QXMMJ1KCGHYU8ZH"



def get_token_txn_by_address(data, address, apikey, sort="desc"):
    print(address)
    url = "http://api.etherscan.io/api?module=account&action=tokentx&address=" + str(
        address) + "&startblock=0&endblock=999999999&sort=" + sort + "&apikey=" + apikey
    response = requests.get(url).json()
    address_lower = address.lower()

    for n, transaction in enumerate(response['result']):
        blockNumber = transaction['blockNumber']
        timestamp = transaction['timeStamp']
        hash = transaction['hash']
        blockHash = transaction['blockHash']
        txn_from = transaction['from'].lower()
        txn_to = transaction['to'].lower()
        value = transaction['value']
        tokenName = transaction['tokenName']
        tokenSymbol = transaction['tokenSymbol']
        send = 1 if txn_from == address_lower else 0
        receive = 1 if txn_to == address_lower else 0
        gas_price = int(transaction['gasPrice']) / Decimal("1000000000000000000")
        gas_fee = int(transaction['gasPrice']) * int(transaction['gasUsed'])
        confirmations = transaction['confirmations']
        token_value = float(Decimal(value) / Decimal("1000000000000000000"))
        gas_fee_eth = float(Decimal(gas_fee) / Decimal("1000000000000000000"))
        confirmed = 1 if int(confirmations) >= 16 else 0

        data = data.append({"address": address_lower, "blockNumber": blockNumber, "timestamp": timestamp, "hash": hash,
                            "blockHash": blockHash, "txn_from": txn_from, "txn_to": txn_to, "tokenName": tokenName,
                            "tokenSymbol": tokenSymbol, "token_value": token_value, "gas_price": gas_price,
                            "gas_fee_eth": gas_fee_eth, "send": send, "receive": receive,
                            "confirmations": confirmations, "confirmed": confirmed}, ignore_index=True)

    return data

def create_features(data):
    data['outflow']=data['send']*data['token_value']
    data['inflow']=data['receive']*data['token_value']
    data['netflow']=data['inflow']-data['outflow']
    return data

def create_data():
    data = pd.DataFrame(
        columns=['address', 'blockNumber', 'timestamp', 'hash', 'blockHash', 'txn_from', 'txn_to', 'tokenName',
                 'tokenSymbol', 'token_value', "gas_price", 'gas_fee_eth', 'send', 'receive', 'confirmations',
                 'confirmed'])
    for address in address_list:
        data = get_token_txn_by_address(data, address, apikey)
    print("API call finished")


# def upload_blob(bucket_name, source_file_name, destination_blob_name):
#     """Uploads a file to the bucket."""
#
#     # The name of your GCS bucket
#     # bucket_name = "your-bucket-name"
#
#     # The path and the file to upload
#     # source_file_name = "local/path/to/file"
#
#     # The name of the file in GCS bucket once uploaded
#     # destination_blob_name = "storage-object-name"
#
#     storage_client = storage.Client()
#     bucket = storage_client.bucket(bucket_name)
#     blob = bucket.blob(destination_blob_name)
#
#     blob.upload_from_filename(source_file_name)


def upload_to_bigquery(PROJECT_ID, dataset_id, table_name, table_id, filename):
    client = bigquery.Client(project=PROJECT_ID, location="US")

    # tell the client everything it needs to know to upload our csv
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_name)
    try:
        client.get_table(table_id)  # Make an API request.
        print("Table {} already exists.".format(table_id))
        previous_rows = client.get_table(table_ref).num_rows
        print("previous_rows {}".format(previous_rows))
    # assert previous_rows > 0
    except:
        pass
        print("Table {} is not found.".format(table_id))
        schema = [
            bigquery.SchemaField("address", "STRING"),
            bigquery.SchemaField("blockNumber", "INTEGER"),
            bigquery.SchemaField("timestamp", "INTEGER"),
            bigquery.SchemaField("hash", "STRING"),
            bigquery.SchemaField("blockHash", "STRING"),
            bigquery.SchemaField("txn_from", "STRING"),
            bigquery.SchemaField("txn_to", "STRING"),
            bigquery.SchemaField("tokenName", "STRING"),
            bigquery.SchemaField("tokenSymbol", "STRING"),
            bigquery.SchemaField("token_value", "FLOAT"),
            bigquery.SchemaField("gas_price", "FLOAT"),
            bigquery.SchemaField("gas_fee_eth", "FLOAT"),
            bigquery.SchemaField("send", "INTEGER"),
            bigquery.SchemaField("receive", "INTEGER"),
            bigquery.SchemaField("confirmations", "INTEGER"),
            bigquery.SchemaField("confirmed", "INTEGER"),
            bigquery.SchemaField("ouflow", "FLOAT"),
            bigquery.SchemaField("inflow", "FLOAT"),
            bigquery.SchemaField("netflow", "FLOAT")
            #   bigquery.SchemaField("balance", "FLOAT", mode="REQUIRED")
        ]

        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)
        print("Created table {}".format(table_id))

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    # # load data from cloud storage
    # uri = "gs://dp-etherscan/get_token_txn_by_address.csv"
    # load_job = client.load_table_from_uri(
    #     uri, table_id, job_config=job_config
    # )  # Make an API request.

    # load the csv into bigquery
    with open(filename, "rb") as source_file:
        load_job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

    load_job.result()

    # looks like everything worked :)
    print("Loaded {} rows into {}.".format(load_job.output_rows, table_id))

    after_rows = client.get_table(table_ref).num_rows
    print("after_rows {}".format(after_rows))


def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    data = pd.DataFrame(
        columns=['address', 'blockNumber', 'timestamp', 'hash', 'blockHash', 'txn_from', 'txn_to', 'tokenName',
                 'tokenSymbol', 'token_value', "gas_price", 'gas_fee_eth', 'send', 'receive', 'confirmations',
                 'confirmed'])
    for address in address_list:
        data = get_token_txn_by_address(data, address, apikey)
    print("API call finished")
    # create features
    data['outflow']=data['send']*data['token_value']
    data['inflow']=data['receive']*data['token_value']
    data['netflow']=data['inflow']-data['outflow']
   # new_df = create_features(data)

    print("save get_token_txn_by_address table to csv ")
    data.to_csv("/tmp/get_token_txn_by_address.csv", index=False)

    print("get_token_txn_by_address table saved to local temp file")
    # Write the text 'money' and save the file locally


    # # Upload the file to GCS bucket
    # bucket_name = 'dp-etherscan'
    # local_file_location = '/tmp/' + file_name
    # upload_blob(bucket_name, local_file_location, file_name)
    #
    
    #Upload data to BigQuery

    PROJECT_ID = 'datapipeline-325719'
    filename = '/tmp/get_token_txn_by_address.csv'
    dataset_id = 'etherscan'
    table_id = 'datapipeline-325719.etherscan.token_txn_by_address'
    table_name = 'token_txn_by_address'

    upload_to_bigquery(PROJECT_ID, dataset_id, table_name, table_id, filename)

    print("new data uploaded to bigquery")
