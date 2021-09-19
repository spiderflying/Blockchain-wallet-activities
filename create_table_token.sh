#get data from API
python3 get_token.py

#cp data from local to cloud storage
gsutil cp get_token_txn_by_address.csv gs://dp-etherscan

#create table schema and upload csv
python3 create_table_token.py
