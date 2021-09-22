# Chainlink Trial Project 

Project documentation: https://docs.google.com/document/d/15QBS2zuyj9QBw0CNGGX5CcF8zQjHtSYT8RlcfQ05FQg/edit?usp=sharing

Chainlink Token Activites by Wallet Address Dashboard: https://datastudio.google.com/s/tyJBWhHDbWo

Source of data: Etherscan API 

## How to use this repo 
1. git clone this repo on GCP shell
2. Create account in Data Storage/BigQuery 
```
PROJECT_ID = 'datapipeline-325719'
filename = 'get_token_txn_by_address.csv'
dataset_id = 'etherscan'
table_name ='token_txn_by_address'
table_id ='datapipeline-325719.etherscan.token_txn_by_address'

Cloud storage bucket = 'gs://dp-etherscan/'
```
4. Run the following command to create table in BigQuery

``` 
cd chainlink
python3 main.py 
or
sh create_table_token.sh 
sh create_table_txn.sh
```
4. View wallet activities by selecting address in the dashboard
  https://datastudio.google.com/s/tyJBWhHDbWo

## Steps to build a data pipeline in GCP
1. Extract data features from API (etherscan) 
2. Save csv table to local
3. Create table schema in Bigquery 
4. Upload csv table to schema 
5. Upload csv table to Cloud Storage
6. Pull new data, upload to Cloud Storage 
7. Upload new records to schema 
8. Schedule 6&7 to run on a regular basis 
9. Additional feature engineering with sql 
10. Build data dashboard


## Insights and recommendations 
1. We have observed that the addresses of interest are very active with Chainlink tokens, while dormant on other chains such as eth and bsc
2. Many addresses share the same input fund address. 
3. Output address are much more scattered, but still some share similar pattern
4. These accounts are active since 2021 (may due to etherscan limit), have regular transaction patterns 
5. As follow up we can do a link/graph analysis on the relationship between the accounts, and predict future transaction behaviors
