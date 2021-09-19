# Chainlink Trial Project 

Project documentation: https://docs.google.com/document/d/15QBS2zuyj9QBw0CNGGX5CcF8zQjHtSYT8RlcfQ05FQg/edit?usp=sharing

Chainlink Token Activites by Wallet Address Dashboard: https://datastudio.google.com/s/tyJBWhHDbWo

## How to use this repo 
1. git clone this repo on GCP shell
2. Create account in Data Storage/BigQuery 
3. Run the following command to create table in BigQuery
``` 
cd chainlink
sh create_table_token.sh 
```
4. View wallet activities by selecting address in the dashboard
  https://datastudio.google.com/s/tyJBWhHDbWo

## Steps to build a data pipeline in GCP
1. Extract data features from API (etherscan) 
2. Save csv table to local
3. Upload csv table to Cloud Storage
4. Create table schema in Bigquery 
5. Upload csv table to schema 
6. Pull new data, upload to Cloud Storage 
7. Update table with latest records 
8. Schedule 6&7 to run on a regular basis (WIP)
9. Additional feature engineering with sql 
10. Build data dashboard 
