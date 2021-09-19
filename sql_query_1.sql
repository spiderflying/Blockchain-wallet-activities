SELECT *, TIMESTAMP_SECONDS(timestamp) as Timestamp_value,
sum(netflow)
OVER
  (PARTITION BY address, tokenSymbol
  ORDER BY timestamp ASC) AS balance

 FROM `datapipeline-325719.etherscan.token_txn_by_address` 
 --where address = '0x06812a2035bda4707107539725902e065622cee7'
 
