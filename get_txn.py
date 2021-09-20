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

#set up variables
apikey = "Q7YRMBZ4RY8DWQG56F6QXMMJ1KCGHYU8ZH"
#num_txn = 1000


def get_txn_by_address(data, address, apikey,  sort="desc"):
    print(address)
    url = "https://api.etherscan.io/api?module=account&action=txlist&address=" + str(
        address) + "&startblock=0&endblock=99999999&sort=" + sort + "&apikey=" + apikey
    normal_txn = requests.get(url).json()
    address_lower = address.lower()

    for n, transaction in enumerate(normal_txn['result']):
        blockNumber = transaction['blockNumber']
        timestamp = transaction['timeStamp']
        hash = transaction['hash']
        txn_from = transaction['from'].lower()
        txn_to = transaction['to'].lower()
        value = transaction['value']
        send = 1 if txn_from == address_lower else 0
        receive = 1 if txn_to == address_lower else 0
        gas_price = int(transaction['gasPrice']) / Decimal("1000000000000000000")
        gas_fee = int(transaction['gasPrice']) * int(transaction['gasUsed'])
        confirmations = transaction['confirmations']
        eth_value = float(Decimal(value) / Decimal("1000000000000000000"))
        gas_fee_eth = float(Decimal(gas_fee) / Decimal("1000000000000000000"))
        confirmed = 1 if int(confirmations) >= 16 else 0

        data = data.append({"address": address_lower, "blockNumber": blockNumber, "timestamp": timestamp, "hash": hash,
                            "txn_from": txn_from, "txn_to": txn_to, "eth_value": eth_value, "gas_price": gas_price,
                            "gas_fee_eth": gas_fee_eth, "send": send, "receive": receive,
                            "confirmations": confirmations, "confirmed": confirmed}, ignore_index=True)

    return data


data = pd.DataFrame(
    columns=['address', 'blockNumber', 'timestamp', 'hash', 'txn_from', 'txn_to', 'eth_value', "gas_price",
             'gas_fee_eth', 'send', 'receive', 'confirmations', 'confirmed'])
for address in address_list:
    data = get_txn_by_address(data, address, apikey)
print("API call finished")


# add additional columns
def create_features(data):
    data['outflow']=data['send']*data['eth_value']
    data['inflow']=data['receive']*data['eth_value']
    data['netflow']=data['inflow']-data['outflow']
#     #sort data by address and timestamp
#     # data_sorted = data.sort_values(by=['address','timestamp'])
#     # #calculate balance for each address and token
#     # df1=data_sorted.groupby(['address','timestamp','blockNumber','hash','txn_from','txn_to'])['netflow'].sum().reset_index()
#     # print("step2")
#     # df2=df1.groupby(['address']).cumsum()
#     # df2.rename(columns={'netflow': 'balance'}, inplace=True)
#     # balance=pd.concat([df1,df2],axis=1)
#     # balance = balance.drop('netflow', axis=1)
#     # print("merge")
#     # new_df=pd.merge(balance,data_sorted,"inner",on=['address','timestamp','blockNumber','hash','txn_from','txn_to'])
    return data

print("create additional features")
new_df = create_features(data)

print("save get_token_txn_by_address table to csv ")
new_df.to_csv("get_txn_by_address.csv",index=False)

print("get_txn_by_address table saved to local ")

