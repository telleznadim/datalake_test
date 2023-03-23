
import azure.cosmos.cosmos_client as cosmos_client
import timeit
import pandas as pd
from datetime import datetime
import json
import uuid
from dotenv import dotenv_values
from tqdm import tqdm

date_time = datetime.now()


config = dotenv_values(".env")

HOST = config['cosmosdb_host']
MASTER_KEY = config['cosmosdb_master_key']
DATABASE_ID = config['cosmosdb_database_id']


def create_item(container, item):
    container.create_item(body=item)


def readCsvBcSendCosmosDB(company, endpoint, erp, file_name, container):
    df = pd.read_csv(
        f'C:/Users/eviadmin/Documents/Datawarehouse/python_scripts/DataLake/prod/files/from_bc/{file_name}', compression="gzip")
    # print(df)
    df["DW_EVI_BU"] = company
    df["DW_ERP_System"] = erp
    df["DW_Timestamp"] = date_time
    df["DW_ERP_Source_Table"] = endpoint
    print(df)

    data = df.to_json(orient='records')
    data_dict = json.loads(data)
    for item in tqdm(data_dict):
        #     # Assign id to the item
        item['id'] = str(uuid.uuid4())
        create_item(container, item)


def main():
    client = cosmos_client.CosmosClient(HOST, {'masterKey': MASTER_KEY})
    db = client.get_database_client(DATABASE_ID)

    date_time_string = "011123_131355"
    endpoints = {"PurchaseLines": "Purchase_Lines_518_DW",
                 "SalesLines": "Sales_Lines_516_DW", "ItemLedgerEntries": "Item_Ledger_Entries_38_DW", "Items": "Items_31_DW", "Locations": "Locations_15_DW"}
    for endpoint in endpoints:
        print(f'Sending data to CosmosDB {endpoints[endpoint]}')
        start = timeit.default_timer()
        container = db.get_container_client(endpoint)
        erp = "BC"
        company = "TRSPROD01"

        file_name = f'{company}_{endpoints[endpoint]}_{date_time_string}.csv.gz'
        readCsvBcSendCosmosDB("TRS", endpoint, erp, file_name, container)
        end = timeit.default_timer()
        print("Duration: ", end-start, "secs")
        print("Duration: ", (end-start)/60, "mins")


if __name__ == '__main__':

    main()
