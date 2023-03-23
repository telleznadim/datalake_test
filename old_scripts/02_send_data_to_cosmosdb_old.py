
import azure.cosmos.cosmos_client as cosmos_client
import timeit
import pandas as pd
from datetime import datetime, timedelta
import json
import uuid
from dotenv import dotenv_values
from tqdm import tqdm

date_time = datetime.now()
# date_time = date_time - timedelta(days=1)

config = dotenv_values(".env")

HOST = config['cosmosdb_host']
MASTER_KEY = config['cosmosdb_master_key']
DATABASE_ID = config['cosmosdb_database_id']


def create_item(container, item):
    container.create_item(body=item)


def delete_item(container, doc_id, partition_key):
    # print('\nDeleting Item by Id\n')
    response = container.delete_item(item=doc_id, partition_key=partition_key)
    # print('Deleted item\'s Id is {0}'.format(doc_id))


def query_items_by_bu_and_delete(partition_key, company_short, container):
    print('\nQuerying for an  Item by Partition Key\n')

    # Including the partition key value of account_number in the WHERE filter results in a more efficient query
    item_list = list(container.query_items(
        query="SELECT c.id,c.DW_EVI_BU FROM c WHERE (c.DW_EVI_BU = @dw_evi_bu)",
        parameters=[
            {"name": "@dw_evi_bu", "value": company_short}
        ],
        enable_cross_partition_query=True,
    ))
    df = pd.DataFrame.from_records(item_list)
    print(df)
    print(f'Deleting Items by Partition Key {company_short}, {container}')
    for doc in item_list:
        # print('Item Id: {0}'.format(doc.get('id')))
        # print('Item partition_key: {0}'.format(doc.get(partition_key)))
        delete_item(container, doc.get('id'), doc.get(partition_key))


def readCsvBcSendCosmosDB(company, endpoint, erp, file_name, container):
    df = pd.read_csv(
        f'C:/Users/eviadmin/Documents/Datawarehouse/python_scripts/DataLake/prod/files/from_bc/{file_name}', compression="gzip")
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
    # endpoints = {
    #     "Items": "Items_31_DW"}
    endpoints = {"PurchaseLines": "Purchase_Lines_518_DW",
                 "SalesLines": "Sales_Lines_516_DW", "ItemLedgerEntries": "Item_Ledger_Entries_38_DW", "Items": "Items_31_DW", "Locations": "Locations_15_DW"}
    # endpoints = {"ItemLedgerEntries": "Item_Ledger_Entries_38_DW",
    #              "Items": "Items_31_DW", "Locations": "Locations_15_DW"}

    for endpoint in endpoints:
        print(f'Sending data to CosmosDB {endpoints[endpoint]}')
        companies = {"TRS": "TRSPROD01", "FLR": "FLRPROD", "CTL": "CTLPROD"}
        # companies = {"CTL": "CTLPROD"}
        for company_short in companies:
            print(f'Company: {company_short}')
            start = timeit.default_timer()
            container = db.get_container_client(endpoint)
            erp = "BC"
            file_name = f'{company_short}_{endpoints[endpoint]}_{date_time.strftime("%m%d%y")}.csv.gz'
            query_items_by_bu_and_delete("DW_EVI_BU", company_short, container)
            readCsvBcSendCosmosDB("TRS", endpoint, erp, file_name, container)
            end = timeit.default_timer()
            print("Duration: ", end-start, "secs")
            print("Duration: ", (end-start)/60, "mins")


if __name__ == '__main__':

    main()
