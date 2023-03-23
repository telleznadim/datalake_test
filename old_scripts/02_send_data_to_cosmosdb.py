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
    # logger.debug('\nDeleting Item by Id\n')
    response = container.delete_item(item=doc_id, partition_key=partition_key)
    # logger.debug('Deleted item\'s Id is {0}'.format(doc_id))


def query_items_by_bu_and_delete(partition_key, company_short, container):
    # logger.debug('\nQuerying for an  Item by Partition Key\n')

    # Including the partition key value of account_number in the WHERE filter results in a more efficient query
    item_list = list(container.query_items(
        query="SELECT c.id,c.DW_EVI_BU FROM c WHERE (c.DW_EVI_BU = @dw_evi_bu)",
        parameters=[
            {"name": "@dw_evi_bu", "value": company_short}
        ],
        enable_cross_partition_query=True,
    ))
    df = pd.DataFrame.from_records(item_list)
    # logger.debug(df)
    # logger.debug(
    #     f'Deleting Items by Partition Key {company_short}, {container}')
    for doc in tqdm(item_list):
        # logger.debug('Item Id: {0}'.format(doc.get('id')))
        # logger.debug('Item partition_key: {0}'.format(doc.get(partition_key)))
        delete_item(container, doc.get('id'), doc.get(partition_key))


def query_items_by_bu(container, company_short, column_name):
    print('\nQuerying for an  Item by Partition Key\n')

    items = list(container.query_items(
        # query="SELECT * FROM c WHERE c.DW_BU = \"ILS02\""
        query=f"SELECT c.{column_name} FROM c WHERE (c.DW_EVI_BU = @dw_evi_bu)",
        parameters=[
            {"name": "@dw_evi_bu", "value": company_short}
        ],
        enable_cross_partition_query=True,
    ))

    df = pd.DataFrame.from_records(items)
    print(df)
    return (df)

    # df.to_csv(f'files/from_datalake/{company_short}_{endpoint}_{date_time_string}.csv.gz',
    #           compression="gzip", index=False)


def readCsvBcSendCosmosDB(file_name, container):
    # logger.debug("Reading CSV file {file_name}")
    df = pd.read_csv(
        f'C:/Users/eviadmin/Documents/Datawarehouse/python_scripts/DataLake/test/files/from_bc/{file_name}', compression="gzip")
    # logger.debug(df)

    data = df.to_json(orient='records')
    data_dict = json.loads(data)
    for item in tqdm(data_dict):
        #     # Assign id to the item
        item['id'] = str(uuid.uuid4())
        create_item(container, item)


def read_csv_send_to_cosmosdb(file_name, container, df_from_cosmosdb):
    # logger.debug("Reading CSV file {file_name}")
    df = pd.read_csv(
        f'C:/Users/eviadmin/Documents/Datawarehouse/python_scripts/DataLake/test/files/from_bc/{file_name}', compression="gzip")
    # logger.debug(df)

    print(df)
    print(df_from_cosmosdb.empty)
    # logger.debug("Filtering and Sending to CosmosDB")
    if (not df_from_cosmosdb.empty):
        # logger.debug("The DataFrame is not empty")
        df = df[~df['Entry_No'].isin(df_from_cosmosdb['Entry_No'])]
    # logger.debug(df)
    data = df.to_json(orient='records')
    data_dict = json.loads(data)
    for item in tqdm(data_dict):
        #     # Assign id to the item
        item['id'] = str(uuid.uuid4())
        create_item(container, item)


def main():
    client = cosmos_client.CosmosClient(HOST, {'masterKey': MASTER_KEY})
    db = client.get_database_client(DATABASE_ID)

    # date_time_string = "011123_131355"
    # endpoints = {
    #     "Items": "Items_31_DW"}
    # endpoints = {"PurchaseLines": "Purchase_Lines_518_DW",
    #              "SalesLines": "Sales_Lines_516_DW", "ItemLedgerEntries": "Item_Ledger_Entries_38_DW", "Items": "Items_31_DW", "Locations": "Locations_15_DW"}
    # endpoints = {"ValueEntries": "Value_Entries_5802_DW"}
    endpoints = {"PostedSalesInvoicesHeaders": "Posted_Sales_Invoices_143_DW", "PostedSalesInvoicesLines": "Posted_Sales_Invoice_Lines_526_DW", "PostedSalesCreditMemoHeaders":
                 "Posted_Sales_Credit_Memo_144_DW", "PostedSalesCreditMemoLines": "Posted_Sales_Credit_Memo_Lines_527_DW", "ResourceLedgerEntries": "Resource_Ledger_Entries_202_DW"}
    endpoints = {"Salespeople": "Salespersons_Purchasers_14_DW"}

    for endpoint in endpoints:
        print(f'Sending data to CosmosDB {endpoints[endpoint]}')
        companies = {"FLR": "FLOPROD"}
        # companies = {"TRS": "TRSPROD01", "FLR": "FLRPROD", "CTL": "CTLPROD"}
        # companies = {"CTL": "CTLPROD"}
        for company_short in companies:
            print(f'Company: {company_short}')
            start = timeit.default_timer()
            container = db.get_container_client(endpoint)
            file_name = f'{company_short}_{endpoints[endpoint]}_{date_time.strftime("%m%d%y")}.csv.gz'

            if ((endpoint == "ItemLedgerEntries") | (endpoint == "ValueEntries") | (endpoint == "ResourceLedgerEntries")):
                column_name = "Entry_No"
                df_from_cosmosdb = query_items_by_bu(
                    container, company_short, column_name)
                read_csv_send_to_cosmosdb(
                    file_name, container, df_from_cosmosdb)
            else:
                query_items_by_bu_and_delete(
                    "DW_EVI_BU", company_short, container)
                readCsvBcSendCosmosDB(file_name, container)

            end = timeit.default_timer()
            print("Duration: ", end-start, "secs")
            print("Duration: ", (end-start)/60, "mins")


if __name__ == '__main__':
    main()
