
import azure.cosmos.cosmos_client as cosmos_client
import timeit
import pandas as pd
from datetime import datetime, timedelta
import json
import uuid
from dotenv import dotenv_values
from tqdm import tqdm
import sys
import logging

date_time = datetime.now()
# date_time = date_time - timedelta(days=1)

config = dotenv_values(
    "C:/Users/eviadmin/Documents/Datawarehouse/python_scripts/DataLake/prod/.env")

# prod - test
output_folder = "test"

# log_path = "C:/Users/eviadmin/Documents/Datawarehouse/schedule_scripts/From_BC/python/logs/"
log_path = "C:/Users/eviadmin/Documents/Datawarehouse/python_scripts/DataLake/test/files/logs/"

# logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def define_logger(file_name):
    # create a file handler and set the logging level
    handler = logging.FileHandler(
        f'{log_path}{file_name}.log')
    handler.setLevel(logging.DEBUG)

    # create a formatter and add it to the handler
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s : %(message)s")
    handler.setFormatter(formatter)

    # add the handler to the logger
    logger.addHandler(handler)
    return (logger)


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
    logger.debug('\nQuerying for an  Item by Partition Key\n')

    # Including the partition key value of account_number in the WHERE filter results in a more efficient query
    item_list = list(container.query_items(
        query="SELECT c.id,c.DW_EVI_BU FROM c WHERE (c.DW_EVI_BU = @dw_evi_bu)",
        parameters=[
            {"name": "@dw_evi_bu", "value": company_short}
        ],
        enable_cross_partition_query=True,
    ))
    df = pd.DataFrame.from_records(item_list)
    logger.debug(df)
    logger.debug(
        f'Deleting Items by Partition Key {company_short}, {container}')
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


def query_items_by_bu_columns(container, company_short, column_names):
    print('\nQuerying for an  Item by Partition Key\n')

    table_name_prefix = 'c.'
    query_column_names = ','.join(
        [table_name_prefix + s for s in column_names])
    # print(my_string)
    items = list(container.query_items(
        # query="SELECT * FROM c WHERE c.DW_BU = \"ILS02\""
        query=f"SELECT {query_column_names} FROM c WHERE (c.DW_EVI_BU = @dw_evi_bu)",
        parameters=[
            {"name": "@dw_evi_bu", "value": company_short}
        ],
        enable_cross_partition_query=True,
    ))

    df = pd.DataFrame.from_records(items)
    print(df)
    return (df)


def readCsvBcSendCosmosDB(file_name, container):
    logger.debug("Reading CSV file {file_name}")
    df = pd.read_csv(
        f'C:/Users/eviadmin/Documents/Datawarehouse/python_scripts/DataLake/{output_folder}/files/from_bc/{file_name}', compression="gzip", keep_default_na=False)
    logger.debug(df)

    data = df.to_json(orient='records')
    data_dict = json.loads(data)
    for item in tqdm(data_dict):
        #     # Assign id to the item
        item['id'] = str(uuid.uuid4())
        create_item(container, item)


def read_csv_send_to_cosmosdb(file_name, container, df_from_cosmosdb, column_name):
    logger.debug("Reading CSV file {file_name}")
    df = pd.read_csv(
        f'C:/Users/eviadmin/Documents/Datawarehouse/python_scripts/DataLake/{output_folder}/files/from_bc/{file_name}', compression="gzip", keep_default_na=False)
    logger.debug(df)

    logger.debug("Checking if CosmosDB is not empty")
    if not df_from_cosmosdb.empty:
        logger.debug("Filtering and Sending to CosmosDB")
        df = df[~df[column_name].isin(df_from_cosmosdb[column_name])]
        logger.debug(df)

    data = df.to_json(orient='records')
    data_dict = json.loads(data)
    for item in tqdm(data_dict):
        #     # Assign id to the item
        item['id'] = str(uuid.uuid4())
        create_item(container, item)


def read_csv_send_to_cosmosdb_columns(file_name, container, df_from_cosmosdb, column_names):
    logger.debug("Reading CSV file {file_name}")
    df = pd.read_csv(
        f'C:/Users/eviadmin/Documents/Datawarehouse/python_scripts/DataLake/{output_folder}/files/from_bc/{file_name}', compression="gzip", keep_default_na=False)
    # logger.debug(df)

    print(df)
    # df_from_cosmosdb = pd.DataFrame()

    logger.debug("Checking if CosmosDB is not empty")
    if not df_from_cosmosdb.empty:
        logger.debug("Merging df from BC and df from Cosmos DB")
        merged_df = pd.merge(
            df, df_from_cosmosdb, on=column_names, how='outer', indicator=True)

        logger.debug(merged_df)
        logger.debug("Only inserting the ones that are not in Cosmos DB")
        merged_df = merged_df[merged_df["_merge"] == "left_only"]
        df = merged_df.drop('_merge', axis=1)
        logger.debug(df)

    print(df)

    logger.debug("Inserting Data to Cosmos DB")
    data = df.to_json(orient='records')
    data_dict = json.loads(data)
    for item in tqdm(data_dict):
        #     # Assign id to the item
        item['id'] = str(uuid.uuid4())
        create_item(container, item)
    logger.debug("Inserting Data to Cosmos DB Finished")


def main():

    if (len(sys.argv) == 3):
        company_short = sys.argv[1]
        endpoint = sys.argv[2]
        logger = define_logger(
            f"02_send_data_to_cosmosdb_{company_short}_{endpoint}")
        logger.debug(f'-------- Executing 02 send_data_to_cosmosdb ---------')
        logger.debug(f'Datetime = {date_time.strftime("%m-%d-%y %H:%M:%S") }')
        logger.debug(f'company_short = {company_short}, endpoint = {endpoint}')
        companies = {"TRS": "TRSPROD01", "FLR": "FLOPROD",
                     "CTL": "CTLPROD", "SEED": "SEED01"}
        endpoints = {"PurchaseLines": "Purchase_Lines_518_DW",
                     "PurchaseLines2": "Purchase_Order_Line_54_DW",
                     "Brands": "SSI_Brands_50201_DW",
                     "SalesOrderHeaders": "Sales_Order_List_9305_DW",
                     "Customers": "Customers_22_DW",
                     "Salespeople": "Salespersons_Purchasers_14_DW",
                     "ValueEntries": "Value_Entries_5802_DW",
                     "SalesLines": "Sales_Order_Lines_46_DW",
                     "ItemLedgerEntries": "Item_Ledger_Entries_38_DW",
                     "Items": "Items_31_DW",
                     "Locations": "Locations_15_DW",
                     "PostedSalesInvoiceHeaders": "Posted_Sales_Invoices_143_DW",
                     "PostedSalesInvoiceLines": "Posted_Sales_Invoice_Lines_526_DW",
                     "PostedSalesCreditMemoHeaders": "Posted_Sales_Credit_Memo_144_DW",
                     "PostedSalesCreditMemoLines": "Posted_Sales_Credit_Memo_Lines_527_DW"}
        partition_key = "DW_EVI_BU"

        client = cosmos_client.CosmosClient(HOST, {'masterKey': MASTER_KEY})
        db = client.get_database_client(DATABASE_ID)
        container = db.get_container_client(endpoint)

        # endpoints = {
        #     "Items": "Items_31_DW"}

        logger.debug(f'Sending data to CosmosDB {endpoints[endpoint]}')
        # companies = {"TRS": "TRSPROD01", "FLR": "FLRPROD", "CTL": "CTLPROD"}
        logger.debug(f'Company: {company_short}')

        file_name = f'{company_short}_{endpoints[endpoint]}_{date_time.strftime("%m%d%y")}.csv.gz'
        logger.debug(file_name)

        if ((endpoint == "ItemLedgerEntries") | (endpoint == "ValueEntries") | (endpoint == "ResourceLedgerEntries")):
            column_names = ["Entry_No"]
            df_from_cosmosdb = query_items_by_bu_columns(
                container, company_short, column_names)
            read_csv_send_to_cosmosdb_columns(
                file_name, container, df_from_cosmosdb, column_names)
        elif ((endpoint == "PostedSalesInvoiceHeaders") | (endpoint == "PostedSalesCreditMemoHeaders")):
            column_names = ["No"]
            df_from_cosmosdb = query_items_by_bu_columns(
                container, company_short, column_names)
            read_csv_send_to_cosmosdb_columns(
                file_name, container, df_from_cosmosdb, column_names)
        elif ((endpoint == "PostedSalesInvoiceLines") | (endpoint == "PostedSalesCreditMemoLines")):
            column_names = ["Document_No", "Line_No"]
            df_from_cosmosdb = query_items_by_bu_columns(
                container, company_short, column_names)
            read_csv_send_to_cosmosdb_columns(
                file_name, container, df_from_cosmosdb, column_names)
        else:
            query_items_by_bu_and_delete(
                "DW_EVI_BU", company_short, container)
            readCsvBcSendCosmosDB(file_name, container)

    else:
        logger.debug(f'Error not enought arguments for the script to run')


if __name__ == '__main__':
    start = timeit.default_timer()
    main()
    end = timeit.default_timer()
    logger.debug(f'Duration: {end-start} secs')
    logger.debug(f'Duration: {(end-start)/60} mins')
