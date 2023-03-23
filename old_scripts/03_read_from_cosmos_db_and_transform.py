import azure.cosmos.documents as documents
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.exceptions as exceptions
from azure.cosmos.partition_key import PartitionKey
import json
import timeit
import pandas as pd
from datetime import datetime, timedelta
from dotenv import dotenv_values
import uuid

config = dotenv_values(".env")

HOST = config['cosmosdb_host']
MASTER_KEY = config['cosmosdb_master_key']
DATABASE_ID = config['cosmosdb_database_id']

# ------------- Queries ------------------
# SELECT VALUE COUNT(1) FROM c WHERE c.DW_BU = "ILS02"
# SELECT VALUE COUNT(1) FROM(SELECT distinct value c.No from c WHERE c.DW_BU="ILS02")


def columns_list_to_string(columns_list):
    columns_string = ""
    for i in range(0, len(columns_list)):
        if (i == len(columns_list) - 1):
            columns_string = f'{columns_string} c.{columns_list[i]["name"]} as {columns_list[i]["alias"]}'
        else:
            columns_string = f'{columns_string} c.{columns_list[i]["name"]} as {columns_list[i]["alias"]},'
    return (columns_string)


def query_all_items_by_bu(container, company):
    print('\nQuerying for an  Item by Partition Key\n')

    # Including the partition key value of account_number in the WHERE filter results in a more efficient query

    items = list(container.query_items(
        # query="SELECT * FROM c WHERE c.DW_BU = \"ILS02\""
        query="SELECT * FROM c WHERE (c.DW_EVI_BU = @dw_evi_bu)",
        parameters=[
            {"name": "@dw_evi_bu", "value": company}
        ],
        enable_cross_partition_query=True,
    ))

    df = pd.DataFrame.from_records(items)
    print(df)


def query_items_by_bu_by_columns(container, company_short, endpoint, date_time_string, columns_list):
    print('\nQuerying for an  Item by Partition Key\n')

    # Including the partition key value of account_number in the WHERE filter results in a more efficient query
    # columns_list = [
    #     {"name": "No", "alias": "item_no"},
    #     {"name": "Description", "alias": "item_description"},
    #     {"name": "Blocked", "alias": "item_status"},
    #     {"name": "Inventory_Posting_Group", "alias": "inventory_posting_group"},
    #     {"name": "Gen_Prod_Posting_Group", "alias": "gen_prod_posting_group"},
    #     {"name": "Vendor_No", "alias": "vendor_no"},
    #     {"name": "Vendor_Item_No", "alias": "vendor_item_no"},
    #     {"name": "Item_Category_Code", "alias": "item_category_code"},
    #     {"name": "SSI_Brand", "alias": "brand "},
    #     {"name": "DW_EVI_BU", "alias": "dw_evi_bu"},
    #     {"name": "DW_ERP_System", "alias": "dw_erp_system"},
    #     {"name": "DW_ERP_Source_Table", "alias": "dw_erp_source_table"},
    #     {"name": "id", "alias": "dw_item_id"}
    # ]
    columns = columns_list_to_string(columns_list)
    print(columns)

    items = list(container.query_items(
        # query="SELECT * FROM c WHERE c.DW_BU = \"ILS02\""
        query="SELECT "+columns + \
        " FROM c WHERE (c.DW_EVI_BU = @dw_evi_bu)",
        parameters=[
            {"name": "@dw_evi_bu", "value": company_short}
        ],
        enable_cross_partition_query=True,
    ))

    df = pd.DataFrame.from_records(items)
    print(df)

    df.to_csv(f'files/from_datalake/{company_short}_{endpoint}_{date_time_string}.csv.gz',
              compression="gzip", index=False)


def create_sql_columns(data_to_read, columns_list):
    for data in data_to_read:
        columns_list = data["columns_list"]
        print(f'------- Columns for {data["endpoint"]} --------')
        for row in columns_list:
            if (row["alias"] == "dw_evi_bu"):
                print(f'{row["alias"]} char(10),')
            elif (row["alias"] == "dw_erp_system"):
                print(f'{row["alias"]} char(10),')
            elif (row["alias"] == "dw_erp_system"):
                print(f'{row["alias"]} char(10),')
            else:
                print(f'{row["alias"]} varchar(50),')


def main():
    date_time = datetime.now()
    # date_time = date_time - timedelta(days=1)
    companies = {"TRS": "TRSPROD01", "FLR": "FLOPROD", "CTL": "CTLPROD"}
    # companies = {"CTL": "CTLPROD"}

    data_to_read = [
        {"endpoint": "Items", "columns_list": [
            {"name": "No", "alias": "item_no"},
            {"name": "Description", "alias": "item_description"},
            {"name": "Blocked", "alias": "item_status"},
            {"name": "Inventory_Posting_Group", "alias": "inventory_posting_group"},
            {"name": "Gen_Prod_Posting_Group", "alias": "gen_prod_posting_group"},
            {"name": "Vendor_No", "alias": "vendor_no"},
            {"name": "Vendor_Item_No", "alias": "vendor_item_no"},
            {"name": "Item_Category_Code", "alias": "item_category_code"},
            {"name": "SSI_Brand", "alias": "brand "},
            {"name": "DW_EVI_BU", "alias": "dw_evi_bu"},
            {"name": "DW_ERP_System", "alias": "dw_erp_system"},
            {"name": "DW_ERP_Source_Table", "alias": "dw_erp_source_table"},
            {"name": "DW_Timestamp", "alias": "dw_timestamp"},
            {"name": "id", "alias": "dw_item_id"}
        ]},
        {"endpoint": "Locations", "columns_list": [
            {"name": "Code", "alias": "location_code"},
            {"name": "Name", "alias": "location_name"},
            {"name": "DW_EVI_BU", "alias": "dw_evi_bu"},
            {"name": "DW_ERP_System", "alias": "dw_erp_system"},
            {"name": "DW_ERP_Source_Table", "alias": "dw_erp_source_table"},
            {"name": "DW_Timestamp", "alias": "dw_timestamp"},
            {"name": "id", "alias": "dw_location_id"}
        ]},
        {"endpoint": "ItemLedgerEntries", "columns_list": [
            {"name": "Entry_No", "alias": "entry_no"},
            {"name": "Entry_Type", "alias": "entry_type"},
            {"name": "Document_Type", "alias": "document_type"},
            {"name": "Document_No", "alias": "document_no"},
            {"name": "Item_No", "alias": "item_no"},
            {"name": "Description", "alias": "item_description"},
            {"name": "Global_Dimension_1_Code", "alias": "global_dimension_1_code"},
            {"name": "Global_Dimension_2_Code", "alias": "global_dimension_2_code"},
            {"name": "Location_Code", "alias": "location_code"},
            {"name": "Quantity", "alias": "quantity"},
            {"name": "Remaining_Quantity", "alias": "remaining_quantity"},
            {"name": "Invoiced_Quantity", "alias": "invoiced_quantity"},
            {"name": "DW_EVI_BU", "alias": "dw_evi_bu"},
            {"name": "DW_ERP_System", "alias": "dw_erp_system"},
            {"name": "DW_ERP_Source_Table", "alias": "dw_erp_source_table"},
            {"name": "DW_Timestamp", "alias": "dw_timestamp"},
            {"name": "id", "alias": "dw_item_ledger_entry_id"}
        ]},
        {"endpoint": "PurchaseLines", "columns_list": [
            {"name": "Document_Type", "alias": "document_type"},
            {"name": "Document_No", "alias": "document_no"},
            {"name": "Line_No", "alias": "line_no"},
            {"name": "Buy_from_Vendor_No", "alias": "buy_from_vendor_no"},
            {"name": "Type", "alias": "type"},
            {"name": "No", "alias": "item_no"},
            {"name": "Description", "alias": "item_description"},
            {"name": "Location_Code", "alias": "location_code"},
            {"name": "Quantity", "alias": "quantity"},
            {"name": "Outstanding_Quantity", "alias": "outstanding_quantity"},
            {"name": "DW_EVI_BU", "alias": "dw_evi_bu"},
            {"name": "DW_ERP_System", "alias": "dw_erp_system"},
            {"name": "DW_ERP_Source_Table", "alias": "dw_erp_source_table"},
            {"name": "DW_Timestamp", "alias": "dw_timestamp"},
            {"name": "id", "alias": "dw_purchase_line_id"}
        ]},
        {"endpoint": "SalesLines", "columns_list": [
            {"name": "Document_Type", "alias": "document_type"},
            {"name": "Document_No", "alias": "document_no"},
            {"name": "Line_No", "alias": "line_no"},
            {"name": "Sell_to_Customer_No", "alias": "sell_to_customer_no"},
            {"name": "Type", "alias": "type"},
            {"name": "No", "alias": "item_no"},
            {"name": "Description", "alias": "item_description"},
            {"name": "Location_Code", "alias": "location_code"},
            {"name": "Quantity", "alias": "quantity"},
            {"name": "Outstanding_Quantity", "alias": "outstanding_quantity"},
            {"name": "DW_EVI_BU", "alias": "dw_evi_bu"},
            {"name": "DW_ERP_System", "alias": "dw_erp_system"},
            {"name": "DW_ERP_Source_Table", "alias": "dw_erp_source_table"},
            {"name": "DW_Timestamp", "alias": "dw_timestamp"},
            {"name": "id", "alias": "dw_sales_line_id"}
        ]},
    ]
    # company = "TRS"
    # endpoint = "Locations"
    # container = db.get_container_client(endpoint)
    # query_all_items_by_bu(container, company)

    # endpoint = "Items"
    # company = "TRS"
    # columns_list = [
    #     {"name": "No", "alias": "item_no"},
    #     {"name": "Description", "alias": "item_description"},
    #     {"name": "Blocked", "alias": "item_status"},
    #     {"name": "Inventory_Posting_Group", "alias": "inventory_posting_group"},
    #     {"name": "Gen_Prod_Posting_Group", "alias": "gen_prod_posting_group"},
    #     {"name": "Vendor_No", "alias": "vendor_no"},
    #     {"name": "Vendor_Item_No", "alias": "vendor_item_no"},
    #     {"name": "Item_Category_Code", "alias": "item_category_code"},
    #     {"name": "SSI_Brand", "alias": "brand "},
    #     {"name": "DW_EVI_BU", "alias": "dw_evi_bu"},
    #     {"name": "DW_ERP_System", "alias": "dw_erp_system"},
    #     {"name": "DW_ERP_Source_Table", "alias": "dw_erp_source_table"},
    #     {"name": "id", "alias": "dw_item_id"}
    # ]

    for data in data_to_read:
        date_time_string = date_time.strftime("%m%d%y")
        client = cosmos_client.CosmosClient(HOST, {'masterKey': MASTER_KEY})
        db = client.get_database_client(DATABASE_ID)
        container = db.get_container_client(data["endpoint"])
        for company_short in companies:
            query_items_by_bu_by_columns(
                container, company_short, data["endpoint"], date_time_string, data["columns_list"])


if __name__ == '__main__':
    main()
