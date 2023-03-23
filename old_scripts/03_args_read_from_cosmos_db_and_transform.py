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
import sys
import logging

config = dotenv_values(
    ".env")

# logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def define_logger(file_name):
    # create a file handler and set the logging level
    handler = logging.FileHandler(
        f'files/logs/{file_name}.log')
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
    logger.debug('\nQuerying for an  Item by Partition Key\n')

    items = list(container.query_items(
        query="SELECT * FROM c WHERE (c.DW_EVI_BU = @dw_evi_bu)",
        parameters=[
            {"name": "@dw_evi_bu", "value": company}
        ],
        enable_cross_partition_query=True,
    ))

    df = pd.DataFrame.from_records(items)
    logger.debug(df)


def query_items_by_bu_by_columns(container, company_short, endpoint, date_time_string, columns_list):
    logger.debug('\nQuerying for an  Item by Partition Key\n')
    columns = columns_list_to_string(columns_list)
    logger.debug(columns)

    items = list(container.query_items(
        query="SELECT "+columns +
        " FROM c WHERE (c.DW_EVI_BU = @dw_evi_bu)",
        parameters=[
            {"name": "@dw_evi_bu", "value": company_short}
        ],
        enable_cross_partition_query=True,
    ))

    df = pd.DataFrame.from_records(items)
    logger.debug(df)
    logger.debug(
        f"Saving data to csv file: {company_short}_{endpoint}_{date_time_string}.csv.gz")

    df.to_csv(f'C:/Users/eviadmin/Documents/Datawarehouse/python_scripts/DataLake/test/files/from_datalake/{company_short}_{endpoint}_{date_time_string}.csv.gz',
              compression="gzip", index=False)


def create_sql_columns(data_to_read, columns_list):
    for data in data_to_read:
        columns_list = data["columns_list"]
        logger.debug(f'------- Columns for {data["endpoint"]} --------')
        for row in columns_list:
            if (row["alias"] == "dw_evi_bu"):
                logger.debug(f'{row["alias"]} char(10),')
            elif (row["alias"] == "dw_erp_system"):
                logger.debug(f'{row["alias"]} char(10),')
            elif (row["alias"] == "dw_erp_system"):
                logger.debug(f'{row["alias"]} char(10),')
            else:
                logger.debug(f'{row["alias"]} varchar(50),')


def main():
    date_time = datetime.now()

    if (len(sys.argv) == 3):
        company_short = sys.argv[1]
        endpoint = sys.argv[2]
        logger = define_logger(
            f"03_read_from_cosmos_db_and_transform_{company_short}_{endpoint}")
        logger.debug(
            f'-------- Executing 03 read_from_cosmosdb_and_transform ---------')
        logger.debug(f'Datetime = {date_time.strftime("%m-%d-%y %H:%M:%S")}')
        logger.debug(f'company_short = {company_short}, endpoint = {endpoint}')

        companies = {"TRS": "TRSPROD01", "FLR": "FLOPROD", "CTL": "CTLPROD"}
        # companies = {"TRS": "TRSPROD01", "FLR": "FLOPROD", "CTL": "CTLPROD"}
        # companies = {"CTL": "CTLPROD"}
        endpoints = {"PostedSalesInvoicesHeaders": "Posted_Sales_Invoices_143_DW", "PostedSalesInvoicesLines": "Posted_Sales_Invoice_Lines_526_DW", "PostedSalesCreditMemoHeaders":
                     "Posted_Sales_Credit_Memo_144_DW", "PostedSalesCreditMemoLines": "Posted_Sales_Credit_Memo_Lines_527_DW", "ResourceLedgerEntries": "Resource_Ledger_Entries_202_DW"}

        data_to_read = {
            "Items": {"columns_list": [
                {"name": "No", "alias": "item_no"},
                {"name": "Description", "alias": "item_description"},
                {"name": "Blocked", "alias": "item_status"},
                {"name": "Inventory_Posting_Group",
                    "alias": "inventory_posting_group"},
                {"name": "Gen_Prod_Posting_Group",
                    "alias": "gen_prod_posting_group"},
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
            "Locations": {"columns_list": [
                {"name": "Code", "alias": "location_code"},
                {"name": "Name", "alias": "location_name"},
                {"name": "DW_EVI_BU", "alias": "dw_evi_bu"},
                {"name": "DW_ERP_System", "alias": "dw_erp_system"},
                {"name": "DW_ERP_Source_Table", "alias": "dw_erp_source_table"},
                {"name": "DW_Timestamp", "alias": "dw_timestamp"},
                {"name": "id", "alias": "dw_location_id"}
            ]},
            "ItemLedgerEntries": {"columns_list": [
                {"name": "Entry_No", "alias": "entry_no"},
                {"name": "Entry_Type", "alias": "entry_type"},
                {"name": "Document_Type", "alias": "document_type"},
                {"name": "Document_No", "alias": "document_no"},
                {"name": "Item_No", "alias": "item_no"},
                {"name": "Description", "alias": "item_description"},
                {"name": "Global_Dimension_1_Code",
                    "alias": "global_dimension_1_code"},
                {"name": "Global_Dimension_2_Code",
                    "alias": "global_dimension_2_code"},
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
            "PurchaseLines":
            {"columns_list": [
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
            "SalesLines":
            {"columns_list": [
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
            "ValueEntries":
            {"columns_list": [
                {"name": "Entry_No", "alias": "entry_no"},
                {"name": "Entry_Type", "alias": "entry_type"},
                {"name": "Item_Ledger_Entry_Type",
                    "alias": "item_ledger_entry_type"},
                {"name": "Document_Type", "alias": "document_type"},
                {"name": "Document_No", "alias": "document_no"},
                {"name": "Item_No", "alias": "item_no"},
                {"name": "Global_Dimension_1_Code",
                    "alias": "global_dimension_1_code"},
                {"name": "Global_Dimension_2_Code",
                    "alias": "global_dimension_2_code"},
                {"name": "Location_Code", "alias": "location_code"},
                {"name": "User_ID", "alias": "user_id"},
                {"name": "Item_Ledger_Entry_No", "alias": "item_ledger_entry_no"},
                {"name": "Gen_Bus_Posting_Group", "alias": "gen_bus_posting_group"},
                {"name": "Gen_Prod_Posting_Group",
                    "alias": "gen_prod_posting_group"},
                {"name": "Document_Date", "alias": "document_date"},
                {"name": "Posting_Date", "alias": "posting_date"},
                {"name": "Cost_Amount_Actual", "alias": "cost_amount_actual"},
                {"name": "Sales_Amount_Actual", "alias": "sales_amount_actual"},
                {"name": "Invoiced_Quantity", "alias": "invoiced_quantity"},
                {"name": "Item_Ledger_Entry_Quantity",
                    "alias": "item_ledger_entry_quantity"},
                {"name": "Salespers_Purch_Code", "alias": "salespers_purch_code"},
                {"name": "SystemCreatedBy", "alias": "system_created_by"},
                {"name": "DW_EVI_BU", "alias": "dw_evi_bu"},
                {"name": "DW_ERP_System", "alias": "dw_erp_system"},
                {"name": "DW_ERP_Source_Table", "alias": "dw_erp_source_table"},
                {"name": "DW_Timestamp", "alias": "dw_timestamp"},
                {"name": "id", "alias": "dw_value_entry_id"}
            ]},
            "PostedSalesInvoicesHeaders":
            {"columns_list": [
                {"name": "No", "alias": "no"},
                {"name": "Order_No", "alias": "order_no"},
                {"name": "Sell_to_Customer_No", "alias": "sell_to_customer_no"},
                {"name": "Amount", "alias": "amount"},
                {"name": "Bill_to_Customer_No", "alias": "bill_to_customer_no"},
                {"name": "Posting_Date", "alias": "posting_date"},
                {"name": "Salesperson_Code", "alias": "salesperson_code"},
                {"name": "Shortcut_Dimension_1_Code",
                    "alias": "shortcut_dimension_1_code"},
                {"name": "Shortcut_Dimension_2_Code",
                    "alias": "shortcut_dimension_2_code"},
                {"name": "Location_Code", "alias": "location_code"},
                {"name": "Document_Date", "alias": "document_date"},
                {"name": "External_Document_No", "alias": "external_document_no"},
                {"name": "Payment_Terms_Code", "alias": "payment_terms_code"},
                {"name": "Shipment_Method_Code", "alias": "shipment_method_code"},
                {"name": "SSI_Closed_Date", "alias": "ssi_closed_date"},
                {"name": "System_Created_By_User",
                    "alias": "system_created_by_user"},
                {"name": "SystemCreatedBy", "alias": "system_created_by"},
                {"name": "SystemCreatedAt", "alias": "system_created_at"},
                {"name": "SystemModifiedAt", "alias": "system_modified_at"},
                {"name": "Fieldpoint_WorkOrder", "alias": "fieldpoint_workorder"},
                {"name": "DW_EVI_BU", "alias": "dw_evi_bu"},
                {"name": "DW_ERP_System", "alias": "dw_erp_system"},
                {"name": "DW_Timestamp", "alias": "dw_timestamp"},
                {"name": "DW_ERP_Source_Table", "alias": "dw_erp_source_table"},
                {"name": "id", "alias": "dw_posted_sale_invoice_header_id"}
            ]},
            "PostedSalesInvoicesLines":
            {"columns_list": [
                {"name": "Document_No", "alias": "document_no"},
                {"name": "Line_No", "alias": "line_no"},
                {"name": "Sell_to_Customer_No",
                 "alias": "sell_to_customer_no"},
                {"name": "Type", "alias": "type"},
                {"name": "No", "alias": "no"},
                {"name": "Description", "alias": "description"},
                {"name": "Shortcut_Dimension_1_Code",
                 "alias": "shortcut_dimension_1_code"},
                {"name": "Shortcut_Dimension_2_Code",
                 "alias": "shortcut_dimension_2_code"},
                {"name": "Quantity", "alias": "quantity"},
                {"name": "Unit_Price", "alias": "unit_price"},
                {"name": "Amount", "alias": "amount"},
                {"name": "Amount_Including_VAT", "alias": "amount_including_vat"},
                {"name": "DW_EVI_BU", "alias": "dw_evi_bu"},
                {"name": "DW_ERP_System", "alias": "dw_erp_system"},
                {"name": "DW_ERP_Source_Table", "alias": "dw_erp_source_table"},
                {"name": "DW_Timestamp", "alias": "dw_timestamp"},
                {"name": "id", "alias": "dw_posted_sale_invoice_line_id"}
            ]},
            "PostedSalesCreditMemoHeaders":
            {"columns_list": [
                {"name": "No", "alias": "no"},
                {"name": "Sell_to_Customer_No", "alias": "sell_to_customer_no"},
                {"name": "Due_Date", "alias": "due_date"},
                {"name": "Amount", "alias": "amount"},
                {"name": "Amount_Including_VAT", "alias": "amount_including_vat"},
                {"name": "Sell_to_Contact", "alias": "sell_to_contact"},
                {"name": "Bill_to_Customer_No", "alias": "bill_to_customer_no"},
                {"name": "Posting_Date", "alias": "posting_date"},
                {"name": "Salesperson_Code", "alias": "salesperson_code"},
                {"name": "Shortcut_Dimension_1_Code",
                    "alias": "shortcut_dimension_1_code"},
                {"name": "Shortcut_Dimension_2_Code",
                    "alias": "shortcut_dimension_2_code"},
                {"name": "Location_Code", "alias": "location_code"},
                {"name": "Document_Date", "alias": "document_date"},
                {"name": "System_Created_By_User",
                    "alias": "system_created_by_user"},
                {"name": "Applies_to_Doc_Type", "alias": "applies_to_doc_type"},
                {"name": "SSI_Applies_to_Doc_No", "alias": "ssi_applies_to_doc_no"},
                {"name": "SystemCreatedAt", "alias": "system_created_at"},
                {"name": "SystemCreatedBy", "alias": "system_created_by"},
                {"name": "SystemModifiedAt", "alias": "system_modified_at"},
                {"name": "SystemModifiedBy", "alias": "system_modified_by"},
                {"name": "DW_EVI_BU", "alias": "dw_evi_bu"},
                {"name": "DW_ERP_System", "alias": "dw_erp_system"},
                {"name": "DW_ERP_Source_Table", "alias": "dw_erp_source_table"},
                {"name": "DW_Timestamp", "alias": "dw_timestamp"},
                {"name": "id", "alias": "dw_posted_sale_credit_memo_header_id"}
            ]},
            "PostedSalesCreditMemoLines":
            {"columns_list": [
                {"name": "Document_No", "alias": "document_no"},
                {"name": "Line_No", "alias": "line_no"},
                {"name": "Sell_to_Customer_No", "alias": "sell_to_customer_no"},
                {"name": "Type", "alias": "type"},
                {"name": "No", "alias": "no"},
                {"name": "Description", "alias": "description"},
                {"name": "Shortcut_Dimension_1_Code",
                    "alias": "shortcut_dimension_1_code"},
                {"name": "Shortcut_Dimension_2_Code",
                    "alias": "shortcut_dimension_2_code"},
                {"name": "Quantity", "alias": "quantity"},
                {"name": "Unit_Price", "alias": "unit_price"},
                {"name": "Amount", "alias": "amount"},
                {"name": "Amount_Including_VAT", "alias": "amount_including_vat"},
                {"name": "DW_EVI_BU", "alias": "dw_evi_bu"},
                {"name": "DW_ERP_System", "alias": "dw_erp_system"},
                {"name": "DW_Timestamp", "alias": "dw_timestamp"},
                {"name": "DW_ERP_Source_Table", "alias": "dw_erp_source_table"},
                {"name": "id", "alias": "dw_posted_sale_credit_memo_line_id"}
            ]},
            "ResourceLedgerEntries":
            {"columns_list": [
                {'name': 'Entry_No', 'alias': 'entry_no'},
                {'name': 'Posting_Date', 'alias': 'posting_date'},
                {'name': 'Entry_Type', 'alias': 'entry_type'},
                {'name': 'Document_No', 'alias': 'document_no'},
                {'name': 'Resource_No', 'alias': 'resource_no'},
                {'name': 'Description', 'alias': 'description'},
                {'name': 'Global_Dimension_1_Code',
                    'alias': 'global_dimension_1_code'},
                {'name': 'Global_Dimension_2_Code',
                    'alias': 'global_dimension_2_code'},
                {'name': 'Quantity', 'alias': 'quantity'},
                {'name': 'Unit_Cost', 'alias': 'unit_cost'},
                {'name': 'Total_Cost', 'alias': 'total_cost'},
                {'name': 'Unit_Price', 'alias': 'unit_price'},
                {'name': 'Total_Price', 'alias': 'total_price'},
                {'name': 'User_ID', 'alias': 'user_id'},
                {'name': 'Shortcut_Dimension_3_Code',
                    'alias': 'shortcut_dimension_3_code'},
                {'name': 'Shortcut_Dimension_4_Code',
                    'alias': 'shortcut_dimension_4_code'},
                {'name': 'Shortcut_Dimension_5_Code',
                    'alias': 'shortcut_dimension_5_code'},
                {'name': 'Shortcut_Dimension_6_Code',
                    'alias': 'shortcut_dimension_6_code'},
                {'name': 'Shortcut_Dimension_7_Code',
                    'alias': 'shortcut_dimension_7_code'},
                {'name': 'Shortcut_Dimension_8_Code',
                    'alias': 'shortcut_dimension_8_code'},
                {'name': 'DW_EVI_BU', 'alias': 'dw_evi_bu'},
                {'name': 'DW_ERP_System', 'alias': 'dw_erp_system'},
                {'name': 'DW_Timestamp', 'alias': 'dw_timestamp'},
                {'name': 'DW_ERP_Source_Table', 'alias': 'dw_erp_source_table'},
                {"name": "id", "alias": "dw_resource_ledger_entry_id"}
            ]},
            "Salespeople":
            {"columns_list": [
                {'name': 'Code', 'alias': 'salesperson_no'},
                {'name': 'Name', 'alias': 'salesperson_name'},
                {'name': 'DW_EVI_BU', 'alias': 'dw_evi_bu'},
                {'name': 'DW_ERP_System', 'alias': 'dw_erp_system'},
                {'name': 'DW_Timestamp', 'alias': 'dw_timestamp'},
                {'name': 'DW_ERP_Source_Table', 'alias': 'dw_erp_source_table'},
                {"name": "id", "alias": "dw_salesperson_id"}
            ]}
        }

        data = data_to_read[endpoint]
        logger.debug(data)

        date_time_string = date_time.strftime("%m%d%y")
        client = cosmos_client.CosmosClient(HOST, {'masterKey': MASTER_KEY})
        db = client.get_database_client(DATABASE_ID)
        container = db.get_container_client(endpoint)

        query_items_by_bu_by_columns(
            container, company_short, endpoint, date_time_string, data["columns_list"])
    else:
        logger.debug(f'Error not enought arguments for the script to run')


if __name__ == '__main__':
    start = timeit.default_timer()
    main()
    end = timeit.default_timer()
    logger.debug(f'Duration: {end-start} secs')
    logger.debug(f'Duration: {(end-start)/60} mins')
