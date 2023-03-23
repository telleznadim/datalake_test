import requests
import pandas as pd
import timeit
from datetime import datetime
import logging
from dotenv import dotenv_values
import sys

# .env
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


def add_dw_columns(df, date_time, endpoint, company, erp):
    df["DW_EVI_BU"] = company
    df["DW_ERP_System"] = erp
    df["DW_Timestamp"] = date_time
    df["DW_ERP_Source_Table"] = endpoint
    return (df)


def oauth_post_requests_client_credentials():
    CLIENT_ID = config['bc_api_client_id']
    CLIENT_SECRET = config['bc_api_client_secret']
    TOKEN_URL = config['bc_api_token_url']
    SCOPE = config['bc_api_scope']
    logger.debug(
        f"Retreiving Token from API")
    response = requests.post(
        TOKEN_URL,
        data={
            'grant_type': 'client_credentials',
            'client_id': CLIENT_ID,
            'client_secret': CLIENT_SECRET,
            "scope": SCOPE
        }
    )
    response_dict = response.json()
    # logger.debug(response_dict)
    # logger.debug((response_dict["access_token"])

    return (response_dict["access_token"])


def getWebServicePaginationChatGPT(date_time, endpoint, company_bc, company_short, token):
    base_url = f"https://api.businesscentral.dynamics.com/v2.0/e5082643-6166-4ecc-b73f-5fb7c697d999/Production/ODataV4/Company('{company_bc}')/{endpoint}"
    headers = {'Authorization': f'Bearer {token}'}
    response = requests.get(base_url, headers=headers)
    response_dict = response.json()

    df = pd.DataFrame.from_dict(response_dict["value"])

    while ("@odata.nextLink" in response_dict):
        response = requests.get(
            response_dict["@odata.nextLink"], headers=headers)
        response_dict = response.json()
        df1 = pd.DataFrame.from_dict(response_dict["value"])
        df = pd.concat([df, df1], ignore_index=True, sort=False)

    logger.debug("All API requests completed.")

    df = add_dw_columns(df, date_time, endpoint, company_short, "BC")
    logger.debug(f"Dataframe created: {df}")
    df.to_csv("C:/Users/eviadmin/Documents/Datawarehouse/python_scripts/DataLake/" + output_folder + "/files/from_bc/" + company_short + "_" + endpoint + "_" +
              date_time.strftime("%m%d%y") + '.csv.gz', index=False, compression="gzip")
    logger.debug(
        f"Data saved to file: C:/Users/eviadmin/Documents/Datawarehouse/python_scripts/DataLake/" + output_folder + "/files/from_bc/{company_short}_{endpoint}_{date_time.strftime('%m%d%y')}.csv.gz")


def getWebServicePagination(date_time, endpoint, company_bc, company_short, token):
    response = requests.get(
        f"https://api.businesscentral.dynamics.com/v2.0/e5082643-6166-4ecc-b73f-5fb7c697d999/Production/ODataV4/Company('{company_bc}')/{endpoint}", headers={'Authorization': f'Bearer {token}'})
    # logger.debug((response.text)
    response_dict = response.json()

    try:
        df = pd.DataFrame.from_dict(response_dict["value"])
        logger.debug(df)

        while ("@odata.nextLink" in response_dict):
            response = requests.get(
                response_dict["@odata.nextLink"], headers={'Authorization': f'Bearer {token}'})
            response_dict = response.json()
            df1 = pd.DataFrame.from_dict(response_dict["value"])
            df = pd.concat([df, df1], ignore_index=True, sort=False)

        df = add_dw_columns(df, date_time, endpoint, company_short, "BC")
        logger.debug(df)
        df.to_csv("C:/Users/eviadmin/Documents/Datawarehouse/python_scripts/DataLake/" + output_folder + "/files/from_bc/" + company_short + "_" +
                  endpoint + "_" + date_time.strftime("%m%d%y") + '.csv.gz', index=False, compression="gzip")
    except Exception as e:
        logger.error('Error: %s', str(e))


def main():

    if (len(sys.argv) == 3):
        date_time = datetime.now()
        company_short = sys.argv[1]
        endpoint = sys.argv[2]
        logger = define_logger(
            f"01_get_data_from_bc_{company_short}_{endpoint}")

        logger.debug(f'-------- Executing 01 get_data_from_bc ---------')
        logger.debug(f'Datetime = {date_time.strftime("%m-%d-%y %H:%M:%S") }')
        logger.debug(f'company_short = {company_short}, endpoint = {endpoint}')

        # Retrieving TOKEN
        token = oauth_post_requests_client_credentials()

        # endpoints = ["Purchase_Lines_518_DW",
        #              "Sales_Lines_516_DW", "Item_Ledger_Entries_38_DW", "Items_31_DW", "Locations_15_DW"]
        # companies = {"TRS": "TRSPROD01", "FLR": "FLOPROD"}
        companies = {"TRS": "TRSPROD01", "FLR": "FLOPROD",
                     "CTL": "CTLPROD", "SEED": "SEED01"}
        endpoints = {"PurchaseLines": "Purchase_Lines_518_DW",
                     "PurchaseLines2": "Purchase_Order_Line_54_DW",
                     "Brands": "SSI_Brands_50201_DW",
                     "SalesOrderHeaders": "Sales_Order_List_9305_DW",
                     "Customers": "Customers_22_DW",
                     "Salespeople": "Salespersons_Purchasers_14_DW",
                     "ValueEntries": "Value_Entries_5802_DW",
                     "PostedSalesInvoiceHeaders": "Posted_Sales_Invoices_143_DW",
                     "PostedSalesInvoiceLines": "Posted_Sales_Invoice_Lines_526_DW",
                     "PostedSalesCreditMemoHeaders": "Posted_Sales_Credit_Memo_144_DW",
                     "PostedSalesCreditMemoLines": "Posted_Sales_Credit_Memo_Lines_527_DW",
                     "ItemCard": "Item_Card_30_DW",
                     "SalesLines": "Sales_Order_Lines_46_DW", "SalesOrderLines_nounitcost": "Sales_Lines_516_DW", "ItemLedgerEntries": "Item_Ledger_Entries_38_DW", "Items": "Items_31_DW", "Locations": "Locations_15_DW"}

        logger.debug(
            f'------ Getting {endpoints[endpoint]} data , company: {company_short} ------')
        getWebServicePagination(
            date_time, endpoints[endpoint], companies[company_short], company_short, token)

    else:
        logger.error(f'Error not enought arguments for the script to run')


if __name__ == "__main__":
    start = timeit.default_timer()
    main()
    end = timeit.default_timer()
    logger.debug(f'Duration: {end-start} secs')
    logger.debug(f'Duration: {(end-start)/60} mins')
