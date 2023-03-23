import requests
import pandas as pd
import timeit
from datetime import datetime
import logging
from dotenv import dotenv_values

config = dotenv_values("../.env")
logger = logging.getLogger(__name__)


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
    print(response_dict)
    # print(response_dict["access_token"])
    logger.debug(
        f"Retreiving Token from API")
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
    df.to_csv("files/from_bc/" + company_short + "_" + endpoint + "_" +
              date_time.strftime("%m%d%y") + '.csv.gz', index=False, compression="gzip")
    logger.debug(
        f"Data saved to file: files/from_bc/{company_short}_{endpoint}_{date_time.strftime('%m%d%y')}.csv.gz")


def getWebServicePagination(date_time, endpoint, company_bc, company_short, token):
    response = requests.get(
        f"https://api.businesscentral.dynamics.com/v2.0/e5082643-6166-4ecc-b73f-5fb7c697d999/Sandbox120722/ODataV4/Company('{company_bc}')/{endpoint}", headers={'Authorization': f'Bearer {token}'})
    # print(response.text)
    response_dict = response.json()
    # print(response_dict)
    df = pd.DataFrame.from_dict(response_dict["value"])

    while ("@odata.nextLink" in response_dict):
        response = requests.get(
            response_dict["@odata.nextLink"], headers={'Authorization': f'Bearer {token}'})
        response_dict = response.json()
        df1 = pd.DataFrame.from_dict(response_dict["value"])
        df = pd.concat([df, df1], ignore_index=True, sort=False)

    # print("FINAL FINAL NO VA MAS")

    df = add_dw_columns(df, date_time, endpoint, company_short, "BC")
    print(df)
    print(df["DW_Timestamp"])
    df.to_csv("C:/Users/eviadmin/Documents/Datawarehouse/python_scripts/DataLake/test/item_ledger_tests/files/from_bc/" + company_short + "_" +
              endpoint + "_" + date_time.strftime("%m%d%y") + '.csv.gz', index=False, compression="gzip")


def main():
    date_time = datetime.now()

    # Retrieve TOKEN
    token = oauth_post_requests_client_credentials()

    # endpoints = ["Purchase_Lines_518_DW",
    #              "Sales_Lines_516_DW", "Item_Ledger_Entries_38_DW", "Items_31_DW", "Locations_15_DW"]
    endpoints = ["Item_Ledger_Entries_38_DW"]
    # endpoints = ["Items_31_DW"]

    for endpoint in endpoints:
        # token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6Ii1LSTNROW5OUjdiUm9meG1lWm9YcWJIWkdldyIsImtpZCI6Ii1LSTNROW5OUjdiUm9meG1lWm9YcWJIWkdldyJ9.eyJhdWQiOiJodHRwczovL2FwaS5idXNpbmVzc2NlbnRyYWwuZHluYW1pY3MuY29tIiwiaXNzIjoiaHR0cHM6Ly9zdHMud2luZG93cy5uZXQvZTUwODI2NDMtNjE2Ni00ZWNjLWI3M2YtNWZiN2M2OTdkOTk5LyIsImlhdCI6MTY3NTE3MzA2OCwibmJmIjoxNjc1MTczMDY4LCJleHAiOjE2NzUxNzgxMTQsImFjciI6IjEiLCJhaW8iOiJBVlFBcS84VEFBQUFWRlY3LzRjNUNJQWVZcWFpUlhjMkxHZU1xdW5CWHlNZWJaSVB3UjAydEVyQXh4THovMkRtZzJ1N3BNb2d6NTdtd2F4MXpHazhxQzNJWjFJaFc5YkxRYVhndmhPa1pweUF4YWVrK2RiRHF2UT0iLCJhbXIiOlsicHdkIiwibWZhIl0sImFwcGlkIjoiZTllMWQwNWQtZDRiNS00OWVjLWI0NjgtYzA1NTZmZjAzNzZhIiwiYXBwaWRhY3IiOiIxIiwiZmFtaWx5X25hbWUiOiJUZWxsZXoiLCJnaXZlbl9uYW1lIjoiTmFkaW0iLCJpcGFkZHIiOiIxNzkuMzMuMTQuMTQwIiwibmFtZSI6Ik5hZGltIFRlbGxleiIsIm9pZCI6IjQ3NWM2MTdkLTY3NDEtNDBjNy04NWVkLTYzNDBmZTdmNjdjZCIsInB1aWQiOiIxMDAzMjAwMUZCMDYyOTQ3IiwicmgiOiIwLkFSY0FReVlJNVdaaHpFNjNQMS0zeHBmWm1UM3ZiWmxzczFOQmhnZW1fVHdCdUo4WEFJOC4iLCJzY3AiOiJGaW5hbmNpYWxzLlJlYWRXcml0ZS5BbGwgdXNlcl9pbXBlcnNvbmF0aW9uIiwic3ViIjoieHdxVE1MUFRUMXp5LVZSUUY4ODFwV1dNdG1VNTMwdzB1MTNsU0ZDajI0cyIsInRpZCI6ImU1MDgyNjQzLTYxNjYtNGVjYy1iNzNmLTVmYjdjNjk3ZDk5OSIsInVuaXF1ZV9uYW1lIjoiTnRlbGxlekBldmktaW5kLmNvbSIsInVwbiI6Ik50ZWxsZXpAZXZpLWluZC5jb20iLCJ1dGkiOiJjaUFNQXVSRFRFaXhCYThfNmJjOUFRIiwidmVyIjoiMS4wIiwid2lkcyI6WyIxMTY0ODU5Ny05MjZjLTRjZjMtOWMzNi1iY2ViYjBiYThkY2MiLCJhOWVhODk5Ni0xMjJmLTRjNzQtOTUyMC04ZWRjZDE5MjgyNmMiLCJiNzlmYmY0ZC0zZWY5LTQ2ODktODE0My03NmIxOTRlODU1MDkiXX0.HJY5uNSlpA3r9q7Ot65THEeEpNpTcHXHArosc26MmBGLMHLgml80oEnrjs0ON6u0zRRGvZZeCd2HBMwdY0IOHKmUT11UBSr1NPYdztgkgRWwHqULmzO9o2BP-legn0aE-hSgb0e71acjBopnoLuhcVioLsglQX6AIzHvrVodIovgPcq3g5_Q7Y4D3lpNCpBjMQ0XUO1kisMRn2zv4pks7eSpoQlsfzy4mBUaM36Zz_oG-taWRdzU7dDcM3rUgUNvoRIABEd_qU1yqArsp_nkK0rfgNIiR3xPsbrBLjwqOaP7Pd1mgjwMCaKLn1v7KguP1JiTO0EvaTDoifkcdiS5zQ"
        companies = {"TRS": "TRSPROD01"}
        # companies = {"TRS": "TRSPROD01", "FLR": "FLRPROD", "CTL": "CTLPROD"}
        for company_short in companies:
            start = timeit.default_timer()
            print(
                f'------ Getting {endpoint} data , company: {company_short} ------')
            # company = "TRSPROD01"
            # endpoint = "Purchase_Lines_518_DW"
            getWebServicePagination(
                date_time, endpoint, companies[company_short], company_short, token)
            end = timeit.default_timer()
            print("Duration: ", end-start, "secs")
            print("Duration: ", (end-start)/60, "mins")


if __name__ == "__main__":
    main()
