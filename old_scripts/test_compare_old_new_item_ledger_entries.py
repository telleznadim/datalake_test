from datetime import datetime, timedelta
import pandas as pd


def compare(company_short, date_time, endpoint):
    df_item_ledger_today = pd.read_csv("C:/Users/eviadmin/Documents/Datawarehouse/python_scripts/DataLake/prod/files/from_bc/" + company_short + "_" + endpoint + "_" +
                                       date_time.strftime("%m%d%y") + '.csv.gz', compression="gzip")
    print(df_item_ledger_today)


def main():
    date_time_today = datetime.now()
    date_time_before = date_time_today - timedelta(days=4)
    endpoint = "Item_Ledger_Entries_38_DW"
    company_short = "FLR"
    companies = {"FLR": "FLOPROD"}

    compare(company_short, date_time_today, endpoint)


if __name__ == "__main__":
    main()
