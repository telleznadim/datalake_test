import pandas as pd
from datetime import datetime, timedelta

date_time = datetime.now()
date_time = date_time - timedelta(days=1)
company_short = "FLR"
endpoint = "Items_31_DW"

print(company_short + "_" +
      endpoint + "_" + date_time.strftime("%m%d%y") + '.csv.gz')

# FLR_Items_31_DW_022323.csv.gz


df_flr_items = pd.read_csv("C:/Users/eviadmin/Documents/Datawarehouse/python_scripts/DataLake/prod/files/from_bc/" + company_short + "_" +
                           endpoint + "_" + date_time.strftime("%m%d%y") + '.csv.gz', compression="gzip")


df_flr_items = df_flr_items.loc[:, [
    'No', 'Description', 'DW_EVI_BU', "Inventory_Posting_Group", "Qty_on_Purch_Order", "Qty_on_Sales_Order"]].copy()

# df_flr_items = df_flr_items[df_flr_items["Inventory_Posting_Group"] == "PRT-NEW"]

print(df_flr_items)
print(df_flr_items[df_flr_items["No"] == "INS-BLR"])

company_short = "TRS"

df_trs_items = pd.read_csv("C:/Users/eviadmin/Documents/Datawarehouse/python_scripts/DataLake/prod/files/from_bc/" + company_short + "_" +
                           endpoint + "_" + date_time.strftime("%m%d%y") + '.csv.gz', compression="gzip")

df_trs_items = df_trs_items.loc[:, [
    'No', 'Description', 'DW_EVI_BU', "Inventory_Posting_Group", "Qty_on_Purch_Order", "Qty_on_Sales_Order"]].copy()

# df_trs_items = df_trs_items[df_trs_items["Inventory_Posting_Group"] == "PRT-NEW"]

print(df_trs_items[df_trs_items["No"] == "INS-BLR"])

merged_df = pd.merge(df_flr_items,
                     df_trs_items, on='No', how='inner', suffixes=('_FLR', '_TRS'))

print(merged_df)

merged_df.to_excel(
    f"files/tests/duplicated_items_FLR_TRS.xlsx", index=False)
