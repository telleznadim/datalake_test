import pandas as pd
from datetime import datetime

date_time = datetime.now()
company_short = "FLR"
endpoint = "Sales_Order_Lines_46_DW"

print(company_short + "_" +
      endpoint + "_" + date_time.strftime("%m%d%y") + '.csv.gz')

df_sol_46 = pd.read_csv("C:/Users/eviadmin/Documents/Datawarehouse/python_scripts/DataLake/prod/files/from_bc/" + company_short + "_" +
                        endpoint + "_" + date_time.strftime("%m%d%y") + '.csv.gz', compression="gzip")


endpoint = "Sales_Lines_516_DW"

df_sol_516 = pd.read_csv("C:/Users/eviadmin/Documents/Datawarehouse/python_scripts/DataLake/prod/files/from_bc/" + company_short + "_" +
                         endpoint + "_" + date_time.strftime("%m%d%y") + '.csv.gz', compression="gzip")

df_sol_516 = df_sol_516[df_sol_516["Document_Type"] == 'Order']


sol_46_column = "Qty_Minus_Qty_Shipped"

df_sol_46["Qty_Minus_Qty_Shipped"] = df_sol_46["Quantity"] - \
    df_sol_46["Quantity_Shipped"]
df_sol_46_quantities = df_sol_46.groupby('No')[
    sol_46_column].sum()

# Qty_to_Ship	Quantity_Shipped

df_sol_516_outstanding_quantities = df_sol_516.groupby('No')[
    'Outstanding_Quantity'].sum()

print(df_sol_46_quantities)
print(df_sol_516_outstanding_quantities)

merged_df = pd.merge(df_sol_46_quantities,
                     df_sol_516_outstanding_quantities, on='No', how='outer')

merged_df["Compare"] = merged_df[sol_46_column] == merged_df["Outstanding_Quantity"]
print(merged_df[~merged_df["Compare"]])

merged_df.to_csv(
    f"files/tests/sl_46_{sol_46_column}_to_ship_vs_sl_516_outstanding_quantity")
