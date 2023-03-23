import pandas as pd
from datetime import datetime, timedelta


def compare_cm_si_lines_vs_value_entries(company_short, date_time):

    endpoints = ["Posted_Sales_Invoices_143_DW", "Posted_Sales_Invoice_Lines_526_DW", "Posted_Sales_Credit_Memo_144_DW",
                 "Posted_Sales_Credit_Memo_Lines_527_DW", "Posted_Service_Invoice_5977_DW", "Posted_Service_Invoice_Lines_5979_DW", "Value_Entries_5802_DW"]

    df_value_entries = pd.read_csv("files/from_bc/" + company_short + "_" + "Value_Entries_5802_DW" + "_" +
                                   date_time.strftime("%m%d%y") + '.csv.gz', compression="gzip")

    df_value_entries = df_value_entries[df_value_entries["Document_Type"]
                                        == "Sales Invoice"]
    sales_by_document_no_value_entries = df_value_entries.groupby('Document_No')[
        'Sales_Amount_Actual'].sum()
    print(sales_by_document_no_value_entries)
    print(df_value_entries)

    df_posted_invoice_lines = pd.read_csv("files/from_bc/" + company_short + "_" + "Posted_Sales_Invoice_Lines_526_DW" + "_" +
                                          date_time.strftime("%m%d%y") + '.csv.gz', compression="gzip")

    sales_by_document_no_posted_invoice_lines = df_posted_invoice_lines.groupby('Document_No')[
        'Amount'].sum()
    print(df_posted_invoice_lines)
    print(sales_by_document_no_posted_invoice_lines)

    df_resource_ledger_entries = pd.read_csv("files/from_bc/" + company_short + "_" + "Resource_Ledger_Entries_202_DW" + "_" +
                                             date_time.strftime("%m%d%y") + '.csv.gz', compression="gzip")

    df_resource_ledger_entries["Total_Price"] = df_resource_ledger_entries["Total_Price"] * \
        (-1)
    sales_by_document_no_resource_ledger_entries = df_resource_ledger_entries.groupby('Document_No')[
        'Total_Price'].sum()

    print(df_resource_ledger_entries)
    print(sales_by_document_no_resource_ledger_entries)

    # df_posted_credit_memo_lines = pd.read_csv("files/from_bc/" + company_short + "_" + "Posted_Sales_Credit_Memo_Lines_527_DW" + "_" +
    #                                           date_time.strftime("%m%d%y") + '.csv.gz', compression="gzip")

    # sales_by_document_no_cml = df_posted_credit_memo_lines.groupby('Document_No')[
    #     'Amount'].sum()
    # print(df_posted_credit_memo_lines)
    # print(sales_by_document_no_cml)

    merged_df = pd.merge(sales_by_document_no_value_entries,
                         sales_by_document_no_posted_invoice_lines, on='Document_No', how='outer')
    merged_df = pd.merge(merged_df,
                         sales_by_document_no_resource_ledger_entries, on='Document_No', how='outer')
    merged_df["Compare_Value_Entries_vs_P_Invoice_Lines"] = merged_df["Sales_Amount_Actual"] == merged_df["Amount"]
    merged_df["Compare_Resource_Ledger_Entries_vs_P_Invoice_Lines"] = merged_df["Total_Price"] == merged_df["Amount"]
    print(merged_df)

    merged_df.to_csv(
        "files/tests/sales_by_document_no_ve_sales_by_document_no_pil.csv")


def main():
    date_time = datetime.now()
    date_time = date_time - timedelta(days=4)
    company_short = "FLR"
    companies = {"FLR": "FLOPROD"}

    compare_cm_si_lines_vs_value_entries(company_short, date_time)


if __name__ == "__main__":
    main()
