import pandas as pd
import os
import wrds
import numpy as np
conn = wrds.Connection()

all_currencies = {}
for file in sorted(os.listdir("currencies")):
    if file.endswith(".csv"):
        file_path = os.path.join("currencies", file)
        temp_df = pd.read_csv(file_path)
        temp_df["date"] = pd.to_datetime(temp_df["date"])
        all_currencies[file[:3]] = temp_df

quarterly_not_needed = ["indfmt", "datafmt", "consol", "popsrc", "acctstdq", "fyr", "bsprq", "compstq", "datacqtr", "datafqtr",
                                        "fqtr", "fyearq", "pdq", "pdsa", "pdytd", "rp", "scfq", "srcq", "staltq", "updq", "fdateq", "pdateq", "sedol",
                                        "exchg", "iid", "costat"]
quarterly_metadata = ["gvkey", "datadate", "curcdq", "conm", "isin", "fic", "loc"]

annual_not_needed = ["indfmt", "datafmt", "consol", "popsrc", "acctstd", "acqmeth", "bspr", "compst", "final", "fyear",
                        "ismod", "pddur", "scf", "src", "stalt", "upd", "fdate", "pdate", "sedol", "exchg", "fyr",
                        "iid", "costat", "naicsh", "sich", "rank", "au", "auop", "ajexi", "cshoi", "cshpria", "epsexcon",
                        "epsincon", "epsexnc", "epsinnc", "icapi", "nicon", "ninc", "pv", "tstkni"]
annual_metadata = ["gvkey", "datadate", "curcd", "curcdi", "conm", "isin", "fic", "loc"]

quarterly_data_all = pd.DataFrame()
annual_data_all = pd.DataFrame()

companies = pd.read_csv("Overview Companies.csv", sep=";", converters={"GVKEY (Compustat)": str})
company_subset = companies[["GVKEY (Compustat)"]]
for x, y in company_subset.iterrows():

    quarterly_data = conn.raw_sql(f"""select *
                            from comp_global_daily.g_fundq
                            where gvkey = '270979' AND consol='C' AND datafmt='HIST_STD' AND popsrc='I'""")
    quarterly_data.drop(columns=quarterly_not_needed, axis=1, inplace=True)
    quarterly_data["datadate"] = pd.to_datetime(quarterly_data["datadate"])
    start_date = pd.to_datetime("20040101", format="%Y%m%d")
    quarterly_data = quarterly_data[quarterly_data["datadate"] >= start_date]
    quarterly_data.fillna(value=np.nan, inplace=True)
    for index, row in quarterly_data.iterrows():
        currency = row["curcdq"]
        if currency != "EUR":
            date = row["datadate"]
            currency_df = all_currencies[currency]
            before_date_df = currency_df[currency_df["date"] <= date]
            most_recent_row = before_date_df.loc[before_date_df["date"].idxmax()]
            exchange_value = most_recent_row["PX_LAST"]
            for column in quarterly_data.columns:
                if column in quarterly_metadata:
                    continue
                if pd.notnull(row[column]):
                    quarterly_data.at[index, column] = row[column] * exchange_value

    annual_data = conn.raw_sql(f"""select *
                            from comp_global_daily.g_funda
                            where gvkey = '{y[0]}' AND consol='C' AND datafmt='HIST_STD' AND popsrc='I'""")
    annual_data.drop(columns=annual_not_needed, axis=1, inplace=True)
    annual_data["datadate"] = pd.to_datetime(annual_data["datadate"])
    start_date = pd.to_datetime("20040101", format="%Y%m%d")
    annual_data = annual_data[annual_data["datadate"] >= start_date]
    annual_data.fillna(value=np.nan, inplace=True)
    for index, row in annual_data.iterrows():
        currency = row["curcd"]
        if currency != "EUR":
            date = row["datadate"]
            currency_df = all_currencies[currency]
            before_date_df = currency_df[currency_df["date"] <= date]
            most_recent_row = before_date_df.loc[before_date_df["date"].idxmax()]
            exchange_value = most_recent_row["PX_LAST"]
            for column in annual_data.columns:
                if column in annual_metadata:
                    continue
                if pd.notnull(row[column]):
                    annual_data.at[index, column] = row[column] * exchange_value

    quarterly_data_all = pd.concat([quarterly_data_all, quarterly_data], ignore_index=True)
    annual_data_all = pd.concat([annual_data_all, annual_data], ignore_index=True)

quarterly_data_all.to_csv("full_quarterly_data_all.csv", index=False)
annual_data_all.to_csv("full_annual_data_all.csv", index=False)

conn.close()
