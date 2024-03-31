ToDos:
* Bloomberg: Price, Market Cap
* Compustat: Balance Sheet Data
        Revenue/Income - Sundry (ris)
        Pretax Income (piq)
        Assets - Total (atq)
        Liabilities - Total (ltq)
        Stockholders Equity - Total (teqq)
        Insurance Premiums - Total (Insurance) (iptiq)
        Benefits and Claims - Total (Insurance) (bctq)



import pandas as pd
import wrds
conn = wrds.Connection()

quarterly_data_all = pd.DataFrame()
annual_data_all = pd.DataFrame()

companies = pd.read_csv("Overview Companies.csv", sep=";")
company_subset = companies[["GVKEY (Compustat)"]]
for x, y in company_subset.iterrows():

    quarterly_data = conn.raw_sql(f"""select gvkey, datadate, risq, piq, atq, ltq, teqq, iptiq, bctq
                            from comp_global.g_fundq
                            where gvkey = '{y[0]}'""")

    annual_data = conn.raw_sql(f"""select gvkey, datadate, ris, pi, at, lt, teq, ipti, bct
                            from comp_global.g_funda
                            where gvkey = '{y[0]}'""")

    quarterly_data_all = pd.concat([quarterly_data_all, quarterly_data], ignore_index=True)
    annual_data_all = pd.concat([annual_data_all, annual_data], ignore_index=True)

conn.close()

quarterly_data_all.to_csv("quarterly_data_all.csv", index=False)
annual_data_all.to_csv("annual_data_all.csv", index=False)
