ToDos:
* Bloomberg: Price, Market Cap
* Compustat: Balance Sheet Data
        Revenue - Total (revtq)
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

companies = pd.read_csv("Overview Companies.csv", sep=";", converters={"GVKEY (Compustat)": str})
company_subset = companies[["GVKEY (Compustat)"]]
for x, y in company_subset.iterrows():

    quarterly_data = conn.raw_sql(f"""select *
                            from comp_global_daily.g_fundq
                            where gvkey = '{y[0]}' AND consol='c' AND datafmt='HIST_STD' AND popsrc='I''""")

    annual_data = conn.raw_sql(f"""select *
                            from comp_global_daily.g_funda
                            where gvkey = '{y[0]}' AND consol='c' AND datafmt='HIST_STD' AND popsrc='I'""")

    quarterly_data_all = pd.concat([quarterly_data_all, quarterly_data], ignore_index=True)
    annual_data_all = pd.concat([annual_data_all, annual_data], ignore_index=True)

quarterly_data_all.to_csv("full_quarterly_data_all.csv", index=False)
annual_data_all.to_csv("full_annual_data_all.csv", index=False)

conn.close()



request.set("startDate", "20040101");


quarterly_not_needed = ["indfmt", "datafmt", "consol", "popsrc", "acctstdq", "fyr", "bsprq", "compstq", "datacqtr", "datafqtr",
                                        "fqtr", "fyearq", "pdq", "pdsa", "pdytd", "rp", "scfq", "srcq", "staltq", "updq", "fdateq", "pdateq", "sedol",
                                        "exchg", "iid", "costat"]
quarterly_metadata = ["gvkey", "datadate", "curcdq", "conm", "isin", "fic", "loc"]

annual_not_needed = ["indfmt", "datafmt", "consol", "popsrc", "acctstd", "acqmeth", "bspr", "compst", "final", "fyear",
                        "ismod", "pddur", "scf", "src", "stalt", "upd", "fdate", "pdate", "sedol", "exchg", "fyr"
                        "iid", "costat", "naicsh", "sich", "rank", "au", "auop", "ajexi", "cshoi", "cshpria", "epsexcon",
                        "epsincon", "epsexnc", "epsinnc", "icapi", "nicon", "ninc", "pv", "tstkni"]
annual_metadata = ["gvkey", "datadate", "curcd", "curcdi", "conm", "isin", "fic", "loc"]
