ToDos:
* Bloomberg: Price, Market Cap (to do)
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

quarterly_data = conn.raw_sql("""select gvkey, risq, piq, atq, ltq, teqq, iptiq, bctq
                        from comp_global.g_fundq
                        where gvkey = '270979'""")

annual_data = conn.raw_sql("""select gvkey, ris, pi, at, lt, teq, ipti, bct
                        from comp_global.g_funda
                        where gvkey = '270979'""")

conn.list_tables(library="comp_global")

conn.close()
