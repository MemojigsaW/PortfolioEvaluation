import sys
import pandas as pd

"""
take path of WRDS raw in arg1
output file in arg2
"""

file = sys.argv[1]
outputDir = sys.argv[2]

raw = pd.read_csv(file)
raw['date'] = raw['date'].astype(str).str.zfill(6)
raw['date'] = pd.to_datetime(raw['date'], format='%d%m%y')

# remove companies that undergone merge/acquisition with change in ticker
check = raw.groupby(['PERMNO'])['TICKER'].nunique()
ab_tickers = check[check != 1]
raw = raw[~raw['PERMNO'].isin(ab_tickers.index)]

# PERMNO after being assigned new TICKER
check = raw.groupby(['TICKER'])['PERMNO'].nunique()
ab_tickers = check[check != 1]
raw = raw[~raw['TICKER'].isin(ab_tickers.index)]

assert raw['TICKER'].nunique() == raw['PERMNO'].nunique()

raw = raw[['date', 'TICKER', 'PRC']]
prc_pivot = pd.pivot_table(raw, index=raw.date, columns=raw.TICKER, values='PRC')

# rm series containing missing observation
null_index = prc_pivot.isna().sum()
null_index = null_index[null_index>0]
prc_pivot = prc_pivot.drop(labels=null_index.index, axis=1)

assert prc_pivot.isna().sum().sum() == 0

prc_pivot.to_csv(outputDir+'/cleaned.csv', index=True)






