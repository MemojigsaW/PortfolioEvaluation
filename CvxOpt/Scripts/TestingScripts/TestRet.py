import json
import pandas as pd
import sys
from datetime import datetime

f = open('config.json', "r")
config = json.load(f);
f.close()

df = pd.read_csv(config['sampleTest'], parse_dates=['date'])
df = df.set_index('date')

if len(sys.argv[4:]) != 0:
    df = df[sys.argv[4:]]

df = df.loc[sys.argv[1]: sys.argv[2]]

df_M_min = df.loc[df.groupby(df.index.to_period('M')).apply(lambda x: x.index.min())]
df_M_max = df.loc[df.groupby(df.index.to_period('M')).apply(lambda x: x.index.max())]

df_M_min = df_M_min.reset_index().drop(['date'], axis=1)
df_M_max = df_M_max.reset_index().drop(['date'], axis=1)

df = (df_M_max - df_M_min) / df_M_min



# sample return
print(
    df.tail()
)

print(
    df.mean(axis=0).head()
)

# sample cov
print(
    df.cov()
)
