import pandas as pd
import csv
import numpy as np

df2 = pd.read_csv('data_processing/nyc_MCAR_5.0_jpt.csv')
#df2=df2.dropna()
avg =0
pRz =0
for index, row in df2.iterrows():
	if not pd.isna(row['passenger_count']) and row['store_and_fwd_flag']  ==  'Y' :
			pRz += row['P']
print(pRz)
for store_and_fwd_flag, passenger_count, P in zip(df2['store_and_fwd_flag'],df2['passenger_count'],df2['P']):
	if not pd.isna(passenger_count) and store_and_fwd_flag  ==  'Y' :
		frac = (passenger_count/pRz) * P
		avg += frac
print("avg is "+str(avg))
globals()['avginject'] = avg
