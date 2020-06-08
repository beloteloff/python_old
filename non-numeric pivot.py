from pandas import *
import csv
import numpy as np
df = pandas.read_csv(‘data_raw.csv’, sep=” “, chunksize=5000)
df
appended_data = []
for chunk in df:
    pivot_table = chunk.pivot_table(index=[‘id’],
                             columns=[‘key’],
                             values=[‘value’],
                             aggfunc=lambda x: ‘ ‘.join(str(v) for v in x))
    appended_data.append(pivot_table)
appended_data = pandas.concat(appended_data, axis=0).reset_index()
appended_data.to_csv('data_clean.csv', sep=",")
#and if you wanna clean it a little bit where the chunk trunks it:
appended_data_clean = appended_data.groupby('id', sort=True).agg(np.sum)
appended_data_clean.to_csv('dati_clean_crunched.csv', sep=",")

#http://www.enricobergamini.it/python-pivot-table/
