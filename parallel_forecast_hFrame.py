
# coding: utf-8


import pandas as pd
import numpy as np

df_itemsWOptAll = pd.read_csv('basefile.csv',sep='|')

basic_holy = pd.DataFrame({
  'holiday': 'basic',
  'ds': pd.to_datetime(['2017-12-31', '2016-12-31', '2015-12-31','2017-03-08', '2016-03-08', '2015-03-08',
                        '2018-03-08']),
  'lower_window': -3,
  'upper_window': 1,
})
short_holy=pd.DataFrame({
  'holiday': 'short',
    'ds': pd.to_datetime(['2017-05-09','2016-05-09', '2015-05-09','2018-05-09',
    '2017-02-23', '2016-02-23', '2015-02-23','2018-02-23']),
  'lower_window': -2,
  'upper_window': 0,
})
happy=pd.DataFrame({
  'holiday': 'marketing',
    'ds': pd.to_datetime(['2017-08-09','2017-10-14']),
  'lower_window': -1,
  'upper_window': 0,
})
import matplotlib
holy = pd.concat((basic_holy, short_holy)).sort_values(by='ds',ascending=True,kind='mergesort')
from math import exp
from fbprophet import Prophet
#get_ipython().magic('matplotlib inline')
from fbprophet.diagnostics import cross_validation

def sms_happy(ds):
    date = pd.to_datetime(ds)
    if ((date.month == 10 and (date.day==13 or date.day==14)) or (date.month == 9 and (date.day==8 or date.day==9))) and date.year == 2017:
        return 1
    else:
        return 0
holydays = pd.read_csv('dates.csv')



listok=[]
listok2=[]
UniqueNames = df_itemsWOptAll.NAME.unique()
DataFrameDict = {elem : pd.DataFrame for elem in UniqueNames}
for key in DataFrameDict.keys():
    globals()['df%s' % (key)]= df_itemsWOptAll[['DS','NAME','Y']][:][df_itemsWOptAll.NAME == key]
    listok2.append('df%s' % (key))
    listok.append(globals()['df%s' % (key)])



   
def calculation (df):
    column=np.array_str(df['NAME'].unique()).replace("['","").replace("']","")
    fil_Sales = open("PredictionSales_parallel%s.csv" % str(column), "w")

    counter=0 
    splity=df[['DS','Y']]
    ##print (column)
    splity.columns= ['ds', 'yold']
    splity = splity.query('yold!=0')
    std_yyy=splity['yold'].std()
    splity['MA']=pd.rolling_mean(splity['yold'],window=7,min_periods=1)
    not_in=splity[~splity['ds'].isin(holydays['ds'])]
    in_intersection=splity[splity['ds'].isin(holydays['ds'])]
    not_in['y'] = not_in['yold']
    
    in_intersection['y']=in_intersection['yold']
    frames = [not_in,in_intersection]
        
    fin_df = pd.concat(frames).sort_values(by='ds',ascending=True,kind='mergesort')
    fin_df['y'] = np.log(fin_df['y'])
    fin_df=fin_df[['ds','y','yold']]
    fin_df['sms_happy'] = fin_df['ds'].apply(sms_happy)
    
    filtr=fin_df['ds'][~fin_df['yold'].isnull()].min()
    fin_df=fin_df[fin_df['ds']>=filtr]
    counter = counter+1
    history = fin_df[fin_df['y'].notnull()].copy()
    if (history.shape[0] > 300) and (history['ds'].max() > '2018-04-01'):
        item_prophet = Prophet().add_regressor('sms_happy',prior_scale=1,standardize=False).fit(fin_df)
        item_future = item_prophet.make_future_dataframe(periods=7200,freq = "H")
        item_future=item_future.set_index('ds').between_time('06:00', '00:00').reset_index()
        fin_df['ds']=pd.to_datetime(fin_df['ds'])
        item_future['sms_happy'] = item_future['ds'].apply(sms_happy)
        item_future=pd.concat([pd.merge(item_future,fin_df,on=['ds','sms_happy']),item_future[~item_future['ds'].isin(splity['ds'])]]).sort_values(by='ds',ascending=True,kind='mergesort')
        item_forecast = item_prophet.predict(item_future)
        ##item_prophet['itemName']=column
        item_prophet.plot_components(item_forecast)
        fig = matplotlib.pyplot.gcf()
        fig.savefig('Seasonality for item %s.png' % (str(column)))
        item_predict=item_forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]

        item_predict=pd.merge(item_predict,fin_df,on=['ds'],how='left')
        
        
        item_predict['y'] = np.exp(item_predict['y'])
        item_predict['yhat'] = np.exp(item_predict['yhat'])
        item_predict['yhat_lower'] = np.exp(item_predict['yhat_lower'])
        item_predict['yhat_upper'] = np.exp(item_predict['yhat_upper'])
        
        item_predict['delta'] = 100-item_predict['yhat']/item_predict['yold']*100
        fin_df['y'] = np.exp(fin_df['y'])
        item_predict_upd=item_predict[['ds','yhat','y','yold', 'yhat_lower', 'yhat_upper']].copy()
        item_predict_upd['itemName']=column
        item_predict_upd[item_predict_upd['ds']>='2018-07-01'].to_csv(fil_Sales,index=False,encoding='utf8',sep=';')

        fin_df[['ds','y','yold']].plot(figsize=(12,6),title='item purchase in %s ' % (str(column)),x='ds')
        item_predict[['ds','yhat','yhat_lower','yhat_upper','y']].plot(figsize=(15,6),title="Purchase prediction for item %s " % (str(column)),x='ds')
        fig = matplotlib.pyplot.gcf()
        fig.savefig('Purchase prediction for item %s.png' % (str(column)))

        print(', '.join(item_forecast.columns))
        itemmetrics = item_predict.set_index('ds')[['yhat', 'yhat_lower', 'yhat_upper']].join(fin_df.set_index('ds'))
        lenn1 = round(itemmetrics['yold'].dropna(axis=0, how='all').shape[0])
        lenn = round(itemmetrics['yhat'].shape[0])
        itemmetrics=itemmetrics[itemmetrics['y'] >0].tail(720)
        itemmetrics['e'] = itemmetrics['y'] - itemmetrics['yhat']
        itemmetrics['e_lower'] = itemmetrics['y'] - itemmetrics['yhat_lower']
        itemmetrics['e_upper'] = itemmetrics['y'] - itemmetrics['yhat_upper']
        itemmetrics['p'] = 100*itemmetrics['e']/itemmetrics['y']
        itemmetrics['p_lower'] = 100*itemmetrics['e_lower']/itemmetrics['y']
        itemmetrics['p_upper'] = 100*itemmetrics['e_upper']/itemmetrics['y']
    
        print ("\n" + str(column) + " -- " + str(counter) + "\n" + 'MAPE lower', np.mean(abs(itemmetrics['p_lower'])))
        print ('MAE lower', np.mean(abs(itemmetrics['e_lower'])))
        print ('\tMAPE', np.mean(abs(itemmetrics['p'])))
        print ('\tMAE', np.mean(abs(itemmetrics['e'])))
        print ('MAPE upper', np.mean(abs(itemmetrics['p_upper'])))
        print ('MAE upper', np.mean(abs(itemmetrics['e_upper'])),'\n')
        
        
        std_y = itemmetrics['y'].std()
        std_yhat = itemmetrics['yhat'].std()
       
        print ("Initial data std",std_y)
        print ("Approximating data std",std_yhat)
        print (lenn1)

        print (itemmetrics.tail(48))
        fil_Sales.close()

        
    return (item_predict_upd[item_predict_upd['ds']>='2018-07-01']) 



from multiprocessing import Pool
if __name__ == "__main__":
    pool = Pool(5)
    df = pd.concat(pool.map(calculation, listok))
    df.to_csv('All_predo.csv',index=False,encoding='utf8',sep='|')
    pool.close()
    pool.join()

