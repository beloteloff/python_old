
# coding: utf-8

# In[1]:

import pandas as pd
import numpy as np
#df_skldsWOptAll = pd.read_csv('saless_woOpt2011_brands.csv',sep=';')
df_skldsWOptAll = pd.read_csv('sales2017-12-06-08_woOpt2011_brands.csv',sep=';')

df_Sales=pd.pivot_table(df_skldsWOptAll,aggfunc=np.sum,columns=['NOMGR'], values=["TGROSS"], index=['ds'])
df_Sales.columns = df_Sales.columns.droplevel(level=0)
df_Sales


# In[2]:

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

holy = pd.concat((basic_holy, short_holy)).sort_values(by='ds',ascending=True,kind='mergesort')
from math import exp
from fbprophet import Prophet
get_ipython().magic('matplotlib inline')

holydays = pd.read_csv('dates.csv')
holydays


# In[3]:

fil_Sales = open("PredictionBrands08-12-17.csv", "w")
counter = 0
pd.options.display.max_rows=5000

for column in df_Sales:
    skld_yy=df_Sales[column].to_frame().reset_index()
    skld_yy.columns= ['ds', 'yold']
    skld_yy = skld_yy.query('yold!=0')
    std_yyy=skld_yy['yold'].std()
    skld_yy['MA']=pd.rolling_mean(skld_yy['yold'],window=7,min_periods=1)
    not_in=skld_yy[~skld_yy['ds'].isin(holydays['ds'])]
    in_intersection=skld_yy[skld_yy['ds'].isin(holydays['ds'])]
    not_in['y'] = np.where((not_in['yold'] > 3*not_in['yold'].shift(2) + 3*not_in['yold'].shift(1).rolling(30).std().fillna(0)), not_in['MA'].shift(2), not_in['yold'])
    in_intersection['y']=in_intersection['yold']
    frames = [not_in,in_intersection]
    fin_df = pd.concat(frames).sort_values(by='ds',ascending=True,kind='mergesort')
    fin_df['y'] = np.log(fin_df['y'])
    fin_df=fin_df[['ds','y','yold']]
    #lenn = round(fin_df['y'].dropna(axis=0, how='all').shape[0])
    #lenn = round(fin_df['y'].shape[0])
    #skld_yy.sort_values(by='ds',ascending=True,kind='mergesort')
    counter = counter+1
    #fin_df.loc[(fin_df['ds'] >= '2017-10-01') | (fin_df['ds'] < '2011-01-10'), 'y'] = None
    ########################
   
    #fin_df['sms_happy'] = fin_df['ds'].apply(sms_happy)
    history = fin_df[fin_df['y'].notnull()].copy()
    if (history.shape[0] > 100) and (history['ds'].max() > '2017-09-01'):
        skld_prophet = Prophet(holidays=holy).fit(fin_df)
        #
        skld_future = skld_prophet.make_future_dataframe(periods=500,freq = "d")
        #skld_future['sms_happy'] = skld_future['ds'].apply(sms_happy)
        skld_forecast = skld_prophet.predict(skld_future)
        skld_prophet.plot_components(skld_forecast)
        skld_predict=skld_forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]
        #skld_predict['sms_happy2']=fin_df['sms_happy']
        #skld_predict['yhat'] = np.where(skld_predict['sms_happy2'] ==1, np.exp(skld_predict['yhat_upper']), np.exp(skld_predict['yhat']))
        skld_predict['y'] = np.exp(fin_df['y'])
        skld_predict['yhat'] = np.exp(skld_predict['yhat'])
        skld_predict['yhat_lower'] = np.exp(skld_predict['yhat_lower'])
        skld_predict['yhat_upper'] = np.exp(skld_predict['yhat_upper'])
        skld_predict['yold']=fin_df['yold']
        #skld_predict['DeltAbs'] = abs(100-skld_predict['yhat']/skld_predict['y']*100)
        skld_predict['delta'] = 100-skld_predict['yhat']/skld_predict['yold']*100
        fin_df['y'] = np.exp(fin_df['y'])
        skld_predict_upd=skld_predict[['ds','yhat','y','yold']].copy()
        skld_predict_upd['NOMGR']=column
        skld_predict_upd.to_csv(fil_Sales,index=False,encoding='utf8',sep=';')
        #fil_Sales.write("\n" + str(column)+ "\n\n" + str(skld_predict_upd) + "\n")
        fin_df[['ds','y','yold']].plot(figsize=(12,6),title='skld purchase in %s ' % (str(column)),x='ds')
        #skld_predict[['ds','yhat','yhat_lower','yhat_upper','y']].plot(figsize=(15,6),title="Purchase prediction for skld %s " % (str(column)),x='ds')
        print(', '.join(skld_forecast.columns))
        skldmetrics = skld_predict.set_index('ds')[['yhat', 'yhat_lower', 'yhat_upper']].join(fin_df.set_index('ds'))
        lenn1 = round(skldmetrics['yold'].dropna(axis=0, how='all').shape[0])
        lenn = round(skldmetrics['yhat'].shape[0])
        skldmetrics['e'] = skldmetrics['yold'] - skldmetrics['yhat']
        skldmetrics['e_lower'] = skldmetrics['yold'] - skldmetrics['yhat_lower']
        skldmetrics['e_upper'] = skldmetrics['yold'] - skldmetrics['yhat_upper']
        skldmetrics['p'] = 100*skldmetrics['e']/skldmetrics['yold']
        skldmetrics['p_lower'] = 100*skldmetrics['e_lower']/skldmetrics['yold']
        skldmetrics['p_upper'] = 100*skldmetrics['e_upper']/skldmetrics['yold']
    
        print ("\n" + str(column) + " -- " + str(counter) + "\n" + 'MAPE lower', np.mean(abs(skldmetrics[lenn-530:lenn]['p_lower'])))
        print ('MAE lower', np.mean(abs(skldmetrics[lenn-530:lenn]['e_lower'])))
        print ('\tMAPE', np.mean(abs(skldmetrics[lenn-530:lenn]['p'])))
        print ('\tMAE', np.mean(abs(skldmetrics[lenn-530:lenn]['e'])))
        print ('MAPE upper', np.mean(abs(skldmetrics[lenn-530:lenn]['p_upper'])))
        print ('MAE upper', np.mean(abs(skldmetrics[lenn-530:lenn]['e_upper'])),'\n')
        var_y = skldmetrics['y'].var()
        var_yhat = skldmetrics['yhat'].var()
        print ("Initial data variance",var_y)
        print ("Approximating data variance",var_yhat,'\n')
        std_y = skldmetrics['y'].std()
        std_yhat = skldmetrics['yhat'].std()
        '''if lenn1 > 50:
            df_cv = cross_validation(skld_prophet, horizon = '30 days')
            df_cv['yhat'] = np.exp(df_cv['yhat'])
            df_cv['y'] = np.exp(df_cv['y'])
            df_cv['skldName']=column
            df_cv.to_csv(crossvalid,index=False,encoding='utf8',sep=';')
            else:
                pass'''
        print ("Initial data std",std_y)
        print ("Approximating data std",std_yhat)
        print (lenn1)
        #print (skldmetrics[lenn-30:lenn])
        print (skldmetrics[skldmetrics['y'] >0].tail(30))
    else:
        pass
fil_Sales.close()
#crossvalid.close()


# In[ ]:

np.where((not_in['yold'] > not_in['yold'].shift(2) + 3*not_in['yold'].shift(1).rolling(30).std().fillna(0)), 
         not_in['MA'].shift(1), not_in['yold'])


# In[ ]:

pd.__version__


# In[ ]:

fil_Sales = open("PredictionBrands01-02-17_3.csv", "w")
counter = 0
pd.options.display.max_rows=5000

for column in df_Sales:
    skld_yy=df_Sales[column].to_frame().reset_index()
    skld_yy.columns= ['ds', 'yold']
    skld_yy = skld_yy.query('yold!=0')
    std_yyy=skld_yy['yold'].std()
    skld_yy['MA']=pd.rolling_mean(skld_yy['yold'],window=7,min_periods=1)
    not_in=skld_yy[~skld_yy['ds'].isin(holydays['ds'])]
    in_intersection=skld_yy[skld_yy['ds'].isin(holydays['ds'])]
    q = not_in["yold"].quantile(0.99)
    #not_in['y'] = np.where((not_in['yold'] > 3*not_in['yold'].shift(2) + 3*not_in['yold'].shift(1).rolling(30).std().fillna(0)), not_in['MA'].shift(2), not_in['yold'])
    not_in['y'] = np.where(not_in['yold'] >= q, not_in['MA'].shift(2), not_in['yold'])
    in_intersection['y']=in_intersection['yold']
    frames = [not_in,in_intersection]
    fin_df = pd.concat(frames).sort_values(by='ds',ascending=True,kind='mergesort')
    fin_df['y'] = np.log(fin_df['y'])
    fin_df=fin_df[['ds','y','yold']]
    #lenn = round(fin_df['y'].dropna(axis=0, how='all').shape[0])
    #lenn = round(fin_df['y'].shape[0])
    #skld_yy.sort_values(by='ds',ascending=True,kind='mergesort')
    counter = counter+1
    #fin_df.loc[(fin_df['ds'] >= '2017-10-01') | (fin_df['ds'] < '2011-01-10'), 'y'] = None
    ########################
   
    #fin_df['sms_happy'] = fin_df['ds'].apply(sms_happy)
    history = fin_df[fin_df['y'].notnull()].copy()
    if (history.shape[0] > 100) and (history['ds'].max() > '2017-09-01'):
        skld_prophet = Prophet(holidays=holy).fit(fin_df)
        #
        skld_future = skld_prophet.make_future_dataframe(periods=500,freq = "d")
        #skld_future['sms_happy'] = skld_future['ds'].apply(sms_happy)
        skld_forecast = skld_prophet.predict(skld_future)
        skld_prophet.plot_components(skld_forecast)
        skld_predict=skld_forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]
        #skld_predict['sms_happy2']=fin_df['sms_happy']
        #skld_predict['yhat'] = np.where(skld_predict['sms_happy2'] ==1, np.exp(skld_predict['yhat_upper']), np.exp(skld_predict['yhat']))
        skld_predict['y'] = np.exp(fin_df['y'])
        skld_predict['yhat'] = np.exp(skld_predict['yhat'])
        skld_predict['yhat_lower'] = np.exp(skld_predict['yhat_lower'])
        skld_predict['yhat_upper'] = np.exp(skld_predict['yhat_upper'])
        skld_predict['yold']=fin_df['yold']
        #skld_predict['DeltAbs'] = abs(100-skld_predict['yhat']/skld_predict['y']*100)
        skld_predict['delta'] = 100-skld_predict['yhat']/skld_predict['yold']*100
        fin_df['y'] = np.exp(fin_df['y'])
        skld_predict_upd=skld_predict[['ds','yhat','y','yold']].copy()
        skld_predict_upd['NOMGR']=column
        skld_predict_upd.to_csv(fil_Sales,index=False,encoding='utf8',sep=';')
        #fil_Sales.write("\n" + str(column)+ "\n\n" + str(skld_predict_upd) + "\n")
        fin_df[['ds','y','yold']].plot(figsize=(12,6),title='skld purchase in %s ' % (str(column)),x='ds')
        #skld_predict[['ds','yhat','yhat_lower','yhat_upper','y']].plot(figsize=(15,6),title="Purchase prediction for skld %s " % (str(column)),x='ds')
        print(', '.join(skld_forecast.columns))
        skldmetrics = skld_predict.set_index('ds')[['yhat', 'yhat_lower', 'yhat_upper']].join(fin_df.set_index('ds'))
        lenn1 = round(skldmetrics['yold'].dropna(axis=0, how='all').shape[0])
        lenn = round(skldmetrics['yhat'].shape[0])
        skldmetrics['e'] = skldmetrics['yold'] - skldmetrics['yhat']
        skldmetrics['e_lower'] = skldmetrics['yold'] - skldmetrics['yhat_lower']
        skldmetrics['e_upper'] = skldmetrics['yold'] - skldmetrics['yhat_upper']
        skldmetrics['p'] = 100*skldmetrics['e']/skldmetrics['yold']
        skldmetrics['p_lower'] = 100*skldmetrics['e_lower']/skldmetrics['yold']
        skldmetrics['p_upper'] = 100*skldmetrics['e_upper']/skldmetrics['yold']
    
        print ("\n" + str(column) + " -- " + str(counter) + "\n" + 'MAPE lower', np.mean(abs(skldmetrics[lenn-530:lenn]['p_lower'])))
        print ('MAE lower', np.mean(abs(skldmetrics[lenn-530:lenn]['e_lower'])))
        print ('\tMAPE', np.mean(abs(skldmetrics[lenn-530:lenn]['p'])))
        print ('\tMAE', np.mean(abs(skldmetrics[lenn-530:lenn]['e'])))
        print ('MAPE upper', np.mean(abs(skldmetrics[lenn-530:lenn]['p_upper'])))
        print ('MAE upper', np.mean(abs(skldmetrics[lenn-530:lenn]['e_upper'])),'\n')
        var_y = skldmetrics['y'].var()
        var_yhat = skldmetrics['yhat'].var()
        print ("Initial data variance",var_y)
        print ("Approximating data variance",var_yhat,'\n')
        std_y = skldmetrics['y'].std()
        std_yhat = skldmetrics['yhat'].std()
        '''if lenn1 > 50:
            df_cv = cross_validation(skld_prophet, horizon = '30 days')
            df_cv['yhat'] = np.exp(df_cv['yhat'])
            df_cv['y'] = np.exp(df_cv['y'])
            df_cv['skldName']=column
            df_cv.to_csv(crossvalid,index=False,encoding='utf8',sep=';')
            else:
                pass'''
        print ("Initial data std",std_y)
        print ("Approximating data std",std_yhat)
        print (lenn1)
        #print (skldmetrics[lenn-30:lenn])
        print (skldmetrics[skldmetrics['y'] >0].tail(30))
    else:
        pass
fil_Sales.close()


# In[ ]:

pd.options.display.max_rows=5000

asd=not_in.fillna(0)
asd[asd['yold']>0] 


# In[ ]:

#For each of your dataframe column, you could get quantile with:

q = df["col"].quantile(0.99)

#and then filter with:

df[df["col"] < q]


