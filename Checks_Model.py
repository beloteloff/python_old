
# coding: utf-8

import pandas as pd
import numpy as np
check_=pd.read_csv('Prediction_check.csv',sep=';', encoding='utf8',usecols=['ds','yhat','hopName'])
nsk=pd.read_csv('Predict_nsk.csv',sep=';', encoding='utf8',usecols=['ds','yhat','hopName'])
check_df=nsk.append(check_,ignore_index=True)
check_df=check_df[check_df['yhat']!="yhat"]
check_df=check_df[(check_df['ds']>="2018-08-01") & (check_df['ds']<"2018-09-01")]
check_df.fillna(0,inplace=True)

dateparse = lambda x: pd.datetime.strptime(x, '%d.%m.%Y')
podrazd=pd.read_excel('Статистика по дням_План_Таблица_Чеки_АВГУСТ18_CrazySale_0807.xlsx',sheetname='2018' , date_parser=dateparse,usecols=['ДАТА','РГ город','РГ Москва','РГ регионы','Rivoli','РГ Салоны'])

podrazd=podrazd[(podrazd['ДАТА']>="2018-08-01") & (podrazd['ДАТА']<"2018-09-01")]
podrazd.set_index(['ДАТА'],inplace=True)

d_y_n=pd.DataFrame()
#count=0
for column in podrazd:
    data=podrazd[column].to_frame().reset_index()
    data.columns= ['ds', 'coef']
    #count+=1
    data['Division']=column
    d_y_n=d_y_n.append(data,ignore_index=True)


hop_month=pd.read_excel('temp.xlsx')
hop_month.rename(columns={"Подразделение": "Division", "Историческое наименование": "hopName","еки с лугами": "Total_month"},inplace=True)

high_level_coef=hop_month.merge(d_y_n,on=['Division'],how='inner')
high_level_coef['y']=high_level_coef['coef']*high_level_coef['Total_month']
high_level_coef['ds']=high_level_coef['ds'].astype(str)


div_tot=high_level_coef.groupby(['Division','ds']).agg({'y': sum}).reset_index().rename(columns={'y': 'Division_tot'})

model_division=fin.groupby(['Division','ds']).agg({'yhat': sum}).reset_index().rename(columns={'yhat': 'Model_division'})

fin=check_df.merge(high_level_coef,on=['hopName','ds'],how='inner')#['hopName'].unique()
fin['yhat']=fin['yhat'].astype(float)
fin=fin.merge(model_tot,on=['hopName'],how='inner')
fin=fin.merge(div_tot,on=['Division','ds'],how='inner')
fin=fin.merge(model_division,on=['Division','ds'],how='inner')
fin=fin.merge(fix_model_tot,on=['hopName'],how='inner')
fin['Division_percent']=(fin['yhat']/fin['Model_division'])
fin['Delta_division']=(fin['Division_tot']-fin['Model_division'])

fin['yhat+delta']=(fin['Division_percent']*fin['Delta_division'])+fin['yhat']
fin['Delta_hop']=(fin['Total_month']-fin['Fix_model_tot'])
fin['Predict']=(fin['yhat+delta']/fin['Fix_model_tot']*fin['Delta_hop'])+fin['yhat+delta']

fix_model_tot=fin.groupby('hopName').agg({'yhat+delta': sum}).reset_index().rename(columns={'yhat+delta': 'Fix_model_tot'})

model_tot=fin.groupby('hopName').agg({'yhat': sum}).reset_index().rename(columns={'yhat': 'Model_tot'})

######################################################
reader=pd.read_csv('PredictionSales.csv',sep=';', encoding='utf8')
#reader=reader[reader['yhat']!="yhat"]

reader.fillna(0,inplace=True)



#pd.pivot_table(reader[reader.ds.str.contains("2018-02")],aggfunc=sum,index="Name",values=["yhat","yold"])


reader['yhat']=reader['yhat'].astype(float)
reader['y']=reader['y'].astype(float)
reader['yhat_lower']=reader['yhat_lower'].astype(float)
reader['yhat_upper']=reader['yhat_upper'].astype(float)
reader['yold']=reader['yold'].astype(float)

#[reader['yhat']=="yhat"]


reader['yold'].fillna(0, inplace=True)
reader['yhat'].fillna(0, inplace=True)


#reader['yold']=reader['yold'].astype(float)
#reader['yhat']=reader['yhat'].astype(float)
new=reader[(reader.ds.str.contains("2018-06")) & (reader.yold!=0)]

#new=new.groupby(['Name']).agg({'yhat': sum, 'y': sum,'yhat_lower': sum,'yhat_upper': sum}).reset_index()


new['yhat']=new['yhat'].astype(float)
new['y']=new['y'].astype(float)


new['Acc']=abs(100-new['yhat']/new['y']*100)
new['Acc_lower']=abs(100-new['yhat_lower']/new['y']*100)
new['Acc_upper']=abs(100-new['yhat_upper']/new['y']*100)
new[(new['Acc_lower']<new['Acc'] ) | (new['Acc_upper']<new['Acc'])]
#new.loc[new[['Acc_upper','Acc_lower','Acc']].idxmin()]

new[['Acc_upper','Acc_lower','Acc']].min(axis=1).mean()


for i in new['hopName'].unique():
    acc=new['Acc'][new['hopName']==i].mean()
    print(i,acc)



for i in new['hopName'][(new['hopName']!="Северодвинск-1 ЦУМ") & (~new['hopName'].str.contains('ivoli ' )) & (~new['hopName'].str.contains('лон ' ) )].unique():
    acc=abs(100-(new['yhat'][new['hopName']==i].sum()/new['y'][new['hopName']==i].sum())*100)
    print(i,acc)


df_acc[['Acc']].median()

