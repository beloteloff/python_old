
# coding: utf-8

import pandas as pd
import os
import cx_Oracle
pd.set_option('display.max_columns', 77)
os.environ["NLS_LANG"] = ".UTF8"
import numpy as np
import matplotlib
import sys 
reload(sys)
sys.setdefaultencoding('utf-8')
import csv
from transliterate import translit, get_available_language_codes
dsnStr = cx_Oracle.makedsn("inner.local", "4327", "birg")
conn = cx_Oracle.connect(user="vasil", password="passwordo", dsn=dsnStr,encoding='utf-8')
get_ipython().magic(u'matplotlib inline')
#connect.version

pd.read_sql("select distinct phone_ids FROM db_crm.sales_rtb INNER JOIN db_crm.DISCOUNT_CARDS ON sales_rtb.CLNTNO=DISCOUNT_CARDS.CARDNO where yorty > TO_DATE('2016-11-27','YYYY-MM-DD') and sales_rtb.nomgrid=221", con=conn).to_csv('purchShiseido.csv',index=False,sep='|')

pd.read_sql ("SELECT distinct phone_ids,siebel_client_id FROM db_crm.DISCOUNT_CARDS", con=conn).to_csv('Siebelid.csv',index=False,sep='|')

sample_ids = pd.read_sql ("with subscrib as (select distinct tel as telo from db_crm.ref_telephone INNER JOIN db_crm.discount_cards ON DISCOUNT_CARDS.siebel_client_id=ref_telephone.contact_sid inner join (select distinct tel as phone from ( select distinct tel ,count(distinct subscr_flg) over (partition by tel ) as filt, subscr_flg from db_crm.ref_telephone ) where filt >1 or (filt=1 and subscr_flg='Y')) s ON ref_telephone.tel=s.phone where agg_card_type!='Прочие' and card_status=1) SELECT idsPurch,Recency,minDate,cur_age,phone_ids,segm,tovarkateg_eng,NOMGR,SUM(TGROSS)as monetary_value, COUNT(distinct hdrid) as checkquant,COUNT(*) as quant FROM db_crm.sales_rtb INNER JOIN db_crm.DISCOUNT_CARDS ON sales_rtb.CLNTNO=DISCOUNT_CARDS.CARDNO INNER JOIN (SELECT distinct phone_ids as clid, (SUM(CASE WHEN (NOMGR='SHISEIDO') THEN 1 ELSE 0 end) - ABS((SUM(CASE WHEN (NOMGR='SHISEIDO') THEN 1 ELSE 0 end )- 1))) as idsPurch FROM db_crm.sales_rtb INNER JOIN db_crm.nomenklatura_base ON sales_rtb.Goodid=nomenklatura_base.Goodid INNER JOIN db_crm.DISCOUNT_CARDS ON sales_rtb.CLNTNO=DISCOUNT_CARDS.CARDNO WHERE Tovtype ='Товар' AND yorty >= TO_DATE('2016-12-11','YYYY-MM-DD') AND B2B_type = 0 GROUP BY phone_ids) purch ON purch.clid=DISCOUNT_CARDS.phone_ids INNER JOIN db_crm.nomenklatura_base ON sales_rtb.Goodid=nomenklatura_base.Goodid LEFT JOIN db_crm.DISCOUNTCARDS_B2B ON sales_rtb.CLNTNO=DISCOUNTCARDS_B2B.DC_NO INNER JOIN ( SELECT MIN(TO_CHAR(yorty,'YYYY')) || MIN(TO_CHAR(yorty,'Q')) as minDate,phone_ids as telef, (TO_DATE('2017-12-12','YYYY-MM-DD') - MAX(TRUNC(yorty))) as Recency FROM db_crm.sales_hdr INNER JOIN db_crm.DISCOUNT_CARDS ON sales_hdr.CLNTNO=DISCOUNT_CARDS.cardno WHERE CLNTNO NOT in ('5550012345678','9991000445568','0101000000004','0100000020005') AND yorty <= TO_DATE('2017-12-12','YYYY-MM-DD') HAVING MIN(TO_CHAR(yorty,'YYYY-MM-DD')) > '2011-01-01' GROUP BY phone_ids ) first ON DISCOUNT_CARDS.phone_ids=first.telef INNER JOIN subscrib on DISCOUNT_CARDS.phone_ids=subscrib.telo WHERE yorty >= TO_DATE('2016-12-11','YYYY-MM-DD') AND yorty < TO_DATE('2017-12-12','YYYY-MM-DD') AND Tovtype ='Товар' AND CLNTNO NOT in ('5550012345678','9991000445568','0101000000004','0100000020005') AND DISCOUNTCARDS_B2B.DC_NO IS NULL AND nomenklatura_base.nomgrid not in (372,458,1092) AND phone_ids LIKE '9%' GROUP BY idsPurch,Recency,minDate,cur_age,phone_ids,segm,tovarkateg_eng,NOMGR HAVING SUM(TGROSS) > 10", con=conn)
#.to_csv('ScoringSampleids_.csv',index=False,sep='|')
#where yorty >= TO_DATE('2017-01-01','YYYY-MM-DD') AND yorty < TO_DATE('2017-11-19','YYYY-MM-DD') ) clntz \
#ON DISCOUNT_CARDS.CARDNO=clntz.clients \
#AND comm_use_type_cd='Сотовый' \ 
sample_ids['Response']=0


df_respo = pd.read_sql ("with subscrib as (select distinct tel as telo from db_crm.ref_telephone INNER JOIN db_crm.discount_cards ON DISCOUNT_CARDS.siebel_client_id=ref_telephone.contact_sid inner join (select distinct tel as phone from ( select distinct tel ,count(distinct subscr_flg) over (partition by tel ) as filt, subscr_flg from db_crm.ref_telephone ) where filt >1 or (filt=1 and subscr_flg='Y')) s ON ref_telephone.tel=s.phone where agg_card_type!='Прочие' and card_status=1)  SELECT idsPurch,Recency,minDate,cur_age,phone_ids,segm,tovarkateg_eng,NOMGR,SUM(TGROSS) as monetary_value, COUNT(distinct hdrid) as checkquant,COUNT(*) as quant FROM db_crm.sales_rtb INNER JOIN db_crm.DISCOUNT_CARDS ON sales_rtb.CLNTNO=DISCOUNT_CARDS.CARDNO INNER JOIN (SELECT distinct phone_ids as clid, (SUM(CASE WHEN (NOMGR='SHISEIDO') THEN 1 ELSE 0 end) - ABS((SUM(CASE WHEN (NOMGR='SHISEIDO') THEN 1 ELSE 0 end )- 1))) as idsPurch FROM db_crm.sales_rtb INNER JOIN db_crm.nomenklatura_base ON sales_rtb.Goodid=nomenklatura_base.Goodid INNER JOIN db_crm.DISCOUNT_CARDS ON sales_rtb.CLNTNO=DISCOUNT_CARDS.CARDNO WHERE Tovtype ='Товар' AND yorty >= TO_DATE('2016-12-11','YYYY-MM-DD') AND B2B_type = 0 GROUP BY phone_ids) purch ON purch.clid=DISCOUNT_CARDS.phone_ids INNER JOIN db_crm.nomenklatura_base ON sales_rtb.Goodid=nomenklatura_base.Goodid LEFT JOIN db_crm.DISCOUNTCARDS_B2B ON sales_rtb.CLNTNO=DISCOUNTCARDS_B2B.DC_NO INNER JOIN (SELECT MIN(TO_CHAR(yorty,'YYYY')) || MIN(TO_CHAR(yorty,'Q')) as minDate,tel as telef, (TO_DATE('2017-12-12','YYYY-MM-DD') - MAX(TRUNC(yorty))) as Recency FROM db_crm.sales_hdr INNER JOIN db_crm.DISCOUNT_CARDS ON sales_hdr.CLNTNO=DISCOUNT_CARDS.cardno INNER JOIN (SELECT distinct tel,contact_sid FROM db_crm.ref_telephone where subscr_flg!='N') distitel ON DISCOUNT_CARDS.siebel_client_id=distitel.contact_sid RIGHT JOIN (select distinct checkk, idsile FROM (SELECT idsile,TO_CHAR(yorty,'YYYY-MM-DD') as dtransact, CASE WHEN (a.Fourdt > sales_rtb.yorty AND a.finish_dt < sales_rtb.yorty) then 1 else 0 end as Response,hdrid as checkk FROM db_crm.sales_rtb INNER JOIN db_crm.DISCOUNT_CARDS ON sales_rtb.CLNTNO=DISCOUNT_CARDS.CARDNO LEFT JOIN db_crm.DISCOUNTCARDS_B2B ON sales_rtb.CLNTNO=DISCOUNTCARDS_B2B.DC_NO INNER JOIN db_crm.nomenklatura_base ON sales_rtb.Goodid=nomenklatura_base.Goodid INNER JOIN db_crm.ref_telephone ON DISCOUNT_CARDS.siebel_client_id=ref_telephone.contact_sid RIGHT JOIN (SELECT X_DELIVERY_ADDRESS as idsile,x_provider_camp_id,(finish_dt + INTERVAL '4' DAY) as Fourdt,finish_dt FROM db_crm.MARKET_CAMPAIGN INNER JOIN db_crm.MARKET_CAMPAIGN_DETAIL ON MARKET_CAMPAIGN.campid=MARKET_CAMPAIGN_detail.campid WHERE X_DELIVERY_STATUS='Успешно доставлено' AND LOWER (message) LIKE '%shiseido%' AND MARKET_CAMPAIGN.start_date >= TO_DATE('2017-01-01','YYYY-MM-DD') GROUP BY X_DELIVERY_ADDRESS , x_provider_camp_id,(finish_dt + INTERVAL '4' DAY),finish_dt) a ON ref_telephone.tel=a.idsile WHERE yorty > TO_DATE('2016-12-11','YYYY-MM-DD') AND DISCOUNTCARDS_B2B.DC_NO IS NULL AND CLNTNO NOT in ('5550012345678','9991000445568','0101000000004','0100000020005') AND B2B_type = 0 AND NOMGR='SHISEIDO' AND Tovtype ='Товар' AND comm_use_type_cd='Сотовый' GROUP BY idsile,TO_CHAR(yorty,'YYYY-MM-DD'), hdrid, CASE WHEN (a.Fourdt > sales_rtb.yorty AND a.finish_dt < sales_rtb.yorty) then 1 else 0 end order by dtransact) WHERE Response=1 ) b ON distitel.tel=b.idsile AND sales_hdr.hdrid!=b.checkk WHERE CLNTNO NOT in ('5550012345678','9991000445568','0101000000004','0100000020005') AND yorty <= TO_DATE('2017-11-29','YYYY-MM-DD') GROUP BY tel) first ON DISCOUNT_CARDS.phone_ids=first.telef INNER JOIN subscrib on DISCOUNT_CARDS.phone_ids=subscrib.telo WHERE yorty >= TO_DATE('2016-12-11','YYYY-MM-DD') AND yorty < TO_DATE('2017-12-12','YYYY-MM-DD') AND Tovtype ='Товар' AND CLNTNO NOT in ('5550012345678','9991000445568','0101000000004','0100000020005') AND DISCOUNTCARDS_B2B.DC_NO IS NULL AND nomenklatura_base.nomgrid not in (372,458,1092) GROUP BY idsPurch,Recency, minDate,cur_age,phone_ids,segm,tovarkateg_eng,NOMGR HAVING SUM(TGROSS) > 10", con=conn)
df_respo['Response']=1


df_noresp = pd.read_sql ("with subscrib as (select distinct tel as telo from db_crm.ref_telephone INNER JOIN db_crm.discount_cards ON DISCOUNT_CARDS.siebel_client_id=ref_telephone.contact_sid inner join (select distinct tel as phone from ( select distinct tel ,count(distinct subscr_flg) over (partition by tel ) as filt, subscr_flg from db_crm.ref_telephone ) where filt >1 or (filt=1 and subscr_flg='Y')) s ON ref_telephone.tel=s.phone where agg_card_type!='Прочие' and card_status=1)  SELECT idsPurch,Recency,minDate,cur_age,phone_ids,segm,tovarkateg_eng,NOMGR,SUM(TGROSS) as monetary_value, COUNT(distinct hdrid) as checkquant,COUNT(*) as quant FROM db_crm.sales_rtb INNER JOIN db_crm.DISCOUNT_CARDS ON sales_rtb.CLNTNO=DISCOUNT_CARDS.CARDNO INNER JOIN (SELECT distinct phone_ids as clid, (SUM(CASE WHEN (NOMGR='SHISEIDO') THEN 1 ELSE 0 end) - ABS((SUM(CASE WHEN (NOMGR='SHISEIDO') THEN 1 ELSE 0 end )- 1))) as idsPurch FROM db_crm.sales_rtb INNER JOIN db_crm.nomenklatura_base ON sales_rtb.Goodid=nomenklatura_base.Goodid INNER JOIN db_crm.DISCOUNT_CARDS ON sales_rtb.CLNTNO=DISCOUNT_CARDS.CARDNO WHERE Tovtype ='Товар' AND yorty >= TO_DATE('2016-12-11','YYYY-MM-DD') AND B2B_type = 0 GROUP BY phone_ids) purch ON purch.clid=DISCOUNT_CARDS.phone_ids INNER JOIN db_crm.nomenklatura_base ON sales_rtb.Goodid=nomenklatura_base.Goodid LEFT JOIN db_crm.DISCOUNTCARDS_B2B ON sales_rtb.CLNTNO=DISCOUNTCARDS_B2B.DC_NO INNER JOIN ( SELECT MIN(TO_CHAR(yorty,'YYYY')) || MIN(TO_CHAR(yorty,'Q')) as minDate,tel as telef, (TO_DATE('2017-12-12','YYYY-MM-DD') - MAX(TRUNC(yorty))) as Recency FROM db_crm.sales_hdr INNER JOIN db_crm.DISCOUNT_CARDS ON sales_hdr.CLNTNO=DISCOUNT_CARDS.cardno INNER JOIN (SELECT distinct tel,contact_sid FROM db_crm.ref_telephone where subscr_flg!='N') distitel ON DISCOUNT_CARDS.siebel_client_id=distitel.contact_sid RIGHT JOIN(select distinct X_DELIVERY_ADDRESS as idsile FROM db_crm.MARKET_CAMPAIGN INNER JOIN db_crm.MARKET_CAMPAIGN_DETAIL ON MARKET_CAMPAIGN.campid=MARKET_CAMPAIGN_detail.campid LEFT JOIN ( select distinct checkk, idsile FROM (SELECT idsile,TO_CHAR(yorty,'YYYY-MM-DD') as dtransact, CASE WHEN (a.Fourdt > sales_rtb.yorty AND a.finish_dt < sales_rtb.yorty) then 1 else 0 end as Response,hdrid as checkk FROM db_crm.sales_rtb INNER JOIN db_crm.DISCOUNT_CARDS ON sales_rtb.CLNTNO=DISCOUNT_CARDS.CARDNO LEFT JOIN db_crm.DISCOUNTCARDS_B2B ON sales_rtb.CLNTNO=DISCOUNTCARDS_B2B.DC_NO INNER JOIN db_crm.nomenklatura_base ON sales_rtb.Goodid=nomenklatura_base.Goodid INNER JOIN db_crm.ref_telephone ON DISCOUNT_CARDS.siebel_client_id=ref_telephone.contact_sid RIGHT JOIN (SELECT X_DELIVERY_ADDRESS as idsile,x_provider_camp_id,(finish_dt + INTERVAL '4' DAY) as Fourdt,finish_dt FROM db_crm.MARKET_CAMPAIGN INNER JOIN db_crm.MARKET_CAMPAIGN_DETAIL ON MARKET_CAMPAIGN.campid=MARKET_CAMPAIGN_detail.campid WHERE X_DELIVERY_STATUS='Успешно доставлено' AND LOWER (message) LIKE '%shiseido%' AND MARKET_CAMPAIGN.start_date >= TO_DATE('2017-01-01','YYYY-MM-DD') GROUP BY X_DELIVERY_ADDRESS, x_provider_camp_id,(finish_dt + INTERVAL '4' DAY),finish_dt) a ON ref_telephone.tel=a.idsile WHERE yorty >= TO_DATE('2017-01-01','YYYY-MM-DD') AND DISCOUNTCARDS_B2B.DC_NO IS NULL AND CLNTNO NOT in ('5550012345678','9991000445568','0101000000004','0100000020005') AND B2B_type = 0 AND NOMGR='SHISEIDO' AND Tovtype ='Товар' AND comm_use_type_cd='Сотовый' GROUP BY idsile,TO_CHAR(yorty,'YYYY-MM-DD'), hdrid, CASE WHEN (a.Fourdt > sales_rtb.yorty AND a.finish_dt < sales_rtb.yorty) then 1 else 0 end order by dtransact) WHERE Response=1) respurch ON MARKET_CAMPAIGN_detail.X_DELIVERY_ADDRESS=respurch.idsile WHERE X_DELIVERY_STATUS='Успешно доставлено' AND LOWER (message) LIKE '%shiseido%' AND MARKET_CAMPAIGN.start_date >= TO_DATE('2017-01-01','YYYY-MM-DD') and respurch.idsile is null) b ON distitel.tel=b.idsile WHERE CLNTNO NOT in ('5550012345678','9991000445568','0101000000004','0100000020005') AND yorty <= TO_DATE('2017-12-12','YYYY-MM-DD') GROUP BY tel) first ON DISCOUNT_CARDS.phone_ids=first.telef INNER JOIN subscrib on DISCOUNT_CARDS.phone_ids=subscrib.telo WHERE yorty >= TO_DATE('2016-12-11','YYYY-MM-DD') AND yorty < TO_DATE('2017-12-12','YYYY-MM-DD') AND Tovtype ='Товар' AND CLNTNO NOT in ('5550012345678','9991000445568','0101000000004','0100000020005') AND DISCOUNTCARDS_B2B.DC_NO IS NULL AND nomenklatura_base.nomgrid not in (372,458,1092) GROUP BY idsPurch,Recency, minDate,cur_age,phone_ids,segm,tovarkateg_eng,NOMGR HAVING SUM(TGROSS) > 10", con=conn)
df_noresp['Response']=-1

df_noresp2 = pd.read_sql ("with subscrib as (select distinct tel as telo from db_crm.ref_telephone INNER JOIN db_crm.discount_cards ON DISCOUNT_CARDS.siebel_client_id=ref_telephone.contact_sid inner join (select distinct tel as phone from ( select distinct tel ,count(distinct subscr_flg) over (partition by tel ) as filt, subscr_flg from db_crm.ref_telephone ) where filt >1 or (filt=1 and subscr_flg='Y')) s ON ref_telephone.tel=s.phone where agg_card_type!='Прочие' and card_status=1)  SELECT idsPurch,Recency,minDate,cur_age,phone_ids,segm,tovarkateg_eng,sales_rtb.NOMGRID,SUM(TGROSS) as monetary_value, COUNT(distinct hdrid) as checkquant,COUNT(*) as quant FROM db_crm.sales_rtb INNER JOIN db_crm.DISCOUNT_CARDS ON sales_rtb.CLNTNO=DISCOUNT_CARDS.CARDNO INNER JOIN (SELECT distinct phone_ids as clid, (SUM(CASE WHEN (NOMGR='SHISEIDO') THEN 1 ELSE 0 end) - ABS((SUM(CASE WHEN (NOMGR='SHISEIDO') THEN 1 ELSE 0 end )- 1))) as idsPurch FROM db_crm.sales_rtb INNER JOIN db_crm.nomenklatura_base ON sales_rtb.Goodid=nomenklatura_base.Goodid INNER JOIN db_crm.DISCOUNT_CARDS ON sales_rtb.CLNTNO=DISCOUNT_CARDS.CARDNO WHERE Tovtype ='Товар' AND yorty >= TO_DATE('2016-11-28','YYYY-MM-DD') AND B2B_type = 0 GROUP BY phone_ids) purch ON purch.clid=DISCOUNT_CARDS.phone_ids INNER JOIN db_crm.nomenklatura_base ON sales_rtb.Goodid=nomenklatura_base.Goodid LEFT JOIN db_crm.DISCOUNTCARDS_B2B ON sales_rtb.CLNTNO=DISCOUNTCARDS_B2B.DC_NO INNER JOIN ( SELECT MIN(TO_CHAR(yorty,'YYYY')) || MIN(TO_CHAR(yorty,'Q')) as minDate,tel as telef, (TO_DATE('2017-11-29','YYYY-MM-DD') - MAX(TRUNC(yorty))) as Recency FROM db_crm.sales_hdr INNER JOIN db_crm.DISCOUNT_CARDS ON sales_hdr.CLNTNO=DISCOUNT_CARDS.cardno INNER JOIN (SELECT distinct tel,contact_sid FROM db_crm.ref_telephone where subscr_flg!='N') distitel ON DISCOUNT_CARDS.siebel_client_id=distitel.contact_sid RIGHT JOIN( select sum(response), idsile FROM ( SELECT idsile,TO_CHAR(yorty,'YYYY-MM-DD') as dtransact, CASE WHEN (a.Fourdt > sales_rtb.yorty AND a.finish_dt < sales_rtb.yorty) then 1 else 0 end as Response,hdrid as checkk FROM db_crm.sales_rtb INNER JOIN db_crm.DISCOUNT_CARDS ON sales_rtb.CLNTNO=DISCOUNT_CARDS.CARDNO LEFT JOIN db_crm.DISCOUNTCARDS_B2B ON sales_rtb.CLNTNO=DISCOUNTCARDS_B2B.DC_NO INNER JOIN db_crm.nomenklatura_base ON sales_rtb.Goodid=nomenklatura_base.Goodid INNER JOIN db_crm.ref_telephone ON DISCOUNT_CARDS.siebel_client_id=ref_telephone.contact_sid RIGHT JOIN ( SELECT X_DELIVERY_ADDRESS as idsile,x_provider_camp_id,(finish_dt + INTERVAL '4' DAY) as Fourdt,finish_dt FROM db_crm.MARKET_CAMPAIGN INNER JOIN db_crm.MARKET_CAMPAIGN_DETAIL ON MARKET_CAMPAIGN.campid=MARKET_CAMPAIGN_detail.campid WHERE X_DELIVERY_STATUS='Успешно доставлено' AND LOWER (message) LIKE '%shiseido%' AND x_provider_camp_id!='2538379' AND MARKET_CAMPAIGN.start_date >= TO_DATE('2017-01-01','YYYY-MM-DD') GROUP BY X_DELIVERY_ADDRESS,x_provider_camp_id,(finish_dt + INTERVAL '4' DAY),finish_dt) a ON ref_telephone.tel=a.idsile WHERE yorty > TO_DATE('2016-11-28','YYYY-MM-DD') AND DISCOUNTCARDS_B2B.DC_NO IS NULL AND CLNTNO NOT in ('5550012345678','9991000445568','0101000000004','0100000020005') AND B2B_type = 0 AND NOMGR='SHISEIDO' AND Tovtype ='Товар' AND comm_use_type_cd='Сотовый' GROUP BY idsile,TO_CHAR(yorty,'YYYY-MM-DD'), hdrid, CASE WHEN (a.Fourdt > sales_rtb.yorty AND a.finish_dt < sales_rtb.yorty) then 1 else 0 end order by dtransact) GROUP BY idsile HAVING SUM(response) < 1) b ON distitel.tel=b.idsile WHERE CLNTNO NOT in ('5550012345678','9991000445568','0101000000004','0100000020005') AND yorty <= TO_DATE('2017-11-29','YYYY-MM-DD') GROUP BY tel) first ON DISCOUNT_CARDS.phone_ids=first.telef INNER JOIN subscrib on DISCOUNT_CARDS.phone_ids=subscrib.telo WHERE yorty >= TO_DATE('2016-11-28','YYYY-MM-DD') AND yorty < TO_DATE('2017-11-29','YYYY-MM-DD') AND Tovtype ='Товар' AND CLNTNO NOT in ('5550012345678','9991000445568','0101000000004','0100000020005') AND DISCOUNTCARDS_B2B.DC_NO IS NULL AND nomenklatura_base.nomgrid not in (372,458,1092) GROUP BY idsPurch,Recency, minDate,cur_age,phone_ids,segm,tovarkateg_eng,sales_rtb.NOMGRID HAVING SUM(TGROSS) > 10", con=conn)
df_noresp2['Response']=-1


#df_noresp[df_noresp['PHONE_ids']=='9059501855']
sample_ids=sample_ids[(~sample_ids['PHONE_ids'].isin(df_noresp['PHONE_ids'])) & (~sample_ids['PHONE_ids'].isin(df_respo['PHONE_ids']))]#.nunique()
#sample_ids[sample_ids['PHONE_ids'].isin(df_respo['PHONE_ids'])]
new=pd.concat([df_noresp,df_respo,sample_ids])
#new['SEGM']= new['SEGM'].apply(lambda x: translit(x,'ru', reversed=True))
new.to_csv('ScoringSampleids_.csv',index=False,sep='|')

# !gcc-6/c63++ reducer_mappa.cpp -o mapping -static-libstdc++
get_ipython().system(u'awk -F \'|\' \'{print $1"~"$2"~"$3"~"$4"~"$5"~"$12"|"$6"_"$7"_"$8"|"$9}\' ScoringSampleids_.csv > ScoringSampleids_upd.csv')

# !gcc-6/c63++ reducer_mappa.cpp -o mapping -static-libstdc++
get_ipython().system(u'./monetary_value ScoringSampleids_upd.csv pivotids.csv')
get_ipython().system(u'awk -F \'|\' \'{print $1"~"$2"~"$3"~"$4"~"$5"~"$12"|"$6"_"$7"_"$8"|"$10}\' ScoringSampleids_.csv > ScoringSampleids_CHECKQUANT.csv')


# !gcc-6/c63++ reducer_mappa.cpp -o mapping -static-libstdc++
get_ipython().system(u'./checkquant ScoringSampleids_CHECKQUANT.csv pivotids_CHECKQUANT.csv')
get_ipython().system(u'awk -F \'|\' \'{print $1"~"$2"~"$3"~"$4"~"$5"~"$12"|"$6"_"$7"_"$8"|"$11}\' ScoringSampleids_.csv > ScoringSampleids_QUANT.csv')


get_ipython().system(u'./quant ScoringSampleids_QUANT.csv pivotids_QUANT.csv')


# $$ P(A \mid B) = \frac{P(B \mid A) \, P(A)}{P(B)} $$

get_ipython().system(u'parallel "sort -t \'|\' -k1,1 -o {} {}" ::: pivotids*.csv')

get_ipython().system(u"join -t '|' -1 1 -2 1 pivotids.csv pivotids_CHECKQUANT.csv > pivotids_CQ_MV.csv")

get_ipython().system(u"sort -t '|' -k1,1 -o pivotids_CQ_MV.csv pivotids_CQ_MV.csv")

get_ipython().system(u"join -t '|' -1 1 -2 1 pivotids_CQ_MV.csv pivotids_QUANT.csv > pivotids_tot.csv")

get_ipython().system(u'tail -n1 pivotids_tot.csv | sed "s/~/|/g; s/\'/!/g" > header.txt')


import fileinput
from __future__ import print_function
for line in fileinput.FileInput("header.txt",inplace=1):
    line = translit(line,'ru', reversed=True)
    print (line, end='') 

import fileinput
from __future__ import print_function
for line in fileinput.FileInput("pivotids_tot.csv",inplace=1):
    line = line.replace("~","|")
    print (line, end='') #!sed -i 's/~/|/g' pivotids.csv'''

with open("pivotids_tot.csv", "r") as infile, open("pivotids_inp.csv", "w") as outfile:
    reader = csv.reader(infile, delimiter="|")
    writer = csv.writer(outfile, delimiter="|")
    for row in reader:
        #row.replace("~","|")
        if row[4].isdigit():
            writer.writerow(row)
            #print (line, end='')


get_ipython().system(u"sort -u -t'|' --key=5,5 -o pivotids_inp.csv pivotids_inp.csv ")



get_ipython().system(u'cat header.txt pivotids_inp.csv > pivotids_input.csv')


for line in fileinput.FileInput("pivotids_tot.csv",inplace=1, backup='.bak'):
    #line = line.split('|')
    #line = line.replace("~","|")
    if line[4].isdigit():
        print (line, end='')#не определяет разделитель

get_ipython().magic(u'matplotlib inline')
from pandas.tools.plotting import scatter_matrix
scatter_matrix(df_reader, alpha=0.05, figsize=(15, 15));
#df_MV=pd.read_csv('pivotids.csv',delimiter="|")
#df_CQ=pd.read_csv('pivotids_CHECKQUANT.csv',delimiter="|")
#pd.merge(tablo,df_full, how='inner', on=['DTRANSACT', 'SKLAD','NOMGR'],suffixes=('_left', '_right'))
#df_Quant=pd.read_csv('pivotids_QUANT.csv',delimiter="|")
pd.merge(df_CQ,df_MV, how='outer', on=['RECENCY', 'MINDATE','CUR_AGE','PHONE_ids','Response'],left_on=)



import graphlab as gl
read_file=gl.SFrame.read_csv('pivotids_input.csv',delimiter='|')
#read_file=read_file.to_dataframe()


import graphlab.aggregate as agg
neww=read_file.groupby(key_columns='PHONE_ids', operations={'count': agg.COUNT()})
neww[neww['count'] > 1] 

read_file.remove_column('MONETARY_VALUE_SEGM_TOVARKATEG_ENG_NOMGR')
read_file.remove_column('CHECKQUANT_SEGM_TOVARKATEG_ENG_NOMGR')
read_file.remove_column('QUANT_SEGM_TOVARKATEG_ENG_NOMGR')
read_file.remove_column('QUANT_')
read_file.remove_column('CHECKQUANT_')
read_file.remove_column('MONETARY_VALUE_')
#read_file=read_file.iloc[:,:-2].replace(np.nan, 0)


"','".join(read_file.column_names())

user_schema = {
    'conversion_status': 'Response',
    'account_id': 'PHONE_ids',
     'features': ['idsPURCH','RECENCY','MINDATE','CUR_AGE','MONETARY_VALUE']}

read_file.remove_column('MONETARY_VALUE_Lux_Fragrance_CARTIER')
read_file.remove_column('MONETARY_VALUE_Lux_Fragrance_CAUDALIE')
read_file.remove_column('MONETARY_VALUE_Lux_Fragrance_CELLCOSMET')
read_file.remove_column('MONETARY_VALUE_Lux_Related goods_ACCA KAPPA')
read_file.remove_column('MONETARY_VALUE_Lux_Related goods_MAGHIALI')
read_file.remove_column('MONETARY_VALUE_Lux_Skin Care_CAROL JOY')
read_file.remove_column('MONETARY_VALUE_Mass market_Make Up_INM')
read_file.remove_column('MONETARY_VALUE_Mass market_Related goods_NIEGELOH')
read_file.remove_column('MONETARY_VALUE_Mass market_Related goods_OPI')
read_file.remove_column('MONETARY_VALUE_Mass market_Related goods_FEN ShUJ')
read_file.remove_column('MONETARY_VALUE_Mass market_Skin Care_DOCTOR NATURE')
read_file.remove_column('MONETARY_VALUE_Niche_Fragrance_ANDREA MAACK')
read_file.remove_column('MONETARY_VALUE_Niche_Fragrance_FRAPIN')
read_file.remove_column('MONETARY_VALUE_Niche_Fragrance_NOBILE 1942')
read_file.remove_column('CHECKQUANT_Lux_Fragrance_CARTIER')
read_file.remove_column('CHECKQUANT_Lux_Fragrance_CAUDALIE')
read_file.remove_column('CHECKQUANT_Lux_Fragrance_CELLCOSMET')
read_file.remove_column('CHECKQUANT_Lux_Related goods_ACCA KAPPA')
read_file.remove_column('CHECKQUANT_Lux_Related goods_MAGHIALI')
read_file.remove_column('CHECKQUANT_Lux_Skin Care_CAROL JOY')
read_file.remove_column('CHECKQUANT_Mass market_Make Up_INM')
read_file.remove_column('CHECKQUANT_Mass market_Related goods_NIEGELOH')
read_file.remove_column('CHECKQUANT_Mass market_Related goods_OPI')
read_file.remove_column('CHECKQUANT_Mass market_Related goods_FEN ShUJ')
read_file.remove_column('CHECKQUANT_Mass market_Skin Care_DOCTOR NATURE')
read_file.remove_column('CHECKQUANT_Niche_Fragrance_ANDREA MAACK')
read_file.remove_column('CHECKQUANT_Niche_Fragrance_FRAPIN')
read_file.remove_column('CHECKQUANT_Niche_Fragrance_NOBILE 1942')
read_file.remove_column('QUANT_Lux_Fragrance_CARTIER')
read_file.remove_column('QUANT_Lux_Fragrance_CAUDALIE')
read_file.remove_column('QUANT_Lux_Fragrance_CELLCOSMET')
read_file.remove_column('QUANT_Lux_Related goods_ACCA KAPPA')
read_file.remove_column('QUANT_Lux_Related goods_MAGHIALI')
read_file.remove_column('QUANT_Lux_Skin Care_CAROL JOY')
read_file.remove_column('QUANT_Mass market_Make Up_INM')
read_file.remove_column('QUANT_Mass market_Related goods_NIEGELOH')
read_file.remove_column('QUANT_Mass market_Related goods_OPI')
read_file.remove_column('QUANT_Mass market_Related goods_FEN ShUJ')
read_file.remove_column('QUANT_Mass market_Skin Care_DOCTOR NATURE')
read_file.remove_column('QUANT_Niche_Fragrance_ANDREA MAACK')
read_file.remove_column('QUANT_Niche_Fragrance_FRAPIN')
read_file.remove_column('QUANT_Niche_Fragrance_NOBILE 1942')


# In[30]:


model = gl.lead_scoring.create(read_file, user_schema,verbose=True, max_segments=9)
model
#read_file[(read_file['PHONE_ids'] !=91) & (read_file['PHONE_ids'] !=9)]


# In[15]:


model.open_account_scores.select_columns(['account_id', 'conversion_prob', 'segment_id']).export_csv('shise_opnPower.csv', delimiter='|')


# In[ ]:


model.show()
