# coding: utf-8

import pandas as pd
import requests
from lxml.html import fromstring
from bs4 import BeautifulSoup
import numpy as np
#from math import sin, cos, sqrt, atan2, radians
import re

r = requests.get('https://xxx1.ru/company/shops/')
soup = BeautifulSoup(r.text, "lxml",from_encoding="utf-8")
towns=[]
tr= soup.find_all('li')
regularka=re.compile('/company/shops/(.*?)/', re.DOTALL) 
for td in tr:
    try:
        if td.a!=None and regularka.search(str(td.a['href'])) is not None and td.a['href']!='/company/shops/events/' and td.a['href']!='/company/shops/online/':
            towns.append(td.a['href']) #
    except KeyError:
        pass
shops=[]
#while (len(shops))< 127:
for k in range(0,44):
    r = requests.get('https://xxx1.ru'+towns[k])
    soup = BeautifulSoup(r.text, "lxml",from_encoding="utf-8")
    tr= soup.find_all('td')    
    for td in tr:
        if td.a!=None:
            shops.append(td.a['href'])
print (len(shops))

fil=open("x1_parsing.csv",'w')
fil.write("City|Address|Link|GeoCoord\n")
r = requests.get('https://xxx1.ru/company/shops/')
soup = BeautifulSoup(r.text, "lxml",from_encoding="utf-8")
towns=[]
tr= soup.find_all('li')
regularka=re.compile('/company/shops/(.*?)/', re.DOTALL) 
for td in tr:
    try:
        if td.a!=None and regularka.search(str(td.a['href'])) is not None and td.a['href']!='/company/shops/events/' and td.a['href']!='/company/shops/online/':
            towns.append(td.a['href']) #
    except KeyError:
        pass
shops=[]
while (len(shops))< 126:
    shops=[]
    for k in range(0,44):
        r = requests.get('https://xxx1.ru'+towns[k])
        soup = BeautifulSoup(r.text, "lxml",from_encoding="utf-8")
        tr= soup.find_all('td')    
        for td in tr:
            if td.a!=None:
                shops.append(td.a['href'])
                if (len(shops))>=126:
                    break
                #print (len(shops))
                
rega = re.compile('point_center = new GLatLng((.*?));', re.DOTALL)                
if (len(shops))>=126:
    #address=None
    #k=0
    for link in range(0,126):
        r2 = requests.get('https://xxx1.ru/'+shops[link])
        souper = BeautifulSoup(r2.text, "lxml",from_encoding="utf-8")
        geoloc=souper.find_all('script', {'type': 'text/javascript'})  
        address = fromstring(r2.content)
        while address.findtext('.//h1') is None:
            address = fromstring(requests.get('https://xxx1.ru/'+shops[link]).content)
            if address.findtext('.//h1') is not None:
                break
            #print (tree.findtext('.//h3'))
            
        matches = rega.search(str(geoloc))
        #k=k+1
        #print(k)
        if matches is not None:
            fil.write (address.findtext('.//h1').replace('рестораны, ', '', 1) + "|" + address.findtext('.//h3') + "|" + 'https://xxx1.ru' + shops[link] + "|" + matches.group(0).replace('point_center = new GLatLng(', '', 1).replace(');', '', 1) + "\n")
        else:
            fil.write (address.findtext('.//h1').replace('рестораны, ', '', 1)  + "|" + (address.findtext('.//h3') ) + "|" + 'https://xxx1.ru' + shops[link] + "\n")
fil.close()

base=BeautifulSoup(requests.get('http://b2b.xxx2.ru/retail/?change_city').text, "lxml",from_encoding="utf-8").find_all('div', {'class': 'g-item'})
file=open("x2_parsing.csv",'w')
file.write("Address|Link|GeoCoord|City\n")
listik = []
for th in base:
    listik.append(th.a['href'].replace('.', ''))
#n=0   
shops=[]
for k in range(0,245):
    yar=(BeautifulSoup(requests.get('http://b2b.xxx2.ru/retail'+listik[k]+ '&all=yes').text, "lxml",from_encoding="utf-8").find_all('div', {'class': 'g-item'}))
    
    for i in yar:
        #n=n+1
        #print(n)        
        shops.append(listik[k] + "&" + i.a['onclick'].replace("show_info(this, '/shop.php?","").replace("&all=yes&24h='); return false",""))
print(len(shops))

for el in range(0,941):
    x2pars = requests.get('http://b2b.xxx2.ru/retail/'+shops[el]+'/')
    soup = BeautifulSoup(x2pars.text, "lxml",from_encoding="utf-8")
    geoloc=soup.find_all('script', {'type': 'text/javascript'})
    #rega = re.compile('[(.*?)], {', re.DOTALL)
    rega = re.compile('map_center = \[(.*?)\],', re.DOTALL) 
    matches = rega.search(str(geoloc))
    addres=soup.find_all('div', {'class': 'shop-single-info'})[0].find('b')
    
    for row in addres:
        addr=row
    for row in soup.find_all('div', {'class': 'main-right-title'}):
        if matches is not None:
            file.write (addr + "|" + 'http://b2b.xxx2.ru/retail/' + shops[el] + " |" + matches.group(0).replace('map_center = [','').replace('],','') + " |" + row.find('h1').text.replace('рестораны – ', '').replace(' сменить город','') + "\n")
    
    
file.close()    

import geopy.distance
x1=pd.read_excel('our.xlsm',header=1,sheet_name='Справочник')

x1=x1[['Наименование ресторана','Город','Координаты']][(x1['Статус\n6']=="активный")].dropna()
x1['Chain']='RG'
x1.rename(columns={'Наименование ресторана': 'Address','Город': 'City','Координаты': 'GeoCoord'}, inplace=True)


fin=x1.append(x2).append(x1)
fin.fillna("0.000000, 0.000000",inplace=True)
fin['GeoCoord'].replace(' ', "0.000000, 0.000000", inplace = True)

loca=open("Distance_Matrix.csv",'w')
loca.write("Chain1|City1|Address1|GeoCoord1|Distance|Chain2|City2|Address2|GeoCoord2\n")
for index, row1 in fin.iterrows():
    name1=row1["Address"]
    city1=row1["City"]
    geo1=row1["GeoCoord"]
    chain1=row1["Chain"]
    for index, row2 in fin.iterrows():
        name2=row2["Address"]
        city2=row2["City"]
        geo2=row2["GeoCoord"]
        chain2=row2["Chain"]
        
        try:
            if chain1=="RG" and geo2!=0 and geo2!=' ' and (geopy.distance.vincenty(geo1, geo2).km < 30) and geo1!=0 and (geopy.distance.vincenty(geo1, geo2).km !=0) :
                loca.write (chain1 + "|" + city1 + "|" + name1 + "|" + geo1 + "|" + str(geopy.distance.vincenty(geo1, geo2).km) + "|" + chain2 + "|" + city2 + "|" + name2 + "|" +  geo2 + "\n")
                #break
        except ValueError:
            loca.write (chain1 + "|" + city1 + "|" + name1 + "|" + geo1 + "|"  + chain2  + "|" + city2 + "|" + name2 + "|" +  geo2 + "\n")
loca.close()

compet_quant=pd.read_csv('Distance_Matrix.csv',sep='|')
competit_quant=compet_quant[(compet_quant['Chain2']!="RG") & (compet_quant['Distance']<3)].groupby(['City1','Address1','GeoCoord1']).agg({'Distance': [len,np.median]}).reset_index()
competit_quant.columns = competit_quant.columns.map('_'.join)
competit_quant.rename(columns={'Address1_': 'SKLAD'}, inplace=True)

