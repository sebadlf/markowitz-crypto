import requests, pandas as pd
import numpy as np
import tqdm
import datetime as dt
from  keys import *
import utils


###### API ######
'''
La base de lo utilizado en este archivo se encuentra en la pagina.
https://min-api.cryptocompare.com/documentation
'''

#Listado de modedas disponibles.
def AvailableCoinList():
    url='https://min-api.cryptocompare.com/data/blockchain/list'
    api_key = CRYPTOCOMPARE_KEY
    params = {'api_key': api_key}
    r = requests.get(url, params=params)
    js = r.json()
    df = pd.DataFrame(js)
    
    return df.index
#Ejemplo de uso.
#data=AvailableCoinList()

#OHLCV

def HourlyPairOHLCV(e='CCCAGG',fsym='BTC',tsym='USDT',limit=2000,FechaFinal=None):
    url='https://min-api.cryptocompare.com/data/v2/histohour'
    api_key = CRYPTOCOMPARE_KEY
    if FechaFinal==None:
        params = {'api_key': api_key,'e':e, 'fsym':fsym,'tsym':tsym,'limit':limit}
    else:
        toTs=utils.conviertoFechaS(utils.conviertoFecha(FechaFinal))
        params = {'api_key': api_key,'e':e, 'fsym':fsym,'tsym':tsym,'limit':limit,'toTs':toTs}
    r = requests.get(url, params=params)
    js = r.json()['Data']['Data']
    df = pd.DataFrame(js)
    df.time = pd.to_datetime(df.time, unit = 's')
    df = df.set_index('time').drop(['conversionType','conversionSymbol'],axis=1)
    df = df[(df.T != 0).any()]
    df = df.dropna()
    df = df.reset_index()
    return df
#hasta esta fecha funciona. sino de vacio. '01-11-2010'
#df=HourlyPairOHLCV()


def histoDay(e='CCCAGG',fsym='BTC',tsym='USDT',limit=2000,toTs=None, aggregate=1, allData='false'):
    url = 'https://min-api.cryptocompare.com/data/v2/histoday'
    api_key = CRYPTOCOMPARE_KEY
    params = {'api_key': api_key, 'e': e, 'fsym': fsym, 'tsym': tsym, 'allData': allData, 'toTs': toTs, 'limit': limit, 'aggregate': aggregate}
    r = requests.get(url, params=params)
    js = r.json()['Data']['Data']
    df = pd.DataFrame(js)
    df.time = pd.to_datetime(df.time, unit = 's')
    df = df.set_index('time').drop(['conversionType','conversionSymbol'],axis=1)
    df = df[(df.T != 0).any()]
    df = df.dropna()
    df = df.reset_index()
    #df = df[df['time'].dt.year>2016]
    return df


##### FIN API ############
from db import BD_CONNECTION
from sqlalchemy import create_engine
import pytz

listaTicker=['BTC','ETH','DAI']
listaBase=['USD','BTC']
engine = create_engine(BD_CONNECTION)

def actualizaBase():
    for ticker in listaTicker:
        for base in listaBase:
            if (ticker!= base): 
                tabla='cryptocompare_'+base.lower()
                fecha=None
                crearId=False
                #Si falla, es pq la tabla no existe.
                try:
                    busquedaUltimaFecha=f'SELECT `id`,`time` FROM `{tabla}` WHERE `ticker` = "{ticker}" ORDER BY `id` DESC limit 0,1'
              
                    
                    ultimaFecha=engine.execute(busquedaUltimaFecha).fetchall()
                    if (len(ultimaFecha)>0):                                        
                        id=ultimaFecha[0][0]
                        fecha=ultimaFecha[0][1]   
                        borradoUltimoRegistro=f'DELETE FROM `{tabla}` WHERE `id`={id}'
                        resultado=engine.execute(borradoUltimoRegistro)
                    else:
                        #print('aca seria poner la fecha desde')
                        pass
                except:
                    print(f'Creo la tabla de la base={base}')
                    crearId=True
                
                    
                #Definiendo la cantidad de registros hacia el pasado.
                if(fecha==None):
                    limit=2000
                else:
                    utczone=pytz.utc.zone
                    fecha=pytz.timezone(utczone).localize(fecha)
                    hoy=dt.datetime.today().astimezone(pytz.utc)
                    print(hoy,fecha)
                    diferencia=hoy-fecha
                    if(diferencia.days==0): 
                        limit=1
                    else:
                        limit=diferencia.days
                
                print(limit)
                df=histoDay(fsym=ticker,tsym=base,limit=limit)
                
#                print(df)
                df.set_index('time',inplace=True)
#                df.drop(columns=['index'],axis=1,inplace=True)
 
                df['ticker']=ticker           
                df.to_sql(tabla, engine, if_exists='append')
                
                if(crearId):
                    SQLcrearIndice=f'ALTER TABLE `{tabla}` ADD `id` INT NOT NULL AUTO_INCREMENT FIRST, ADD PRIMARY KEY (`id`)'
                    resultado=engine.execute(SQLcrearIndice)
actualizaBase()

            
'''
df['ticker']='BTC-USDT'
df=histoDay()    

engine = create_engine(BD_CONNECTION)


df.to_sql('cryptocompare_history', engine, if_exists='replace')
'''


    
    