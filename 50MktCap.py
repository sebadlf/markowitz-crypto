import requests, pandas as pd
import numpy as np
import tqdm
import datetime as dt
import claveCryptocompare


def AvailableCoinList():
    url='https://min-api.cryptocompare.com/data/blockchain/list'
    api_key = claveCryptocompare.CLAVE
    params = {'api_key': api_key}
    r = requests.get(url, params=params)
    js = r.json()
    df = pd.DataFrame(js)
    
    return df.index

def DailySymbolVolSingleExchange(fsym='BTC',tsym='USD',limit=30):
    url='https://min-api.cryptocompare.com/data/exchange/symbol/histoday?'
    api_key = claveCryptocompare.CLAVE
    params = {'api_key': api_key,'fsym':fsym,'tsym':tsym,'limit':limit}
    r = requests.get(url, params=params)
    js = r.json()['Data']
    df = pd.DataFrame(js)
#    df.time = pd.to_datetime(df.time, unit = 's')
#    df = df.set_index('time')
#    df = df[(df.T != 0).any()]
#    df = df.reset_index()
    try:
        total=df['volumetotal'].sum()
    except: 
        total=0
    return total



    
def histoDay(e,fsym,tsym,toTs=None,limit = 2000, aggregate=1, allData='true'):
    url = 'https://min-api.cryptocompare.com/data/v2/histoday'
    api_key = claveCryptocompare.CLAVE
    params = {'api_key': api_key, 'e': e, 'fsym': fsym, 'tsym': tsym, 'allData': allData, 'toTs': toTs, 'limit': limit, 'aggregate': aggregate}
    r = requests.get(url, params=params)
    js = r.json()['Data']['Data']
    df = pd.DataFrame(js)
    df.time = pd.to_datetime(df.time, unit = 's')
    df = df.set_index('time').drop(['conversionType','conversionSymbol'],axis=1)
    df = df[(df.T != 0).any()]
    df = df.reset_index()
    df = df[df['time'].dt.year>2016]    
    return df



def mutiple_close_prices():
    tablas = {}  
    for ticker in tqdm.tqdm(tickers):
        data = histoDay('CCCAGG',ticker,'USD')
        data = data.set_index('time')
        tablas[ticker] = data.drop(['open','high','low','volumefrom','volumeto'],axis=1)
        
    tabla = pd.DataFrame(tablas[tickers[0]])
    for i in range (1,len(tickers)):
        tabla = pd.concat([tabla, tablas[tickers[i]]],axis=1)
    tabla.columns = tickers
    return tabla

def topXExchange(cantidad=50):
    todosLosTickers= list(AvailableCoinList())

    values = []
    for i in tqdm.tqdm(range(len(todosLosTickers))):
        ticker = todosLosTickers[i]
        try:            
            values.append({
                'ticker': ticker,
                'volumen': DailySymbolVolSingleExchange(ticker)
            })
        except:
            print("Error con ticker " + ticker)
        resultado = pd.DataFrame(values)                     
    return resultado.sort_values("volumen", ascending=False).head(cantidad)

def estadoSeleccion(dias=30,tickers=50):
    hoy=dt.date.today()
    ultimaCorrida=claveCryptocompare.ULTIMACORRIDA
    try:
        if (ultimaCorrida == ''):
            print('Entro por vacio')
            claveCryptocompare.ULTIMACORRIDA=hoy
    except:
        if (ultimaCorrida < (hoy-dt.timedelta(days=dias))):
            print('Entro por fecha')

            
