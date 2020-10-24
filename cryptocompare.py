import requests, pandas as pd
import numpy as np
import tqdm
import datetime as dt
from  keys import *
import utils

def AvailableCoinList():
    url='https://min-api.cryptocompare.com/data/blockchain/list'
    api_key = CRYPTOCOMPARE_KEY
    params = {'api_key': api_key}
    r = requests.get(url, params=params)
    js = r.json()
    df = pd.DataFrame(js)
    
    return df.index

def DailySymbolVolSingleExchange(fsym='BTC',tsym='USD',limit=30):
    url='https://min-api.cryptocompare.com/data/exchange/symbol/histoday?'
    api_key = CRYPTOCOMPARE_KEY
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

   
def histoDay(e,fsym,tsym,limit,toTs=None, aggregate=1, allData='false'):
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



def mutiple_close_prices(tickers,limit):
    tablas = {}
    for ticker in tqdm.tqdm(tickers):                
        try:
            data = histoDay('CCCAGG',ticker,'USD',limit=limit)
            data = data.set_index('time')
            tablas[ticker] = data.drop(['open','high','low','volumefrom','volumeto'],axis=1)
        except:
            print('Error al descargar ' + ticker)

    tickers = list(tablas.keys())

    tabla = pd.DataFrame(tablas[tickers[0]])
    for i in range (1,len(tickers)):
        tabla = pd.concat([tabla, tablas[tickers[i]]],axis=1)
    tabla.columns = tickers
    return tabla

def get_mutiple_close_prices(tickers, cache_days = 1,limit=365):
    if (utils.file_exists('prices') and (utils.is_older_than('prices', cache_days) == False)):
        top = utils.open('prices')
    else:
        top = mutiple_close_prices(tickers,limit=limit)
        utils.save('prices', top)    
        
    return top


def download_top_tickers(cantidad=50):
    '''Descarga los X tickers con mayor volumen dentro de  los ultimos 30 dias'''

    todosLosTickers= list(AvailableCoinList())

    values = []
    for i in tqdm.tqdm(range(len(todosLosTickers))):
    #for i in tqdm.tqdm(range(50)):
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

def get_top_tickers(cantidad = 50, cache_days = 30):
    if (utils.file_exists('top-tickers') and (utils.is_older_than('top-tickers', cache_days) == False)):
        top = utils.open('top-tickers')
    else:
        top = download_top_tickers(cantidad)
        utils.save('top-tickers', top)    
        
    return top
    
    
    
    