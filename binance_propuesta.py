import requests, pandas as pd
import numpy as np
import tqdm
import datetime as dt
from  keys import *
import utils

###### API ######
'''
La base de lo utilizado en este archivo se encuentra en la pagina.
https://binance-docs.github.io/apidocs/spot/en/#general-info
General API Information
The base endpoint is: https://api.binance.com
All endpoints return either a JSON object or array.
Data is returned in ascending order. Oldest first, newest last.
All time and timestamp related fields are in milliseconds.
'''

#Listado de modedas disponibles.

'''https://binance-docs.github.io/apidocs/spot/en/#market-data-endpoints'''
def AvailableCoinList():   
    url='https://api.binance.com/api/v3/exchangeInfo'
    r = requests.get(url)
    js = r.json()['symbols']
    df = pd.DataFrame(js).quoteAsset
    df.drop_duplicates(inplace=True)
    return df

#Ejemplo de uso.
#data=AvailableCoinList()

''' Descripcion de la logica.
Kline/Candlestick chart intervals:
1m,3m,5m,15m,30m,1h,2h,4h,6h,8h,12h,1d,3d,1w,1M
Kline/Candlestick Data
GET /api/v3/klines

Kline/candlestick bars for a symbol.
Klines are uniquely identified by their open time.

Weight: 1

Parameters:

Name	Type	Mandatory	Description
symbol	STRING	YES	
interval	ENUM	YES	
startTime	LONG	NO	
endTime	LONG	NO	
limit	INT	NO	Default 500; max 1000.
If startTime and endTime are not sent, the most recent klines are returned.

Response:

[
  [
    1499040000000,      // Open time
    "0.01634790",       // Open
    "0.80000000",       // High
    "0.01575800",       // Low
    "0.01577100",       // Close
    "148976.11427815",  // Volume
    1499644799999,      // Close time
    "2434.19055334",    // Quote asset volume
    308,                // Number of trades
    "1756.87402397",    // Taker buy base asset volume
    "28.46694368",      // Taker buy quote asset volume
    "17928899.62484339" // Ignore.
  ]
]
'''

def DailySymbolVolSingleExchange(symbol, interval='1d', startTime=None, endTime=None, limit=30):
    '''bajadaSimple('BTCUSDT',interval='2h',startTime=1597719600000,endTime=1600398000000)'''
    url = 'https://api.binance.com/api/v3/klines'
    params = {'symbol':symbol, 'interval':interval,'startTime':startTime, 'endTime':endTime, 'limit':limit}
    r = requests.get(url, params=params)
    js = r.json()
    # Armo el dataframe
    cols = ['openTime','Open','High','Low','Close','Volume','cTime',
            'qVolume','trades','takerBase','takerQuote','Ignore']
    df = pd.DataFrame(js, columns=cols)
    
    #Convierto los valores strings a numeros
    df = df.apply(pd.to_numeric)
    
    # Le mando indice de timestamp
    df.index = pd.to_datetime(df.openTime, unit='ms')
    df.drop(['openTime','cTime','qVolume','trades','takerBase','takerQuote','Ignore'],axis=1,inplace=True)
    return df


#Ejemplo de uso.
data=DailySymbolVolSingleExchange('BTCUSDT')






