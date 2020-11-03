import requests, pandas as pd
import numpy as np
import tqdm
import datetime as dt
from  keys import *
import utils
from db import BD_CONNECTION
from sqlalchemy import create_engine
engine = create_engine(BD_CONNECTION)
import datetime

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

def fechaEnMs(dt):
    epoch = datetime.datetime.utcfromtimestamp(0)
    return int((dt - epoch).total_seconds() * 1000)

def DailySymbolVolSingleExchange(symbol, interval='1d', startTime=None, endTime=None, limit=30):
    '''bajadaSimple('BTCUSDT',interval='2h',startTime=1597719600000,endTime=1600398000000)'''
    url = 'https://api.binance.com/api/v3/klines'
    params = {'symbol':symbol, 'interval':interval,'startTime':startTime, 'endTime':endTime, 'limit':limit}
    r = requests.get(url, params=params)
    js = r.json()
    # Armo el dataframe
    cols = ['open_time','open','high','low','close','volume','c_time',
            'q_volume','trades','taker_base','taker_quote','ignore']
    df = pd.DataFrame(js, columns=cols)
    
    #Convierto los valores strings a numeros
    df = df.apply(pd.to_numeric)
    
    # Le mando indice de timestamp
    df['time'] = pd.to_datetime(df.open_time, unit='ms')
    #df.drop(['openTime','cTime','qVolume','trades','takerBase','takerQuote','Ignore'],axis=1,inplace=True)
    return df


#Ejemplo de uso.
tickers = ['BTC', 'ETH', 'LTC', 'ETC', 'XRP', 'EOS', 'BCH', 'BSV', 'TRX']

#tickers = ['BTC']

create_table = '''
CREATE TABLE IF NOT EXISTS `binance` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `ticker` varchar(20) DEFAULT '',
  `time` datetime DEFAULT NULL,
  `open` double DEFAULT NULL,
  `high` double DEFAULT NULL,
  `low` double DEFAULT NULL,
  `close` double DEFAULT NULL,
  `volume` double DEFAULT NULL,
  `open_time` bigint(20) DEFAULT NULL,
  `c_time` bigint(20) DEFAULT NULL,
  `q_volume` double DEFAULT NULL,
  `trades` bigint(20) DEFAULT NULL,
  `taker_base` double DEFAULT NULL,
  `taker_quote` double DEFAULT NULL,
  `ignore` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_ticker_time` (`ticker`,`time`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=latin1;
'''
engine.execute(create_table)

for ticker in tickers:

    print(ticker)

    finished = False
    while not finished:

        busquedaUltimaFecha = f'SELECT `id`,`time` FROM binance WHERE `ticker` = "{ticker}" ORDER BY `time` DESC limit 0,1'
        ultimaFecha = engine.execute(busquedaUltimaFecha).fetchone()

        start = datetime.datetime.now() - datetime.timedelta(days=10)

        if (ultimaFecha):
            id = ultimaFecha[0]
            start = ultimaFecha[1]

            query_borrado = f'DELETE FROM binance WHERE `id`={id}'
            engine.execute(query_borrado)

        end = start + datetime.timedelta(minutes=1000)

        start = fechaEnMs(start)

        df = DailySymbolVolSingleExchange(f'{ticker}USDT', interval='1m', startTime=start, limit=1000)

        df['ticker'] = ticker

        df.set_index('time', inplace=True)

        df.to_sql('binance', engine, if_exists='append')

        finished = len(df) < 2

        print(df)


# create_vista = '''
# CREATE VIEW  margenes AS
# SELECT b.ticker, b.time, b.close as 'binance_close', o.close as 'okex_close', ((b.close / o.close - 1.0) * 100.0) as 'diferencia'
# FROM binance b JOIN okex o
# ON b.ticker = o.ticker and b.time = o.time
# where b.ticker != 'TRX'
# order by abs(diferencia) desc
# '''
# engine.execute(create_vista)





