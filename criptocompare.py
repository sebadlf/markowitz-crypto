import requests as rq
import pandas as pd
import datetime

def getDataHist_cryptcompare(ticker, currency = 'USD', limit = 2000):
    '''
    ticker: ticker de la criptomoneda
    currency: es la moneda contra la cual queremos ver la criptomoneda
    limit: es la cantidad de datos que te tira cada descarga. El máximo es 2000
    '''
    
    import requests as rq
    import pandas as pd
    import datetime
    
    url = 'https://min-api.cryptocompare.com/data/v2/histoday?' 

    fsym = ticker
    tsym = currency
    limit = limit

    parametros = {'fsym': fsym, 'tsym': tsym, 'limit': limit}
    data = rq.get(url, params = parametros)

    df = pd.DataFrame.from_dict(data.json()['Data']['Data'])
    df["time"] = pd.to_datetime(df.time, unit="s")
    df.set_index('time', inplace = True)
    df.index.name = 'Date'
    df = df[['open', 'high', 'low', 'close']]
    df.columns = ['Open', 'High', 'Low', 'Close']
    return df

def calc_variacion(data):
    import datetime
    import numpy as np
    inicio_año = datetime.date(data.index[len(data)-1].year,1,1).strftime('%Y-%m-%d') #calcula el inicio del año corriente para el calculo del YTD
    #data.iloc[data.index == inicio_año].Close[0]

    ult_precio = data.Close[len(data)-1]
    ytd = (data.Close[len(data)-1] / data.iloc[data.index == inicio_año].Close[0] - 1) * 100
    ult_dia = float(np.where(len(data) < 2, 0, (data.Close[len(data)-1] / data.Close[len(data)-2] -1 ) * 100))
    ult_3dias = float(np.where(len(data) < 4, 0, (data.Close[len(data)-1] / data.Close[len(data)-4] -1 ) * 100))
    ult_7dias = float(np.where(len(data) < 8, 0, (data.Close[len(data)-1] / data.Close[len(data)-8] -1 ) * 100))
    ult_15dias = float(np.where(len(data) < 16, 0, (data.Close[len(data)-1] / data.Close[len(data)-16] -1 ) * 100))
    ult_30dias = float(np.where(len(data) < 31, 0, (data.Close[len(data)-1] / data.Close[len(data)-31] -1 ) * 100))
    ult_60dias = float(np.where(len(data) < 61, 0, (data.Close[len(data)-1] / data.Close[len(data)-61] -1 ) * 100))
    ult_90dias = float(np.where(len(data) < 91, 0, (data.Close[len(data)-1] / data.Close[len(data)-91] -1 ) * 100))
    ult_180dias = float(np.where(len(data) < 181, 0, (data.Close[len(data)-1] / data.Close[len(data)-181] -1 ) * 100))
    ult_año = float(np.where(len(data) < 366, 0, (data.Close[len(data)-1] / data.Close[len(data)-366] -1 ) * 100))
    ult_2años = float(np.where(len(data) < (365*2+1), 0, (data.Close[len(data)-1] / data.Close[len(data)-(365*2+1)] -1 ) * 100))
    ult_3años = float(np.where(len(data) < (365*3+1), 0, (data.Close[len(data)-1] / data.Close[len(data)-(365*3+1)] -1 ) * 100))
    ult_5años = float(np.where(len(data) < (365*5+1), 0, (data.Close[len(data)-1] / data.Close[len(data)-(365*5+1)] -1 ) * 100))

    lista = [ult_5años, ult_3años, ult_2años, ult_año, ult_180dias, ult_90dias, 
             ult_60dias, ult_30dias, ult_15dias, ult_7dias, ult_3dias, ult_dia, ytd, ult_precio]

    return lista

cryptos = ['BTC', 'ETH', 'XRP', 'BCH', 'LINK', 'CRO', 'BNB', 'ADA', 'LTC', 'EOS', 'TRX', 'VET', 'MIOTA', 'DASH', 'ETC', 'OMG', 'DOGE', 'BAT', 'DGB', 'YFI', 'YFII', 'SXP', 'UNI', 'DOT', 'XLM', 'COMP', 'ATOM', 'LEND', 'BAND', 'BAL', 'MKR', 'ALGO', 'ZIL', 'KAVA']

print('hola')