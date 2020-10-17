import os
import random
import datetime as dt
import pandas as pd
import numpy as np

def is_older_than(filename, days):
    time = os.path.getmtime('./files/' + filename + '.h5') 
    fecha_modificacion = dt.datetime.fromtimestamp(time) 
    ahora = dt.datetime.now()

    return fecha_modificacion < (ahora - dt.timedelta(days=days))

def file_exists(filename):
    return os.path.isfile('./files/' + filename + '.h5')

def save(filename, data):
    
    data.to_hdf('./files/' + filename + '.h5', 'data')    
    data.to_csv('./files/' + filename + '.csv')
    data.to_excel('./files/' + filename + '.xlsx')
        
    return data
        
def open(filename):
    data = pd.read_hdf('./files/' + filename + '.h5')
        
    return pd.DataFrame(data)


def sample_sin_repetir(list, k):
    result= []
    while len(result) < k:
        item = random.choice(list)
        if item not in result:
            result.append(item)
    
    return result

def get_operaciones_long(data):
    df = data.copy()

    # Una sola entrada y salida por vez
    trades = df.loc[df.signal != 'hold'].copy()
    trades['signal'] = np.where(trades.signal != trades.signal.shift(), trades.signal, 'hold')
    trades = trades.loc[trades.signal != 'hold'].copy()
    
    # Supuesto estrategia long, debe empezar con compra y terminar con venta
    if trades.iloc[0]['signal'] =='sell':
        trades = trades.iloc[1:]

    if trades.iloc[-1]['signal']=='buy':
        trades = trades.iloc[:-1]

    fechas_compra = trades.iloc[::2].index
    fechas_venta = trades.iloc[1::2].index

    operaciones = (fechas_compra).to_frame()
    operaciones['fecha_venta'] = fechas_venta
    
    operaciones.columns = ['fecha_compra', 'fecha_venta']
    
    return operaciones



def cruce_medias(data,ticker,rapida=5,lenta=10):
    '''
    En todo momento que la media movil RAPIDA este sobre la LENTA, estoy comprado.
    '''
    retornos = pd.DataFrame(np.log((data[ticker]/data[ticker].shift(1))))
    sma_rapida = data[ticker].rolling(rapida).mean()
    sma_lenta = data[ticker].rolling(lenta).mean()
    comprado = (sma_rapida / sma_lenta) >= 1
    retornos['estoyComprado'] = comprado
    retornos[ticker+'_sma'] = np.where(retornos.estoyComprado, retornos[ticker], 0)
    retornos = retornos.drop(['estoyComprado',f'{ticker}'], axis=1) 
    return retornos	

def rsi(data,ticker,rsi_q=9):
    '''
    Si el RSI es menor a 30 compramos y vendemos cuando llega a 70.
	'''
    retornos = pd.DataFrame(np.log((data[ticker]/data[ticker].shift(1))))
    dif = data[ticker].diff()
    win =  pd.DataFrame(np.where(dif > 0, dif, 0))
    loss = pd.DataFrame(np.where(dif < 0, abs(dif), 0))
    ema_win = win.ewm(alpha=1/rsi_q).mean()
    ema_loss = loss.ewm(alpha=1/rsi_q).mean()
    rs = ema_win / ema_loss
    rsi = 100 - (100 / (1+rs))
    rsi.index = retornos.index
    retornos['rsi'] = rsi    
    retornos['signal'] = np.where(retornos.rsi < 30, 'buy', np.where(retornos.rsi > 70, 'sell', 'hold'))
    operaciones = get_operaciones_long(retornos)
    retornos['estoyComprado'] = False
    for i in range(len(operaciones)):
        op = operaciones.iloc[i]
        retornos['estoyComprado'] = np.where((retornos.index >= op.fecha_compra) & (retornos.index <= op.fecha_venta), True, False)
    
    retornos[ticker+'_rsi'] = np.where(retornos.estoyComprado, retornos[ticker], 0)
    retornos = retornos.drop(['estoyComprado', 'rsi', 'signal',f'{ticker}'], axis=1)
    return retornos	




def agrego_indicadores(data):
    
    tickers=data.columns
    retornos=pd.DataFrame()
    for ticker in tickers:
        estrategia0=pd.DataFrame(np.log((data[ticker]/data[ticker].shift(1))))
        estrategia1=cruce_medias(data, ticker)
        estrategia2=rsi(data,ticker)
        frame=[retornos,estrategia0,estrategia1,estrategia2] 
        retornos=pd.concat(frame,axis=1)
#        retornos=pd.concat(retornos,estrategia1,estrategia2)
    return retornos 
