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

def get_operaciones(data):
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

def agrego_indicadores(prices):
    retornos = np.log((prices/prices.shift(1)))

    for ticker in prices.columns:
        sma_5 = prices[ticker].rolling(5).mean()
        sma_10 = prices[ticker].rolling(10).mean()
    
        comprado = (sma_5 / sma_10) >= 1
        
        retornos['estoyComprado'] = comprado
        
        retornos[ticker+'_sma'] = np.where(retornos.estoyComprado, retornos[ticker], 0)    
        
        rsi_q = 9
        
        dif = prices[ticker].diff()
        win =  pd.DataFrame(np.where(dif > 0, dif, 0))
        loss = pd.DataFrame(np.where(dif < 0, abs(dif), 0))
        ema_win = win.ewm(alpha=1/rsi_q).mean()
        ema_loss = loss.ewm(alpha=1/rsi_q).mean()
        rs = ema_win / ema_loss
        rsi = 100 - (100 / (1+rs))
        rsi.index = retornos.index
        retornos['rsi'] = rsi   
        
        retornos['signal'] = np.where(retornos.rsi < 30, 'buy', np.where(retornos.rsi > 70, 'sell', 'hold'))
        
        operaciones = get_operaciones(retornos)
        
        retornos['estoyComprado'] = False
        
        for i in range(len(operaciones)):
            op = operaciones.iloc[i]
            retornos['estoyComprado'] = np.where((retornos.index >= op.fecha_compra) & (retornos.index <= op.fecha_venta), True, False)
        
        retornos[ticker+'_rsi'] = np.where(retornos.estoyComprado, retornos[ticker], 0) 
    
    retornos = retornos.drop(['estoyComprado', 'rsi', 'signal'], axis=1)    
    
    return retornos 
