#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct  8 19:13:08 2020

@author: sebastian
"""


import cryptocompare
import markowitzevolution
import utils
import numpy as np

# Obtiene el top 50 de tickers ordenados por volumen
top = cryptocompare.get_top_tickers(50, cache_days=30)

tickers = top.ticker

# Obtiene el historico de precios de los tickers pasados como parametros
prices = cryptocompare.get_mutiple_close_prices(tickers, cache_days= 1)

retornos = np.log((prices/prices.shift(1)))
                                   
for ticker in prices.columns:
    sma_5 = prices[ticker].rolling(5).mean()
    sma_10 = prices[ticker].rolling(10).mean()

    comprado = (sma_5 / sma_10) >= 1
    
    retornos['estoyComprado'] = comprado
    
    retornos[ticker+'_sma'] = np.where(retornos.estoyComprado, retornos[ticker], 0)    
    
    dif = prices[tickers].diff()
    win =  pd.DataFrame(np.where(dif > 0, dif, 0))
    loss = pd.DataFrame(np.where(dif < 0, abs(dif), 0))
    ema_win = win.ewm(alpha=1/rsi_q).mean()
    ema_loss = loss.ewm(alpha=1/rsi_q).mean()
    rs = ema_win / ema_loss
    rsi = 100 - (100 / (1+rs))
    rsi.index = data.index
    
    
    
    data['rsi'] = rsi    
    
    
    
        
retornos = retornos.drop('estoyComprado', axis=1)

#evolution = markowitzevolution.markowitz_evolution(retornos, step=7, count=52, q_inicial=1500)
#utils.save('mark', evolution) 

resultado = markowitzevolution.markowitz_rolling(retornos, '2020-10-15')
