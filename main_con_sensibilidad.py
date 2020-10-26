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
import pandas as pd
import sensibilidad

best_sma = []
best_rsi = []

def get_retornos_sma_optimo(ticker, prices, retornos):
    data = pd.DataFrame(prices)
    data.columns = ['Close']
    data['retornos'] = retornos

    data_sma = sensibilidad.get_sensitivity(prices, sensibilidad.add_signals_sma, sensibilidad.get_params_sma, cicles=200)

    row0_sma = data_sma.iloc[0]
    sma_params = {
        'sma_fast': row0_sma.sma_fast,
        'sma_slow': row0_sma.sma_slow,
        'diff_buy': row0_sma.diff_buy,
        'diff_sell': row0_sma.diff_sell,
    }

    sensibilidad.add_signals_sma(data, sma_params)
    trades = sensibilidad.get_trades(data)
    yields = sensibilidad.get_yields(trades)

    best_sma.append({
        'ticker': ticker,
        'result': row0_sma['result'],
        **sma_params
    })

    data['estoyComprado'] = False

    for i in range(len(yields)):
        op = yields.iloc[i]
        data['estoyComprado'] = np.where(
            (data.index >= op.start) & (data.index <= op.end), True, data.estoyComprado)

    data['result'] = np.where(data.estoyComprado, data.retornos, 0)

    return data.result

def get_retornos_rsi_optimo(ticker, prices, retornos):
    data = pd.DataFrame(prices)
    data.columns = ['Close']
    data['retornos'] = retornos

    data_rsi = sensibilidad.get_sensitivity(prices, sensibilidad.add_signals_rsi, sensibilidad.get_params_rsi, cicles=200)

    if len(data_rsi > 0):
        row0_rsi = data_rsi.iloc[0]
        rsi_params = {
            'rsi_q': row0_rsi.rsi_q,
            'rsi_buy': row0_rsi.rsi_buy,
            'rsi_sell': row0_rsi.rsi_sell,
        }

        best_rsi.append({
            'ticker': ticker,
            'result': row0_rsi['result'],
            **rsi_params
        })

        sensibilidad.add_signals_rsi(data, rsi_params)
        trades = sensibilidad.get_trades(data)
        yields = sensibilidad.get_yields(trades)

        data['estoyComprado'] = False

        for i in range(len(yields)):
            op = yields.iloc[i]
            data['estoyComprado'] = np.where(
                (data.index >= op.start) & (data.index <= op.end), True, data.estoyComprado)

        data['result'] = np.where(data.estoyComprado, data.retornos, 0)
    else:
        data['result'] = 0

    return data.result

def agrego_indicadores(prices):
    retornos = np.log((prices / prices.shift(1)))

    result = pd.DataFrame(index=retornos.index)

    for ticker in list(prices.columns):
        print(ticker)

        result[ticker] = retornos[ticker]

        #result[ticker + '_sma'] = get_retornos_sma_optimo(ticker, prices[ticker], retornos[ticker])
        #result[ticker + '_rsi'] = get_retornos_rsi_optimo(ticker, prices[ticker], retornos[ticker])

        try:
            result[ticker + '_sma'] = get_retornos_sma_optimo(ticker, prices[ticker], retornos[ticker])
        except:
            print("Error en " + ticker + '_sma' )

        try:
            result[ticker + '_rsi'] = get_retornos_rsi_optimo(ticker, prices[ticker], retornos[ticker])
        except:
            print("Error en " + ticker + '_rsi' )

    return result.dropna()


# Obtiene el top 50 de tickers ordenados por volumen
top = cryptocompare.get_top_tickers(50, cache_days=30)

tickers = top.ticker

# Obtiene el historico de precios de los tickers pasados como parametros
prices = cryptocompare.get_mutiple_close_prices(tickers, cache_days=0, limit=500)

prices = prices.drop(['USDC' ,'TUSD', 'BUSD', 'USDT', 'DAI', 'PAX'], axis=1, errors='ignore')

#prices = utils.open('prices')

#columns = prices.columns[:10]
#columns = ['USDC', 'BSV']

#prices = prices[columns]

retornos = agrego_indicadores(prices)

utils.save('retornos', retornos)

utils.save('best_sma', pd.DataFrame(best_sma))
utils.save('best_rsi', pd.DataFrame(best_rsi))

#retornos = utils.open('retornos')

evolution = markowitzevolution.markowitz_evolution(retornos, step=7, count=52, q_inicial=100)
utils.save('mark', evolution)

#resultado = markowitzevolution.markowitz_rolling(retornos, '2020-10-15')
