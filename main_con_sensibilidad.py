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


def agrego_indicadores(prices):
    retornos = np.log((prices / prices.shift(1)))

    for ticker in prices.columns:

        data_sma = sensibilidad.get_sensitivity(prices[ticker], sensibilidad.add_signals_sma, sensibilidad.get_params_sma)
        data_rsi = sensibilidad.get_sensitivity(prices[ticker], sensibilidad.add_signals_rsi, sensibilidad.get_params_rsi)

        row0_sma = data_sma.iloc[0]
        sma_params = {
            'sma_fast': row0_sma.sma_fast,
            'sma_slow': row0_sma.sma_slow,
            'diff_buy': row0_sma.diff_buy,
            'diff_sell': row0_sma.diff_sell,
        }

        data = pd.DataFrame(prices[ticker])
        sensibilidad.add_signals_sma(data, sma_params)
        trades = sensibilidad.get_trades(data)
        yields = sensibilidad.get_yields(trades)

        retornos['signal'] = np.where(retornos.rsi < 30, 'buy', np.where(retornos.rsi > 70, 'sell', 'hold'))

        operaciones = get_operaciones(retornos)

        retornos['estoyComprado'] = False

        for i in range(len(operaciones)):
            op = operaciones.iloc[i]
            retornos['estoyComprado'] = np.where(
                (retornos.index >= op.fecha_compra) & (retornos.index <= op.fecha_venta), True, False)

        retornos[ticker + '_rsi'] = np.where(retornos.estoyComprado, retornos[ticker], 0)




        print('hola')

# Obtiene el top 50 de tickers ordenados por volumen
top = cryptocompare.get_top_tickers(50, cache_days=30)

tickers = top.ticker

# Obtiene el historico de precios de los tickers pasados como parametros
prices = cryptocompare.get_mutiple_close_prices(tickers, cache_days= 30)

retornos = np.log((prices / prices.shift(1)))

agrego_indicadores(prices)

#analisis =sensibilidad.get_sensitivity(prices['BTC'], sensibilidad.add_signals_rsi, sensibilidad.get_params_rsi)

print('fin')
#n = markowitzevolution.markowitz_evolution(retornos, step=7, count=52, q_inicial=1500)
# utils.save('mark', evolution)

#resultado = markowitzevolution.markowitz_rolling(retornos, '2020-10-15')
