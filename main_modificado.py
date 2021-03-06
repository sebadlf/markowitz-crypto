#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct  8 19:13:08 2020

@author: sebastian
"""


import cryptocompare
import markowitzevolution
import utils20201016
import numpy as np
import pandas as pd
import binance


# Obtiene el top 50 de tickers ordenados por volumen de CRYPTOCOMPARE
#top = cryptocompare.get_top_tickers(50, cache_days=30)
#tickers = top.ticker

# Obtiene el top de tickers de BINANCE y luego filtra por volumen (top 75 por default)
#top = binance.listaTickers(nombre="tickersBinance", numeroTickers=75)
#tickers = list(top.tickers)

# lo uso para hacer pruebas post seleccion de tickers.
#tickers = utils20201016.open('tickersBinance')
#tickers = list(tickers["tickers"])


# Obtiene el historico de precios de los tickers pasados como parametros
#prices = cryptocompare.get_mutiple_close_prices(tickers, cache_days= 1,limit=365)

# lo uso para hacer pruebas post adquisicion de precios.
#prices = utils20201016.open('prices')
#estrategias=utils20201016.agrego_indicadores(data=prices)
#utils20201016.save("estrategias1",estrategias)

                                   


#evolution = markowitzevolution.markowitz_evolution(retornos, step=7, count=52, q_inicial=1500)
#utils.save('mark', evolution) 

#resultado = markowitzevolution.markowitz_rolling(retornos, '2020-10-15')
