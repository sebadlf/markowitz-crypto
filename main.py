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

# Obtiene el top 50 de tickers ordenados por volumen
top = cryptocompare.get_top_tickers(50, cache_days=30)

tickers = top.ticker

# lo uso para hacer pruebas post seleccion de tickers.
# Comentar lineas 17 y 19. Descomentar 23
#tickers = utils.open('top-tickers')


# Obtiene el historico de precios de los tickers pasados como parametros
prices = cryptocompare.get_mutiple_close_prices(tickers, cache_days= 1)

# lo uso para hacer pruebas post adquisicion de precios.
#Comentar la linea 27, descomentar 31
#prices = utils.open('prices')
                            
retornos = utils.agrego_indicadores(prices)

evolution = markowitzevolution.markowitz_evolution(retornos, step=7, count=52, q_inicial=1500)
utils.save('mark', evolution) 

#resultado = markowitzevolution.markowitz_rolling(retornos, '2020-10-15')
