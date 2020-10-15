#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct  8 19:13:08 2020

@author: sebastian
"""


import cryptocompare
from markowitzevolution import  markowitz_evolution
import utils


# Obtiene el top 50 de tickers ordenados por volumen
#top = cryptocompare.get_top_tickers(50, cache_days=30)
top = cryptocompare.get_top_tickers(cantidad =7,cache_days=30)

tickers = top.ticker

# Obtiene el historico de precios de los tickers pasados como parametros
prices = cryptocompare.get_mutiple_close_prices(tickers, cache_days= 1)

evolution = markowitz_evolution(prices, step=7, count=5, q_inicial=1500)
utils.save('mark', evolution) 
