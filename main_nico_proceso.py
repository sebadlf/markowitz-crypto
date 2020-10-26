import cryptocompare
import markowitz_nico_proceso
import utils20201016
import numpy as np
import pandas as pd
import binance
import utils


# Obtiene el top de tickers de BINANCE y luego filtra por volumen (top 75 por default).
# Guarda el archivo con el nombre indicado.

#top = binance.listaTickers(nombre="tickersBinance", numeroTickers=75)
#tickers = list(top.tickers)


# lo uso para hacer pruebas post seleccion de tickers.
#tickers = utils20201016.open('tickersBinance')
#tickers = list(tickers["tickers"])
#print(tickers)


# Obtiene el historico de precios de los tickers pasados como parametros
#prices = cryptocompare.get_mutiple_close_prices(tickers, cache_days= 1,limit=365)

# lo uso para hacer pruebas post adquisicion de precios.
#prices = utils20201016.open('prices')
#retornos=utils20201016.agrego_indicadores(data=prices)
#utils20201016.save("retornos",retornos)

# MARKOWITZ
retornos = utils20201016.open('retornos')
retornos = retornos.dropna()

# Parametros:
n_stocks = 5
count = 30
step = 7
q_inicial = 1500
rolling_size = 100

evolution = markowitz_nico_proceso.markowitz_evolution(data=retornos, step=step, count=count, rolling_size=rolling_size,
                                                       q_inicial=q_inicial,n_stocks=n_stocks)
print(evolution)
utils.save('markPrueba', evolution)


