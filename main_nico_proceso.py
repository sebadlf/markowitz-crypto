import cryptocompare
import markowitz_nico_proceso
import utils20201016
import numpy as np
import pandas as pd
import binance
import utils
from sqlalchemy import create_engine
import keys


# Seteo el USER : PASS @ HOST / BBDD_NAME

sql_engine = create_engine(keys.DB_MARKOWITZ)
sql_conn = sql_engine.connect()

# Obtiene el top de tickers de BINANCE y luego filtra por volumen (top 75 por default).

# Guarda el archivo con el nombre indicado (tambien en la DB).
#top = binance.listaTickers(nombre="tickersBinance", numeroTickers=75)
#tickers = list(top.tickers)


# lo uso para hacer pruebas post seleccion de tickers.
# con DB
tickers = pd.read_sql('tickersbinance', con=sql_conn)
#tickers = utils20201016.open('tickersBinance')
tickers = list(tickers["tickers"])
#print(tickers)


# Obtiene el historico de precios de los tickers pasados como parametros
#prices = cryptocompare.get_mutiple_close_prices(tickers, cache_days= 1,limit=365)

# lo uso para hacer pruebas post adquisicion de precios.
# con DB
prices = pd.read_sql('prices', con=sql_conn)
prices = prices.dropna()
prices = prices.set_index("time")
#prices = utils20201016.open('prices')
#retornos=utils20201016.agrego_indicadores(data=prices)
# guardo en DB
#retornos.to_sql(con=sql_conn, name='retornos', if_exists='replace')
#utils20201016.save("retornos",retornos)


# MARKOWITZ
retornos = pd.read_sql("retornos",con=sql_conn)
#retornos = utils20201016.open('retornos2')
retornos = retornos.dropna()
retornos.set_index("time",inplace=True)

# PARAMETROS:
n_stocks = 5
count = 10
step = 7
q_inicial = 1500
rolling_size = 100

evolution = markowitz_nico_proceso.markowitz_evolution(data=retornos, step=step, count=count, rolling_size=rolling_size,
                                                       q_inicial=q_inicial,n_stocks=n_stocks)
print(evolution)

#utils.save('markPrueba2', evolution)
#evolution.to_sql(con=sql_conn, name='mark', if_exists='replace')
