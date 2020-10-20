import requests
import pandas as pd
import numpy as np
import logging
from cryptocompare import *
from utils import save
from keys import *

"""
Created on Oct 11 2020

@author: nico p

Info
-------
Toma totalidad de tickers BINANCE, elimina estables, verifica antiguedad y volumen.

Returns 
-------
Top 75 en base al volumen. (csv,excel,h5)

"""

logging.basicConfig(level=logging.INFO, format='{asctime} {levelname} ({threadName:11s}) {message}', style='{')


def promedioVolumen(data, plazo=365):
    """ Función que recibe un json y entrega un promedio del volumen"""

    listaVolumen = []
    for z in range(plazo):
        volumenDiario = data['Data']['Data'][z]['volumeto']
        listaVolumen.append(volumenDiario)
    resultado = sum(listaVolumen) / plazo
    return resultado


def descargaInfoBinance(url):
    """ funcion que descarga info de un url y la pasa como json"""

    logging.info("Descargando info completa de precios")
    res = requests.get(url)
    res = res.json()
    return res


def descargaInfoCryptocompare(lista, moneda="USD", key=CRYPTOCOMPARE_KEY, limit=365):
    """Descarga información de cryptocompare de 1 año antiguedad.
        Returns
        -------
        lista de tickers y lista de volumenes"""

    listaTickers, volumenes = [], []  # genero listas para armar un df

    for ticker in lista:
        url = "https://min-api.cryptocompare.com/data/v2/histoday?"
        api_key = key
        params = {'api_key': api_key, 'fsym': ticker, 'tsym': moneda, "limit": limit}
        r = requests.get(url, params=params)
        r = r.json()

        antiguedad = filtroAntiguos(json=r,ticker=ticker)  # verifico antiguedad mayor al tiempo limite
        if antiguedad:
            try:
                logging.info(f"Calculando volumen de {ticker}")
                promedio = promedioVolumen(data=r)  # llamo a la funcion promedio volumen
                listaTickers.append(ticker)
                volumenes.append(promedio)
            except:
                logging.error(f"no pude calcular volumen de {ticker}")
                print("no pude extraer info de " + ticker)
                continue
    return listaTickers, volumenes


def filtroAntiguos(json,ticker):
    """Recibe un json y verifica si tiene antiguedad mayor a 1 año"""

    logging.info("Verificando antiguedad")

    try:
        volumen = json["Data"]["Data"][0]["volumefrom"]
        if volumen != 0:
            return True
        else:
            return False
    except:
        print("no pude extraer info de " + ticker)
        return False


def depuroUSDT(lista):
    """recibe una lista con pares y les elimina la palabra "USDT" para luego poder buscar el precio con otra api"""
    logging.info("Depurando la palabra USDT de tickers")
    res = []
    for i in range(len(lista)):
        ticker = lista[i]
        buscar = "USDT"
        reemplazar = ""
        ticker = ticker.replace(buscar, reemplazar)
        res.append(ticker)
    return res


def listaTickers(nombre="tickersBinance", numeroTickers=75):
    """
    Info
    -------
    Toma totalidad de tickers BINANCE, elimina estables, verifica antiguedad y volumen.

    Inputs
    -------
    -Nombre archivo (optativo, default "tickersBinance"
    -numeroTickers para el top (optativo, default 75)

    Returns
    -------
    Top 75 en base al volumen (volumen en millones). (csv,excel,h5)
    """

    # Descargo el precio de todos los tickers que cotizan en Binance
    endpointBase = "https://api.binance.com/api/v3/ticker/price"
    lista = descargaInfoBinance(endpointBase)

    # Genero la lista de tickers
    logging.info("Generando lista tickers")
    tickers = []
    for i in range(len(lista)):
        ticker = lista[i]['symbol']
        if "USDT" in ticker:
            tickers.append(ticker)

    # Elimino los pares UDST vs otras estables
    estables = ["DAIUSDT", "USDTDAI", "BUSDUSDT", "USDCUSDT", "PAXUSDT", "TUSDUSDT"]
    for ticker in tickers:
        for estable in estables:
            if estable in ticker:
                tickers.remove(ticker)

    # Creo una lista de solo tickers sin el par "USDT"
    tickers = depuroUSDT(tickers)

    # Descargo info de cryptocompare para ver antiguedad y volumen
    tickerYvolumen = descargaInfoCryptocompare(tickers)

    # Genero df con tickers y volumenes
    agrupados = {"tickers": tickerYvolumen[0], "volumenPromedio": tickerYvolumen[1]}
    agrupados = pd.DataFrame(agrupados)
    agrupados = agrupados.sort_values(by="volumenPromedio", ascending=False)
    agrupados["volumenPromedio"] = agrupados["volumenPromedio"] / 1000000
    agrupados = agrupados.iloc[0:numeroTickers]

    # Guardo en un h5 para luego poder trabajar con esto
    logging.info("Exportando info")
    utils.save(nombre, agrupados)
    logging.info("Proceso terminado con exito")

    return agrupados
