#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct  8 17:50:53 2020

@author: sebastian
"""

import requests, pandas as pd
import numpy as np
import tqdm
import random
import utils
from cryptocompare import *
import time

def filter_tickers(data, date, minimum_rows):
    """
    Inputs
    ------
    Data frame
    Fecha determinada
    Cantidad minima para atras de datos.

    Returns
    ------
    Lista de tickers que poseen los datos historicos requeridos
    """

    filtered = data.loc[data.index <= date].tail(minimum_rows)

    columns_to_drop = []
    for ticker in list(filtered.columns):
        if (len(filtered[ticker].dropna()) < minimum_rows):  # si el ticker tiene menos datos que el rolling, lo elimina
            columns_to_drop.append(ticker)

    filtered = filtered.drop(columns_to_drop, axis=1)

    return filtered


def markowitz_rolling(data,hasta, rolling_size=10, q=1500, tickers=None, proportions=None, n_stocks=5,calc = True):
    '''

    Returns
    -------

    Las primeras 3 corridas, toma la totalidad de los tickes en igualdad de condiciones.
    En la 4, comienza a ponderar los mejores

    '''

    """filtered_data = filter_tickers(data = data, date = date,
                                   minimum_rows = rolling_size)  # elimina tickers que no tengan data historica = rolling_size
                                   """

    # las primeras 3 corridas trae tickers en igualdad de condiciones
    if (tickers == None):
        tickers = list(data.columns)
    desde = len(data) - hasta - rolling_size
    filtro_fecha = data.iloc[desde:(len(data)-hasta)]
    datos = []
    for i in range(q):

        # muestra
        lista_tickers = utils.sample_sin_repetir(tickers, n_stocks)
        muestra = filtro_fecha.loc[:,lista_tickers]  # muestra en base al listado aleatorio anterior
        # hago los ponderados
        if calc:
            pond = []
            for ticker in muestra:
                if (proportions != None):
                    minimum = proportions[ticker]['min']
                    maximum = proportions[ticker]['max']
                else:
                    minimum = 0.01
                    maximum = 0.6

                pond.append(random.randint(int(minimum * 10000), int(maximum * 10000)) / 10000)

            pond = np.array(pond)

        else:
            pond = np.array(np.random.random(len(muestra.columns)))


        pond = pond / np.sum(pond)

        # If para evitar activos sin dataFeed
        if len(muestra):
            try:
                r = {}
                r['activos'] = list(muestra.columns)
                r['pesos'] = np.round(pond, 3)
                r['retorno'] = np.sum((muestra.mean() * pond * 252))
                r['volatilidad'] = np.sqrt(np.dot(pond, np.dot(muestra.cov() * 252, pond)))
                r['sharpe'] = r['retorno'] / r['volatilidad']
                datos.append(r)

            except:
                pass
    df = pd.DataFrame(datos).sort_values("sharpe",ascending=False)
    df = df.head(200)

    return df


# Obtiene la minima y maxima proporcion de un ticker en base al DataFrame pasado como parametro
def calc_proportions(df):
    values = {}

    for i in range(len(df)):
        row = df.iloc[i]
        activos = row.activos
        pesos = row.pesos

        activos_zipped = list(zip(activos, pesos))  # junta cada activo con cada peso en tuplas dentro de una lista

        for j in range(len(activos_zipped)):
            activo = activos_zipped[j][0]
            value = activos_zipped[j][1]

            if activo not in values:
                values[activo] = []

            values[activo].append(value)

    result = {}
    for key in values.keys():

        result[key] = {
            # 'min': max(min(values[key]) * 0.95, 0.01),
            # 'max': min(max(values[key]) * 1.05, 0.5)
            'min': max(min(values[key]), 0.01),
            'max': min(max(values[key]), 0.6)
        }

        if (result[key]['max'] < result[key]['min']):
            result[key]['max'] = result[key]['min']

            # print(result)

    return result


def get_activos_pond_sorted(activos, pond):
    """ funcion que recibe 2 listas de tickers con pesos y entrega 2 listas con tickers y pesos pero ordenado
    segun el ticker con el mayor peso de mayor a menor."""

    zipped = zip(activos, pond)
    zipped_list = list(zipped)
    zipped_list.sort(key=lambda tup: tup[1], reverse=True)
    zipped_list = list(zip(*zipped_list))

    activos = list(zipped_list[0])
    pond = list(zipped_list[1])

    return activos, pond


# Corre markowitz hasta que los 5 primeros resultados tienen las mismos 5 tickers en cualquier orden
# Optimizacion pendiente, que todos los tickers tengan una diferencia de proporcion de menos del
# 5% entre la primer y la quinta fila
def markowitz_while(data, hasta, rolling_size=100, q_inicial=1500, n_stocks=5,calc = True):
    '''

    Returns
    -------
    best_porfolios : TYPE
        Conjunto de los mejores portafolios a una fecha y un Rolling.

    '''
    # Creo un dataframe
    best_porfolios = pd.DataFrame()

    df_filtrado = data.copy()

    i = 1

    sigue = True
    proportions = None
    lista_tickers = None

    while (sigue and (i <= 20)):
        # llamo a la funcion markowitz_rolling
        portfolios = markowitz_rolling(data=df_filtrado, hasta=hasta, tickers=lista_tickers, rolling_size=rolling_size,
                                       q=q_inicial, proportions=proportions, n_stocks=n_stocks, calc = calc)

        # cantidad de filas
        cant_rows = int(200 / i)  # toma primero los 200 mejores y luego va tomando cada vez menos
        if (cant_rows < 5):
            cant_rows = 5

        best_porfolios = pd.concat([best_porfolios, portfolios])
        best_porfolios = best_porfolios.sort_values('sharpe', ascending=False).head(cant_rows)

        if i > 3:
            proportions = calc_proportions(best_porfolios)

        lista_tickers = list(np.array(best_porfolios.activos.apply(pd.Series).stack()))

        #df_filtrado = data[lista_tickers] #no se si tiene mucho sentido

        i = i + 1

        # ordeno ticker y peso de la primer fila
        rowZero = best_porfolios.iloc[0]
        activosZero, pondZero = get_activos_pond_sorted(rowZero.activos, rowZero.pesos)
        j = 1

        if i >9:
            while j < 5:
            # el markowitz while continua hasta que las primeras 5 filas tengan mismo orden y dif menor al 5%

                # ordeno ticker y peso de la fila j
                row = best_porfolios.iloc[j]
                activos, pond = get_activos_pond_sorted(row.activos, row.pesos)

                if (activos != activosZero):
                    #print((activos != activosZero))
                    break

                elif (np.all(abs((np.array(pondZero) - np.array(pond))) < 0.05) == False):
                    #print(np.all(abs((np.array(pondZero) - np.array(pond)))))
                    break

                elif j == 4:
                    sigue = False

                j= j + 1

    return best_porfolios


# Devuelve un DataFrame con "count" filas con una diferencia de "step" dias empezando con el dia de hoy
# DISCLAMER: Cada fila del DataFrame tarda aproximadamente dos minutos en ser calculara.
def markowitz_evolution(data, step=7, count=5, rolling_size=50, q_inicial=1500, n_stocks=5):
    '''
    Inputs
    ------
        - Dataframe de retornos
        - Cada cuantos dias se realizara el markowitz
        - Cuantas veces se realizara
        - Datos historicos para atras que tomara desde cada step
        - Cantidad inicial de iteraciones
        - Cantidad de ticker

    Returns
    -------
        un Dataframe con la mejor evolucion del rolling.

    '''

    start_time = time.time()

    # Primero me fijo si existe data de 1 año para el step y el count datos
    if (365 - step*count - rolling_size) > 0:

        indexes = list(data.index.values)  # listado de los datetime
        best_mark = []
        for i in tqdm.tqdm(range(count)):
            # ubicacion hasta cada step
            index = i * step

            # llamo a la funcion markowitz_while
            best_for_date = markowitz_while(data=data, hasta=index, rolling_size=rolling_size,
                                            q_inicial=q_inicial, n_stocks=n_stocks)

            best_row = best_for_date.iloc[0].to_dict()
            best_row['date'] = indexes[-1 - (i * step)] # fecha hasta cada step

            # Ordeno los activos por ponderacion
            activos = best_row['activos']
            pesos = best_row['pesos']

            lista = get_activos_pond_sorted(activos,pesos)

            best_row['activos'] = lista[0]
            best_row['pesos'] = lista[1]
            # fin de ordenamiento

            best_mark.append(best_row)

        print("--- %s seconds ---" % (time.time() - start_time))
        return pd.DataFrame(best_mark)
    else:
        return "Segun el step, count y rolling_size dados, no alcanzan los datos historicos para poder calcular."



