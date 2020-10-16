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

def filter_tickers(data, date, minimum_rows):
    # Devuelve una ventana de "minimum_rows" a partir de la fecha "date"
    filtered = data.loc[data.index <= date].tail(minimum_rows)   
    
    columns_to_drop = []
    for ticker in list(filtered.columns):
        if (len(filtered[ticker].dropna()) < minimum_rows):
            columns_to_drop.append(ticker)  
        
    filtered = filtered.drop(columns_to_drop, axis=1)
        
    return filtered


def markowitz_rolling(data, date, rolling_size = 100, q = 1500, tickers=None, proportions = None,n_stocks = 5):
    '''

    Returns
    -------

    Las primeras 3 corridas, toma la totalidad de los tickes en igualdad de condiciones.
    En la 4, comienza a ponderar los mejores
        
    '''
    filtered_data = filter_tickers(data, date, rolling_size)
    
    if (tickers == None):
        tickers = list(filtered_data.columns)
    #tickers = list(filtered_data.columns)
    
    
    datos = []
    for i in range(q):
        
        lista_tickers = utils.sample_sin_repetir(tickers, n_stocks)
        muestra = filtered_data[lista_tickers]
        
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
        pond = pond/np.sum(pond)
                
        # Filtro sin ceros
        #muestra = muestra.loc[:, (muestra != 0).all(axis=0)]
        #retornos = np.log((muestra/muestra.shift(1)).dropna())
        retornos = muestra.dropna()

        activos = list(muestra.columns)

        # If para evitar activos sin dataFeed
        if len(retornos):   
            #print(lista_tickers)
            #print(retornos)
            #print(np.sqrt(np.dot(pond, np.dot(retornos.cov()*252, pond))))
            
            r={}
            r['activos'] = activos
            r['pesos'] = np.round(pond, 3)
            r['retorno'] = np.sum( (retornos.mean() * pond * 252))
            r['volatilidad'] = np.sqrt(np.dot(pond, np.dot(retornos.cov()*252, pond)))
            r['sharpe'] = r['retorno'] / r['volatilidad']
            datos.append(r)
            
    df = pd.DataFrame(datos)#.sort_values('sharpe', ascending=False)
    return df


# Obtiene la minima y maxima proporcion de un ticker en base al DataFrame pasado como parametro
def calc_proportions(df):
    values = {}

    for i in range(len(df)) :
        row = df.iloc[i]
        activos = row.activos
        pesos = row.pesos

        activos_zipped = list(zip(activos, pesos))

        for j in range(len(activos_zipped)) :
            activo = activos_zipped[j][0]
            value = activos_zipped[j][1]

            if activo not in values:
                values[activo] = []

            values[activo].append(value)

    result = {}
    for key in values.keys() :

        result[key] = {
            #'min': max(min(values[key]) * 0.95, 0.01),
            #'max': min(max(values[key]) * 1.05, 0.5)
            'min': max(min(values[key]), 0.01),
            'max': min(max(values[key]), 0.6)
        }

        if (result[key]['max'] < result[key]['min']):
            result[key]['max'] = result[key]['min']           

    #print(result)

    return result

def get_activos_pond_sorted(activos, pond):
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
def markowitz_while(data, date, rolling_size = 100, q_inicial = 1500,n_stocks = 5):
    '''

    Returns
    -------
    best_porfolios : TYPE
        Conjunto de los mejores portafolios con una fecha y un Rolling.
        
    '''
    data = data.drop(['USDC' ,'TUSD', 'BUSD', 'USDT', 'DAI', 'PAX'], axis=1, errors='ignore')
    
    best_porfolios = pd.DataFrame()
    
    df_filtrado = data
    
    i = 1
    sigue = True
                
    proportions = None       
    
    lista_tickers = None
    
    while (sigue and (i <= 20)): 
        portfolios = markowitz_rolling(df_filtrado, date, tickers=lista_tickers, rolling_size=rolling_size, q=q_inicial, proportions=proportions, n_stocks = 5)
        
        cant_rows = int(200/i)

        if (cant_rows < 5):
            cant_rows = 5

        best_porfolios = pd.concat([best_porfolios, portfolios])
        best_porfolios = best_porfolios.sort_values('sharpe', ascending=False).head(cant_rows)
        
        if i > 3:
            proportions = calc_proportions(best_porfolios)
        
        #top = best_porfolios.iloc[ : int(200/(i+1))]
        #chequear
        #top = best_porfolios.iloc[ : int(200/(i+1))]

        lista_tickers = list(np.array(best_porfolios.activos.apply(pd.Series).stack()))
        
        lista_tickers_set = set(lista_tickers)

        print(i, end=" ")
        print(len(lista_tickers_set), end=" ")

        df_filtrado = data[lista_tickers_set]
        
        i = i + 1
        
        sigue = False

        rowZero = best_porfolios.iloc[0]
        activosZero, pondZero = get_activos_pond_sorted(rowZero.activos, rowZero.pesos)
        j = 1
        
        while ((sigue == False) and j < 5):
            row = best_porfolios.iloc[j]
            activos, pond = get_activos_pond_sorted(row.activos, row.pesos)

            j = j + 1

            # el while siempre continua, si los 5 activos, mantienen el mismo orden 
            # y tienen una diferencia mayor al 5%    
            sigue = sigue or (activos != activosZero) or (np.all(abs((np.array(pondZero) - np.array(pond))) < 0.05) == False)
            
        rowFive = best_porfolios.iloc[4] 
        activosFive, pondFive = get_activos_pond_sorted(rowFive.activos, rowFive.pesos)
        # Rueda
        # Elementos analizados.
        # Max Valor absoluto entre la ponderacion de la fila 0 y 5
        print(max(abs((np.array(pondZero) - np.array(pondFive)))))
        
    return best_porfolios

# Devuelve un DataFrame con "count" filas con una diferencia de "step" dias empezando con el dia de hoy
# DISCLAMER: Cada fila del DataFrame tarda aproximadamente dos minutos en ser calculara. 
def markowitz_evolution(data, step=7, count = 5, rolling_size = 100, q_inicial = 1500):
    '''

    Returns
    -------
        un Dataframe con la mejor evolucion del rolling.
        
    '''
    best_mark = []
    indexes = list(data.index.values)
    for i in tqdm.tqdm(range(count)):
        index = indexes[-1 - (i * step)]

        best_for_date = markowitz_while(data, index, q_inicial = q_inicial)

        best_row = best_for_date.iloc[0].to_dict()

        best_row['date'] = index
        
        # Ordeno los activos por ponderacion
        activos = best_row['activos']
        pesos = best_row['pesos']
        
        zipped = zip(activos, pesos)
        zipped_list = list(zipped)
        zipped_list.sort(key=lambda tup: tup[1], reverse=True)
        zipped_list = list(zip(*zipped_list))
        
        best_row['activos'] = list(zipped_list[0])
        best_row['pesos'] = list(zipped_list[1])
        #fin de ordenamiento        

        best_mark.append(best_row)

    return pd.DataFrame(best_mark)
