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


def markowitz_rolling(data, date, rolling_size = 100, q = 1500, proportions = None):
    
    filtered_data = filter_tickers(data, date, rolling_size)
    
    tickers = list(filtered_data.columns)
    
    n_stocks = 5
    datos = []
    for i in range(q):
        
        muestra = filtered_data[random.sample(tickers, n_stocks)]
        
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
        muestra = muestra.loc[:, (muestra != 0).all(axis=0)]
        retornos = np.log((muestra/muestra.shift(1)).dropna())
        #pond = np.array(np.random.random(len(muestra.columns)))
        
        activos = list(muestra.columns)
        
        # Ordeno los activos por ponderacion
#         activos = list(muestra.columns)
#         pond = pond.round(3)
        
#         zipped = zip(activos, pond)
#         zipped_list = list(zipped)
#         zipped_list.sort(key=lambda tup: tup[1], reverse=True)
#         zipped_list = list(zip(*zipped_list))
        
#         activos = list(zipped_list[0])
#         pond = list(zipped_list[1])
        #fin de ordenamiento

        # If para evitar activos sin dataFeed
        if len(retornos):    
            r={}
            r['activos'] = activos
            r['pesos'] = np.round(pond, 3)
            r['retorno'] = np.sum( (retornos.mean() * pond * 252))
            r['volatilidad'] = np.sqrt(np.dot(pond, np.dot(retornos.cov()*252, pond)))
            r['sharpe'] = r['retorno'] / r['volatilidad']
            datos.append(r)
            
    df = pd.DataFrame(datos).sort_values('sharpe', ascending=False)
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
            'min': min(values[key]) ,
            'max': max(values[key])
        }


    return result

# Corre markowitz hasta que los 5 primeros resultados tienen las mismos 5 tickers en cualquier orden
# Optimizacion pendiente, que todos los tickers tengan una diferencia de proporcion de menos del 
# 5% entre la primer y la quinta fila
def markowitz_while(data, date, rolling_size = 100, q_inicial = 1500):
    
    #data = data.drop(['USDC' ,'TUSD', 'BUSD', 'USDT', 'DAI', 'PAX'], axis=1)
    
    best_porfolios = pd.DataFrame()
    
    df_filtrado = data
    
    i = 0
    sigue = True
    
    default_proportions = {}
    for ticker in df_filtrado.columns:
        default_proportions[ticker] = {
            'min': 0.01,
            'max': 0.60
        }
                
    proportions = default_proportions       
    
    while (sigue and (i < 200)): 
        portfolios = markowitz_rolling(df_filtrado, date, rolling_size=rolling_size, q=q_inicial, proportions=proportions)

        best_porfolios = pd.concat([best_porfolios, portfolios.iloc[:200]])
        best_porfolios = best_porfolios.sort_values('sharpe', ascending=False).head(200)
        
        if i >= 3:
            proportions = calc_proportions(best_porfolios)
        
        top = best_porfolios.iloc[ : int(200/(i+1))]

        lista_tickers = list(set(np.array(top.activos.apply(pd.Series).stack())))

        print(len(lista_tickers), end=" - ")

        df_filtrado = data[lista_tickers]
        
        i = i + 1
        
        zero = sorted(best_porfolios.iloc[0].activos) 
        one = sorted(best_porfolios.iloc[1].activos)
        two = sorted(best_porfolios.iloc[2].activos)
        three = sorted(best_porfolios.iloc[3].activos)        
        four = sorted(best_porfolios.iloc[4].activos)
        
        sigue = (zero != one) or (zero != two) or (zero != three) or (zero != four)
        
    return best_porfolios

# Devuelve un DataFrame con "count" filas con una diferencia de "step" dias empezando con el dia de hoy
# DISCLAMER: Cada fila del DataFrame tarda aproximadamente dos minutos en ser calculara. 
def markowitz_evolution(data, step=7, count = 5, rolling_size = 100, q_inicial = 1500):
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
