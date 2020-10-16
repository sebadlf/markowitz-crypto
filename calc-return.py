import pandas as pd
import numpy as np
import utils

data = utils.open('mark')
prices = utils.open('prices')

indexes = list(range(len(data['date'])))
indexes.reverse()

cumprod = 1
cartera = {}

cumprod_acc = []

for i in indexes:
    row = data.iloc[i]
    row_prices = prices.loc[prices.index == row.date]

    #Vendo
    for ticker in cartera.keys():
        ticker_price = float(row_prices[ticker])
        cumprod = cumprod + (cartera[ticker] * ticker_price)

    cumprod_acc.append({
        'date': row.date,
        'cumprod': cumprod
    })

    cartera = {}

    pesos_ajustados = np.array(row.pesos) * cumprod

    #Compro
    mark_row = list(zip(row.activos, pesos_ajustados))
    for activo in mark_row:
        ticker = activo[0]
        
        ticker = ticker.split('_')[0]
        
        value = activo[1]

        cumprod = cumprod - value

        ticker_price = float(row_prices[ticker])

        cartera[ticker] = value / ticker_price
        
#Vendo

print(cartera)

row = data.iloc[0]
row_prices = prices.loc[prices.index == row.date]        
        
for ticker in cartera.keys():
    ticker_price = float(row_prices[ticker])
    cumprod = cumprod + (cartera[ticker] * ticker_price)
    
cartera={}

print(cumprod)

print(pd.DataFrame(cumprod_acc))


