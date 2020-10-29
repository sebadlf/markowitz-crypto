import pandas as pd
import numpy as np
import utils
from datetime import datetime
import ast

data = utils.open('mark').sort_values("date", ascending=True)
retornos = utils.open('retornos')

data['start_date'] = data['date']
data['end_date'] = data['date'].shift(-1)

# data.set_index("date", inplace=True)
# retornos.reset_index(inplace=True)
#
data['return'] = 0
#
# data.drop(["index"], axis=1, inplace=True)

data.drop(data.tail(1).index,inplace=True)


for index, row in data.iterrows():
    # print(index)
    # print(row)

    activos = row.activos
    pesos = row.pesos

    # activos = activos.strip('}{').split(',')
    # pesos = np.array(pesos.strip('}{').split(',')).astype(np.float)

    retorno_activos = retornos.loc[(retornos.index > row.start_date) & (retornos.index <= row.end_date)][activos]

    retornos_actumulatos = (retorno_activos + 1).cumprod().values.tolist()[-1]
    retornos_actumulatos = np.array(retornos_actumulatos)
    
    
    # print(retornos_actumulatos)
    # print(pesos)
    
    result = (retornos_actumulatos * pesos).sum()
    
    #print(result)

    data['return'] = np.where(data.start_date == row.start_date, result, data['return'])


data['cumprod'] = data['return'].cumprod()

print(data[['start_date', 'end_date', 'return', 'cumprod']])

