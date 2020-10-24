import pandas as pd
import numpy as np
import utils
from datetime import datetime

data = utils.open('mark').sort_values("date", ascending=True)
retornos = utils.open('retornos')

#data['start_date'] = data['date']
data['end_date'] = data['date'].shift(-1)

#data.set_index("date")
#retornos.reset_index(inplace=True)

data['return'] = 0
data['cumprod'] = 0

data = pd.DataFrame(data.to_json())


retornos = retornos.loc[retornos.index > "2020-01-01"]

print(retornos)



# for i in range(len(data)-1):
#     row = data.iloc[i]
#     activos = row.activos

#     retorno_activos = retornos[activos]
    
    
#     start_date = row.date
#     end_date = row.end_date

#     print(type(start_date))
#     print(type(end_date))    

    
#     returns = retorno_activos.loc[retorno_activos.index <= datetime('2020-10-14')]

#     print(returns)


