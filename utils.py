import os
import random
import datetime as dt
import pandas as pd

def is_older_than(filename, days):
    time = os.path.getmtime('./files/' + filename + '.h5') 
    fecha_modificacion = dt.datetime.fromtimestamp(time) 
    ahora = dt.datetime.now()

    return fecha_modificacion < (ahora - dt.timedelta(days=days))

def file_exists(filename):
    return os.path.isfile('./files/' + filename + '.h5')

def save(filename, data):
    
    data.to_hdf('./files/' + filename + '.h5', 'data')    
    data.to_csv('./files/' + filename + '.csv')
    data.to_excel('./files/' + filename + '.xlsx')
        
    return data
        
def open(filename):
    data = pd.read_hdf('./files/' + filename + '.h5')
        
    return pd.DataFrame(data)


def sample_sin_repetir(list, k):
    result= []
    while len(result) < k:
        item = random.choice(list)
        if item not in result:
            result.append(item)
    
    return result