import pandas as pd
import os
import datetime as dt

def is_older_than(filename, days):
    time = os.path.getmtime('./files/' + filename + '.h5') 
    fecha_modificacion = dt.datetime.fromtimestamp(time) 
    ahora = dt.datetime.now()

    return fecha_modificacion < (ahora - dt.timedelta(days=days))

def file_exists(filename):
    return os.path.isfile('./files/' + filename + '.h5')

def save(filename, data):
    
    store = pd.HDFStore('./files/' + filename + '.h5')
    
    store['data'] = data
    
    return data
        
def open(filename):
    store = pd.HDFStore('./files/' + filename + '.h5')
    
    data = store['data']
    
    return pd.DataFrame(data)