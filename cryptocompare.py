import requests, pandas as pd
import numpy as np
import tqdm
import datetime as dt
from  keys import *
import utils

####### Para el calculo de periodos######
def controlLimit(limit):
    dev=False
    if (type(limit)==int):
        dev=True
    return dev

def controlValido(interval, startTime=None, endTime=None, limit=2000):
    ''' Verificamos la validez de las entradas. 
        Las salidas se posicionan de la siguiente manera:
        1) Intervalo
        2) Fecha de Inicio y Fecha de Fin
        3) Limite.(Si bien limite por default es 2000, existe la posibilidad que el usuario
            ingrese Valores erroneos)'''
    
    #Control de intervalos
    dev1=False
    if controlIntervalo(interval):
        dev1=True
    else:
        help(controlIntervalo)
    
    #Control de Fechas
    try:
        dev2=False
        if(startTime!=None):
            if(type(conviertoFechaMs(startTime))==int):
                if(endTime!=None):
                    if(type(conviertoFechaMs(endTime))==int):
                        #startTime y endTime, tienen valores validos.
                        dev2=True
                    else:
                        #startTime tiene valor valido, pero endtime no. Algo Falla.
                        dev2=False
                else:
                    #startTime tiene valor valido y endTime es None. endtime se convierte en HOY
                    dev2=True
                    pass
            else:
                #startTime no es valido, no se puede usar.
                dev2=False
    except:
        print('\nUna de las variables startTime o endTime ingresadas no es valida,\n en caso de existir un limite valido,\n se continuara con la operacion')
        dev2=False
    
    #Control de limites, en caso que el usuario ingrese un valor no valido
    dev3=False
    if(controlLimit(limit)):
        dev3=True  
    
    return dev1,dev2,dev3

def calculoIteracionesFechas(base,startTime,endTime):
    inicio=utils.conviertoFechaS(startTime)
    fin=utils.conviertoFechaS(endTime)
    #Multiplico la base por 1000 pq es el maximo de elementos que puedo traer.
    iteraciones=(fin-inicio)//(base*2000)
    if(((fin-inicio)%(base*2000))>0):
        iteraciones+=1
    return iteraciones

def calculoIteracionesLimite(limit):
    #Multiplico la base por 1000 pq es el maximo de elementos que puedo traer.
    iteraciones=(limit)//(1000)
    if(((limit)%(1000))>0):
        iteraciones+=1
    return iteraciones

def controlCantidades(interval, startTime=None, endTime=None, limit=2000):

    
    #Solo uso Horas, ya que la funcion solo recibe, lo conviero en Segundos
    intervalos = {'1h':3600}
    
    condicionantes=controlValido(interval,startTime,endTime,limit)
    iteraciones=0
    if(condicionantes[0]&condicionantes[1]):
        base=intervalos[interval]
        if(startTime!=None):
            if(endTime!=None):
                iteraciones=calculoIteracionesFechas(base,startTime,endTime)
            else:
                endTime=dt.datetime.now()
                iteraciones=calculoIteracionesFechas(base,startTime,endTime)
    else:
        if(condicionantes[0]&condicionantes[2]):
            if(limit>1000):
                iteraciones=calculoIteracionesLimite(limit)
            else:
                iteraciones=1
    return iteraciones


###### Fin ######

###### API ######

def AvailableCoinList():
    url='https://min-api.cryptocompare.com/data/blockchain/list'
    api_key = CRYPTOCOMPARE_KEY
    params = {'api_key': api_key}
    r = requests.get(url, params=params)
    js = r.json()
    df = pd.DataFrame(js)
    
    return df.index

def DailySymbolVolSingleExchange(fsym='BTC',tsym='USD',limit=30):
    url='https://min-api.cryptocompare.com/data/exchange/symbol/histoday?'
    api_key = CRYPTOCOMPARE_KEY
    params = {'api_key': api_key,'fsym':fsym,'tsym':tsym,'limit':limit}
    r = requests.get(url, params=params)
    js = r.json()['Data']
    df = pd.DataFrame(js)
#    df.time = pd.to_datetime(df.time, unit = 's')
#    df = df.set_index('time')
#    df = df[(df.T != 0).any()]
#    df = df.reset_index()
    try:
        total=df['volumetotal'].sum()
    except: 
        total=0
    return total


def HourlyPairOHLCV(e='CCCAGG',fsym='BTC',tsym='USD',limit=2000,FechaFinal=None):
    url='https://min-api.cryptocompare.com/data/v2/histohour'
    api_key = CRYPTOCOMPARE_KEY
    if FechaFinal==None:
        params = {'api_key': api_key,'e':e, 'fsym':fsym,'tsym':tsym,'limit':limit}
    else:
        toTs=utils.conviertoFechaS(utils.conviertoFecha(FechaFinal))
        params = {'api_key': api_key,'e':e, 'fsym':fsym,'tsym':tsym,'limit':limit,'toTs':toTs}
    r = requests.get(url, params=params)
    js = r.json()['Data']['Data']
    df = pd.DataFrame(js)
    df.time = pd.to_datetime(df.time, unit = 's')
    df = df.set_index('time').drop(['conversionType','conversionSymbol'],axis=1)
    df = df[(df.T != 0).any()]
    df = df.dropna()
    df = df.reset_index()
    return df
#hasta esta fecha funciona. sino de vacio. '01-11-2010'
#df=HourlyPairOHLCV(FechaFinal='01-11-2010')


def histoDay(e,fsym,tsym,limit,toTs=None, aggregate=1, allData='false'):
    url = 'https://min-api.cryptocompare.com/data/v2/histoday'
    api_key = CRYPTOCOMPARE_KEY
    params = {'api_key': api_key, 'e': e, 'fsym': fsym, 'tsym': tsym, 'allData': allData, 'toTs': toTs, 'limit': limit, 'aggregate': aggregate}
    r = requests.get(url, params=params)
    js = r.json()['Data']['Data']
    df = pd.DataFrame(js)
    df.time = pd.to_datetime(df.time, unit = 's')
    df = df.set_index('time').drop(['conversionType','conversionSymbol'],axis=1)
    df = df[(df.T != 0).any()]
    df = df.dropna()
    df = df.reset_index()
    #df = df[df['time'].dt.year>2016]
    return df


##### FIN API ############
    

def bajadaComplejaHoraria(e='CCCAGG',fsym='BTC',tsym='USD', interval='1h', startTime=None, endTime=None, intervalos=2):
    
    df=pd.DataFrame(columns=['time', 'high', 'low', 'open', 'volumefrom', 'volumeto', 'close'])
    df.set_index('time')
    if((startTime!=None)&(endTime!=None)):
        startTimeDate=utils.conviertoFecha(startTime)
        endTimeDate=utils.conviertoFecha(endTime)
        unidad=(endTimeDate-startTimeDate)/intervalos

        for i in range(intervalos):
            fFinal=startTimeDate+unidad*(i+1)
            bajadaActual=HourlyPairOHLCV(e=e,fsym=fsym,tsym=tsym,limit=unidad,FechaFinal=fFinal)
            df=pd.concat([df,bajadaActual], join='inner', axis=0)

    return df

def historico2(symbol, interval='1h', startTime=None, endTime=None, limit=1000):
    ''' La funcion historico, puede ser utilizado con los paramotros startTime/endTimen o con el limit, 
    en caso que se complete startTime, se ha de ignorar el limite.'''
    iteraciones=controlCantidades(interval,startTime,endTime,limit)
    df=[]

    if (iteraciones==1):
        fInicio=utils.conviertoFecha(startTime)
        fFinal=utils.conviertoFecha(endTime)   
        fInicioS=utils.conviertoFechaS(fInicio)
        fFinalS=utils.conviertoFechaS(fFinal)      
        df=HourlyPairOHLCV(e=e,fsym=fsym,tsym=tsym,limit=unidad,FechaFinal=fFinal)        
    else:
        if (iteraciones>1):
            df=bajadaCompleja(symbol,interval,startTime,endTime,limit,iteraciones)
        else:
            if(iteraciones==0):
                print('Algo paso, no hay iteraciones')
    return df

def mutiple_close_prices(tickers,limit):
    tablas = {}
    for ticker in tqdm.tqdm(tickers):                
        try:
            data = histoDay('CCCAGG',ticker,'USD',limit=limit)
            data = data.set_index('time')
            tablas[ticker] = data.drop(['open','high','low','volumefrom','volumeto'],axis=1)
        except:
            print('Error al descargar ' + ticker)

    tickers = list(tablas.keys())

    tabla = pd.DataFrame(tablas[tickers[0]])
    for i in range (1,len(tickers)):
        tabla = pd.concat([tabla, tablas[tickers[i]]],axis=1)
    tabla.columns = tickers
    return tabla

def get_mutiple_close_prices(tickers, cache_days = 1,limit=365):
    if (utils.file_exists('prices') and (utils.is_older_than('prices', cache_days) == False)):
        top = utils.open('prices')
    else:
        top = mutiple_close_prices(tickers,limit=limit)
        utils.save('prices', top)    
        
    return top


def download_top_tickers(cantidad=50):
    '''Descarga los X tickers con mayor volumen dentro de  los ultimos 30 dias'''

    todosLosTickers= list(AvailableCoinList())

    values = []
    for i in tqdm.tqdm(range(len(todosLosTickers))):
    #for i in tqdm.tqdm(range(50)):
        ticker = todosLosTickers[i]
        try:            
            values.append({
                'ticker': ticker,
                'volumen': DailySymbolVolSingleExchange(ticker)
            })
        except:
            print("Error con ticker " + ticker)

        resultado = pd.DataFrame(values)     
                
    return resultado.sort_values("volumen", ascending=False).head(cantidad)

def get_top_tickers(cantidad = 50, cache_days = 30):
    if (utils.file_exists('top-tickers') and (utils.is_older_than('top-tickers', cache_days) == False)):
        top = utils.open('top-tickers')
    else:
        top = download_top_tickers(cantidad)
        utils.save('top-tickers', top)    
        
    return top
    
    
    
    