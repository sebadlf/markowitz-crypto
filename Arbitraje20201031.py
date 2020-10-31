#!pip install PyMySQL
from sqlalchemy import create_engine

#Seteo el USER : PASS @ HOST / BBDD_NAME
sql_engine = create_engine('mysql+pymysql://root:@localhost/tpmodulo3')
sql_conn = sql_engine.connect()



def B_Bajada(fsym='BTC',tsym='USDT', interval='1d', startTime=None, endTime=None, limit=1000):
    '''bajadaSimple('BTCUSDT',interval='2h',startTime=1597719600000,endTime=1600398000000)'''
    symbol=fsym+tsym
    url = 'https://api.binance.com/api/v3/klines'
    params = {'symbol':symbol, 'interval':interval,'startTime':startTime, 'endTime':endTime, 'limit':limit}
    r = requests.get(url, params=params)
    js = r.json()
    # Armo el dataframe
    cols = ['time','open','high','low','close','volume','cTime',
            'qVolume','trades','takerBase','takerQuote','Ignore']
    df = pd.DataFrame(js, columns=cols)
    
    #Convierto los valores strings a numeros
    df = df.apply(pd.to_numeric)
    
    # Le mando indice de timestamp
    df.time=pd.to_datetime(df.time, unit='ms')
    df.drop(columns=['cTime','qVolume','trades','takerBase','takerQuote','Ignore'],axis=1,inplace=True)
    df['ticker']=fsym
    return df

def O_Bajada(ticker):
    url = f'https://okex.com/api/spot/v3/instruments/{ticker}-USDT/history/candles'

    # params = {
    #     'start': '2020-07-25T02:31:00.000Z',
    #     'end': '2020-07-24T02:55:00.000Z',
    #     'granularity': 60
    # }
    params ={}
    r = requests.get(url, params=params)
    js = r.json()
    df = pd.DataFrame(js)
    df.columns = ['time', 'open', 'high', 'low', 'close', 'volume']
    df.time = pd.to_datetime(df.time)
    df.open = df.open.astype(float)
    df.high = df.high.astype(float)
    df.low = df.low.astype(float)
    df.close = df.close.astype(float)
    df.volume = df.volume.astype(float)
    df['ticker'] = ticker
    return df

tickers=['BTC', 'ETH', 'LTC', 'ETC', 'XRP', 'EOS', 'BCH', 'BSV', 'TRX']


for ticker in list(tickers):
    df=B_Bajada(fsym=ticker,interval='1m')
    #Guardo la Tabla en SQL
    df.to_sql(con=sql_conn, name='base_binance', if_exists='append')

for ticker in list(tickers):
    df=O_Bajada(ticker)
    #Guardo la Tabla en SQL
    df.to_sql(con=sql_conn, name='base_okex', if_exists='append')

q = '''CREATE VIEW diferencia AS
    SELECT B.time,B.ticker, B.close AS 'binance', O.close as 'okex', (O.close/B.close-1)*100 as 'diferencia' 
    FROM base_binance B JOIN base_okex O 
    ON B.time=O.time AND b.ticker=O.ticker 
    WHERE b.ticker = 'BTC'
    ORDER BY B.time
    '''
sql_conn.execute(q)

q='SELECT * FROM diferencia'
pd.read_sql(q,sql_conn)



  