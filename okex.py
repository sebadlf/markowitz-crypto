import requests
import pandas as pd
from db import BD_CONNECTION
from sqlalchemy import create_engine
engine = create_engine(BD_CONNECTION)

tickers = ['BTC', 'ETH', 'LTC', 'ETC', 'XRP', 'EOS', 'BCH', 'BSV', 'TRX']

for ticker in tickers:

    print(ticker)

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
    df.to_sql('okex', engine, if_exists='append')







