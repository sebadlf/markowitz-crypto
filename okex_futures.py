import requests
import pandas as pd
from db import BD_CONNECTION
from sqlalchemy import create_engine
engine = create_engine(BD_CONNECTION)



# instrument_id: "LINK-USD-201113",
# underlying_index: "LINK",
# quote_currency: "USD",
# tick_size: "0.001",
# contract_val: "10",
# listing: "2020-10-30",
# delivery: "2020-11-13",
# trade_increment: "1",
# alias: "next_week",
# underlying: "LINK-USD",
# base_currency: "LINK",
# settlement_currency: "LINK",
# is_inverse: "true",
# contract_val_currency: "USD",
# category: "2"

r = requests.get('https://www.okex.com/api/futures/v3/instruments')
contracts = r.json()

create_table = '''
CREATE TABLE IF NOT EXISTS `okex_futures` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `contract` varchar(30) DEFAULT NULL,
  `ticker` varchar(20) DEFAULT NULL,
  `time` timestamp NULL DEFAULT NULL,
  `open` double DEFAULT NULL,
  `high` double DEFAULT NULL,
  `low` double DEFAULT NULL,
  `close` double DEFAULT NULL,
  `count_contracts` bigint(20) DEFAULT NULL,
  `volume` double DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_contract_ticker_time` (`contract`,`ticker`,`time`),
  KEY `idx_ticker_time` (`ticker`,`time`)
) ENGINE=InnoDB AUTO_INCREMENT=14401 DEFAULT CHARSET=latin1;
'''
engine.execute(create_table)

for contract in contracts:

    ticker = contract['underlying_index']
    contract_id = contract['instrument_id']

    if (ticker != 'LINK') and (ticker != 'DOT'):

        last_row_query = f'SELECT `id`,`time` FROM okex_futures WHERE `contract` = "{contract_id}" ORDER BY `id` DESC limit 0,1'
        last_row = engine.execute(last_row_query).fetchone()

        print(last_row)

        params = {}

        # if last_row:
        #     id = last_row[0]
        #     time = last_row[1]
        #
        #     delete_last_row_query = f'DELETE FROM okex_futures WHERE `id`={id}'
        #     engine.execute(delete_last_row_query)
        #
        #     params['time'] = time

        busquedaUltimaFecha = f'SELECT `id`,`time` FROM `{tabla}` WHERE `ticker` = "{ticker}" ORDER BY `id` DESC limit 0,1'

        print(ticker)

        url = f'https://www.okex.com/api/futures/v3/instruments/{contract_id}/history/candles'

        # params = {
        #     'start': '2020-07-25T02:31:00.000Z',
        #     'end': '2020-07-24T02:55:00.000Z',
        #     'granularity': 60
        # }

        print(url)

        r = requests.get(url)
        js = r.json()

        df = pd.DataFrame(js)

        df.columns = ['time', 'open', 'high', 'low', 'close', 'count_contracts', 'volume']

        df.time = pd.to_datetime(df.time)
        df.open = df.open.astype(float)
        df.high = df.high.astype(float)
        df.low = df.low.astype(float)
        df.close = df.close.astype(float)
        df['count_contracts'] = df['count_contracts'].astype(int)
        df.volume = df.volume.astype(float)

        df['ticker'] = ticker
        df['contract'] = contract_id

        df.to_sql('okex_futures', engine, if_exists='append')










