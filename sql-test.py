from db import BD_CONNECTION
from sqlalchemy import create_engine
import pandas as pd
import utils

engine = create_engine(BD_CONNECTION)

data = utils.open('top-tickers')

data.to_sql('top_tickers', engine, if_exists='replace')