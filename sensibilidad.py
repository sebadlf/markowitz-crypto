import numpy as np
import pandas as pd

def add_signals_sma(data, params):
    add_sma(data, int(params['sma_fast']), 'sma_fast')
    add_sma(data, int(params['sma_slow']), 'sma_slow')

    data['cruce'] = data['sma_fast'] / data['sma_slow']

    data['signal'] = np.where(data.cruce > params['diff_buy'], 'buy', 'hold')
    data['signal'] = np.where(data.cruce < params['diff_sell'], 'sell', data.signal)

    data.dropna(inplace=True)

def get_params_sma():
    import random

    sma_fast = random.randrange(5, 35)
    sma_slow = int(sma_fast * random.uniform(2, 3))

    params = {
        'sma_fast': sma_fast,
        'sma_slow': sma_slow,
        'diff_buy': 1 + random.randrange(0, 5) / 100,
        'diff_sell': 1 - random.randrange(0, 5) / 100,
    }

    return params

def add_signals_rsi(data, params):
    add_rsi(data, params['rsi_q'])

    data['signal'] = np.where(data.rsi < params['rsi_buy'], 'buy', 'hold')
    data['signal'] = np.where(data.rsi > params['rsi_sell'], 'sell', data.signal)

    data.dropna(inplace=True)

def get_params_rsi():
    import random

    params = {
        'rsi_q': random.randrange(10, 20),
        'rsi_buy': random.randrange(20, 40),
        'rsi_sell': random.randrange(50, 80),
    }

    return params

def get_trades(data):
    df = data.copy()

    # Una sola entrada y salida por vez
    trades = df.loc[df.signal != 'hold'].copy()
    trades['signal'] = np.where(trades.signal != trades.signal.shift(), trades.signal, 'hold')
    trades = trades.loc[trades.signal != 'hold'].copy()

    # Supuesto estrategia long, debe empezar con compra y terminar con venta
    if (len(trades) >= 1) and (trades.iloc[0]['signal'] == 'sell'):
        trades = trades.iloc[1:]

    if (len(trades) >= 1) and (trades.iloc[-1]['signal'] == 'buy'):
        trades = trades.iloc[:-1]

    return trades


def get_yields(trades):
    precios_compra = trades.iloc[::2].reset_index().Close
    precios_venta = trades.iloc[1::2].reset_index().Close

    fechas_compra = trades.iloc[::2].index
    fechas_venta = trades.iloc[1::2].index

    days = 0
    if (len(fechas_venta) > 0) and (len(fechas_compra) > 0):
        days = (fechas_venta - fechas_compra).days

    if len(fechas_compra) == 0:
        fechas_compra = ''

    if len(fechas_venta) == 0:
        fechas_venta = ''

    yields = (precios_venta / precios_compra - 1).to_frame()
    yields.columns = ['yield']
    yields['days'] = days
    yields['start'] = fechas_compra
    yields['end'] = fechas_venta
    yields['ok'] = np.where(yields['yield'] > 0, True, False)
    yields['yield_cum'] = (yields['yield'] + 1).cumprod()

    return yields


def get_result(yields):
    resultado = float((yields.iloc[-1:].yield_cum - 1) * 100) if len(yields) > 0 else 0
    return resultado

def get_analytics(yields):
    counts = yields.groupby('ok').size()
    avg_yield = yields.groupby('ok').mean()['yield']
    days = yields.groupby('ok').sum()['days']

    counts_true = counts.get(True) or 0
    counts_false = counts.get(False) or 0

    avg_yield_true = avg_yield.get(True) or 0
    avg_yield_false = avg_yield.get(False) or 0

    days_true = days.get(True) or 0
    days_false = days.get(False) or 0


    result = {
        'result': get_result(yields),
        'count_ok': counts_true,
        'count_fail': counts_false,
        'avg_yield_ok': avg_yield_true,
        'avg_yield_fail': avg_yield_false,
        'days_ok': days_true,
        'days_fail': days_false,
        'math_hope': counts_true * avg_yield_true + counts_false * avg_yield_false,
        'time_bougth_ok': days_false / (days_true + days_false) if days_true else -1
    }

    return result

def get_sensitivity(data_serie, add_signals, get_params, cicles=1000):
    results = []
    for i in range(cicles):

        if ((i + 1) % 100 == 0):
            print((i + 1), end=' - ')

        local_data = pd.DataFrame(data_serie.copy())
        local_data.columns = ['Close']

        params = get_params()
        add_signals(local_data, params)

        trades = get_trades(local_data)
        yields = get_yields(trades)

        #try:
        analytics = get_analytics(yields)
        res = {**params, **analytics}
        results.append(res)
        #except:
        #    pass

    df = pd.DataFrame(results)

    if len(df) > 0:
        df = df.sort_values(by="result", ascending=False)

    return df

def add_sma(data, span, alias=''):
    if (len(alias) == 0):
        alias = 'sma_' + str(span)

    data[alias] = data.Close.rolling(span).mean()
    return data

def add_rsi(data, rsi_q = 14):
    dif = data['Close'].diff()
    win =  pd.DataFrame(np.where(dif > 0, dif, 0))
    loss = pd.DataFrame(np.where(dif < 0, abs(dif), 0))
    ema_win = win.ewm(alpha=1/rsi_q).mean()
    ema_loss = loss.ewm(alpha=1/rsi_q).mean()
    rs = ema_win / ema_loss
    rsi = 100 - (100 / (1+rs))
    rsi.index = data.index
    data['rsi'] = rsi
    return data

def get_operaciones(data):
    df = data.copy()

    # Una sola entrada y salida por vez
    trades = df.loc[df.signal != 'hold'].copy()
    trades['signal'] = np.where(trades.signal != trades.signal.shift(), trades.signal, 'hold')
    trades = trades.loc[trades.signal != 'hold'].copy()

    # Supuesto estrategia long, debe empezar con compra y terminar con venta
    if trades.iloc[0]['signal'] == 'sell':
        trades = trades.iloc[1:]

    if trades.iloc[-1]['signal'] == 'buy':
        trades = trades.iloc[:-1]

    fechas_compra = trades.iloc[::2].index
    fechas_venta = trades.iloc[1::2].index

    operaciones = (fechas_compra).to_frame()
    operaciones['fecha_venta'] = fechas_venta

    operaciones.columns = ['fecha_compra', 'fecha_venta']

    return operaciones