import matplotlib.pyplot as plt, pandas as pd
from utils import * 


def grafico_tickers_repetidos(mark):   
    """
    Pasar nombre de archivo en str para visualizar los que más se repiten.
    """
       
    data = open(mark)

    tickers = []
    for activo in data.activos:
        for ticker in activo:
            tickers.append(ticker)

    tickers_df = pd.DataFrame(tickers)
    tickers_df.columns = ['tickers']

    barras = tickers_df.groupby(tickers_df.tickers).size()

    fig, axes = plt.subplots(1, figsize=(18,5))

    axes.bar(barras.index, height = barras, width=0.9)
    axes.legend(["Tickers que se repiten"], loc='upper left', fontsize=14)
    plt.xticks(barras.index, rotation=85)

    plt.style.use('fivethirtyeight')
    plt.gca().grid(False)

    plt.show()

grafico_tickers_repetidos("mark")