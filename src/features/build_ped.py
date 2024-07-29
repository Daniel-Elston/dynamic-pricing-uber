from __future__ import annotations

import logging

import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler as scaler

from config.data import DataConfig
from config.data import DataState
from utils.file_access import FileAccess


class BuildPED:
    """Build Price Elasticity of Demand Features"""

    def __init__(self, data_state: DataState, data_config: DataConfig):
        self.ds = data_state
        self.dc = data_config

    def resample_mean(self, df, period='h'):
        df = df.set_index('timestamp')
        return df[['price', 'count']].resample(period).mean().dropna()

    def get_pct_change(self, df, sma_cols):
        for col in sma_cols:
            df[col + '_pct_change'] = df[col].pct_change()
        return df

    def pipeline(self, load_path, save_path):
        logging.debug(
            'Starting Feature Building Pipeline')

        try:
            df = FileAccess.load_file(f'{load_path}/sma.parquet')

            sma_cols = [col for col in df.columns if str(self.dc.window_select) in col]
            cols = ['timestamp', 'price', 'count'] + sma_cols
            df = df[cols]

            df = self.get_pct_change(df, sma_cols)
            df[['price_sma_4_pct_change', 'price_sma_4', 'price']] = scaler().fit_transform(df[['price_sma_4_pct_change', 'price_sma_4', 'price']])

            df = df[:24*7*1]
            plt.plot(df['timestamp'], df['price_sma_4_pct_change'], label='pct_change')
            plt.plot(df['timestamp'], df['price_sma_4'], label='price_sma')
            plt.plot(df['timestamp'], df['price'], label='price')
            plt.legend()
            plt.show()
            print(df)

        except Exception as e:
            logging.exception(f'Error: {e}', exc_info=e)
            raise
        logging.debug(
            'Completed Feature Building Pipeline')
