from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.signal import savgol_filter
from sklearn.preprocessing import MinMaxScaler

from config.data import DataConfig
from config.data import DataState
pd.options.mode.chained_assignment = None


class BuildPED:
    """Build Price Elasticity of Demand Features"""

    def __init__(self, data_state: DataState, data_config: DataConfig):
        self.ds = data_state
        self.dc = data_config

    def resample_mean(self, df, period='h'):
        df = df.set_index('timestamp')
        return df[['price', 'count']].resample(period).mean().dropna()

    def apply_iqr_cap(self, df, col):
        Q1_price = df[col].quantile(0.05)
        Q3_price = df[col].quantile(0.95)
        IQR_price = Q3_price - Q1_price

        lower_bound_price = Q1_price - 1.5 * IQR_price
        upper_bound_price = Q3_price + 1.5 * IQR_price
        df[col] = df[col].clip(lower=lower_bound_price, upper=upper_bound_price)
        return df

    def get_diff_change(self, df, sma_cols) -> pd.DataFrame:
        for col in sma_cols:
            df[col + '_diff'] = df[col].diff()
            df.replace([np.inf, -np.inf], np.nan, inplace=True)
            df = self.apply_iqr_cap(df, col + '_diff')
        return df

    def get_pct_change(self, df, sma_cols):
        for col in sma_cols:
            df[col + '_pct_change'] = df[col].pct_change()
            df.replace([np.inf, -np.inf], np.nan, inplace=True)
            df = self.apply_iqr_cap(df, col + '_pct_change')
        return df

    def get_price_elasticity_demand(self, df, sma_cols):
        price, count = sma_cols[0], sma_cols[1]
        df['ped'] = df[f'{count}_diff'] / df[f'{price}_diff']
        df.replace([np.inf, -np.inf], np.nan, inplace=True)
        df = self.apply_iqr_cap(df, 'ped')
        return df

    def smooth_ped(self, ped_series, smoothness=0.1, window_size=None):
        smoothness = max(0, min(smoothness, 1))
        ema_span = int(2 + (len(ped_series) - 2) * smoothness)

        ema_smoothed = ped_series.ewm(span=ema_span, adjust=False).mean()
        if window_size is None:
            window_size = int(len(ped_series) * smoothness * 0.1)
            window_size = max(5, window_size + 1 if window_size % 2 == 0 else window_size)

        window_size = window_size + 1 if window_size % 2 == 0 else window_size  # odd windoow size

        poly_order = min(3, window_size - 1)
        savgol_smoothed = savgol_filter(ema_smoothed, window_size, poly_order)
        return pd.Series(savgol_smoothed, index=ped_series.index, name='smoothed_ped')

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        sma_cols = [col for col in df.columns if str(self.dc.window_select) in col]
        cols = ['timestamp', 'price', 'count'] + sma_cols
        df = df[cols]

        # df = self.get_pct_change(df, sma_cols)
        df = self.get_diff_change(df, sma_cols)
        df = self.get_price_elasticity_demand(df, sma_cols)

        df['ped'] = self.smooth_ped(df['ped'], smoothness=0.0001)
        df[['ped', 'price_sma_6', 'count_sma_6']] = MinMaxScaler().fit_transform(df[['ped', 'price_sma_6', 'count_sma_6']])

        # Select a subset of data for plotting (e.g., one week)
        df = df[-24*7*2:]

        # Plotting the scaled data
        plt.figure(figsize=(10, 6))
        plt.plot(df['timestamp'], df['ped'], label='PED', linewidth=0.5)
        plt.plot(df['timestamp'], df['price_sma_6'], label='Price SMA 6', linewidth=0.5)
        plt.plot(df['timestamp'], df['count_sma_6'], label='Count SMA 6', linewidth=0.5)
        plt.legend()
        plt.xlabel('Timestamp')
        plt.ylabel('Scaled Values')
        plt.title('Scaled PED, Price SMA, and Price Over Time')
        plt.savefig('reports/figures/ped.png')
        plt.show()
        # return df
