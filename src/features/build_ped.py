from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler

from config.data import DataConfig
from config.data import DataState


class BuildPED:
    def __init__(self, data_state: DataState, data_config: DataConfig):
        self.ds = data_state
        self.dc = data_config

    def resample_hourly(self, df):
        df = df.sort_values('timestamp')
        df = df.set_index('timestamp')
        resampled = df.resample('H').agg({
            'price': 'mean',
            'count_per_mile': 'mean',
            'distance': 'mean',
            'price_per_mile': 'mean'
        })
        return resampled.reset_index()

    def apply_iqr_cap(self, df, col):
        Q1 = df[col].quantile(0.05)
        Q3 = df[col].quantile(0.95)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        df[col] = df[col].clip(lower=lower_bound, upper=upper_bound)
        return df

    def get_diff(self, df, cols, change_type='_pct_change'):
        if change_type == '_pct_change':
            for col in cols:
                df[col + change_type] = df[col].pct_change()
        if change_type == '_log_diff':
            for col in cols:
                df[col + change_type] = np.log(df[col]).diff()
        if change_type == '_diff':
            for col in cols:
                df[col + change_type] = df[col].diff()

        for col in cols:
            df.replace([np.inf, -np.inf], np.nan, inplace=True)
        return df

    def get_price_elasticity_demand(self, df, change_type):
        df['ped'] = df[f'count_per_mile{change_type}'] / df[f'price_per_mile{change_type}']
        df.replace([np.inf, -np.inf], np.nan, inplace=True)
        return df

    def normalize_for_visualization(self, df, cols):
        scaler = MinMaxScaler()
        # scaler = StandardScaler()
        df[f'{cols}_normalized'] = scaler.fit_transform(df[[cols]])
        return df

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        cols = ['price_per_mile', 'count_per_mile']
        change_type = '_pct_change'

        df = self.resample_hourly(df)
        df = self.get_diff(df, cols, change_type)
        df = self.get_price_elasticity_demand(df, change_type)

        for col in [f'price_per_mile{change_type}', f'count_per_mile{change_type}', 'ped']:
            df = self.apply_iqr_cap(df, col)
        for col in [f'price_per_mile{change_type}', f'count_per_mile{change_type}', 'ped']:
            df = self.normalize_for_visualization(df, col)

        df = df[-24*7*1:]

        # Plotting
        plt.figure(figsize=(12, 6))
        plt.plot(df['timestamp'], df['ped_normalized'], label='PED', linewidth=0.8)
        plt.plot(df['timestamp'], df[f'price_per_mile{change_type}_normalized'], label='price_per_mile % Change', linewidth=0.8)
        plt.plot(df['timestamp'], df[f'count_per_mile{change_type}_normalized'], label='count_per_mile % Change', linewidth=0.8)
        plt.legend()
        plt.xlabel('Timestamp')
        plt.ylabel('Normalized Values')
        plt.title('Normalized PED, Price % Change, and Count % Change Over Time')
        plt.show()
        plt.savefig('reports/figures/ped_normalized.png')
        plt.close()

        # Plotting non-normalized data
        plt.figure(figsize=(12, 6))
        plt.plot(df['timestamp'], df['ped'], label='PED', linewidth=0.8)
        plt.plot(df['timestamp'], df[f'price_per_mile{change_type}'], label='price_per_mile % Change', linewidth=0.8)
        plt.plot(df['timestamp'], df[f'count_per_mile{change_type}'], label='count_per_mile % Change', linewidth=0.8)
        plt.legend()
        plt.xlabel('Timestamp')
        plt.ylabel('Values')
        plt.title('PED, Price % Change, and Count % Change Over Time')
        plt.show()
        plt.savefig('reports/figures/ped.png')
        plt.close()

        return df
