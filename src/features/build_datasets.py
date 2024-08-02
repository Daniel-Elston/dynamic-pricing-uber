from __future__ import annotations

from typing import List

import pandas as pd

from config.data import DataConfig
from config.data import DataState
from utils.file_access import FileAccess


class BuildMovingAverages:
    def __init__(self, data_state: DataState, data_config: DataConfig):
        self.ds = data_state
        self.dc = data_config

    def resample_mean(self, df, period='h'):
        df = df.set_index('timestamp')
        return df[['price', 'count']].resample(period).mean().dropna()

    def resample_sum(self, df, period='h'):
        df = df.set_index('timestamp')
        return df[['price', 'count']].resample(period).sum().dropna()

    def apply_moving_average(self, df, column, window):
        def lambda_logic(x): return x.rolling(window=window).mean().bfill()
        df[f'{column}_sma_{window}'] = df[column].transform(lambda_logic)
        df[f'{column}_sma_{window}'] = round(df[f'{column}_sma_{window}'], 2)
        return df

    def generate_sma_dataset(self, df):
        df_sample_mean = self.resample_mean(df, period='h')
        df_sample_sum = self.resample_sum(df, period='h')

        for hour in self.dc.sma_windows:
            df_price_sma = self.apply_moving_average(df_sample_mean, column='price', window=hour)
            df_count_sma = self.apply_moving_average(df_sample_sum, column='count', window=hour)

        # sample = 24*7*2
        # Visualiser(self.ds, self.dc).generate_sma_plots(df_price_sma[:sample], 'price')
        # Visualiser(self.ds, self.dc).generate_sma_plots(df_count_sma[:sample], 'count')
        return df_price_sma, df_count_sma

    def merge_sma_data(self, df_price_sma, df_count_sma):
        df = pd.merge(df_price_sma, df_count_sma, how='left', left_index=True, right_index=True, suffixes=('', '_x'))
        df = df.drop(columns=['price_x', 'count_x'])
        return df

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        df_price_sma, df_count_sma = self.generate_sma_dataset(df)
        df = self.merge_sma_data(df_price_sma, df_count_sma)
        df = df.reset_index()
        return df


class BuildBounds:
    def __init__(self, data_state: DataState, data_config: DataConfig):
        self.ds = data_state
        self.dc = data_config

    def resample_mean(self, df, period='h'):
        df = df.set_index('timestamp')
        return df[['price', 'count']].resample(period).mean().dropna()

    def resample_sum(self, df, period='h'):
        df = df.set_index('timestamp')
        return df[['price', 'count']].resample(period).sum().dropna()

    def generate_group_stats(self, df, col, group_cols):
        grouped = df.groupby(group_cols)[col].sum().reset_index()
        idx_max = grouped.groupby('date')[col].idxmax()
        idx_min = grouped.groupby('date')[col].idxmin()
        grouped_max = grouped.loc[idx_max].reset_index(drop=True)
        grouped_min = grouped.loc[idx_min].reset_index(drop=True)
        return grouped_max, grouped_min

    def generate_bounds_dataset(self, df):
        df_resample_sum = self.resample_sum(df, period='h')
        df_resample_sum = df_resample_sum.reset_index()

        df_resample_sum['hour'] = df_resample_sum['timestamp'].dt.hour
        df_resample_sum['date'] = df_resample_sum['timestamp'].dt.date

        max_price_hour, min_price_hour = self.generate_group_stats(df_resample_sum, 'price', ['date', 'hour'])
        max_count_hour, min_count_hour = self.generate_group_stats(df_resample_sum, 'count', ['date', 'hour'])

        max_price_hour = max_price_hour.rename(columns={'price': 'price_max', 'hour': 'max_price_hour'})
        min_price_hour = min_price_hour.rename(columns={'price': 'price_min', 'hour': 'min_price_hour'})
        max_count_hour = max_count_hour.rename(columns={'count': 'count_max', 'hour': 'max_count_hour'})
        min_count_hour = min_count_hour.rename(columns={'count': 'count_min', 'hour': 'min_count_hour'})

        return max_price_hour, min_price_hour, max_count_hour, min_count_hour

    def amend_bound_results(self, df, df_store):
        df = df[['date']]
        for frame in df_store:
            frame['date'] = pd.to_datetime(frame['date'])
            df = pd.merge(frame, df, how='left', left_on='date', right_on='date')
        return df

    def pipeline(self, df: pd.DataFrame, save_paths: List[str]) -> pd.DataFrame:
        interim_path, result_path, frames_path = save_paths

        max_price_hour, min_price_hour, max_count_hour, min_count_hour = self.generate_bounds_dataset(df)
        df_store = [max_price_hour, min_price_hour, max_count_hour, min_count_hour]
        file_names = ['max_price_hour', 'min_price_hour', 'max_count_hour', 'min_count_hour']

        bound_hour_store = {}
        for frame, filename in zip(df_store, file_names):
            FileAccess.save_file(frame, f'{interim_path}/{filename}.parquet', self.dc.overwrite)

            top_hours = frame[filename].value_counts().nlargest(12).index.tolist()
            bound_hour_store[filename] = list(top_hours)
        FileAccess.save_json(bound_hour_store, result_path, overwrite=self.dc.overwrite)

        df_bounds = self.amend_bound_results(df, df_store)
        FileAccess.save_file(df_bounds, f'{frames_path}/bounds.parquet', self.dc.overwrite)
        return df_store, bound_hour_store, df_bounds
