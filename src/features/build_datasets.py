from __future__ import annotations

import logging

import pandas as pd

from config.data import DataConfig
from config.data import DataState
from src.visuals.visualize import Visualiser
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

        Visualiser(self.ds, self.dc).generate_sma_plots(df_price_sma, 'price')
        Visualiser(self.ds, self.dc).generate_sma_plots(df_count_sma, 'count')
        return df_price_sma, df_count_sma

    def pipeline(self, load_path, save_path):
        logging.debug(
            'Starting Feature Building Pipeline')

        try:
            df = FileAccess.load_file(load_path)
            df_price_sma, df_count_sma = self.generate_sma_dataset(df)
            for df, file_name in zip([df_price_sma, df_count_sma], ['price_sma', 'count_sma']):
                FileAccess.save_file(df, f'{save_path}/{file_name}.parquet', self.dc.overwrite)

        except Exception as e:
            logging.exception(f'Error: {e}', exc_info=e)
            raise
        logging.debug(
            'Completed Feature Building Pipeline')


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

        return max_price_hour, min_price_hour, max_count_hour, min_count_hour

    def amend_bound_results(self, df, bound_hour_store, max_min, metric):
        bound_hour_store['date'] = pd.to_datetime(bound_hour_store['date'])
        merged_df = pd.merge(bound_hour_store, df, how='left', left_on='date', right_on='date', suffixes=(f'_{max_min}_{metric}', ''))
        # merged_df.drop(columns=['date'], inplace=True)

        if metric == 'price':
            merged_df = merged_df.rename(columns={f'price_{max_min}_price': f'{max_min}_price'})
        if metric == 'count':
            merged_df = merged_df.rename(columns={f'count_{max_min}_count': f'{max_min}_count'})
        return merged_df

    def run_amending(self, df, df_store):
        df = self.amend_bound_results(df, df_store[0], 'max', 'price')
        df = self.amend_bound_results(df, df_store[1], 'min', 'price')
        df = self.amend_bound_results(df, df_store[2], 'max', 'count')
        df = self.amend_bound_results(df, df_store[3], 'min', 'count')
        return df

    def pipeline(self, load_path, interim_path, result_path, save_path):
        logging.debug(
            'Starting Feature Building Pipeline')

        try:
            df = FileAccess.load_file(load_path)
            max_price_hour, min_price_hour, max_count_hour, min_count_hour = self.generate_bounds_dataset(df)
            df_store = [max_price_hour, min_price_hour, max_count_hour, min_count_hour]
            file_names = ['max_price_hour', 'min_price_hour', 'max_count_hour', 'min_count_hour']

            bound_hour_store = {}
            for frame, filename in zip(df_store, file_names):
                FileAccess.save_file(frame, f'{interim_path}/{filename}.parquet', self.dc.overwrite)

                top_hours = frame['hour'].value_counts().nlargest(12).index.tolist()
                bound_hour_store[filename] = list(top_hours)
            FileAccess.save_json(bound_hour_store, result_path, overwrite=self.dc.overwrite)

            df = self.run_amending(df, df_store)
            print(frame)
            df.head(200).to_excel('sample.xlsx')
            FileAccess.save_file(df, save_path, self.dc.overwrite)

        except Exception as e:
            logging.exception(f'Error: {e}', exc_info=True)
            raise
        logging.debug('Completed Feature Building Pipeline')
        return df
