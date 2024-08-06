from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler

from config.data import DataConfig
from config.data import DataState
from utils.running import Running


class BuildModelFeatures:
    """Build/compose simple datetime features"""

    def __init__(self, data_state: DataState, data_config: DataConfig):
        self.ds = data_state
        self.dc = data_config
        self.runner = Running(self.ds, self.dc)
        self.scaler = MinMaxScaler(feature_range=(0, 1))

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        steps = [
            self.build_dt_features,
            self.build_haversine_distance,
            self.build_price_per_mile,
            self.build_count_per_mile,
            self.build_demand_features,
            self.build_bound_features,
            self.build_ratios,
            self.build_scales,
            self.build_moving_averages,
            self.round_df
        ]
        for step in steps:
            df = self.runner.run_child_step(step, df)
        return df

    def build_dt_features(self, df):
        df['date'] = df['timestamp'].dt.date.astype('datetime64[ns]')
        df['hour'] = df['timestamp'].dt.hour.astype('int32')
        df['dow_num'] = df['timestamp'].dt.dayofweek
        df['is_weekend'] = df['dow_num'].isin([5, 6]).astype(int)
        df['week'] = df['timestamp'].dt.isocalendar().week.astype('UInt32')
        df['date_hour'] = df['timestamp'].dt.strftime('%Y-%m-%d %H').astype('datetime64[ns]')
        df['day_part_3hr'] = pd.cut(
            df['hour'], bins=[-1, 3, 6, 9, 12, 15, 18, 21, 23],
            labels=['Night', 'Early Morning', 'Morning', 'Early Afternoon', 'Afternoon', 'Early Evening', 'Evening', 'Early Night'])
        return df

    def calc_haversine_distance(self, lat1, lon1, lat2, lon2):
        R = 3959  # Earth radius in miles
        lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
        dlat = lat2 - lat1
        dlon = lon2 - lon1

        a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
        c = 2 * np.arcsin(np.sqrt(a))
        return R * c

    def build_haversine_distance(self, df):
        df['distance'] = self.calc_haversine_distance(
            df['pickup_latitude'], df['pickup_longitude'],
            df['dropoff_latitude'], df['dropoff_longitude'])
        return df

    def build_price_per_mile(self, df):
        df['price_per_mile'] = df['price'] / df['distance']
        df = df[(df['price_per_mile'] > 0) & (df['price_per_mile'] < 100)]
        return df

    def build_count_per_mile(self, df):
        df['count_per_mile'] = df['count'] / df['distance']
        df = df[(df['count_per_mile'] > 0) & (df['count_per_mile'] < 100)]
        return df

    def build_demand_features(self, df):
        # Hourly demand
        hourly_demand = df.groupby(['date', 'hour'])['count_per_mile'].sum().reset_index()
        hourly_demand = hourly_demand.groupby(['date', 'hour'])['count_per_mile'].mean().reset_index()
        hourly_demand.columns = ['date', 'hour', 'avg_hourly_demand']

        # Day part demand
        day_part_3hr_demand = df.groupby(['date', 'day_part_3hr'])['count_per_mile'].sum().reset_index()
        day_part_3hr_demand = day_part_3hr_demand.groupby(['date', 'day_part_3hr'])['count_per_mile'].mean().reset_index()
        day_part_3hr_demand.columns = ['date', 'day_part_3hr', 'avg_day_part_3hr_demand']

        # Daily demand
        daily_demand = df.groupby(['date', 'dow_num'])['count_per_mile'].sum().reset_index()
        daily_demand = daily_demand.groupby(['date', 'dow_num'])['count_per_mile'].mean().reset_index()
        daily_demand.columns = ['date', 'dow_num', 'avg_daily_demand']

        # Merge features back to the original dataframe
        df = pd.merge(df, hourly_demand, on=['date', 'hour'], how='left')
        df = pd.merge(df, day_part_3hr_demand, on=['date', 'day_part_3hr'], how='left')
        df = pd.merge(df, daily_demand, on=['date', 'dow_num'], how='left')

        # Forward fill to ensure each hour has a value
        df = df.sort_values(['date', 'hour'])
        df['avg_day_part_3hr_demand'] = df.groupby('date')['avg_day_part_3hr_demand'].ffill()
        df['avg_daily_demand'] = df.groupby('date')['avg_daily_demand'].ffill()
        return df

    def build_bound_features(self, df):
        df_day_part_3hr = df.groupby(['date', 'day_part_3hr']).agg({
            'count_per_mile': ['sum', 'max', 'min', 'mean']
        }).reset_index()
        df_day_part_3hr.columns = [
            'date', 'day_part_3hr',
            '3h_partly_cpm_sum', '3h_partly_cpm_max', '3h_partly_cpm_min', '3h_partly_cpm_mean']
        df = pd.merge(df, df_day_part_3hr, on=['date', 'day_part_3hr'], how='left')
        df = df.replace([np.inf, -np.inf], np.nan)
        df = df.dropna()
        return df

    def build_ratios(self, df):
        df['3h_partly_cpm_max_ratio'] = df['count_per_mile'] / df['3h_partly_cpm_max']
        df['3h_partly_cpm_min_ratio'] = df['3h_partly_cpm_min'] / df['count_per_mile']
        df['3h_partly_cpm_mean_ratio'] = df['count_per_mile'] / df['3h_partly_cpm_mean']
        df = df.replace([np.inf, -np.inf], np.nan)
        df = df.dropna()
        return df

    def build_scales(self, df):
        df['3h_partly_cpm_max_ratio_scaled'] = self.scaler.fit_transform(df[['3h_partly_cpm_max_ratio']])
        df['3h_partly_cpm_min_ratio_scaled'] = self.scaler.fit_transform(df[['3h_partly_cpm_min_ratio']])
        df['3h_partly_cpm_mean_ratio_scaled'] = self.scaler.fit_transform(df[['3h_partly_cpm_mean_ratio']])
        df = df.replace([np.inf, -np.inf], np.nan)
        df = df.dropna()
        return df

    def build_moving_averages(self, df, window=6):
        cols = [col for col in df.columns if '3h_partly_cpm_mean_ratio' in col]
        for col in cols:
            df[col + '_rolling'] = df[col].rolling(window=window).mean()
        return df

    def round_df(self, df):
        float_cols = df.dtypes[df.dtypes == 'float64'].index
        df[float_cols] = df[float_cols].round(2)
        return df.dropna()
