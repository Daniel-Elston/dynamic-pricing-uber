from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler

from config.data import DataConfig
from config.data import DataState
from utils.running import Running


class BuildFeatures:
    """Build/compose Extensive features for analysis"""

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
            self.build_moving_averages,
            self.build_price_features,
            self.build_distance_features,
            self.build_lagged_features
        ]
        for step in steps:
            df = self.runner.run_child_step(step, df)
        return df

    def build_dt_features(self, df):
        df['hour'] = df['timestamp'].dt.hour.astype('int32')
        df['date'] = df['timestamp'].dt.date.astype('datetime64[ns]')
        df['week'] = df['timestamp'].dt.isocalendar().week.astype('UInt32')
        df['month'] = df['timestamp'].dt.month.astype('int32')
        df['dow'] = df['timestamp'].dt.day_name().str[:3]
        df['dow_num'] = df['timestamp'].dt.dayofweek
        df['date_hour'] = df['timestamp'].dt.strftime('%Y-%m-%d %H').astype('datetime64[ns]')
        df['is_weekend'] = df['dow_num'].isin([5, 6]).astype(int)
        df['day_part_6hr'] = pd.cut(
            df['hour'], bins=[-1, 6, 12, 18, 23],
            labels=['Night', 'Morning', 'Afternoon', 'Evening'])
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

    def build_pct_change(self, df):
        df['pct_change_ppm'] = df['price_per_mile'].pct_change()
        df['pct_change_cpm'] = df['count_per_mile'].pct_change()
        return df

    def build_demand_features(self, df):
        # Hourly demand
        hourly_demand = df.groupby(['date', 'hour'])['count_per_mile'].sum().reset_index()
        hourly_demand = hourly_demand.groupby(['date', 'hour'])['count_per_mile'].mean().reset_index()
        hourly_demand.columns = ['date', 'hour', 'avg_hourly_demand']

        # Day part demand
        df['date'] = pd.to_datetime(df['date'])
        day_part_3hr_demand = df.groupby(['date', 'day_part_3hr'])['count_per_mile'].sum().reset_index()
        day_part_3hr_demand = day_part_3hr_demand.groupby(['date', 'day_part_3hr'])['count_per_mile'].mean().reset_index()
        day_part_3hr_demand.columns = ['date', 'day_part_3hr', 'avg_day_part_3hr_demand']

        day_part_6hr_demand = df.groupby(['date', 'day_part_6hr'])['count_per_mile'].sum().reset_index()
        day_part_6hr_demand = day_part_6hr_demand.groupby(['date', 'day_part_6hr'])['count_per_mile'].mean().reset_index()
        day_part_6hr_demand.columns = ['date', 'day_part_6hr', 'avg_day_part_6hr_demand']

        # Daily demand
        daily_demand = df.groupby(['date', 'dow_num'])['count_per_mile'].sum().reset_index()
        daily_demand = daily_demand.groupby(['date', 'dow_num'])['count_per_mile'].mean().reset_index()
        daily_demand.columns = ['date', 'dow_num', 'avg_daily_demand']

        # Merge features back to the original dataframe
        df = pd.merge(df, hourly_demand, on=['date', 'hour'], how='left')
        df = pd.merge(df, day_part_3hr_demand, on=['date', 'day_part_3hr'], how='left')
        df = pd.merge(df, day_part_6hr_demand, on=['date', 'day_part_6hr'], how='left')
        df = pd.merge(df, daily_demand, on=['date', 'dow_num'], how='left')

        # Forward fill to ensure each hour has a value
        df = df.sort_values(['date', 'hour'])
        df['avg_day_part_3hr_demand'] = df.groupby('date')['avg_day_part_3hr_demand'].ffill()
        df['avg_day_part_6hr_demand'] = df.groupby('date')['avg_day_part_6hr_demand'].ffill()
        df['avg_daily_demand'] = df.groupby('date')['avg_daily_demand'].ffill()

        return df

    def build_bound_features(self, df):
        df['date'] = pd.to_datetime(df['date'])
        df['date_hour'] = pd.to_datetime(df['date_hour'])

        df_hourly = df.groupby(['date', 'hour']).agg({
            'price_per_mile': ['mean', 'max', 'min'],
            'count_per_mile': ['sum', 'max', 'min', 'mean']
        }).reset_index()

        df_day_part_6hr = df.groupby(['date', 'day_part_6hr']).agg({
            'price_per_mile': ['mean', 'max', 'min'],
            'count_per_mile': ['sum', 'max', 'min', 'mean']
        }).reset_index()

        df_day_part_3hr = df.groupby(['date', 'day_part_3hr']).agg({
            'price_per_mile': ['mean', 'max', 'min'],
            'count_per_mile': ['sum', 'max', 'min', 'mean']
        }).reset_index()

        df_daily = df.groupby('date').agg({
            'price_per_mile': ['mean', 'max', 'min'],
            'count_per_mile': ['sum', 'max', 'min', 'mean']
        }).reset_index()

        df_hourly.columns = [
            'date', 'hour', 'hourly_ppm_mean', 'hourly_ppm_max', 'hourly_ppm_min',
            'hourly_cpm_sum', 'hourly_cpm_max', 'hourly_cpm_min', 'hourly_cpm_mean']
        df_day_part_3hr.columns = [
            'date', 'day_part_3hr', '3h_partly_ppm_mean', '3h_partly_ppm_max', '3h_partly_ppm_min',
            '3h_partly_cpm_sum', '3h_partly_cpm_max', '3h_partly_cpm_min', '3h_partly_cpm_mean']
        df_day_part_6hr.columns = [
            'date', 'day_part_6hr', '6h_partly_ppm_mean', '6h_partly_ppm_max', '6h_partly_ppm_min',
            '6h_partly_cpm_sum', '6h_partly_cpm_max', '6h_partly_cpm_min', '6h_partly_cpm_mean']
        df_daily.columns = [
            'date', 'daily_ppm_mean', 'daily_ppm_max', 'daily_ppm_min',
            'daily_cpm_sum', 'daily_cpm_max', 'daily_cpm_min', 'daily_cpm_mean']

        df = pd.merge(df, df_hourly, on=['date', 'hour'], how='left')
        df = pd.merge(df, df_day_part_3hr, on=['date', 'day_part_3hr'], how='left')
        df = pd.merge(df, df_day_part_6hr, on=['date', 'day_part_6hr'], how='left')
        df = pd.merge(df, df_daily, on=['date'], how='left')

        df = df.replace([np.inf, -np.inf], np.nan)
        df = df.dropna()
        return df

    def build_ratios(self, df):
        for col in ['hourly_', '3h_partly_', '6h_partly_', 'daily_']:
            df[col + 'ppm_max_ratio'] = df['price_per_mile'] / df[col + 'ppm_max']
            df[col + 'ppm_min_ratio'] = df[col + 'ppm_min'] / df['price_per_mile']
            df[col + 'cpm_max_ratio'] = df['count_per_mile'] / df[col + 'cpm_max']
            df[col + 'cpm_min_ratio'] = df[col + 'cpm_min'] / df['count_per_mile']
            df[col + 'cpm_sum_ratio'] = df['count_per_mile'] / df[col + 'cpm_sum']
            df[col + 'cpm_mean_ratio'] = df['count_per_mile'] / df[col + 'cpm_mean']

            df[col + 'ppm_max_ratio_scaled'] = self.scaler.fit_transform(df[[col + 'ppm_max_ratio']])
            df[col + 'ppm_min_ratio_scaled'] = self.scaler.fit_transform(df[[col + 'ppm_min_ratio']])
            df[col + 'cpm_max_ratio_scaled'] = self.scaler.fit_transform(df[[col + 'cpm_max_ratio']])
            df[col + 'cpm_min_ratio_scaled'] = self.scaler.fit_transform(df[[col + 'cpm_min_ratio']])
            df[col + 'cpm_sum_ratio_scaled'] = self.scaler.fit_transform(df[[col + 'cpm_sum_ratio']])
            df[col + 'cpm_mean_ratio_scaled'] = self.scaler.fit_transform(df[[col + 'cpm_mean_ratio']])

        df = df.replace([np.inf, -np.inf], np.nan)
        df = df.dropna()
        return df

    def build_moving_averages(self, df, window=6):
        cols = [col for col in df.columns if '3h_partly' in col or '6h_partly' in col]
        for col in cols:
            df[col + '_rolling'] = df[col].rolling(window=window).mean()
        return df

    def build_price_features(self, df):
        df['ppm_scaled'] = self.scaler.fit_transform(df[['price_per_mile']])
        df['expensive_trip'] = (df['price_per_mile'] > df['price_per_mile'].quantile(0.95)).astype(int)
        df['cheap_trip'] = (df['price_per_mile'] < df['price_per_mile'].quantile(0.05)).astype(int)
        df = df.replace([np.inf, -np.inf], np.nan)
        return df

    def build_distance_features(self, df):
        df['distance_scaled'] = self.scaler.fit_transform(df[['distance']])
        df['long_trip'] = (df['distance'] > df['distance'].quantile(0.95)).astype(int)
        df['short_trip'] = (df['distance'] < df['distance'].quantile(0.05)).astype(int)
        df = df.replace([np.inf, -np.inf], np.nan)
        return df

    def build_lagged_features(self, df):
        for lag in self.dc.lag_windows:
            df[f'ppm_lag_{lag}'] = df.groupby('date')['price_per_mile'].shift(lag).ffill()
            df[f'cpm_lag_{lag}'] = df.groupby('date')['count_per_mile'].shift(lag).ffill()
        return df

    def round_df(self, df):
        float_cols = df.dtypes[df.dtypes == 'float64'].index
        df[float_cols] = df[float_cols].round(2)
        return df.dropna()

    # def save_summary_stats(self, df):
    #     summary_stats = df.groupby(['hour', 'dow_num']).agg({
    #         'price_per_mile': ['mean', 'min', 'max'],
    #         'cpm': ['sum', 'mean'],
    #         'distance': ['mean', 'max'],
    #         'avg_hourly_demand': 'first',
    #         'avg_daily_demand': 'first'
    #     }).reset_index()
