from __future__ import annotations

import logging
import os
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from utils.exceptions import InvalidConfigError
from utils.setup_env import setup_project_env
from utils.view_file import FileHandler
project_dir, project_config, setup_logs = setup_project_env()


class ExploreTimestamps:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.load_path = Path(project_config['path']['interim'])
        self.save_path = Path(project_config['path']['interim'])

    def generate_max_min_timestamps(self, df, dow='Mon'):
        """Find the Timestamps corresponding to the Max and Min Pressure"""
        store = {
            'sensor': [],
            'max_timestamp': [],
            'max_pressure': [],
            'min_timestamp': [],
            'min_pressure': []
        }

        for sensor in df.sensor.unique():
            df_sensor = df[df['sensor'] == sensor].copy()

            week_days = ['Mon', 'Tue', 'Wed', 'Thu']
            for current_dow in week_days:
                df_dow = df_sensor[df_sensor['dow'] == current_dow]
                if not df_dow.empty:
                    break

            if df_dow.empty:
                self.logger.warning(f"No valid data for sensor {sensor} on any day in {week_days}")
                continue

            max_row = df_sensor[df_sensor['pressure_val'] == df_sensor['pressure_val'].max()]
            min_row = df_sensor[df_sensor['pressure_val'] == df_sensor['pressure_val'].min()]

            if not max_row.empty and not min_row.empty:
                max_row = max_row.iloc[0]
                min_row = min_row.iloc[0]

                store['sensor'].append(sensor)
                store['max_timestamp'].append(max_row['timestamp'])
                store['max_pressure'].append(max_row['pressure_val'])
                store['min_timestamp'].append(min_row['timestamp'])
                store['min_pressure'].append(min_row['pressure_val'])
            else:
                self.logger.warning(f"No valid max/min data for sensor {sensor} on {dow}")
        FileHandler().save_parquet(pd.DataFrame(store), f'{self.save_path}/max_min_timestamps.parquet')

    def extract_assign_hour(self, df, df_demand):
        """Extract max/min hour from timestamp and assign to sensor"""
        df_demand['demand_max_hour'] = df_demand['max_timestamp'].dt.hour
        df_demand['demand_min_hour'] = df_demand['min_timestamp'].dt.hour

        df_merged = df.merge(
            df_demand[['sensor', 'demand_max_hour']], on='sensor', how='left', suffixes=('', '_max_demand_hour'))
        df_merged = df_merged.merge(
            df_demand[['sensor', 'demand_min_hour']], on='sensor', how='left', suffixes=('', '_min_demand_hour'))
        return df_merged

    def generate_plots(self, df):
        plt.figure(figsize=(25, 13))
        sns.histplot(df['demand_max_hour'], bins=24, kde=True, color='blue', label='Max Pressure Times')
        sns.histplot(df['demand_min_hour'], bins=24, kde=True, color='red', label='Min Pressure Times')
        plt.xlabel('Hour of the Day')
        plt.ylabel('Frequency')
        plt.title('Distribution of Max and Min Pressure Times')
        plt.legend()
        plt.savefig('reports/figures/max_min/max_min_timestamps.png')

    def pipeline(self, df, selected_df):
        self.logger.debug('Starting Timestamp Exploration Pipeline')
        if not os.path.isfile(Path(f'{self.load_path}/max_min_timestamps.parquet')):
            if selected_df != 'week':
                self.logger.error("Invalid configuration value for selected_df. Please re-run with selected_df='week'")
                raise InvalidConfigError("Invalid configuration value for selected_df. Please re-run with selected_df='week'")
            self.generate_max_min_timestamps(df)
        try:
            df_demand = FileHandler().load_parquet(f'{self.load_path}/max_min_timestamps.parquet')
            df_merged = self.extract_assign_hour(df, df_demand)
            self.generate_plots(df_demand)
        except Exception as e:
            self.logger.exception(f'Error {self.__class__.__name__}: {e}', exc_info=e)
            raise
        self.logger.debug('Completed Timestamp Exploration Pipeline')
        return df_merged
