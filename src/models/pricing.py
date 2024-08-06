from __future__ import annotations

import logging
from typing import Dict

import pandas as pd

from config.data import DataConfig
from config.data import DataState
from config.model import ModelConfig
from utils.running import Running


class DynamicPricing:
    def __init__(self, data_state: DataState, data_config: DataConfig, model_config: ModelConfig):
        self.ds = data_state
        self.dc = data_config
        self.runner = Running(self.ds, self.dc)
        self.config = model_config

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        base_profit = df['price'].sum()

        steps = [
            self.apply_dynamic_pricing
        ]
        for step in steps:
            df = self.runner.run_child_step(step, df)

        dynamic_profit = df['dynamic_price'].sum()
        logging.info(f"Base price: {base_profit:.2f}, Dynamic price: {dynamic_profit:.2f}")
        logging.info(f"Price difference: {dynamic_profit - base_profit:.2f}")
        return df

    def calculate_surge_multiplier(self, mean_ratio: float) -> float:
        for (lower, upper), multiplier in self.config.mean_ratio_bins.items():
            if lower <= mean_ratio < upper:
                return multiplier
        return 1.0

    def calculate_base_multiplier(self, max_ratio: float) -> float:
        for (lower, upper), multiplier in self.config.max_ratio_bins.items():
            if lower <= max_ratio < upper:
                return multiplier
        return 1.0

    def calculate_time_multiplier(self, time_period: str) -> float:
        return self.config.day_parts.get(time_period, 1.0)

    def calculate_demand_multiplier(self, row: Dict) -> float:
        surge_multiplier = self.calculate_surge_multiplier(row['3h_partly_cpm_mean_ratio'])
        base_multiplier = self.calculate_base_multiplier(row['3h_partly_cpm_max_ratio'])
        return max(base_multiplier, surge_multiplier)

    def calculate_final_multiplier(self, row: Dict) -> float:
        time_multiplier = self.calculate_time_multiplier(row['day_part_3hr'])
        demand_multiplier = self.calculate_demand_multiplier(row)
        weekend_multiplier = 1.1 if row['is_weekend'] else 1.0

        return (time_multiplier + demand_multiplier + weekend_multiplier) / 3

    def calculate_dynamic_price(self, row: Dict) -> float:
        final_multiplier = self.calculate_final_multiplier(row)
        return row['price'] * final_multiplier

    def apply_dynamic_pricing(self, df: pd.DataFrame) -> pd.DataFrame:
        df['dynamic_price'] = df.apply(self.calculate_dynamic_price, axis=1)
        return df
