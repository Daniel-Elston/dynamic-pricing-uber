from __future__ import annotations

import io
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from pprint import pprint
from typing import Dict
from typing import List
from typing import Optional

import pandas as pd


@dataclass
class ColumnMetadata:
    name: str
    dtype: str
    description: str
    missing_percentage: float = 0.0
    unique_values: int = 0
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    mean_value: Optional[float] = None
    median_value: Optional[float] = None
    mode_value: Optional[str] = None

    def __post_init__(self):
        if self.dtype in ['int64', 'float64']:
            self.min_value = float(self.min_value) if self.min_value is not None else None
            self.max_value = float(self.max_value) if self.max_value is not None else None
            self.mean_value = float(self.mean_value) if self.mean_value is not None else None
            self.median_value = float(self.median_value) if self.median_value is not None else None


@dataclass
class DatasetMetadata:
    name: str
    description: str
    creation_date: datetime
    last_updated: datetime
    num_rows: int
    num_columns: int
    columns: List[ColumnMetadata]
    target_variable: str
    features: List[str]
    time_variable: str
    geographical_variables: List[str]
    categorical_variables: List[str]
    numerical_variables: List[str]
    id_variable: str
    tags: List[str] = field(default_factory=list)
    additional_info: Dict[str, str] = field(default_factory=dict)
    df_info: str = field(default="")
    df_describe: Dict[str, Dict[str, float]] = field(default_factory=dict)

    def get_column(self, column_name: str) -> Optional[ColumnMetadata]:
        return next((col for col in self.columns if col.name == column_name), None)

    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "description": self.description,
            "creation_date": self.creation_date.isoformat(),
            "last_updated": self.last_updated.isoformat(),
            "num_rows": self.num_rows,
            "num_columns": self.num_columns,
            "columns": [vars(col) for col in self.columns],
            "target_variable": self.target_variable,
            "features": self.features,
            "time_variable": self.time_variable,
            "geographical_variables": self.geographical_variables,
            "categorical_variables": self.categorical_variables,
            "numerical_variables": self.numerical_variables,
            "id_variable": self.id_variable,
            "tags": self.tags,
            "additional_info": self.additional_info,
            "df_info": self.df_info,
            "df_describe": self.df_describe
        }


def capture_df_info(df: pd.DataFrame) -> str:
    buffer = io.StringIO()
    df.info(buf=buffer)
    return buffer.getvalue()


def capture_df_describe(df: pd.DataFrame) -> Dict[str, Dict[str, float]]:
    describe_df = df.describe(include='all')
    return {col: describe_df[col].to_dict() for col in describe_df.columns}


def create_dataset_metadata(df: pd.DataFrame) -> DatasetMetadata:
    columns = []
    for col in df.columns:
        col_data = df[col]
        missing_percentage = (col_data.isna().sum() / len(df)) * 100
        unique_values = col_data.nunique()

        column_metadata = ColumnMetadata(
            name=col,
            dtype=str(col_data.dtype),
            description=f"Description for {col}",
            missing_percentage=missing_percentage,
            unique_values=unique_values,
            min_value=col_data.min() if col_data.dtype in ['int64', 'float64'] else None,
            max_value=col_data.max() if col_data.dtype in ['int64', 'float64'] else None,
            mean_value=col_data.mean() if col_data.dtype in ['int64', 'float64'] else None,
            median_value=col_data.median() if col_data.dtype in ['int64', 'float64'] else None,
            mode_value=col_data.mode().iloc[0] if col_data.dtype == 'object' else None
        )
        columns.append(column_metadata)

    return DatasetMetadata(
        name="Uber Dynamic Pricing Dataset",
        description="Dataset containing Uber ride information for dynamic pricing analysis",
        creation_date=datetime.now(),
        last_updated=datetime.now(),
        num_rows=len(df),
        num_columns=len(df.columns),
        columns=columns,
        target_variable="price",
        features=["pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude", "count"],
        time_variable="timestamp",
        geographical_variables=["pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude"],
        categorical_variables=[],
        numerical_variables=["price", "count"],
        id_variable="uid",
        tags=["uber", "dynamic pricing", "ride-sharing"],
        additional_info={"data_source": "Uber API", "currency": "USD"},
        df_info=capture_df_info(df),
        df_describe=capture_df_describe(df)
    )


if __name__ == "__main__":
    df = pd.read_parquet('data/sdo/uber.parquet')
    uber_metadata = create_dataset_metadata(df)
    pprint(uber_metadata)
