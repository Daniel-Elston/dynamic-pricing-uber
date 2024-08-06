from __future__ import annotations

import gc

import joblib
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error
from sklearn.metrics import mean_squared_error
from sklearn.metrics import r2_score
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import KFold
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.tree import DecisionTreeRegressor
from xgboost import XGBRegressor


class TrainModel:
    def __init__(self):
        self.models = [
            ('Linear Regression', LinearRegression()),
            ('Decision Tree', DecisionTreeRegressor(random_state=42)),
            # ('Random Forest', RandomForestRegressor(n_estimators=100, random_state=42)),
            # ('SVR', SVR()),
            ('XGBoost', XGBRegressor(random_state=42))
        ]
        self.best_model = None
        self.best_score = float('-inf')
        self.scaler = StandardScaler()

    def preprocess_data(self, df):

        features = [
            'distance', 'is_weekend', 'hour', 'dow_num',
            'price_per_mile', 'price',
            # 'price',
            # 'distance',
            # 'count_per_mile',
            # 'price_per_mile',
            # 'hourly_cpm_mean', 'hourly_cpm_max', 'hourly_cpm_min',
            # 'hourly_cpm_sum',
            # 'avg_hourly_demand',
            # 'avg_day_part_3hr_demand', 'avg_daily_demand',
            # '3h_partly_cpm_max_ratio', '3h_partly_cpm_mean_ratio',
            # '3h_partly_cpm_max_ratio_scaled', '3h_partly_cpm_mean_ratio_scaled',
            # 'is_weekend', 'hour', 'dow_num',
            # 'hourly_ppm_mean', 'hourly_ppm_max', 'hourly_ppm_min',
            '3h_partly_ppm_mean', '3h_partly_ppm_max', '3h_partly_ppm_min',
            '3h_partly_ppm_max_ratio', '3h_partly_ppm_min_ratio',
            # 'day_part_6hr', 'day_part_3hr'
        ]

        df = df.dropna()

        X = df[features]
        # X = pd.get_dummies(df[features], columns=['day_part_6hr'])
        # X = pd.get_dummies(df[features], columns=['day_part_3hr'])
        y = df['count_per_mile']  # 'avg_hourly_demand', 'avg_day_part_demand', 'avg_daily_demand',

        return X, y

    def train_and_evaluate_models(self, X, y):
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)

        results = []
        for name, model in self.models:
            kfold = KFold(n_splits=5, shuffle=True, random_state=42)
            cv_results = cross_val_score(model, X_train_scaled, y_train, cv=kfold, scoring='neg_mean_squared_error')
            cv_results = np.sqrt(-cv_results)  # Convert MSE to RMSE
            results.append((name, cv_results))

            print(f"{name}: RMSE = {cv_results.mean():.4f} (+/- {cv_results.std() * 2:.4f})")

            if cv_results.mean() > self.best_score:
                self.best_score = cv_results.mean()
                self.best_model = model

        self.best_model.fit(X_train_scaled, y_train)
        y_pred = self.best_model.predict(X_test_scaled)

        mse = mean_squared_error(y_test, y_pred)
        mae = mean_absolute_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)

        print(f"\nBest Model: {self.best_model.__class__.__name__}")
        print(f"Mean Squared Error: {mse:.4f}")
        print(f"Mean Absolute Error: {mae:.4f}")
        print(f"R-squared Score: {r2:.4f}")

        return results

    def save_model(self, model_path, scaler_path):
        joblib.dump(self.best_model, model_path)
        joblib.dump(self.scaler, scaler_path)
        print(f"Best model saved to {model_path}")
        print(f"Scaler saved to {scaler_path}")

    def print_feature_importance(self, X):
        if hasattr(self.best_model, 'feature_importances_'):
            feature_importance = pd.DataFrame({'feature': X.columns, 'importance': self.best_model.feature_importances_})
            feature_importance = feature_importance.sort_values('importance', ascending=False)
            print("\nTop 10 Most Important Features:")
            print(feature_importance.head(10))
        else:
            print("\nFeature importance NA.")

    def pipeline(self, df: pd.DataFrame) -> pd.DataFrame:
        X, y = self.preprocess_data(df)
        results = self.train_and_evaluate_models(X, y)
        self.save_model('best_ride_price_model.joblib', 'ride_price_scaler.joblib')
        self.print_feature_importance(X)
        gc.collect()
        return results
