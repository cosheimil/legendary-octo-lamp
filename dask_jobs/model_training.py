"""
ML Pipeline for Fashion E-commerce Sales Prediction.

This module implements machine learning models for predicting sales revenue
using historical Amazon sales data and engineered features.

It uses Dask for distributed machine learning and includes:
- Feature engineering
- Model training and evaluation
- Model persistence to MinIO
- Categorical analysis
"""

import sys
import os

sys.path.insert(0, "/app")

import dask.dataframe as dd
import pandas as pd
import numpy as np
from dask_ml.model_selection import train_test_split
from dask_ml.linear_model import LinearRegression
from dask.distributed import Client
from sklearn.metrics import mean_squared_error, r2_score
from xgboost.dask import DaskXGBRegressor
from sqlalchemy import create_engine
import joblib
from minio import Minio
from datetime import datetime
import json

from config import (
    DB_URL,
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_BUCKET_ML,
    TEMP_PATH,
)
from logger import get_logger

logger = get_logger(__name__)


class MLPipeline:
    """
    Machine Learning Pipeline for sales prediction.

    This class orchestrates the complete ML workflow including
    data loading, feature engineering, model training, and evaluation.
    """

    def __init__(self):
        """Initialize ML pipeline with database and MinIO connections."""
        self.engine = create_engine(DB_URL)
        self.minio_client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False,
        )
        logger.info("✓ ML Pipeline initialized")

    def create_ml_bucket(self):
        """
        Create MinIO bucket for ML models if it doesn't exist.

        Returns:
            bool: True if bucket exists or was created successfully.
        """
        try:
            if not self.minio_client.bucket_exists(MINIO_BUCKET_ML):
                self.minio_client.make_bucket(MINIO_BUCKET_ML)
                logger.info(f"✓ Created MinIO bucket: {MINIO_BUCKET_ML}")
            return True
        except Exception as e:
            logger.error(f"Failed to create ML bucket: {e}")
            return False

    def load_data(self) -> "dd.DataFrame":
        """
        Load Amazon sales data from PostgreSQL.

        Extracts detailed sales transactions with revenue > 0
        for training purposes.

        Returns:
            dd.DataFrame: Dask DataFrame with sales data.
                         Columns: date, sku, category, size, quantity, amount, status, fulfilment, b2b

        Raises:
            sqlalchemy.exc.DatabaseError: If database query fails.
        """
        logger.info("Loading training data from PostgreSQL...")
        try:
            query = """
                SELECT 
                    date,
                    sku,
                    category,
                    size,
                    quantity,
                    amount,
                    status,
                    fulfilment,
                    b2b
                FROM amazon_sales_detail
                WHERE quantity > 0 AND amount > 0
                -- LIMIT 10000
            """

            df = pd.read_sql(query, self.engine)
            logger.info(f"✓ Loaded {len(df)} records from database")

            dask_df = dd.from_pandas(df, npartitions=4)
            return dask_df

        except Exception as e:
            logger.error(f"Failed to load training data: {e}")
            raise

    def feature_engineering(self, df: "dd.DataFrame") -> "dd.DataFrame":
        """
        Perform feature engineering on sales data.

        Creates temporal features (day of week, month) and
        encodes categorical variables for ML model input.

        Args:
            df (dd.DataFrame): Raw sales Dask DataFrame.

        Returns:
            dd.DataFrame: DataFrame with engineered features.
                         New columns:
                         - day_of_week: 0-6 (0=Monday)
                         - day_of_month: 1-31
                         - month: 1-12
                         - is_weekend: 0 or 1
                         - category_encoded: numeric category ID
                         - size_encoded: numeric size ID
                         - b2b_encoded: 0 or 1
                         - fulfilment_encoded: 0 or 1

        Example:
            >>> df_features = pipeline.feature_engineering(df_raw)
            >>> print(df_features.columns)
        """
        logger.info("Performing feature engineering...")
        try:
            df["date"] = dd.to_datetime(df["date"])
            df["day_of_week"] = df["date"].dt.dayofweek
            df["day_of_month"] = df["date"].dt.day
            df["month"] = df["date"].dt.month
            df["is_weekend"] = df["day_of_week"].isin([5, 6]).astype(int)

            logger.debug("✓ Temporal features created")

            category_mapping = {
                "kurta": 1,
                "Set": 2,
                "Western Dress": 3,
                "Top": 4,
                "Blouse": 5,
                "Unknown": 0,
            }

            size_mapping = {
                "S": 1,
                "M": 2,
                "L": 3,
                "XL": 4,
                "XXL": 5,
                "3XL": 6,
                "Free": 7,
                "Unknown": 0,
            }

            df["category_encoded"] = (
                df["category"]
                .map(category_mapping, meta=("category", "int8"))
                .fillna(0)
            )
            df["size_encoded"] = (
                df["size"].map(size_mapping, meta=("size", "int8")).fillna(0)
            )

            df["b2b_encoded"] = df["b2b"].astype(int)
            df["fulfilment_encoded"] = (df["fulfilment"] == "Amazon").astype(int)

            logger.debug("✓ Categorical features encoded")
            logger.info("✓ Feature engineering complete")

            return df

        except Exception as e:
            logger.error(f"Feature engineering failed: {e}")
            raise

    def train_linear_model(self):
        """
        Train linear regression model for sales prediction.

        Uses Dask-ML for distributed training on temporal and categorical features.
        Evaluates model performance on test set and saves to MinIO.

        Model Metrics:
        - Mean Squared Error (MSE): Lower is better
        - R² Score: Closer to 1 is better

        Returns:
            dict: Training results containing metrics and feature names.

        Example:
            >>> results = pipeline.train_model()
            >>> print(f"Test R²: {results['test_r2']}")
        """
        logger.info("=" * 70)
        logger.info("🤖 TRAINING REVENUE PREDICTION MODEL")
        logger.info("=" * 70)

        try:
            df = self.load_data()
            df = self.feature_engineering(df)

            features = [
                "day_of_week",
                "day_of_month",
                "month",
                "is_weekend",
                "category_encoded",
                "size_encoded",
                "b2b_encoded",
                "fulfilment_encoded",
                "quantity",
            ]
            target = "amount"

            df_clean = df[features + [target]].dropna()

            logger.info("Removing constant features...")
            nunique = df_clean[features].nunique().compute()
            non_constant_features = nunique[nunique > 1].index.tolist()

            if len(non_constant_features) == 0:
                raise ValueError("All features are constant! Cannot train model.")

            logger.info(f"Using features: {non_constant_features}")
            logger.info(
                f"Dropped constant features: {sorted(set(features) - set(non_constant_features))}"
            )

            features = non_constant_features

            X = df_clean[features]
            y = df_clean[target]

            X_arr = X.to_dask_array(lengths=True)
            y_arr = y.to_dask_array(lengths=True)

            logger.info(f"Training data: {len(X)} samples, {len(features)} features")

            logger.debug("Splitting data into train/test sets...")
            X_train, X_test, y_train, y_test = train_test_split(
                X_arr, y_arr, test_size=0.2, random_state=42, shuffle=True
            )

            logger.info("⏳ Training Linear Regression model...")
            model = LinearRegression(fit_intercept=True)
            model.fit(X_train, y_train)
            logger.info("✓ Model training complete")

            logger.debug("Evaluating model on train and test sets...")
            y_pred_train = model.predict(X_train)
            y_pred_test = model.predict(X_test)

            train_mse = mean_squared_error(y_train, y_pred_train)
            test_mse = mean_squared_error(y_test, y_pred_test)
            train_r2 = r2_score(y_train, y_pred_train)
            test_r2 = r2_score(y_test, y_pred_test)

            logger.info("📊 Model Metrics:")
            logger.info(f"   Train MSE: {train_mse:.2f}")
            logger.info(f"   Test MSE:  {test_mse:.2f}")
            logger.info(f"   Train R²:  {train_r2:.4f}")
            logger.info(f"   Test R²:   {test_r2:.4f}")

            self.create_ml_bucket()

            model_filename = (
                f"revenue_model_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pkl"
            )
            metrics_filename = (
                f"model_metrics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            )

            model_path = os.path.join(TEMP_PATH, model_filename)
            metrics_path = os.path.join(TEMP_PATH, metrics_filename)

            joblib.dump(model, model_path)
            logger.debug(f"✓ Model saved locally: {model_path}")

            metrics = {
                "train_mse": float(train_mse),
                "test_mse": float(test_mse),
                "train_r2": float(train_r2),
                "test_r2": float(test_r2),
                "features": features,
                "timestamp": datetime.now().isoformat(),
            }

            with open(metrics_path, "w") as f:
                json.dump(metrics, f, indent=2)
            logger.debug(f"✓ Metrics saved locally: {metrics_path}")

            self.minio_client.fput_object(MINIO_BUCKET_ML, model_filename, model_path)
            logger.info(
                f"✓ Model uploaded to MinIO: {MINIO_BUCKET_ML}/{model_filename}"
            )

            self.minio_client.fput_object(
                MINIO_BUCKET_ML, metrics_filename, metrics_path
            )
            logger.info(
                f"✓ Metrics uploaded to MinIO: {MINIO_BUCKET_ML}/{metrics_filename}"
            )

            os.remove(model_path)
            os.remove(metrics_path)

            logger.info("✓ Model training complete and uploaded")
            return metrics

        except Exception as e:
            logger.error(f"Model training failed: {e}", exc_info=True)
            raise

    def train_xgb_model(self):
        """
        Train XGBoost model for sales prediction with improved features.

        Key improvements:
        - Log-transform target (amount)
        - Target encoding for category
        - Removal of constant features
        - Use of Dask-compatible XGBoost
        """
        logger.info("=" * 70)
        logger.info("🤖 TRAINING XGBOOST SALES PREDICTION MODEL")
        logger.info("=" * 70)

        try:
            df = self.load_data()
            df = self.feature_engineering(df)

            logger.info("Applying target encoding for 'category'...")

            category_avg = df.groupby("category")["amount"].mean().compute()
            category_avg_dict = category_avg.fillna(category_avg.mean()).to_dict()
            df["category_target_enc"] = df["category"].map(
                category_avg_dict, meta=("category", "float64")
            )
            logger.debug(
                f"Target encoding applied. Sample: {list(category_avg_dict.items())[:3]}"
            )

            base_features = [
                "day_of_week",
                "day_of_month",
                "month",
                "is_weekend",
                "size_encoded",
                "b2b_encoded",
                "fulfilment_encoded",
                "quantity",
            ]
            features = base_features + ["category_target_enc"]

            df_clean = df[features + ["amount"]].dropna()
            logger.info(f"Data after cleaning: {len(df_clean)} samples")

            logger.info("Checking for constant features...")
            nunique = df_clean[features].nunique().compute()
            non_constant_features = nunique[nunique > 1].index.tolist()

            if len(non_constant_features) == 0:
                raise ValueError("All features are constant!")

            dropped = set(features) - set(non_constant_features)
            if dropped:
                logger.info(f"Dropped constant features: {sorted(dropped)}")
            features = non_constant_features
            logger.info(f"Using features: {features}")

            X = df_clean[features]
            y = df_clean["amount"]

            logger.info("Applying log1p transform to target variable...")
            y = y.apply(lambda x: np.log1p(x), meta=("amount", "float64"))

            logger.debug("Splitting data into train/test sets...")
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=0.2, random_state=42, shuffle=True
            )

            logger.info("Converting to Dask Arrays...")
            X_train = X_train.astype("float64")
            X_test = X_test.astype("float64")
            y_train = y_train.astype("float64")
            y_test = y_test.astype("float64")

            X_train_arr = X_train.to_dask_array(lengths=True)
            X_test_arr = X_test.to_dask_array(lengths=True)
            y_train_arr = y_train.to_dask_array(lengths=True)
            y_test_arr = y_test.to_dask_array(lengths=True)

            logger.info("⏳ Training XGBoost model...")

            try:
                client = Client.current()
            except ValueError:
                client = Client(processes=False, threads_per_worker=2, n_workers=1)
                logger.info("Started local Dask client for XGBoost")

            model = DaskXGBRegressor(
                n_estimators=200,
                max_depth=6,
                learning_rate=0.1,
                subsample=0.8,
                colsample_bytree=0.8,
                random_state=42,
                tree_method="hist",
            )
            model.fit(X_train_arr, y_train_arr)
            logger.info("✓ XGBoost training complete")

            y_pred_train_log = model.predict(X_train_arr)
            y_pred_test_log = model.predict(X_test_arr)

            y_train_actual = y_train_arr.compute()
            y_test_actual = y_test_arr.compute()
            y_pred_train = np.expm1(y_pred_train_log.compute())
            y_pred_test = np.expm1(y_pred_test_log.compute())
            y_train_original = np.expm1(y_train_actual)
            y_test_original = np.expm1(y_test_actual)

            train_mse = mean_squared_error(y_train_original, y_pred_train)
            test_mse = mean_squared_error(y_test_original, y_pred_test)
            train_r2 = r2_score(y_train_original, y_pred_train)
            test_r2 = r2_score(y_test_original, y_pred_test)

            logger.info("📊 Model Metrics (on original scale):")
            logger.info(f"   Train MSE: {train_mse:.2f}")
            logger.info(f"   Test MSE:  {test_mse:.2f}")
            logger.info(f"   Train R²:  {train_r2:.4f}")
            logger.info(f"   Test R²:   {test_r2:.4f}")

            self.create_ml_bucket()
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            model_filename = f"revenue_xgb_model_{timestamp}.pkl"
            metrics_filename = f"model_metrics_{timestamp}.json"
            model_path = os.path.join(TEMP_PATH, model_filename)
            metrics_path = os.path.join(TEMP_PATH, metrics_filename)

            joblib.dump(model, model_path)
            logger.debug(f"✓ Model saved locally: {model_path}")

            metrics = {
                "train_mse": float(train_mse),
                "test_mse": float(test_mse),
                "train_r2": float(train_r2),
                "test_r2": float(test_r2),
                "features": features,
                "model_type": "XGBoost",
                "target_transform": "log1p",
                "timestamp": datetime.now().isoformat(),
            }

            with open(metrics_path, "w") as f:
                json.dump(metrics, f, indent=2)
            logger.debug(f"✓ Metrics saved locally: {metrics_path}")

            self.minio_client.fput_object(MINIO_BUCKET_ML, model_filename, model_path)
            self.minio_client.fput_object(
                MINIO_BUCKET_ML, metrics_filename, metrics_path
            )
            logger.info("✓ Model and metrics uploaded to MinIO")

            os.remove(model_path)
            os.remove(metrics_path)

            logger.info("✅ XGBoost model training complete and uploaded")
            return metrics

        except Exception as e:
            logger.error(f"Model training failed: {e}", exc_info=True)
            raise

    def analyze_categories(self):
        """
        Analyze product category statistics.

        Computes aggregated metrics by category:
        - Order count
        - Total quantity sold
        - Total revenue
        - Average order value

        Results are saved to MinIO for future reference.

        Returns:
            pd.DataFrame: Category statistics.
        """
        logger.info("=" * 70)
        logger.info("📈 ANALYZING PRODUCT CATEGORIES")
        logger.info("=" * 70)

        try:
            query = """
                SELECT 
                    category,
                    COUNT(*) as order_count,
                    SUM(quantity) as total_quantity,
                    SUM(amount) as total_revenue,
                    AVG(amount) as avg_order_value
                FROM amazon_sales_detail
                GROUP BY category
                ORDER BY total_revenue DESC
            """

            category_stats = pd.read_sql(query, self.engine)
            logger.info(f"✓ Analyzed {len(category_stats)} categories")

            logger.info("🏆 Top Categories by Revenue:")
            logger.info("\n" + category_stats.to_string(index=False))

            self.create_ml_bucket()

            stats_filename = f"category_stats_{datetime.now().strftime('%Y%m%d')}.csv"
            stats_path = os.path.join(TEMP_PATH, stats_filename)

            category_stats.to_csv(stats_path, index=False)
            logger.debug(f"✓ Statistics saved locally: {stats_path}")

            self.minio_client.fput_object(MINIO_BUCKET_ML, stats_filename, stats_path)
            logger.info(
                f"✓ Statistics uploaded to MinIO: {MINIO_BUCKET_ML}/{stats_filename}"
            )

            os.remove(stats_path)

            return category_stats

        except Exception as e:
            logger.error(f"Category analysis failed: {e}")
            raise

    def run_full_pipeline(self):
        """
        Execute complete ML pipeline.

        Orchestrates:
        1. Model training with evaluation
        2. Category analysis
        3. Result logging and persistence

        Returns:
            dict: Pipeline execution summary.
        """
        logger.info("=" * 70)
        logger.info("🚀 STARTING COMPLETE ML PIPELINE")
        logger.info("=" * 70)

        try:
            # model_results = self.train_linear_model()
            model_results = self.train_xgb_model()

            category_results = self.analyze_categories()


            logger.info("=" * 70)
            logger.info("✅ ML PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("=" * 70)

            summary = {
                "status": "SUCCESS",
                "model_test_r2": model_results["test_r2"],
                "model_test_mse": model_results["test_mse"],
                "categories_analyzed": len(category_results),
                "timestamp": datetime.now().isoformat(),
            }

            logger.info(f"Pipeline Summary: {summary}")
            return summary

        except Exception as e:
            logger.error(f"❌ ML Pipeline failed: {e}", exc_info=True)
            raise


if __name__ == "__main__":
    logger.info("Starting ML pipeline execution...")

    try:
        pipeline = MLPipeline()
        results = pipeline.run_full_pipeline()
        logger.info("✅ ML pipeline completed successfully")

    except Exception as e:
        logger.error(f"❌ ML pipeline execution failed: {e}")
        sys.exit(1)
