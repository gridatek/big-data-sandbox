#!/usr/bin/env python3
"""
Machine Learning Pipeline Example
End-to-end ML workflow with feature engineering, model training, and evaluation
"""

import sys
import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer, OneHotEncoder
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression
from pyspark.ml.regression import RandomForestRegressor, LinearRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator, RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.stat import Correlation
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MLPipeline:
    def __init__(self, app_name="ML_Pipeline"):
        """Initialize Spark session with ML libraries"""
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .master("spark://spark-master:7077") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")
        logger.info(f"ML Pipeline session initialized: {self.spark.version}")

    def load_data(self, data_path):
        """Load and prepare data for ML"""
        logger.info(f"Loading data from: {data_path}")

        try:
            if data_path.endswith('.csv'):
                df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(data_path)
            else:
                df = self.spark.read.parquet(data_path)

            logger.info(f"Data loaded successfully: {df.count()} records, {len(df.columns)} features")
            return df

        except Exception as e:
            logger.error(f"Failed to load data: {str(e)}")
            raise

    def customer_churn_features(self, df):
        """Create features for customer churn prediction"""
        logger.info("Engineering features for churn prediction...")

        # Calculate customer metrics
        customer_features = df.groupBy("customer_id") \
            .agg(
                sum("total_amount").alias("total_spent"),
                count("transaction_id").alias("transaction_count"),
                avg("total_amount").alias("avg_transaction_value"),
                countDistinct("product_name").alias("product_diversity"),
                max("date").alias("last_purchase_date"),
                min("date").alias("first_purchase_date"),
                countDistinct("category").alias("category_diversity")
            )

        # Calculate recency and frequency metrics
        analysis_date = df.agg(max("date")).collect()[0][0]

        customer_features = customer_features \
            .withColumn("days_since_last_purchase",
                       datediff(lit(analysis_date), col("last_purchase_date"))) \
            .withColumn("customer_lifetime_days",
                       datediff("last_purchase_date", "first_purchase_date") + 1) \
            .withColumn("purchase_frequency",
                       col("transaction_count") / col("customer_lifetime_days"))

        # Create churn label (customers who haven't purchased in 30+ days)
        customer_features = customer_features \
            .withColumn("is_churned",
                       when(col("days_since_last_purchase") > 30, 1).otherwise(0))

        # Add customer value segments
        customer_features = customer_features \
            .withColumn("customer_value_segment",
                       when(col("total_spent") > 1000, "High")
                       .when(col("total_spent") > 500, "Medium")
                       .otherwise("Low"))

        logger.info("✅ Customer churn features created")
        return customer_features

    def product_recommendation_features(self, df):
        """Create features for product recommendation model"""
        logger.info("Engineering features for product recommendations...")

        # Customer-product interaction matrix
        customer_product = df.groupBy("customer_id", "product_name") \
            .agg(
                sum("quantity").alias("total_quantity"),
                sum("total_amount").alias("total_spent_on_product"),
                count("transaction_id").alias("purchase_frequency")
            )

        # Product popularity features
        product_stats = df.groupBy("product_name") \
            .agg(
                countDistinct("customer_id").alias("product_popularity"),
                avg("price").alias("avg_product_price"),
                sum("quantity").alias("total_product_sales")
            )

        # Customer preferences
        customer_preferences = df.groupBy("customer_id") \
            .agg(
                avg("price").alias("avg_price_preference"),
                collect_list("category").alias("preferred_categories"),
                collect_list("product_name").alias("purchased_products")
            )

        logger.info("✅ Product recommendation features created")
        return customer_product, product_stats, customer_preferences

    def price_prediction_features(self, df):
        """Create features for price prediction model"""
        logger.info("Engineering features for price prediction...")

        # Add time-based features
        df_features = df \
            .withColumn("transaction_date", to_date(col("date"), "yyyy-MM-dd")) \
            .withColumn("year", year("transaction_date")) \
            .withColumn("month", month("transaction_date")) \
            .withColumn("day_of_week", dayofweek("transaction_date")) \
            .withColumn("is_weekend", col("day_of_week").isin([1, 7]).cast("int"))

        # Category-based features
        category_stats = df.groupBy("category") \
            .agg(avg("price").alias("category_avg_price"))

        df_features = df_features.join(category_stats, "category") \
            .withColumn("price_vs_category_avg",
                       col("price") / col("category_avg_price"))

        # Product popularity features
        product_popularity = df.groupBy("product_name") \
            .agg(count("*").alias("product_sales_count"))

        df_features = df_features.join(product_popularity, "product_name")

        logger.info("✅ Price prediction features created")
        return df_features

    def build_churn_model(self, df):
        """Build customer churn prediction model"""
        logger.info("Building churn prediction model...")

        # Prepare features
        feature_cols = ["total_spent", "transaction_count", "avg_transaction_value",
                       "product_diversity", "days_since_last_purchase",
                       "customer_lifetime_days", "purchase_frequency", "category_diversity"]

        # Handle missing values
        for col_name in feature_cols:
            df = df.fillna(0, subset=[col_name])

        # Create feature vector
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

        # Scale features
        scaler = StandardScaler(inputCol="features", outputCol="scaled_features")

        # Create model
        rf = RandomForestClassifier(
            featuresCol="scaled_features",
            labelCol="is_churned",
            predictionCol="prediction",
            probabilityCol="probability",
            numTrees=100,
            maxDepth=10,
            seed=42
        )

        # Create pipeline
        pipeline = Pipeline(stages=[assembler, scaler, rf])

        # Split data
        train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

        logger.info(f"Training set: {train_df.count()} records")
        logger.info(f"Test set: {test_df.count()} records")

        # Train model
        model = pipeline.fit(train_df)

        # Make predictions
        predictions = model.transform(test_df)

        # Evaluate model
        evaluator = BinaryClassificationEvaluator(
            labelCol="is_churned",
            rawPredictionCol="rawPrediction",
            metricName="areaUnderROC"
        )

        auc = evaluator.evaluate(predictions)

        # Additional metrics
        accuracy_evaluator = MulticlassClassificationEvaluator(
            labelCol="is_churned",
            predictionCol="prediction",
            metricName="accuracy"
        )
        accuracy = accuracy_evaluator.evaluate(predictions)

        logger.info(f"✅ Churn model trained - AUC: {auc:.3f}, Accuracy: {accuracy:.3f}")

        return model, predictions, {"auc": auc, "accuracy": accuracy}

    def build_price_prediction_model(self, df):
        """Build price prediction model"""
        logger.info("Building price prediction model...")

        # Encode categorical features
        category_indexer = StringIndexer(inputCol="category", outputCol="category_index")
        category_encoder = OneHotEncoder(inputCol="category_index", outputCol="category_vector")

        product_indexer = StringIndexer(inputCol="product_name", outputCol="product_index")
        product_encoder = OneHotEncoder(inputCol="product_index", outputCol="product_vector")

        region_indexer = StringIndexer(inputCol="region", outputCol="region_index")
        region_encoder = OneHotEncoder(inputCol="region_index", outputCol="region_vector")

        # Prepare numeric features
        numeric_cols = ["quantity", "year", "month", "day_of_week", "is_weekend",
                       "price_vs_category_avg", "product_sales_count"]

        # Create feature vector
        assembler = VectorAssembler(
            inputCols=numeric_cols + ["category_vector", "product_vector", "region_vector"],
            outputCol="features"
        )

        # Scale features
        scaler = StandardScaler(inputCol="features", outputCol="scaled_features")

        # Create regression model
        lr = LinearRegression(
            featuresCol="scaled_features",
            labelCol="price",
            predictionCol="predicted_price"
        )

        # Create pipeline
        pipeline = Pipeline(stages=[
            category_indexer, category_encoder,
            product_indexer, product_encoder,
            region_indexer, region_encoder,
            assembler, scaler, lr
        ])

        # Split data
        train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

        # Train model
        model = pipeline.fit(train_df)

        # Make predictions
        predictions = model.transform(test_df)

        # Evaluate model
        evaluator = RegressionEvaluator(
            labelCol="price",
            predictionCol="predicted_price",
            metricName="rmse"
        )

        rmse = evaluator.evaluate(predictions)

        # Additional metrics
        r2_evaluator = RegressionEvaluator(
            labelCol="price",
            predictionCol="predicted_price",
            metricName="r2"
        )
        r2 = r2_evaluator.evaluate(predictions)

        logger.info(f"✅ Price prediction model trained - RMSE: {rmse:.2f}, R²: {r2:.3f}")

        return model, predictions, {"rmse": rmse, "r2": r2}

    def feature_importance_analysis(self, model, feature_names):
        """Analyze feature importance from trained models"""
        logger.info("Analyzing feature importance...")

        try:
            # Extract Random Forest model from pipeline
            rf_model = None
            for stage in model.stages:
                if hasattr(stage, 'featureImportances'):
                    rf_model = stage
                    break

            if rf_model and hasattr(rf_model, 'featureImportances'):
                importances = rf_model.featureImportances.toArray()

                # Create feature importance DataFrame
                importance_data = list(zip(feature_names, importances))
                importance_df = self.spark.createDataFrame(importance_data, ["feature", "importance"]) \
                    .orderBy(col("importance").desc())

                logger.info("🔍 Top 10 Most Important Features:")
                importance_df.show(10, truncate=False)

                return importance_df

        except Exception as e:
            logger.warning(f"Could not extract feature importance: {e}")
            return None

    def model_validation(self, model, df, target_col, cv_folds=3):
        """Perform cross-validation on model"""
        logger.info(f"Performing {cv_folds}-fold cross-validation...")

        try:
            # Define evaluator based on problem type
            if target_col == "is_churned":
                evaluator = BinaryClassificationEvaluator(
                    labelCol=target_col,
                    rawPredictionCol="rawPrediction",
                    metricName="areaUnderROC"
                )
            else:
                evaluator = RegressionEvaluator(
                    labelCol=target_col,
                    predictionCol="prediction",
                    metricName="rmse"
                )

            # Create cross-validator
            cv = CrossValidator(
                estimator=model,
                estimatorParamMaps=[{}],  # No parameter tuning, just validation
                evaluator=evaluator,
                numFolds=cv_folds,
                seed=42
            )

            # Run cross-validation
            cv_model = cv.fit(df)
            avg_metric = cv_model.avgMetrics[0]

            logger.info(f"✅ Cross-validation completed - Average metric: {avg_metric:.3f}")
            return avg_metric

        except Exception as e:
            logger.error(f"Cross-validation failed: {e}")
            return None

    def save_model(self, model, model_path):
        """Save trained model to storage"""
        logger.info(f"Saving model to: {model_path}")

        try:
            model.write().overwrite().save(model_path)
            logger.info("✅ Model saved successfully")

        except Exception as e:
            logger.error(f"Failed to save model: {e}")
            raise

    def run_ml_pipeline(self, data_path, output_path, model_type="churn"):
        """Run complete ML pipeline"""
        logger.info("🚀 Starting ML Pipeline")
        start_time = datetime.now()

        try:
            # Load data
            df = self.load_data(data_path)

            if model_type == "churn":
                # Customer churn prediction
                features_df = self.customer_churn_features(df)
                model, predictions, metrics = self.build_churn_model(features_df)
                target_col = "is_churned"

            elif model_type == "price":
                # Price prediction
                features_df = self.price_prediction_features(df)
                model, predictions, metrics = self.build_price_prediction_model(features_df)
                target_col = "price"

            else:
                raise ValueError(f"Unsupported model type: {model_type}")

            # Validate model with cross-validation
            cv_score = self.model_validation(model, features_df, target_col)

            # Save model and predictions
            self.save_model(model, f"{output_path}/model_{model_type}")
            predictions.write.mode("overwrite").parquet(f"{output_path}/predictions_{model_type}")

            # Calculate runtime
            end_time = datetime.now()
            runtime = (end_time - start_time).total_seconds()

            logger.info(f"🎉 ML Pipeline completed in {runtime:.2f} seconds")

            # Print results
            print("\n" + "="*70)
            print("🤖 MACHINE LEARNING PIPELINE RESULTS")
            print("="*70)
            print(f"📊 Model Type: {model_type.title()} Prediction")
            print(f"📁 Data Source: {data_path}")
            print(f"💾 Output Path: {output_path}")
            print(f"⏱️  Training Time: {runtime:.2f} seconds")
            print(f"📈 Model Metrics:")
            for metric, value in metrics.items():
                print(f"   • {metric.upper()}: {value:.3f}")
            if cv_score:
                print(f"   • Cross-validation Score: {cv_score:.3f}")
            print("="*70)

            return {
                'status': 'success',
                'model_type': model_type,
                'metrics': metrics,
                'cv_score': cv_score,
                'runtime_seconds': runtime,
                'output_path': output_path
            }

        except Exception as e:
            logger.error(f"❌ ML Pipeline failed: {str(e)}")
            return {
                'status': 'failed',
                'error': str(e)
            }

    def cleanup(self):
        """Clean up resources"""
        if hasattr(self, 'spark'):
            self.spark.stop()
            logger.info("Spark session stopped")

def main():
    """Main function for command-line execution"""
    parser = argparse.ArgumentParser(description='ML Pipeline for Big Data Sandbox')
    parser.add_argument('--data', required=True, help='Input data path')
    parser.add_argument('--output', required=True, help='Output path for model and results')
    parser.add_argument('--model-type', choices=['churn', 'price'], default='churn',
                       help='Type of model to train')

    args = parser.parse_args()

    pipeline = MLPipeline()

    try:
        result = pipeline.run_ml_pipeline(args.data, args.output, args.model_type)

        if result['status'] == 'success':
            sys.exit(0)
        else:
            sys.exit(1)

    finally:
        pipeline.cleanup()

if __name__ == "__main__":
    # Example usage when run directly
    pipeline = MLPipeline()

    try:
        print("🤖 Starting ML Pipeline Demo")
        print("📊 Training customer churn prediction model...")

        result = pipeline.run_ml_pipeline(
            data_path="/data/sales_data.csv",
            output_path="s3a://processed/ml_models",
            model_type="churn"
        )

        print(f"\n🎯 ML Pipeline result: {result['status']}")

    except Exception as e:
        print(f"❌ ML Pipeline failed: {e}")
    finally:
        pipeline.cleanup()