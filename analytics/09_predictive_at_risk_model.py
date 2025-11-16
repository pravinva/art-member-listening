"""
Predictive At-Risk Member Scoring Model
========================================
ML model to predict member churn/dissatisfaction using historical patterns.

Features:
- Gradient Boosted Trees classifier
- Feature engineering from Member 360 view
- SHAP analysis for explainability
- Daily batch scoring
- Model performance tracking

Usage:
    from analytics.predictive_at_risk_model import AtRiskModel

    model = AtRiskModel()

    # Train model on historical data
    model.train()

    # Score all current members
    model.score_members()

    # Analyze feature importance
    model.analyze_drivers()
"""

from pyspark.sql import SparkSession, functions as F, Window
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.classification import GBTClassifier, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
from datetime import datetime, timedelta
import mlflow
import mlflow.spark


class AtRiskModel:
    """Predictive model for identifying at-risk members"""

    def __init__(self):
        self.spark = SparkSession.builder.getOrCreate()
        self.catalog = "art_member_listening"
        self.model = None
        self.feature_cols = []

        # MLflow tracking
        mlflow.set_experiment("/ART/member_at_risk_prediction")

    def prepare_training_data(self, lookback_days=365):
        """
        Create labeled training dataset from historical data

        Label definition (at_risk = 1):
        - Member churned within 90 days
        - Filed formal complaint
        - NPS score < 6
        - Account became inactive
        """

        print(f"📊 Preparing training data (last {lookback_days} days)...")

        query = f"""
        WITH member_features AS (
            SELECT
                member_id,
                DATE(interaction_date) as feature_date,

                -- === Engagement Features ===
                DATEDIFF(CURRENT_DATE(), MAX(login_date)) as days_since_last_login,
                COUNT(DISTINCT CASE WHEN login_date >= DATE_SUB(feature_date, 30) THEN login_date END) as login_frequency_30d,
                SUM(CASE WHEN portal_activity_date >= DATE_SUB(feature_date, 30) THEN page_views ELSE 0 END) as page_views_30d,

                -- === Sentiment Features ===
                AVG(CASE WHEN interaction_date >= DATE_SUB(feature_date, 30) THEN sentiment_score END) as avg_sentiment_30d,
                AVG(CASE WHEN interaction_date >= DATE_SUB(feature_date, 7) THEN sentiment_score END) as avg_sentiment_7d,
                COUNT(CASE WHEN interaction_date >= DATE_SUB(feature_date, 30) AND sentiment_label = 'negative' THEN 1 END) as negative_interaction_count_30d,

                -- Sentiment trend (30d avg vs 90d avg)
                AVG(CASE WHEN interaction_date >= DATE_SUB(feature_date, 30) THEN sentiment_score END) -
                AVG(CASE WHEN interaction_date >= DATE_SUB(feature_date, 90) THEN sentiment_score END) as sentiment_trend_30d,

                -- === Contact Behavior ===
                COUNT(CASE WHEN interaction_date >= DATE_SUB(feature_date, 30) THEN 1 END) as contact_frequency_30d,
                COUNT(CASE WHEN interaction_date >= DATE_SUB(feature_date, 7) THEN 1 END) as contact_frequency_7d,

                -- Repeat contact on same issue (topic)
                COUNT(DISTINCT CASE WHEN interaction_date >= DATE_SUB(feature_date, 30) THEN primary_topic END) /
                NULLIF(COUNT(CASE WHEN interaction_date >= DATE_SUB(feature_date, 30) THEN 1 END), 0) as topic_diversity_30d,

                -- Channel diversity (using multiple channels = frustration?)
                COUNT(DISTINCT CASE WHEN interaction_date >= DATE_SUB(feature_date, 30) THEN channel END) as channel_diversity_30d,

                -- === Topic Features ===
                COUNT(CASE WHEN interaction_date >= DATE_SUB(feature_date, 30) AND primary_topic = 'Insurance' THEN 1 END) as insurance_topic_count_30d,
                COUNT(CASE WHEN interaction_date >= DATE_SUB(feature_date, 30) AND primary_topic = 'Contribution' THEN 1 END) as contribution_topic_count_30d,
                COUNT(CASE WHEN interaction_date >= DATE_SUB(feature_date, 30) AND primary_topic = 'Technical' THEN 1 END) as technical_topic_count_30d,
                COUNT(CASE WHEN interaction_date >= DATE_SUB(feature_date, 30) AND primary_topic = 'Claims' THEN 1 END) as claims_topic_count_30d,

                -- === Urgency ===
                AVG(CASE WHEN interaction_date >= DATE_SUB(feature_date, 30) THEN urgency_score END) as avg_urgency_30d,

                -- === Member Attributes ===
                MAX(member_tier) as member_tier,
                MAX(account_balance) as account_balance,
                MAX(years_as_member) as years_as_member,
                MAX(age_group) as age_group,
                MAX(employment_status) as employment_status,

                -- === Outcome Labels (for training) ===
                -- Look ahead 90 days to see if member became at-risk
                MAX(CASE
                    WHEN interaction_date BETWEEN feature_date AND DATE_ADD(feature_date, 90)
                         AND (churned = 1 OR complaint_filed = 1 OR nps_score < 6)
                    THEN 1
                    ELSE 0
                END) as at_risk_label

            FROM {self.catalog}.silver.interactions_analyzed i
            LEFT JOIN {self.catalog}.gold.member_360_view m ON i.member_id = m.member_id
            WHERE interaction_date >= DATE_SUB(CURRENT_DATE(), {lookback_days})
              AND interaction_date <= DATE_SUB(CURRENT_DATE(), 90)  -- Need 90 days look-ahead for labels
            GROUP BY member_id, DATE(interaction_date)
        )
        SELECT
            *,
            -- Derived features
            CASE WHEN days_since_last_login > 30 THEN 1 ELSE 0 END as inactive_flag,
            CASE WHEN contact_frequency_7d >= 3 THEN 1 ELSE 0 END as frequent_contact_flag,
            CASE WHEN channel_diversity_30d >= 3 THEN 1 ELSE 0 END as multichannel_flag
        FROM member_features
        WHERE at_risk_label IS NOT NULL  -- Only records with labels
        """

        df = self.spark.sql(query)

        # Filter out nulls and handle missing values
        df = df.fillna({
            'days_since_last_login': 999,
            'login_frequency_30d': 0,
            'page_views_30d': 0,
            'avg_sentiment_30d': 0,
            'avg_sentiment_7d': 0,
            'negative_interaction_count_30d': 0,
            'sentiment_trend_30d': 0,
            'contact_frequency_30d': 0,
            'contact_frequency_7d': 0,
            'topic_diversity_30d': 1,
            'channel_diversity_30d': 1,
            'insurance_topic_count_30d': 0,
            'contribution_topic_count_30d': 0,
            'technical_topic_count_30d': 0,
            'claims_topic_count_30d': 0,
            'avg_urgency_30d': 0,
            'account_balance': 0,
            'years_as_member': 0
        })

        total_count = df.count()
        positive_count = df.filter(F.col("at_risk_label") == 1).count()
        positive_rate = positive_count / total_count if total_count > 0 else 0

        print(f"✅ Training data prepared:")
        print(f"   Total samples: {total_count:,}")
        print(f"   At-risk (positive): {positive_count:,} ({positive_rate:.1%})")
        print(f"   Not at-risk (negative): {total_count - positive_count:,}")

        return df

    def train(self, test_split=0.2):
        """Train the at-risk prediction model"""

        print("🤖 Training at-risk prediction model...")

        # Start MLflow run
        with mlflow.start_run(run_name=f"at_risk_model_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):

            # Prepare data
            training_df = self.prepare_training_data()

            # Define feature columns
            numeric_features = [
                'days_since_last_login', 'login_frequency_30d', 'page_views_30d',
                'avg_sentiment_30d', 'avg_sentiment_7d', 'negative_interaction_count_30d',
                'sentiment_trend_30d', 'contact_frequency_30d', 'contact_frequency_7d',
                'topic_diversity_30d', 'channel_diversity_30d',
                'insurance_topic_count_30d', 'contribution_topic_count_30d',
                'technical_topic_count_30d', 'claims_topic_count_30d',
                'avg_urgency_30d', 'account_balance', 'years_as_member',
                'inactive_flag', 'frequent_contact_flag', 'multichannel_flag'
            ]

            categorical_features = ['member_tier', 'age_group', 'employment_status']

            # Index categorical features
            indexers = [
                StringIndexer(inputCol=col, outputCol=f"{col}_indexed", handleInvalid="keep")
                for col in categorical_features
            ]

            # Assemble features
            all_feature_cols = numeric_features + [f"{col}_indexed" for col in categorical_features]
            self.feature_cols = all_feature_cols

            assembler = VectorAssembler(
                inputCols=all_feature_cols,
                outputCol="features_raw",
                handleInvalid="skip"
            )

            # Scale features
            scaler = StandardScaler(
                inputCol="features_raw",
                outputCol="features",
                withStd=True,
                withMean=True
            )

            # Gradient Boosted Trees Classifier
            gbt = GBTClassifier(
                featuresCol="features",
                labelCol="at_risk_label",
                maxDepth=6,
                maxIter=50,
                stepSize=0.1,
                seed=42
            )

            # Create pipeline
            pipeline = Pipeline(stages=indexers + [assembler, scaler, gbt])

            # Split data
            train_df, test_df = training_df.randomSplit([1 - test_split, test_split], seed=42)

            print(f"   Train samples: {train_df.count():,}")
            print(f"   Test samples: {test_df.count():,}")

            # Train model
            print("   Training GBT classifier...")
            model = pipeline.fit(train_df)
            self.model = model

            # Evaluate
            predictions = model.transform(test_df)

            # Metrics
            auc_evaluator = BinaryClassificationEvaluator(
                labelCol="at_risk_label",
                rawPredictionCol="rawPrediction",
                metricName="areaUnderROC"
            )

            accuracy_evaluator = MulticlassClassificationEvaluator(
                labelCol="at_risk_label",
                predictionCol="prediction",
                metricName="accuracy"
            )

            precision_evaluator = MulticlassClassificationEvaluator(
                labelCol="at_risk_label",
                predictionCol="prediction",
                metricName="weightedPrecision"
            )

            recall_evaluator = MulticlassClassificationEvaluator(
                labelCol="at_risk_label",
                predictionCol="prediction",
                metricName="weightedRecall"
            )

            auc = auc_evaluator.evaluate(predictions)
            accuracy = accuracy_evaluator.evaluate(predictions)
            precision = precision_evaluator.evaluate(predictions)
            recall = recall_evaluator.evaluate(predictions)

            print(f"\n📊 Model Performance:")
            print(f"   AUC: {auc:.3f}")
            print(f"   Accuracy: {accuracy:.3f}")
            print(f"   Precision: {precision:.3f}")
            print(f"   Recall: {recall:.3f}")

            # Log metrics to MLflow
            mlflow.log_metric("auc", auc)
            mlflow.log_metric("accuracy", accuracy)
            mlflow.log_metric("precision", precision)
            mlflow.log_metric("recall", recall)

            # Feature importance
            gbt_model = model.stages[-1]
            feature_importance = gbt_model.featureImportances.toArray()

            print(f"\n🔍 Top 10 Most Important Features:")
            importance_list = list(zip(all_feature_cols, feature_importance))
            importance_list.sort(key=lambda x: x[1], reverse=True)

            for i, (feature, importance) in enumerate(importance_list[:10], 1):
                print(f"   {i}. {feature}: {importance:.4f}")

            # Save model
            model_path = f"/tmp/at_risk_model_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            model.write().overwrite().save(model_path)
            mlflow.spark.log_model(model, "model")

            print(f"\n✅ Model training complete and saved to {model_path}")

            return model

    def score_members(self, model=None):
        """Score all active members with at-risk probability"""

        if model is None and self.model is None:
            raise ValueError("No model available. Train a model first.")

        model = model or self.model

        print("🎯 Scoring all active members...")

        # Prepare current member data (same features, no label)
        query = f"""
        SELECT
            member_id,
            CURRENT_DATE() as score_date,

            -- Same features as training
            DATEDIFF(CURRENT_DATE(), MAX(login_date)) as days_since_last_login,
            COUNT(DISTINCT CASE WHEN login_date >= DATE_SUB(CURRENT_DATE(), 30) THEN login_date END) as login_frequency_30d,
            SUM(CASE WHEN portal_activity_date >= DATE_SUB(CURRENT_DATE(), 30) THEN page_views ELSE 0 END) as page_views_30d,

            AVG(CASE WHEN interaction_date >= DATE_SUB(CURRENT_DATE(), 30) THEN sentiment_score END) as avg_sentiment_30d,
            AVG(CASE WHEN interaction_date >= DATE_SUB(CURRENT_DATE(), 7) THEN sentiment_score END) as avg_sentiment_7d,
            COUNT(CASE WHEN interaction_date >= DATE_SUB(CURRENT_DATE(), 30) AND sentiment_label = 'negative' THEN 1 END) as negative_interaction_count_30d,

            AVG(CASE WHEN interaction_date >= DATE_SUB(CURRENT_DATE(), 30) THEN sentiment_score END) -
            AVG(CASE WHEN interaction_date >= DATE_SUB(CURRENT_DATE(), 90) THEN sentiment_score END) as sentiment_trend_30d,

            COUNT(CASE WHEN interaction_date >= DATE_SUB(CURRENT_DATE(), 30) THEN 1 END) as contact_frequency_30d,
            COUNT(CASE WHEN interaction_date >= DATE_SUB(CURRENT_DATE(), 7) THEN 1 END) as contact_frequency_7d,

            COUNT(DISTINCT CASE WHEN interaction_date >= DATE_SUB(CURRENT_DATE(), 30) THEN primary_topic END) /
            NULLIF(COUNT(CASE WHEN interaction_date >= DATE_SUB(CURRENT_DATE(), 30) THEN 1 END), 0) as topic_diversity_30d,

            COUNT(DISTINCT CASE WHEN interaction_date >= DATE_SUB(CURRENT_DATE(), 30) THEN channel END) as channel_diversity_30d,

            COUNT(CASE WHEN interaction_date >= DATE_SUB(CURRENT_DATE(), 30) AND primary_topic = 'Insurance' THEN 1 END) as insurance_topic_count_30d,
            COUNT(CASE WHEN interaction_date >= DATE_SUB(CURRENT_DATE(), 30) AND primary_topic = 'Contribution' THEN 1 END) as contribution_topic_count_30d,
            COUNT(CASE WHEN interaction_date >= DATE_SUB(CURRENT_DATE(), 30) AND primary_topic = 'Technical' THEN 1 END) as technical_topic_count_30d,
            COUNT(CASE WHEN interaction_date >= DATE_SUB(CURRENT_DATE(), 30) AND primary_topic = 'Claims' THEN 1 END) as claims_topic_count_30d,

            AVG(CASE WHEN interaction_date >= DATE_SUB(CURRENT_DATE(), 30) THEN urgency_score END) as avg_urgency_30d,

            MAX(member_tier) as member_tier,
            MAX(account_balance) as account_balance,
            MAX(years_as_member) as years_as_member,
            MAX(age_group) as age_group,
            MAX(employment_status) as employment_status

        FROM {self.catalog}.silver.interactions_analyzed i
        LEFT JOIN {self.catalog}.gold.member_360_view m ON i.member_id = m.member_id
        WHERE member_status = 'Active'
        GROUP BY member_id
        """

        current_members = self.spark.sql(query)

        # Fill nulls
        current_members = current_members.fillna({
            'days_since_last_login': 999,
            'login_frequency_30d': 0,
            'page_views_30d': 0,
            'avg_sentiment_30d': 0,
            'avg_sentiment_7d': 0,
            'negative_interaction_count_30d': 0,
            'sentiment_trend_30d': 0,
            'contact_frequency_30d': 0,
            'contact_frequency_7d': 0,
            'topic_diversity_30d': 1,
            'channel_diversity_30d': 1,
            'insurance_topic_count_30d': 0,
            'contribution_topic_count_30d': 0,
            'technical_topic_count_30d': 0,
            'claims_topic_count_30d': 0,
            'avg_urgency_30d': 0,
            'account_balance': 0,
            'years_as_member': 0
        })

        # Add derived features
        current_members = current_members.withColumn(
            "inactive_flag",
            F.when(F.col("days_since_last_login") > 30, 1).otherwise(0)
        ).withColumn(
            "frequent_contact_flag",
            F.when(F.col("contact_frequency_7d") >= 3, 1).otherwise(0)
        ).withColumn(
            "multichannel_flag",
            F.when(F.col("channel_diversity_30d") >= 3, 1).otherwise(0)
        )

        # Score
        predictions = model.transform(current_members)

        # Extract probability
        from pyspark.sql.functions import udf
        from pyspark.sql.types import DoubleType

        def extract_prob(probability_vector):
            return float(probability_vector[1]) if probability_vector else 0.0

        extract_prob_udf = udf(extract_prob, DoubleType())

        predictions = predictions.withColumn(
            "at_risk_probability",
            extract_prob_udf(F.col("probability"))
        )

        # Write to Gold table
        output_df = predictions.select(
            "member_id",
            "at_risk_probability",
            F.lit("ML_Model").alias("scoring_method"),
            F.current_timestamp().alias("scored_at"),
            F.lit(datetime.now().strftime('%Y%m%d')).alias("model_version")
        )

        output_df.write.mode("overwrite").saveAsTable(f"{self.catalog}.gold.member_at_risk_scores_ml")

        high_risk_count = output_df.filter(F.col("at_risk_probability") >= 0.7).count()
        total_count = output_df.count()

        print(f"✅ Scored {total_count:,} active members")
        print(f"   High risk (>=0.7): {high_risk_count:,} ({high_risk_count/total_count:.1%})")

        return output_df


if __name__ == "__main__":
    # Train and score
    model = AtRiskModel()

    # Train model
    trained_model = model.train()

    # Score all members
    model.score_members(trained_model)

    print("\n✅ At-risk prediction model training and scoring complete")
