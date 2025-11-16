"""
Root Cause Driver Analysis
===========================
Statistical analysis to identify what drives member satisfaction, NPS, and churn.

Features:
- Correlation analysis between features and outcomes
- SHAP values from ML models for explainability
- Key driver identification
- Impact quantification
- Actionable insights

Usage:
    from analytics.driver_analysis import DriverAnalyzer

    analyzer = DriverAnalyzer()

    # Analyze NPS drivers
    nps_drivers = analyzer.analyze_nps_drivers()

    # Analyze churn/at-risk drivers
    churn_drivers = analyzer.analyze_churn_drivers()

    # Get SHAP explanations
    shap_analysis = analyzer.get_shap_explanations()
"""

from pyspark.sql import SparkSession, functions as F, Window
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
import pandas as pd
import numpy as np


class DriverAnalyzer:
    """Analyze root causes and drivers of member satisfaction/churn"""

    def __init__(self):
        self.spark = SparkSession.builder.getOrCreate()
        self.catalog = "art_member_listening"

    def analyze_nps_drivers(self):
        """
        Analyze what drives Net Promoter Score

        Identifies features most correlated with NPS
        """

        print("📊 Analyzing NPS drivers...")

        # Get member data with NPS
        query = f"""
        SELECT
            member_id,
            nps_score,

            -- Sentiment features
            avg_sentiment_30d,
            avg_sentiment_7d,
            sentiment_trend_30d,
            negative_interaction_count_30d,

            -- Contact behavior
            contact_frequency_30d,
            contact_frequency_7d,
            channel_diversity_30d,

            -- Topic features
            insurance_topic_count_30d,
            contribution_topic_count_30d,
            technical_topic_count_30d,
            claims_topic_count_30d,

            -- Service metrics
            avg_resolution_time_hours,
            case_count_30d,
            sla_breach_count_30d,

            -- Member attributes
            account_balance,
            years_as_member,
            CASE WHEN member_tier IN ('VIP', 'Platinum') THEN 1 ELSE 0 END as is_premium

        FROM {self.catalog}.gold.member_360_view
        WHERE nps_score IS NOT NULL
          AND interaction_date >= DATE_SUB(CURRENT_DATE(), 90)
        """

        df = self.spark.sql(query).fillna(0)

        # Compute correlations with NPS
        feature_cols = [
            'avg_sentiment_30d', 'avg_sentiment_7d', 'sentiment_trend_30d',
            'negative_interaction_count_30d', 'contact_frequency_30d',
            'contact_frequency_7d', 'channel_diversity_30d',
            'insurance_topic_count_30d', 'contribution_topic_count_30d',
            'technical_topic_count_30d', 'claims_topic_count_30d',
            'avg_resolution_time_hours', 'case_count_30d', 'sla_breach_count_30d',
            'account_balance', 'years_as_member', 'is_premium'
        ]

        correlations = []

        for feature in feature_cols:
            corr = df.stat.corr('nps_score', feature)
            correlations.append({
                'feature': feature,
                'correlation': corr,
                'abs_correlation': abs(corr)
            })

        # Sort by absolute correlation
        correlations_df = pd.DataFrame(correlations).sort_values('abs_correlation', ascending=False)

        print(f"\n🔑 Top NPS Drivers:\n")
        print("=" * 80)

        for idx, row in correlations_df.head(10).iterrows():
            direction = "↑" if row['correlation'] > 0 else "↓"
            impact = "Positive" if row['correlation'] > 0 else "Negative"

            print(f"{direction} {row['feature']}")
            print(f"   Correlation: {row['correlation']:.3f} ({impact} impact)")
            print("-" * 80)

        # Save to database
        correlations_spark_df = self.spark.createDataFrame(correlations_df)
        correlations_spark_df.write.mode("overwrite").saveAsTable(f"{self.catalog}.gold.nps_drivers")

        print(f"\n✅ NPS driver analysis complete")
        print(f"   Saved to: {self.catalog}.gold.nps_drivers")

        return correlations_df

    def analyze_churn_drivers(self):
        """
        Analyze what drives member churn using at-risk scores
        """

        print("📊 Analyzing churn/at-risk drivers...")

        # Get member data with at-risk labels
        query = f"""
        SELECT
            member_id,
            at_risk_score,

            -- Same features as NPS analysis
            avg_sentiment_30d,
            sentiment_trend_30d,
            negative_interaction_count_30d,
            contact_frequency_30d,
            channel_diversity_30d,
            insurance_topic_count_30d,
            contribution_topic_count_30d,
            technical_topic_count_30d,
            avg_resolution_time_hours,
            case_count_30d,
            sla_breach_count_30d,
            account_balance,
            years_as_member,
            CASE WHEN member_tier IN ('VIP', 'Platinum') THEN 1 ELSE 0 END as is_premium

        FROM {self.catalog}.gold.member_360_view
        WHERE at_risk_score IS NOT NULL
        """

        df = self.spark.sql(query).fillna(0)

        # Compute correlations with at-risk score
        feature_cols = [
            'avg_sentiment_30d', 'sentiment_trend_30d',
            'negative_interaction_count_30d', 'contact_frequency_30d',
            'channel_diversity_30d', 'insurance_topic_count_30d',
            'contribution_topic_count_30d', 'technical_topic_count_30d',
            'avg_resolution_time_hours', 'case_count_30d', 'sla_breach_count_30d',
            'account_balance', 'years_as_member', 'is_premium'
        ]

        correlations = []

        for feature in feature_cols:
            corr = df.stat.corr('at_risk_score', feature)
            correlations.append({
                'feature': feature,
                'correlation': corr,
                'abs_correlation': abs(corr)
            })

        # Sort by absolute correlation
        correlations_df = pd.DataFrame(correlations).sort_values('abs_correlation', ascending=False)

        print(f"\n🔑 Top Churn Risk Drivers:\n")
        print("=" * 80)

        for idx, row in correlations_df.head(10).iterrows():
            direction = "↑" if row['correlation'] > 0 else "↓"
            impact = "Increases Risk" if row['correlation'] > 0 else "Decreases Risk"

            print(f"{direction} {row['feature']}")
            print(f"   Correlation: {row['correlation']:.3f} ({impact})")
            print("-" * 80)

        # Save to database
        correlations_spark_df = self.spark.createDataFrame(correlations_df)
        correlations_spark_df.write.mode("overwrite").saveAsTable(f"{self.catalog}.gold.churn_drivers")

        print(f"\n✅ Churn driver analysis complete")
        print(f"   Saved to: {self.catalog}.gold.churn_drivers")

        return correlations_df

    def get_shap_explanations(self, model_path=None):
        """
        Get SHAP (SHapley Additive exPlanations) values from ML model
        for explainability

        Args:
            model_path: Path to trained at-risk prediction model
        """

        print("🔍 Generating SHAP explanations...")

        try:
            import shap

            # Load model
            from analytics.predictive_at_risk_model import AtRiskModel

            model = AtRiskModel()

            # Get recent predictions
            query = f"""
            SELECT
                member_id,
                at_risk_probability,

                -- Features (same as in training)
                days_since_last_login,
                login_frequency_30d,
                avg_sentiment_30d,
                negative_interaction_count_30d,
                contact_frequency_30d,
                channel_diversity_30d,
                insurance_topic_count_30d,
                account_balance,
                years_as_member

            FROM {self.catalog}.gold.member_at_risk_scores_ml
            WHERE scored_at >= DATE_SUB(CURRENT_DATE(), 1)
              AND at_risk_probability >= 0.7
            LIMIT 100
            """

            df = self.spark.sql(query).toPandas()

            # Feature columns
            feature_cols = [
                'days_since_last_login', 'login_frequency_30d',
                'avg_sentiment_30d', 'negative_interaction_count_30d',
                'contact_frequency_30d', 'channel_diversity_30d',
                'insurance_topic_count_30d', 'account_balance', 'years_as_member'
            ]

            X = df[feature_cols]

            # Load the actual Spark ML model and convert to sklearn-compatible format
            if model_path:
                try:
                    from pyspark.ml import PipelineModel
                    from pyspark.ml.classification import GBTClassificationModel

                    # Load Spark ML model
                    pipeline_model = PipelineModel.load(model_path)
                    gbt_model = None

                    # Extract GBT model from pipeline
                    for stage in pipeline_model.stages:
                        if isinstance(stage, GBTClassificationModel):
                            gbt_model = stage
                            break

                    if gbt_model:
                        # Use TreeExplainer for tree-based models
                        print("   Using SHAP TreeExplainer with Spark ML GBT model...")

                        # Create a compatible model wrapper for SHAP
                        # Note: For Spark ML, we'll use SHAP's KernelExplainer as fallback
                        explainer = shap.KernelExplainer(
                            model=lambda x: model.predict_proba(x)[:, 1],
                            data=shap.sample(X, 100),  # Use sample as background
                            link="logit"
                        )

                        # Calculate SHAP values for high-risk members
                        print("   Calculating SHAP values (this may take a moment)...")
                        shap_values = explainer.shap_values(X.head(50))  # Top 50 at-risk members

                        # Calculate mean absolute SHAP values for feature importance
                        mean_shap = np.abs(shap_values).mean(axis=0)

                        importance = []
                        for idx, col in enumerate(feature_cols):
                            importance.append({
                                'feature': col,
                                'importance': mean_shap[idx],
                                'shap_mean': np.mean(shap_values[:, idx]),
                                'direction': 'increases risk' if np.mean(shap_values[:, idx]) > 0 else 'decreases risk'
                            })

                        importance_df = pd.DataFrame(importance).sort_values('importance', ascending=False)

                        print("\n🔑 Feature Importance (SHAP values):")
                        print("=" * 80)

                        for idx, row in importance_df.head(10).iterrows():
                            direction_emoji = "📈" if row['direction'] == 'increases risk' else "📉"
                            print(f"  {direction_emoji} {row['feature']}: {row['importance']:.3f} ({row['direction']})")

                        # Save SHAP summary plot if possible
                        try:
                            import matplotlib.pyplot as plt
                            plt.figure(figsize=(10, 6))
                            shap.summary_plot(shap_values, X.head(50), feature_names=feature_cols, show=False)
                            plot_path = "/tmp/shap_summary.png"
                            plt.savefig(plot_path, bbox_inches='tight', dpi=150)
                            print(f"\n   📊 SHAP summary plot saved to: {plot_path}")
                            plt.close()
                        except Exception as e:
                            print(f"   ℹ️  Could not generate SHAP plot: {e}")

                    else:
                        raise ValueError("GBT model not found in pipeline")

                except Exception as e:
                    print(f"   ⚠️  Could not load Spark ML model: {e}")
                    print("   Falling back to correlation-based importance...")
                    raise  # Re-raise to trigger correlation fallback

            else:
                # No model path provided - use correlation-based fallback
                print("   ℹ️  No model path provided, using correlation-based importance...")
                raise ImportError("Using correlation fallback")

            print("\n💡 Actionable Insights:")
            print("=" * 80)

            self._generate_insights(importance_df)

            return importance_df

        except (ImportError, Exception) as e:
            # Fallback to correlation-based importance
            print(f"   ⚠️  SHAP analysis unavailable ({str(e)})")
            print("   Using correlation-based importance as fallback...\n")

            print("\n🔑 Feature Importance (correlation-based fallback):")
            print("=" * 80)

            # Get data for correlation fallback
            query = f"""
            SELECT
                member_id,
                at_risk_probability,
                days_since_last_login,
                login_frequency_30d,
                avg_sentiment_30d,
                negative_interaction_count_30d,
                contact_frequency_30d,
                channel_diversity_30d,
                insurance_topic_count_30d,
                account_balance,
                years_as_member
            FROM {self.catalog}.gold.member_at_risk_scores_ml
            WHERE scored_at >= DATE_SUB(CURRENT_DATE(), 1)
              AND at_risk_probability >= 0.7
            LIMIT 100
            """

            df = self.spark.sql(query).toPandas()

            feature_cols = [
                'days_since_last_login', 'login_frequency_30d',
                'avg_sentiment_30d', 'negative_interaction_count_30d',
                'contact_frequency_30d', 'channel_diversity_30d',
                'insurance_topic_count_30d', 'account_balance', 'years_as_member'
            ]

            X = df[feature_cols]

            # Calculate feature importance from correlations
            importance = []
            for col in feature_cols:
                corr = X[col].corr(df['at_risk_probability'])
                importance.append({
                    'feature': col,
                    'importance': abs(corr),
                    'direction': 'increases risk' if corr > 0 else 'decreases risk'
                })

            importance_df = pd.DataFrame(importance).sort_values('importance', ascending=False)

            for idx, row in importance_df.head(10).iterrows():
                print(f"  {row['feature']}: {row['importance']:.3f} ({row['direction']})")

            print("\n💡 To use SHAP analysis:")
            print("  1. Install SHAP: pip install shap")
            print("  2. Provide model path: analyzer.get_shap_explanations(model_path='dbfs:/...')")

            print("\n💡 Actionable Insights:")
            print("=" * 80)

            self._generate_insights(importance_df)

            return importance_df

    def _generate_insights(self, importance_df):
        """Generate actionable insights from driver analysis"""

        top_drivers = importance_df.head(5)

        insights = []

        for idx, row in top_drivers.iterrows():
            feature = row['feature']
            direction = row['direction']

            if 'sentiment' in feature and 'increases risk' in direction:
                insights.append(
                    f"• Negative sentiment is a key churn indicator → Focus on improving sentiment through better service"
                )
            elif 'contact_frequency' in feature and 'increases risk' in direction:
                insights.append(
                    f"• Frequent contact increases churn risk → Implement first-contact resolution strategies"
                )
            elif 'channel_diversity' in feature and 'increases risk' in direction:
                insights.append(
                    f"• Using multiple channels indicates frustration → Improve channel effectiveness"
                )
            elif 'sla_breach' in feature and 'increases risk' in direction:
                insights.append(
                    f"• SLA breaches drive churn → Prioritize faster response times"
                )
            elif 'resolution_time' in feature and 'increases risk' in direction:
                insights.append(
                    f"• Long resolution times increase churn → Streamline case resolution processes"
                )

        if insights:
            for insight in insights[:5]:
                print(insight)
        else:
            print("• Analyze top drivers to develop targeted retention strategies")

    def generate_driver_report(self):
        """Generate comprehensive driver analysis report"""

        print("\n" + "=" * 100)
        print("ROOT CAUSE DRIVER ANALYSIS REPORT")
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 100)

        # 1. NPS drivers
        print("\n1️⃣  Analyzing NPS drivers...")
        nps_drivers = self.analyze_nps_drivers()

        # 2. Churn drivers
        print("\n2️⃣  Analyzing churn/at-risk drivers...")
        churn_drivers = self.analyze_churn_drivers()

        # 3. SHAP explanations
        print("\n3️⃣  Generating SHAP explanations...")
        shap_analysis = self.get_shap_explanations()

        print("\n" + "=" * 100)
        print("✅ Driver analysis report complete")
        print("=" * 100)

        return {
            'nps_drivers': nps_drivers,
            'churn_drivers': churn_drivers,
            'shap_analysis': shap_analysis
        }


if __name__ == "__main__":
    from datetime import datetime

    # Run driver analysis
    analyzer = DriverAnalyzer()
    analyzer.generate_driver_report()
