"""
Topic Modeling and Trend Analysis
==================================
Extract topics from member feedback using Databricks AI services and batch inference.

Features:
- Databricks Foundation Models for topic extraction (ai_extract_topics)
- LLM-powered topic clustering
- Topic trend tracking over time
- Emerging topic detection
- Topic-sentiment correlation
- Batch inference for scale

Usage:
    from analytics.topic_modeling import TopicModeler

    modeler = TopicModeler()

    # Extract topics using AI
    topics = modeler.extract_topics_ai()

    # Track topic trends
    trends = modeler.track_topic_trends()

    # Detect emerging topics
    emerging = modeler.detect_emerging_topics()
"""

from pyspark.sql import SparkSession, functions as F, Window
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
from datetime import datetime, timedelta
import json


class TopicModeler:
    """AI-powered topic modeling and trend analysis for member feedback"""

    def __init__(self):
        self.spark = SparkSession.builder.getOrCreate()
        self.catalog = "art_member_listening"
        self.workspace_client = WorkspaceClient()

        # Foundation model for topic extraction
        self.topic_model_endpoint = "databricks-meta-llama-3-1-70b-instruct"

        # Predefined topic categories (can be discovered from data)
        self.topic_categories = [
            "Insurance (Life, TPD, Income Protection)",
            "Contributions (Employer, Personal, Salary Sacrifice)",
            "Investment Options and Performance",
            "Account Balance and Statements",
            "Beneficiary Nominations",
            "Claims (Insurance, Death, Disability)",
            "Member Portal and Online Access",
            "Customer Service and Support",
            "Fees and Charges",
            "Retirement Planning",
            "Fund Switching",
            "Compliance and Regulatory",
            "Technical Issues",
            "Other"
        ]

    def prepare_text_data(self, lookback_days=90, min_text_length=20):
        """Prepare text data for topic modeling"""

        print(f"📝 Preparing text data for topic modeling (last {lookback_days} days)...")

        query = f"""
        SELECT
            interaction_id,
            member_id,
            interaction_date,
            channel,
            text,
            sentiment_score,
            sentiment_label,
            primary_topic as manual_topic
        FROM {self.catalog}.silver.interactions_analyzed
        WHERE interaction_date >= DATE_SUB(CURRENT_DATE(), {lookback_days})
          AND text IS NOT NULL
          AND LENGTH(text) >= {min_text_length}
        """

        df = self.spark.sql(query)

        # Clean text
        df = df.withColumn("text_clean", F.lower(F.col("text")))

        # Remove special characters, keep only letters and spaces
        df = df.withColumn(
            "text_clean",
            F.regexp_replace(F.col("text_clean"), r"[^a-z\s]", " ")
        )

        # Remove extra spaces
        df = df.withColumn(
            "text_clean",
            F.regexp_replace(F.col("text_clean"), r"\s+", " ")
        )

        # Trim
        df = df.withColumn("text_clean", F.trim(F.col("text_clean")))

        count = df.count()
        print(f"✅ Prepared {count:,} text samples for topic modeling")

        return df

    def extract_topics_ai(self, batch_size=1000):
        """
        Extract topics using Databricks AI Foundation Models with batch inference

        Uses SQL AI functions for efficient batch processing
        """

        print(f"🤖 Extracting topics using Databricks AI (batch size: {batch_size})...")

        # Prepare data
        df = self.prepare_text_data()

        # Create UDF that calls Foundation Model API for topic extraction
        @F.pandas_udf("struct<topic:string, confidence:double, subtopics:array<string>>")
        def extract_topic_udf(texts):
            """Batch extract topics using Foundation Model"""
            import pandas as pd

            results = []

            for text in texts:
                if not text or len(text) < 20:
                    results.append({
                        'topic': 'Other',
                        'confidence': 0.0,
                        'subtopics': []
                    })
                    continue

                try:
                    # Call Foundation Model
                    prompt = f"""Analyze this member feedback and extract the primary topic.

Member Feedback: "{text[:500]}"

Available Topic Categories:
{chr(10).join([f"- {t}" for t in self.topic_categories])}

Respond in JSON format:
{{
    "topic": "primary topic from the list",
    "confidence": 0.0-1.0,
    "subtopics": ["subtopic1", "subtopic2"]
}}
"""

                    response = self.workspace_client.serving_endpoints.query(
                        name=self.topic_model_endpoint,
                        messages=[
                            ChatMessage(
                                role=ChatMessageRole.USER,
                                content=prompt
                            )
                        ],
                        temperature=0.1,
                        max_tokens=200
                    )

                    # Parse response
                    response_text = response.choices[0].message.content

                    # Extract JSON from response
                    import re
                    json_match = re.search(r'\{.*\}', response_text, re.DOTALL)

                    if json_match:
                        result = json.loads(json_match.group())
                    else:
                        result = {
                            'topic': 'Other',
                            'confidence': 0.5,
                            'subtopics': []
                        }

                    results.append(result)

                except Exception as e:
                    results.append({
                        'topic': 'Other',
                        'confidence': 0.0,
                        'subtopics': []
                    })

            return pd.DataFrame(results)

        # Use SQL AI function for topic extraction (faster alternative)
        # This leverages Databricks' built-in AI functions for batch inference

        print("   Using Databricks SQL AI functions for batch topic extraction...")

        # Register as temp view
        df.createOrReplaceTempView("feedback_for_topics")

        # Use ai_extract_topics SQL function (if available) or custom UDF
        topic_extraction_sql = f"""
        SELECT
            interaction_id,
            member_id,
            interaction_date,
            text,
            sentiment_score,
            manual_topic,

            -- Use SQL AI function for topic extraction
            ai_classify(
                text,
                ARRAY('{("', '").join(self.topic_categories)}')
            ) as ai_topic_result

        FROM feedback_for_topics
        """

        # Execute SQL with AI function
        try:
            df_with_topics = self.spark.sql(topic_extraction_sql)
        except Exception as e:
            # Fallback to UDF if ai_classify not available
            print(f"   Note: Using UDF fallback (ai_classify not available in this workspace)")
            df_with_topics = df.withColumn("ai_topic_result", extract_topic_udf(F.col("text")))

        # Extract topic fields
        df_with_topics = df_with_topics.withColumn(
            "ai_extracted_topic",
            F.col("ai_topic_result.topic") if "ai_topic_result.topic" else F.col("ai_topic_result")
        ).withColumn(
            "topic_confidence",
            F.when(F.col("ai_topic_result.confidence").isNotNull(), F.col("ai_topic_result.confidence")).otherwise(0.8)
        )

        # Save to gold table
        df_with_topics.select(
            "interaction_id",
            "member_id",
            "interaction_date",
            "ai_extracted_topic",
            "topic_confidence",
            "manual_topic",
            "sentiment_score"
        ).write.mode("overwrite").saveAsTable(f"{self.catalog}.gold.ai_extracted_topics")

        # Generate topic statistics
        topic_stats = df_with_topics.groupBy("ai_extracted_topic").agg(
            F.count("*").alias("mention_count"),
            F.avg("sentiment_score").alias("avg_sentiment"),
            F.avg("topic_confidence").alias("avg_confidence")
        ).orderBy(F.desc("mention_count"))

        print(f"\n📚 AI-Extracted Topics:\n")
        print("=" * 100)

        for row in topic_stats.collect():
            print(f"{row['ai_extracted_topic']}")
            print(f"  Mentions: {row['mention_count']:,} | Avg Sentiment: {row['avg_sentiment']:.2f} | Confidence: {row['avg_confidence']:.2f}")
            print("-" * 100)

        topic_stats.write.mode("overwrite").saveAsTable(f"{self.catalog}.gold.topic_statistics")

        print(f"\n✅ AI topic extraction complete")
        print(f"   Saved to: {self.catalog}.gold.ai_extracted_topics")

        return df_with_topics

    def track_topic_trends(self, days_back=90):
        """Track how topic mentions change over time using AI-extracted topics"""

        print(f"📈 Tracking topic trends (last {days_back} days)...")

        query = f"""
        WITH topic_counts AS (
            SELECT
                DATE_TRUNC('day', interaction_date) as date,
                ai_extracted_topic as topic,
                COUNT(*) as mention_count,
                AVG(sentiment_score) as avg_sentiment
            FROM {self.catalog}.gold.ai_extracted_topics
            WHERE interaction_date >= DATE_SUB(CURRENT_DATE(), {days_back})
            GROUP BY 1, 2
        ),
        topic_trends AS (
            SELECT
                date,
                topic,
                discovered_topic_id,
                mention_count,
                avg_sentiment,

                -- Calculate 7-day moving average
                AVG(mention_count) OVER (
                    PARTITION BY topic
                    ORDER BY date
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ) as mention_count_7d_ma,

                -- Previous week's count
                LAG(mention_count, 7) OVER (
                    PARTITION BY topic ORDER BY date
                ) as mention_count_7d_ago,

                -- Percentage change
                CASE
                    WHEN LAG(mention_count, 7) OVER (PARTITION BY topic ORDER BY date) > 0
                    THEN ((mention_count - LAG(mention_count, 7) OVER (
                        PARTITION BY topic ORDER BY date
                    )) / LAG(mention_count, 7) OVER (
                        PARTITION BY topic ORDER BY date
                    )) * 100
                    ELSE 0
                END as pct_change_7d

            FROM topic_counts
        )
        SELECT * FROM topic_trends
        ORDER BY date DESC, mention_count DESC
        """

        trends_df = self.spark.sql(query)

        # Save to table
        trends_df.write.mode("overwrite").saveAsTable(f"{self.catalog}.gold.topic_trends")

        print(f"✅ Topic trends saved to: {self.catalog}.gold.topic_trends")

        return trends_df

    def detect_emerging_topics(self, threshold_pct=200, min_mentions=10):
        """
        Detect topics with significant increase in mentions

        Args:
            threshold_pct: Percentage increase threshold (default 200% = 3x)
            min_mentions: Minimum mentions to be considered emerging

        Returns:
            DataFrame of emerging topics
        """

        print(f"🚀 Detecting emerging topics (threshold: {threshold_pct}% increase)...")

        query = f"""
        SELECT
            topic,
            discovered_topic_id,
            date,
            mention_count,
            mention_count_7d_ago,
            pct_change_7d,
            avg_sentiment
        FROM {self.catalog}.gold.topic_trends
        WHERE pct_change_7d >= {threshold_pct}
          AND mention_count >= {min_mentions}
          AND date >= DATE_SUB(CURRENT_DATE(), 7)
        ORDER BY pct_change_7d DESC, date DESC
        """

        emerging_df = self.spark.sql(query)

        count = emerging_df.count()

        if count > 0:
            print(f"⚠️  Found {count} emerging topics:")
            for row in emerging_df.limit(10).collect():
                print(f"   • {row['topic']}: {row['mention_count']} mentions (+{row['pct_change_7d']:.0f}%)")
        else:
            print(f"✅ No emerging topics detected")

        # Save emerging topics
        emerging_df.write.mode("overwrite").saveAsTable(f"{self.catalog}.gold.emerging_topics")

        return emerging_df

    def analyze_topic_sentiment(self):
        """Analyze sentiment distribution by topic"""

        print("😊 Analyzing sentiment by topic...")

        query = f"""
        SELECT
            t.description as topic,
            d.discovered_topic_id,
            COUNT(*) as total_mentions,
            AVG(i.sentiment_score) as avg_sentiment,
            STDDEV(i.sentiment_score) as sentiment_std,

            COUNT(CASE WHEN i.sentiment_label = 'positive' THEN 1 END) as positive_count,
            COUNT(CASE WHEN i.sentiment_label = 'neutral' THEN 1 END) as neutral_count,
            COUNT(CASE WHEN i.sentiment_label = 'negative' THEN 1 END) as negative_count,

            COUNT(CASE WHEN i.sentiment_label = 'positive' THEN 1 END) / COUNT(*) * 100 as positive_pct,
            COUNT(CASE WHEN i.sentiment_label = 'negative' THEN 1 END) / COUNT(*) * 100 as negative_pct

        FROM {self.catalog}.gold.document_topics d
        JOIN {self.catalog}.gold.discovered_topics t
            ON d.discovered_topic_id = t.topic_id
        JOIN {self.catalog}.silver.interactions_analyzed i
            ON d.interaction_id = i.interaction_id
        GROUP BY 1, 2
        ORDER BY total_mentions DESC
        """

        sentiment_df = self.spark.sql(query)

        # Save to table
        sentiment_df.write.mode("overwrite").saveAsTable(f"{self.catalog}.gold.topic_sentiment_analysis")

        print(f"\n📊 Topic Sentiment Summary:")
        print("=" * 100)

        for row in sentiment_df.limit(10).collect():
            print(f"Topic: {row['topic']}")
            print(f"  Mentions: {row['total_mentions']:,}")
            print(f"  Avg Sentiment: {row['avg_sentiment']:.2f}")
            print(f"  Positive: {row['positive_pct']:.1f}% | Negative: {row['negative_pct']:.1f}%")
            print("-" * 100)

        return sentiment_df

    def generate_topic_report(self):
        """Generate comprehensive topic analysis report"""

        print("\n" + "=" * 100)
        print("TOPIC MODELING REPORT")
        print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 100)

        # 1. Topic discovery
        if self.lda_model is None:
            print("\n1️⃣  Discovering topics...")
            self.discover_topics(num_topics=15)
        else:
            print("\n1️⃣  Using existing topic model")

        # 2. Track trends
        print("\n2️⃣  Tracking topic trends...")
        self.track_topic_trends()

        # 3. Detect emerging topics
        print("\n3️⃣  Detecting emerging topics...")
        self.detect_emerging_topics()

        # 4. Analyze sentiment
        print("\n4️⃣  Analyzing topic sentiment...")
        self.analyze_topic_sentiment()

        print("\n" + "=" * 100)
        print("✅ Topic modeling report complete")
        print("=" * 100)


if __name__ == "__main__":
    # Run topic modeling pipeline
    modeler = TopicModeler()
    modeler.generate_topic_report()
