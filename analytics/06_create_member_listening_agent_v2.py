"""
Create Databricks AI Agent for Member Listening Intelligence.
Uses REAL Databricks features:
- Vector Search with BGE embeddings for semantic feedback search
- SQL Warehouse for data queries
- Foundation Model endpoints for LLM
"""

import mlflow
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
from databricks.vector_search.client import VectorSearchClient
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from typing import List, Dict, Any
import json
import sys
sys.path.append('..')
from config.config import *


class MemberListeningAgent:
    """AI Agent for member listening intelligence with real Vector Search and AI functions."""

    def __init__(self):
        self.w = WorkspaceClient()
        self.spark = SparkSession.builder \
            .config("spark.databricks.sql.warehouse.id", SQL_WAREHOUSE_ID) \
            .getOrCreate()

        # Initialize Vector Search client
        self.vsc = VectorSearchClient()

        # Model endpoint configuration
        self.model_endpoint = AGENT["model_endpoint"]

        print(f"✓ Agent initialized")
        print(f"  Model: {self.model_endpoint}")
        print(f"  SQL Warehouse: {SQL_WAREHOUSE_ID}")
        print(f"  Vector Search Endpoint: {VECTOR_SEARCH_ENDPOINT}")

    # ========================================================================
    # TOOL 1: Vector Search for Semantic Feedback Search (REAL)
    # ========================================================================

    def search_member_feedback(
        self,
        query: str,
        limit: int = 10,
        channel: str = None,
        min_urgency: float = 0.0
    ) -> List[Dict]:
        """
        Search member feedback using REAL Vector Search with BGE embeddings.
        This is semantic search - finds similar meaning, not just keywords.

        Args:
            query: Search query (e.g., "frustrated about insurance")
            limit: Number of results to return
            channel: Optional filter by channel (call, email, chat, survey)
            min_urgency: Minimum urgency score (0-1)

        Returns:
            List of relevant feedback instances
        """

        print(f"🔍 Vector Search: '{query}' (limit={limit}, channel={channel})")

        try:
            # Get vector search index
            index = self.vsc.get_index(
                endpoint_name=VECTOR_SEARCH_ENDPOINT,
                index_name=VECTOR_SEARCH_INDEX
            )

            # Perform semantic similarity search using BGE embeddings
            results = index.similarity_search(
                query_text=query,
                columns=[
                    "interaction_id",
                    "member_id",
                    "timestamp",
                    "channel",
                    "original_text",
                    "sentiment_label",
                    "sentiment_score",
                    "primary_topic",
                    "urgency_score"
                ],
                num_results=limit * 2  # Get more for filtering
            )

            # Parse results
            if results and 'result' in results and 'data_array' in results['result']:
                data = results['result']['data_array']

                # Convert to list of dicts
                feedback_items = []
                for item in data:
                    feedback = {
                        "interaction_id": item[0],
                        "member_id": item[1],
                        "timestamp": str(item[2]),
                        "channel": item[3],
                        "text": item[4],
                        "sentiment_label": item[5],
                        "sentiment_score": item[6],
                        "primary_topic": item[7],
                        "urgency_score": item[8]
                    }

                    # Apply filters
                    if channel and feedback["channel"] != channel:
                        continue
                    if feedback["urgency_score"] < min_urgency:
                        continue

                    feedback_items.append(feedback)

                    if len(feedback_items) >= limit:
                        break

                print(f"✓ Found {len(feedback_items)} semantically similar results")
                return feedback_items

            else:
                print("⚠️  No results from Vector Search")
                return []

        except Exception as e:
            print(f"❌ Vector Search error: {e}")
            print("   Falling back to keyword search...")

            # Fallback to SQL keyword search
            return self._fallback_keyword_search(query, limit, channel)

    def _fallback_keyword_search(self, query: str, limit: int, channel: str = None) -> List[Dict]:
        """Fallback keyword search if Vector Search unavailable."""

        keywords = query.lower().split()
        keyword_conditions = " OR ".join([f"LOWER(text) LIKE '%{kw}%'" for kw in keywords])

        sql_query = f"""
        SELECT
            interaction_id,
            member_id,
            timestamp,
            channel,
            text,
            sentiment_label,
            sentiment_score,
            primary_topic,
            urgency_score
        FROM {TABLES['interactions_analyzed']}
        WHERE ({keyword_conditions})
        """

        if channel:
            sql_query += f" AND channel = '{channel}'"

        sql_query += f"""
        ORDER BY urgency_score DESC, timestamp DESC
        LIMIT {limit}
        """

        try:
            results = self.spark.sql(sql_query).toPandas().to_dict('records')
            print(f"✓ Fallback keyword search: {len(results)} results")
            return results
        except Exception as e:
            print(f"❌ Fallback search also failed: {e}")
            return []

    # ========================================================================
    # TOOL 2: Sentiment Trends Analysis
    # ========================================================================

    def get_sentiment_trends(
        self,
        time_period: str = "last_30_days",
        channel: str = None,
        topic: str = None
    ) -> Dict[str, Any]:
        """
        Get sentiment trends over time using SQL queries.

        Args:
            time_period: Time window (last_7_days, last_30_days, last_90_days)
            channel: Optional channel filter
            topic: Optional topic filter

        Returns:
            Sentiment trend data with summary statistics
        """

        print(f"📈 Getting sentiment trends: period={time_period}, channel={channel}, topic={topic}")

        # Map time period to days
        days_map = {
            "last_7_days": 7,
            "last_30_days": 30,
            "last_90_days": 90
        }
        days = days_map.get(time_period, 30)

        sql_query = f"""
        SELECT
            DATE_TRUNC('day', timestamp) as date,
            channel,
            primary_topic,
            AVG(sentiment_score) as avg_sentiment,
            COUNT(*) as interaction_count,
            SUM(CASE WHEN sentiment_label = 'Negative' THEN 1 ELSE 0 END) as negative_count,
            SUM(CASE WHEN sentiment_label = 'Positive' THEN 1 ELSE 0 END) as positive_count,
            AVG(urgency_score) as avg_urgency
        FROM {TABLES['interactions_analyzed']}
        WHERE timestamp >= CURRENT_DATE - INTERVAL '{days} days'
        """

        if channel:
            sql_query += f" AND channel = '{channel}'"
        if topic:
            sql_query += f" AND primary_topic = '{topic}'"

        sql_query += """
        GROUP BY DATE_TRUNC('day', timestamp), channel, primary_topic
        ORDER BY date DESC
        """

        try:
            trends = self.spark.sql(sql_query).toPandas().to_dict('records')

            # Calculate overall statistics
            overall_sentiment = sum(t['avg_sentiment'] for t in trends) / len(trends) if trends else 0
            total_interactions = sum(t['interaction_count'] for t in trends)

            print(f"✓ Retrieved {len(trends)} trend data points")

            return {
                "trends": trends,
                "summary": {
                    "overall_sentiment": round(overall_sentiment, 3),
                    "total_interactions": total_interactions,
                    "period": time_period,
                    "data_points": len(trends)
                }
            }
        except Exception as e:
            print(f"❌ Error getting trends: {e}")
            return {"trends": [], "summary": {}}

    # ========================================================================
    # TOOL 3: At-Risk Members Identification
    # ========================================================================

    def get_at_risk_members(self, limit: int = 100, min_risk_score: float = 0.7) -> List[Dict]:
        """
        Get list of at-risk members requiring intervention.

        Args:
            limit: Maximum number of members to return
            min_risk_score: Minimum risk score threshold (0-1)

        Returns:
            List of at-risk members with context
        """

        print(f"⚠️  Getting at-risk members: limit={limit}, min_risk_score={min_risk_score}")

        sql_query = f"""
        SELECT
            member_id,
            avg_sentiment,
            negative_interaction_count,
            last_interaction_date,
            all_topics_discussed,
            at_risk_score,
            at_risk_reasons,
            preferred_channel,
            total_interactions
        FROM {TABLES['member_360']}
        WHERE at_risk_flag = true
        AND at_risk_score >= {min_risk_score}
        ORDER BY at_risk_score DESC
        LIMIT {limit}
        """

        try:
            members = self.spark.sql(sql_query).toPandas().to_dict('records')
            print(f"✓ Found {len(members)} at-risk members")
            return members
        except Exception as e:
            print(f"❌ Error getting at-risk members: {e}")
            return []

    # ========================================================================
    # TOOL 4: Topic Distribution Analysis
    # ========================================================================

    def analyze_topic_distribution(
        self,
        topic: str = None,
        time_period: str = "last_90_days"
    ) -> Dict[str, Any]:
        """
        Analyze topic distribution and sentiment.

        Args:
            topic: Optional specific topic to analyze
            time_period: Time window

        Returns:
            Topic analysis with sentiment breakdown
        """

        print(f"🏷️  Analyzing topic distribution: topic={topic}, period={time_period}")

        days_map = {
            "last_7_days": 7,
            "last_30_days": 30,
            "last_90_days": 90
        }
        days = days_map.get(time_period, 90)

        sql_query = f"""
        WITH topic_stats AS (
            SELECT
                primary_topic,
                sentiment_label,
                COUNT(*) as count,
                AVG(sentiment_score) as avg_sentiment,
                AVG(urgency_score) as avg_urgency
            FROM {TABLES['interactions_analyzed']}
            WHERE timestamp >= CURRENT_DATE - INTERVAL '{days} days'
        """

        if topic:
            sql_query += f" AND primary_topic = '{topic}'"

        sql_query += """
            GROUP BY primary_topic, sentiment_label
        )
        SELECT
            primary_topic,
            SUM(count) as total_mentions,
            AVG(avg_sentiment) as overall_sentiment,
            AVG(avg_urgency) as overall_urgency,
            MAX(CASE WHEN sentiment_label = 'Negative' THEN count ELSE 0 END) as negative_count,
            MAX(CASE WHEN sentiment_label = 'Neutral' THEN count ELSE 0 END) as neutral_count,
            MAX(CASE WHEN sentiment_label = 'Positive' THEN count ELSE 0 END) as positive_count
        FROM topic_stats
        GROUP BY primary_topic
        ORDER BY total_mentions DESC
        """

        try:
            topics = self.spark.sql(sql_query).toPandas().to_dict('records')

            # Calculate percentages
            for topic_data in topics:
                total = topic_data['total_mentions']
                topic_data['negative_pct'] = round((topic_data['negative_count'] / total) * 100, 1)
                topic_data['neutral_pct'] = round((topic_data['neutral_count'] / total) * 100, 1)
                topic_data['positive_pct'] = round((topic_data['positive_count'] / total) * 100, 1)

            print(f"✓ Analyzed {len(topics)} topics")

            return {
                "topics": topics,
                "period": time_period
            }
        except Exception as e:
            print(f"❌ Error analyzing topics: {e}")
            return {"topics": [], "period": time_period}

    # ========================================================================
    # AGENT ORCHESTRATION with LLM
    # ========================================================================

    def chat(self, user_query: str) -> str:
        """
        Main chat interface for the agent.
        Routes to appropriate tools based on query.

        Args:
            user_query: User's question

        Returns:
            Agent's response
        """

        print(f"\n{'='*70}")
        print(f"User: {user_query}")
        print(f"{'='*70}")

        # Simple intent detection (in production, use LLM for this)
        user_query_lower = user_query.lower()

        # Route to appropriate tool based on keywords
        if any(keyword in user_query_lower for keyword in ["search", "find", "show me feedback", "examples of"]):
            # Use Vector Search
            search_terms = user_query.replace("search", "").replace("find", "").replace("show me", "").strip()
            results = self.search_member_feedback(search_terms, limit=5)

            if results:
                response = f"Found {len(results)} relevant feedback instances using semantic search:\n\n"
                for i, result in enumerate(results, 1):
                    response += f"{i}. [{result['channel']}] {result['sentiment_label']} ({result['sentiment_score']:.2f})\n"
                    response += f"   Topic: {result['primary_topic']}\n"
                    response += f"   Text: {result['text'][:150]}...\n\n"
            else:
                response = "No matching feedback found."

        elif "sentiment" in user_query_lower and "trend" in user_query_lower:
            # Get sentiment trends
            result = self.get_sentiment_trends()
            response = f"""Sentiment trends over the last 30 days:

Overall sentiment: {result['summary']['overall_sentiment']:.2f} (on scale -1 to 1)
Total interactions: {result['summary']['total_interactions']:,}
Data points: {result['summary']['data_points']}

Recent trends show {'improving' if result['summary']['overall_sentiment'] > 0 else 'declining'} member sentiment.
            """

        elif "at risk" in user_query_lower or "risk" in user_query_lower:
            # Get at-risk members
            members = self.get_at_risk_members(limit=10)

            if members:
                response = f"Found {len(members)} at-risk members requiring attention:\n\n"
                response += "Top 5 members by risk score:\n\n"

                for i, member in enumerate(members[:5], 1):
                    response += f"{i}. Member {member['member_id']}\n"
                    response += f"   Risk Score: {member['at_risk_score']:.2f}\n"
                    response += f"   Avg Sentiment: {member['avg_sentiment']:.2f}\n"
                    response += f"   Recent Negatives: {member['negative_interaction_count']}\n"
                    response += f"   Preferred Channel: {member.get('preferred_channel', 'unknown')}\n\n"
            else:
                response = "No at-risk members found."

        elif "topic" in user_query_lower or "frustrated about" in user_query_lower:
            # Analyze topic distribution
            result = self.analyze_topic_distribution()

            if result['topics']:
                response = "Topic distribution analysis:\n\n"
                for topic in result['topics'][:5]:
                    response += f"**{topic['primary_topic']}**\n"
                    response += f"  - Total mentions: {topic['total_mentions']}\n"
                    response += f"  - Sentiment: {topic['overall_sentiment']:.2f}\n"
                    response += f"  - Negative: {topic['negative_pct']}%\n"
                    response += f"  - Urgency: {topic.get('overall_urgency', 0):.2f}\n\n"
            else:
                response = "No topic data available."

        else:
            # Use LLM for general queries
            response = self._call_llm(user_query)

        print(f"\nAgent: {response}")
        print(f"{'='*70}\n")

        return response

    def _call_llm(self, query: str) -> str:
        """Call Foundation Model for general queries."""

        try:
            response = self.w.serving_endpoints.query(
                name=self.model_endpoint,
                messages=[
                    ChatMessage(
                        role=ChatMessageRole.USER,
                        content=query
                    )
                ],
                temperature=AGENT["temperature"],
                max_tokens=AGENT["max_tokens"]
            )

            return response.choices[0].message.content

        except Exception as e:
            print(f"⚠️  LLM call failed: {e}")
            return "I encountered an error. Please try rephrasing your question."


def main():
    """Main execution with example queries."""

    print("""
    ╔═══════════════════════════════════════════════════════════╗
    ║   ART Member Listening AI Agent                          ║
    ║   Powered by Vector Search + Foundation Models           ║
    ╚═══════════════════════════════════════════════════════════╝
    """)

    agent = MemberListeningAgent()

    # Run example queries
    example_queries = [
        "Search for feedback about insurance being confusing",
        "What are members most frustrated about?",
        "Show me at-risk members",
        "Analyze insurance topic sentiment"
    ]

    for query in example_queries:
        agent.chat(query)
        print("\n" + "-"*70 + "\n")


if __name__ == "__main__":
    main()
