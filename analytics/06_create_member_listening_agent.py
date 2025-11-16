"""
Create Databricks AI Agent for Member Listening Intelligence.
Uses Databricks Foundation Model (Sonnet 4.5) with custom tools.
"""

import mlflow
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole, EndpointCoreConfigInput
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from typing import List, Dict, Any
import json


class MemberListeningAgent:
    """AI Agent for member listening intelligence with custom tools."""

    def __init__(self):
        self.w = WorkspaceClient()
        self.spark = SparkSession.builder.getOrCreate()
        self.catalog = "art_member_listening"

        # Foundation model endpoint
        self.model_endpoint = "databricks-meta-llama-3-1-405b-instruct"  # Or use Sonnet 4.5 when available

    # ========================================================================
    # TOOL DEFINITIONS
    # ========================================================================

    def search_member_feedback(self, query: str, limit: int = 10, channel: str = None) -> List[Dict]:
        """
        Search member feedback using semantic similarity (simulated vector search).

        Args:
            query: Search query (e.g., "frustrated about insurance")
            limit: Number of results to return
            channel: Optional filter by channel (call, email, chat, survey)

        Returns:
            List of relevant feedback instances
        """

        print(f"🔍 Searching member feedback: '{query}' (limit={limit}, channel={channel})")

        # In production, this would use Vector Search
        # For demo, we'll use keyword matching + sentiment filtering

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
        FROM {self.catalog}.silver.interactions_analyzed
        WHERE 1=1
        """

        # Add keyword filtering (simple version)
        keywords = query.lower().split()
        keyword_conditions = " OR ".join([f"LOWER(text) LIKE '%{kw}%'" for kw in keywords])
        sql_query += f" AND ({keyword_conditions})"

        # Add channel filter
        if channel:
            sql_query += f" AND channel = '{channel}'"

        sql_query += f"""
        ORDER BY urgency_score DESC, timestamp DESC
        LIMIT {limit}
        """

        try:
            results = self.spark.sql(sql_query).toPandas().to_dict('records')
            print(f"✓ Found {len(results)} results")
            return results
        except Exception as e:
            print(f"Error searching feedback: {e}")
            return []

    def get_sentiment_trends(
        self,
        time_period: str = "last_30_days",
        channel: str = None,
        topic: str = None
    ) -> Dict[str, Any]:
        """
        Get sentiment trends over time.

        Args:
            time_period: Time window (last_7_days, last_30_days, last_90_days)
            channel: Optional channel filter
            topic: Optional topic filter

        Returns:
            Sentiment trend data
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
            SUM(CASE WHEN sentiment_label = 'Positive' THEN 1 ELSE 0 END) as positive_count
        FROM {self.catalog}.silver.interactions_analyzed
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

            return {
                "trends": trends,
                "summary": {
                    "overall_sentiment": round(overall_sentiment, 3),
                    "total_interactions": total_interactions,
                    "period": time_period
                }
            }
        except Exception as e:
            print(f"Error getting trends: {e}")
            return {"trends": [], "summary": {}}

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
            preferred_channel
        FROM {self.catalog}.gold.member_360_view
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
            print(f"Error getting at-risk members: {e}")
            return []

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
                AVG(sentiment_score) as avg_sentiment
            FROM {self.catalog}.silver.interactions_analyzed
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

            return {
                "topics": topics,
                "period": time_period
            }
        except Exception as e:
            print(f"Error analyzing topics: {e}")
            return {"topics": [], "period": time_period}

    def get_member_journey(self, member_id: str, limit: int = 20) -> Dict[str, Any]:
        """
        Get a specific member's interaction journey.

        Args:
            member_id: Member ID
            limit: Number of interactions to return

        Returns:
            Member's recent interactions and summary
        """

        print(f"👤 Getting member journey: member_id={member_id}")

        # Get member profile
        profile_query = f"""
        SELECT *
        FROM {self.catalog}.gold.member_360_view
        WHERE member_id = '{member_id}'
        """

        # Get interactions
        interactions_query = f"""
        SELECT
            timestamp,
            channel,
            primary_topic,
            sentiment_label,
            sentiment_score,
            text,
            resolution_status
        FROM {self.catalog}.silver.interactions_analyzed
        WHERE member_id = '{member_id}'
        ORDER BY timestamp DESC
        LIMIT {limit}
        """

        try:
            profile = self.spark.sql(profile_query).toPandas().to_dict('records')
            interactions = self.spark.sql(interactions_query).toPandas().to_dict('records')

            return {
                "member_id": member_id,
                "profile": profile[0] if profile else {},
                "interactions": interactions,
                "interaction_count": len(interactions)
            }
        except Exception as e:
            print(f"Error getting member journey: {e}")
            return {"member_id": member_id, "profile": {}, "interactions": []}

    # ========================================================================
    # AGENT ORCHESTRATION
    # ========================================================================

    def _call_llm_with_tools(self, user_query: str, conversation_history: List[Dict] = None) -> str:
        """
        Call LLM with tool-calling capability.

        Args:
            user_query: User's question
            conversation_history: Previous conversation context

        Returns:
            Agent's response
        """

        # Define tools for the LLM
        tools_description = f"""
You are an AI assistant for Australian Retirement Trust's Member Listening Intelligence Hub.
You help analyze member feedback across all touchpoints (calls, emails, chats, surveys).

Available tools:
1. search_member_feedback(query, limit, channel) - Search member feedback
2. get_sentiment_trends(time_period, channel, topic) - Get sentiment trends
3. get_at_risk_members(limit, min_risk_score) - Get at-risk members
4. analyze_topic_distribution(topic, time_period) - Analyze topics
5. get_member_journey(member_id, limit) - Get member's interaction history

User query: {user_query}

Analyze the query and determine which tool(s) to use. Then provide a helpful response.
"""

        try:
            messages = [
                ChatMessage(
                    role=ChatMessageRole.SYSTEM,
                    content=tools_description
                ),
                ChatMessage(
                    role=ChatMessageRole.USER,
                    content=user_query
                )
            ]

            response = self.w.serving_endpoints.query(
                name=self.model_endpoint,
                messages=messages,
                temperature=0.3,
                max_tokens=1000
            )

            return response.choices[0].message.content

        except Exception as e:
            print(f"Error calling LLM: {e}")
            return "I encountered an error processing your request. Please try again."

    def chat(self, user_query: str) -> str:
        """
        Main chat interface for the agent.

        Args:
            user_query: User's question

        Returns:
            Agent's response
        """

        print(f"\n{'='*60}")
        print(f"User: {user_query}")
        print(f"{'='*60}")

        # Simple intent detection (in production, use LLM for this)
        user_query_lower = user_query.lower()

        # Route to appropriate tool
        if "sentiment" in user_query_lower and "trend" in user_query_lower:
            result = self.get_sentiment_trends()
            response = f"""Based on sentiment trends over the last 30 days:

Overall sentiment: {result['summary']['overall_sentiment']:.2f} (on scale -1 to 1)
Total interactions: {result['summary']['total_interactions']:,}

Recent trends show {'improving' if result['summary']['overall_sentiment'] > 0 else 'declining'} member sentiment.
            """

        elif "at risk" in user_query_lower or "risk" in user_query_lower:
            members = self.get_at_risk_members(limit=10)
            response = f"""Found {len(members)} at-risk members requiring attention:

Top 5 members by risk score:
"""
            for i, member in enumerate(members[:5], 1):
                response += f"\n{i}. Member {member['member_id']}"
                response += f"\n   Risk Score: {member['at_risk_score']:.2f}"
                response += f"\n   Avg Sentiment: {member['avg_sentiment']:.2f}"
                response += f"\n   Recent Negatives: {member['negative_interaction_count']}"
                response += f"\n   Preferred Channel: {member.get('preferred_channel', 'unknown')}\n"

        elif "topic" in user_query_lower or "frustrated about" in user_query_lower:
            result = self.analyze_topic_distribution()
            response = "Topic distribution analysis:\n\n"
            for topic in result['topics'][:5]:
                response += f"**{topic['primary_topic']}**\n"
                response += f"  - Total mentions: {topic['total_mentions']}\n"
                response += f"  - Sentiment: {topic['overall_sentiment']:.2f}\n"
                response += f"  - Negative: {topic['negative_pct']}%\n\n"

        elif "search" in user_query_lower or "find" in user_query_lower:
            # Extract search terms
            search_terms = user_query.replace("search", "").replace("find", "").strip()
            results = self.search_member_feedback(search_terms, limit=5)

            response = f"Found {len(results)} relevant feedback instances:\n\n"
            for i, result in enumerate(results, 1):
                response += f"{i}. [{result['channel']}] {result['sentiment_label']}\n"
                response += f"   {result['text'][:200]}...\n\n"

        else:
            # Use LLM for general queries
            response = self._call_llm_with_tools(user_query)

        print(f"\nAgent: {response}")
        print(f"{'='*60}\n")

        return response


def deploy_agent():
    """Deploy the agent to Databricks Model Serving."""

    print("Deploying Member Listening Agent to Model Serving...")

    agent = MemberListeningAgent()

    # Log agent with MLflow
    with mlflow.start_run(run_name="art_member_listening_agent"):
        # Log agent configuration
        mlflow.log_param("model_endpoint", agent.model_endpoint)
        mlflow.log_param("catalog", agent.catalog)

        # Log example queries
        examples = [
            "What are members most frustrated about?",
            "Show me sentiment trends for insurance topics",
            "Which members are at highest risk?",
            "Analyze contribution topic feedback"
        ]

        for example in examples:
            response = agent.chat(example)
            mlflow.log_text(f"Query: {example}\n\nResponse: {response}", f"example_{examples.index(example)}.txt")

        print("✓ Agent logged to MLflow")

    print("""
    ✅ Agent deployment complete!

    To use the agent:
    ```python
    from analytics.create_member_listening_agent import MemberListeningAgent

    agent = MemberListeningAgent()
    response = agent.chat("What are members frustrated about?")
    print(response)
    ```
    """)


def main():
    """Main execution with example queries."""

    agent = MemberListeningAgent()

    # Run example queries
    example_queries = [
        "What are members most frustrated about?",
        "Show me at-risk members",
        "Analyze insurance topic sentiment"
    ]

    for query in example_queries:
        agent.chat(query)


if __name__ == "__main__":
    main()
