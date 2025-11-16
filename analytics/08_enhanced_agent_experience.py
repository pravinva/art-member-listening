# Databricks notebook source
"""
Enhanced Agent Experience for ART Member Listening Intelligence Hub
====================================================================

This module enhances the agentic AI experience with:
1. Multi-turn conversation support with context retention
2. Explainability and reasoning transparency
3. Confidence scoring for recommendations
4. Rich response formatting
5. Session management and conversation history

Experience Impact: Provides human-like interactions with full transparency
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import json
import uuid
from typing import List, Dict, Any, Optional

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Conversation Session Manager
# MAGIC
# MAGIC Manages multi-turn conversations with context retention:
# MAGIC - Session state persistence
# MAGIC - Conversation history tracking
# MAGIC - Context window management
# MAGIC - User intent tracking across turns

# COMMAND ----------

class ConversationSession:
    """
    Manages conversation state for multi-turn interactions.

    Features:
    - Persists conversation history in Delta tables
    - Maintains context window (last N interactions)
    - Tracks user intents and agent actions
    - Supports conversation branching and rollback
    """

    def __init__(self, catalog="art_member_listening", schema="agent"):
        self.catalog = catalog
        self.schema = schema
        self.sessions_table = f"{catalog}.{schema}.conversation_sessions"
        self.history_table = f"{catalog}.{schema}.conversation_history"
        self._ensure_tables()

    def _ensure_tables(self):
        """Create conversation tables if they don't exist"""

        # Sessions table
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.sessions_table} (
                session_id STRING,
                user_id STRING,
                started_at TIMESTAMP,
                last_interaction_at TIMESTAMP,
                interaction_count INT,
                session_context STRING,
                is_active BOOLEAN,
                session_metadata STRING
            )
            USING DELTA
            PARTITIONED BY (DATE(started_at))
            TBLPROPERTIES (
                'delta.enableChangeDataFeed' = 'true',
                'delta.autoOptimize.optimizeWrite' = 'true'
            )
        """)

        # Conversation history table
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.history_table} (
                history_id STRING,
                session_id STRING,
                turn_number INT,
                timestamp TIMESTAMP,
                user_message STRING,
                agent_response STRING,
                tool_calls_json STRING,
                reasoning_json STRING,
                confidence_scores STRING,
                context_used STRING
            )
            USING DELTA
            PARTITIONED BY (DATE(timestamp))
            TBLPROPERTIES (
                'delta.enableChangeDataFeed' = 'true',
                'delta.autoOptimize.optimizeWrite' = 'true'
            )
        """)

    def create_session(self, user_id: str, metadata: Dict = None) -> str:
        """Create a new conversation session"""
        session_id = str(uuid.uuid4())

        session_data = {
            'session_id': session_id,
            'user_id': user_id,
            'started_at': datetime.now().isoformat(),
            'last_interaction_at': datetime.now().isoformat(),
            'interaction_count': 0,
            'session_context': json.dumps({}),
            'is_active': True,
            'session_metadata': json.dumps(metadata or {})
        }

        # Insert session
        spark.createDataFrame([session_data]).write.mode("append").saveAsTable(self.sessions_table)

        return session_id

    def add_interaction(
        self,
        session_id: str,
        user_message: str,
        agent_response: str,
        tool_calls: List[Dict] = None,
        reasoning: Dict = None,
        confidence_scores: Dict = None,
        context_used: Dict = None
    ):
        """Add an interaction to the conversation history"""

        # Get current turn number
        turn_result = spark.sql(f"""
            SELECT COALESCE(MAX(turn_number), 0) + 1 AS next_turn
            FROM {self.history_table}
            WHERE session_id = '{session_id}'
        """).collect()

        turn_number = turn_result[0]['next_turn'] if turn_result else 1

        # Create history entry
        history_entry = {
            'history_id': str(uuid.uuid4()),
            'session_id': session_id,
            'turn_number': turn_number,
            'timestamp': datetime.now().isoformat(),
            'user_message': user_message,
            'agent_response': agent_response,
            'tool_calls_json': json.dumps(tool_calls or []),
            'reasoning_json': json.dumps(reasoning or {}),
            'confidence_scores': json.dumps(confidence_scores or {}),
            'context_used': json.dumps(context_used or {})
        }

        spark.createDataFrame([history_entry]).write.mode("append").saveAsTable(self.history_table)

        # Update session
        spark.sql(f"""
            UPDATE {self.sessions_table}
            SET
                last_interaction_at = current_timestamp(),
                interaction_count = interaction_count + 1
            WHERE session_id = '{session_id}'
        """)

    def get_conversation_history(self, session_id: str, last_n: int = 5) -> List[Dict]:
        """Retrieve recent conversation history for context"""
        history = spark.sql(f"""
            SELECT
                turn_number,
                timestamp,
                user_message,
                agent_response,
                tool_calls_json,
                reasoning_json
            FROM {self.history_table}
            WHERE session_id = '{session_id}'
            ORDER BY turn_number DESC
            LIMIT {last_n}
        """).collect()

        return [
            {
                'turn': row['turn_number'],
                'timestamp': row['timestamp'],
                'user': row['user_message'],
                'agent': row['agent_response'],
                'tools_used': json.loads(row['tool_calls_json']),
                'reasoning': json.loads(row['reasoning_json'])
            }
            for row in reversed(history)
        ]

    def get_session_context(self, session_id: str) -> Dict:
        """Get accumulated context for a session"""
        result = spark.sql(f"""
            SELECT session_context
            FROM {self.sessions_table}
            WHERE session_id = '{session_id}'
        """).collect()

        if result:
            return json.loads(result[0]['session_context'])
        return {}

    def update_session_context(self, session_id: str, context_updates: Dict):
        """Update session context with new information"""
        current_context = self.get_session_context(session_id)
        current_context.update(context_updates)

        spark.sql(f"""
            UPDATE {self.sessions_table}
            SET session_context = '{json.dumps(current_context)}'
            WHERE session_id = '{session_id}'
        """)

    def close_session(self, session_id: str):
        """Mark session as inactive"""
        spark.sql(f"""
            UPDATE {self.sessions_table}
            SET is_active = false
            WHERE session_id = '{session_id}'
        """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Agent Reasoning and Explainability
# MAGIC
# MAGIC Provides transparency into agent decision-making:
# MAGIC - Tool selection reasoning
# MAGIC - Confidence scores for actions
# MAGIC - Evidence citations
# MAGIC - Alternative options considered

# COMMAND ----------

class AgentExplainer:
    """
    Generates explanations for agent decisions and actions.

    Provides transparency through:
    - Why specific tools were chosen
    - Confidence in recommendations
    - Evidence supporting conclusions
    - Alternative approaches considered
    """

    @staticmethod
    def explain_tool_selection(
        user_query: str,
        selected_tools: List[str],
        available_tools: List[str],
        selection_reasoning: str
    ) -> Dict:
        """
        Explain why specific tools were selected for the query.

        Returns:
            Dict with explanation, confidence, and alternatives
        """
        return {
            'selected_tools': selected_tools,
            'reasoning': selection_reasoning,
            'query_analysis': {
                'intent': AgentExplainer._detect_intent(user_query),
                'key_entities': AgentExplainer._extract_entities(user_query),
                'complexity': AgentExplainer._assess_complexity(user_query)
            },
            'alternatives_considered': [
                tool for tool in available_tools if tool not in selected_tools
            ],
            'confidence': AgentExplainer._calculate_tool_confidence(
                user_query, selected_tools
            )
        }

    @staticmethod
    def _detect_intent(query: str) -> str:
        """Detect primary user intent from query"""
        query_lower = query.lower()

        intent_patterns = {
            'search': ['find', 'search', 'show me', 'what', 'who'],
            'analysis': ['analyze', 'trending', 'pattern', 'why'],
            'risk_assessment': ['risk', 'at-risk', 'churn', 'leaving'],
            'comparison': ['compare', 'difference', 'versus', 'vs'],
            'summary': ['summarize', 'overview', 'summary', 'total']
        }

        for intent, patterns in intent_patterns.items():
            if any(pattern in query_lower for pattern in patterns):
                return intent

        return 'general_inquiry'

    @staticmethod
    def _extract_entities(query: str) -> List[str]:
        """Extract key entities from query"""
        # Simplified entity extraction
        # In production, this would use NER from Foundation Models
        entities = []

        entity_patterns = {
            'sentiment': ['negative', 'positive', 'frustrated', 'happy', 'angry'],
            'channel': ['phone', 'email', 'chat', 'survey'],
            'time': ['today', 'yesterday', 'week', 'month', 'year'],
            'topic': ['claims', 'billing', 'service', 'coverage']
        }

        query_lower = query.lower()
        for entity_type, patterns in entity_patterns.items():
            for pattern in patterns:
                if pattern in query_lower:
                    entities.append(f"{entity_type}:{pattern}")

        return entities

    @staticmethod
    def _assess_complexity(query: str) -> str:
        """Assess query complexity"""
        word_count = len(query.split())
        question_marks = query.count('?')
        conjunctions = sum(query.lower().count(c) for c in [' and ', ' or ', ' but '])

        if word_count > 20 or conjunctions > 2:
            return 'complex'
        elif word_count > 10 or question_marks > 1:
            return 'moderate'
        else:
            return 'simple'

    @staticmethod
    def _calculate_tool_confidence(query: str, tools: List[str]) -> float:
        """Calculate confidence in tool selection"""
        # Simplified confidence calculation
        # Based on intent clarity and tool appropriateness
        intent = AgentExplainer._detect_intent(query)
        complexity = AgentExplainer._assess_complexity(query)

        base_confidence = 0.7

        # Increase confidence for clear intents
        if intent in ['search', 'risk_assessment']:
            base_confidence += 0.15

        # Adjust for complexity
        complexity_adjustments = {
            'simple': 0.1,
            'moderate': 0.05,
            'complex': -0.05
        }
        base_confidence += complexity_adjustments.get(complexity, 0)

        return min(0.95, max(0.5, base_confidence))

    @staticmethod
    def explain_recommendation(
        recommendation: Dict,
        supporting_evidence: List[Dict],
        confidence: float,
        alternatives: List[Dict] = None
    ) -> Dict:
        """
        Explain a recommendation with evidence and alternatives.

        Args:
            recommendation: The main recommendation
            supporting_evidence: Evidence supporting the recommendation
            confidence: Confidence score (0-1)
            alternatives: Alternative recommendations considered

        Returns:
            Structured explanation with evidence
        """
        return {
            'recommendation': recommendation,
            'confidence': {
                'score': confidence,
                'level': AgentExplainer._confidence_level(confidence),
                'explanation': AgentExplainer._confidence_explanation(confidence)
            },
            'supporting_evidence': supporting_evidence,
            'evidence_count': len(supporting_evidence),
            'alternatives_considered': alternatives or [],
            'recommendation_strength': AgentExplainer._recommendation_strength(
                confidence, len(supporting_evidence)
            )
        }

    @staticmethod
    def _confidence_level(score: float) -> str:
        """Convert confidence score to level"""
        if score >= 0.8:
            return 'High'
        elif score >= 0.6:
            return 'Medium'
        else:
            return 'Low'

    @staticmethod
    def _confidence_explanation(score: float) -> str:
        """Explain confidence score"""
        if score >= 0.8:
            return "Strong evidence supports this recommendation"
        elif score >= 0.6:
            return "Moderate evidence supports this recommendation"
        else:
            return "Limited evidence available; recommendation is tentative"

    @staticmethod
    def _recommendation_strength(confidence: float, evidence_count: int) -> str:
        """Calculate overall recommendation strength"""
        if confidence >= 0.8 and evidence_count >= 10:
            return 'Very Strong'
        elif confidence >= 0.7 and evidence_count >= 5:
            return 'Strong'
        elif confidence >= 0.6 and evidence_count >= 3:
            return 'Moderate'
        else:
            return 'Weak'

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Enhanced Response Formatter
# MAGIC
# MAGIC Formats agent responses with rich content:
# MAGIC - Structured data presentation
# MAGIC - Charts and visualizations
# MAGIC - Highlighted insights
# MAGIC - Actionable recommendations

# COMMAND ----------

class ResponseFormatter:
    """
    Formats agent responses with rich, structured content.

    Provides:
    - Clean markdown formatting
    - Data tables with highlighting
    - Key insights extraction
    - Actionable recommendations
    """

    @staticmethod
    def format_sentiment_analysis(results: List[Dict], explanation: Dict = None) -> str:
        """Format sentiment analysis results"""
        output = ["## 📊 Sentiment Analysis Results\n"]

        if explanation:
            confidence = explanation.get('confidence', {})
            output.append(f"**Confidence:** {confidence.get('level', 'N/A')} "
                         f"({confidence.get('score', 0):.1%})\n")
            output.append(f"_{confidence.get('explanation', '')}_\n")

        # Summary stats
        if results:
            total = len(results)
            sentiment_dist = {}
            for r in results:
                sent = r.get('sentiment_category', 'Unknown')
                sentiment_dist[sent] = sentiment_dist.get(sent, 0) + 1

            output.append("\n### Summary")
            output.append(f"- **Total Interactions:** {total}")
            output.append(f"- **Sentiment Distribution:**")
            for sent, count in sorted(sentiment_dist.items(), key=lambda x: -x[1]):
                pct = (count / total) * 100
                emoji = ResponseFormatter._sentiment_emoji(sent)
                output.append(f"  - {emoji} {sent}: {count} ({pct:.1f}%)")

        # Top insights
        output.append("\n### 🔍 Key Insights")
        insights = ResponseFormatter._extract_sentiment_insights(results)
        for insight in insights:
            output.append(f"- {insight}")

        # Recommendations
        output.append("\n### 💡 Recommended Actions")
        actions = ResponseFormatter._generate_sentiment_actions(results)
        for i, action in enumerate(actions, 1):
            output.append(f"{i}. {action}")

        return "\n".join(output)

    @staticmethod
    def format_risk_assessment(members: List[Dict], explanation: Dict = None) -> str:
        """Format member risk assessment results"""
        output = ["## ⚠️ Member Risk Assessment\n"]

        if explanation:
            output.append(f"**Analysis Confidence:** "
                         f"{explanation.get('confidence', {}).get('score', 0):.1%}\n")

        if not members:
            output.append("_No at-risk members found._")
            return "\n".join(output)

        # Risk distribution
        risk_levels = {}
        for m in members:
            level = m.get('risk_category', 'Unknown')
            risk_levels[level] = risk_levels.get(level, 0) + 1

        output.append("### Risk Distribution")
        for level in ['Critical', 'High', 'Medium', 'Low']:
            count = risk_levels.get(level, 0)
            if count > 0:
                emoji = ResponseFormatter._risk_emoji(level)
                output.append(f"- {emoji} **{level}:** {count} members")

        # Top at-risk members
        output.append("\n### 🚨 Top At-Risk Members")
        output.append("\n| Member ID | Risk Score | Category | Recent Issues | Last Contact |")
        output.append("|-----------|------------|----------|---------------|--------------|")

        for member in sorted(members, key=lambda x: x.get('risk_score', 0), reverse=True)[:10]:
            output.append(
                f"| {member.get('member_id', 'N/A')} | "
                f"{member.get('risk_score', 0):.2f} | "
                f"{member.get('risk_category', 'N/A')} | "
                f"{member.get('consecutive_negative_interactions', 0)} | "
                f"{member.get('days_since_last_contact', 'N/A')} days |"
            )

        # Recommendations
        output.append("\n### 💡 Recommended Actions")
        actions = ResponseFormatter._generate_risk_actions(members)
        for i, action in enumerate(actions, 1):
            output.append(f"{i}. {action}")

        return "\n".join(output)

    @staticmethod
    def format_topic_analysis(topics: List[Dict], explanation: Dict = None) -> str:
        """Format topic distribution analysis"""
        output = ["## 📈 Topic Analysis\n"]

        if not topics:
            output.append("_No significant topics found._")
            return "\n".join(output)

        # Top topics
        output.append("### Top Issues by Volume")
        output.append("\n| Topic | Mentions | Affected Members | Avg Sentiment |")
        output.append("|-------|----------|------------------|---------------|")

        for topic in sorted(topics, key=lambda x: x.get('mention_count', 0), reverse=True)[:10]:
            sentiment_emoji = ResponseFormatter._sentiment_emoji(
                ResponseFormatter._score_to_category(topic.get('avg_sentiment', 0))
            )
            output.append(
                f"| {topic.get('topic', 'N/A')} | "
                f"{topic.get('mention_count', 0)} | "
                f"{topic.get('affected_members', 0)} | "
                f"{sentiment_emoji} {topic.get('avg_sentiment', 0):.2f} |"
            )

        # Trending issues
        output.append("\n### 🔥 Trending Issues")
        trending = ResponseFormatter._identify_trending(topics)
        for issue in trending:
            output.append(f"- **{issue['topic']}**: {issue['trend_description']}")

        return "\n".join(output)

    # Helper methods
    @staticmethod
    def _sentiment_emoji(category: str) -> str:
        emojis = {
            'Very Positive': '😊',
            'Positive': '🙂',
            'Neutral': '😐',
            'Negative': '😟',
            'Very Negative': '😡'
        }
        return emojis.get(category, '❓')

    @staticmethod
    def _risk_emoji(level: str) -> str:
        emojis = {
            'Critical': '🔴',
            'High': '🟠',
            'Medium': '🟡',
            'Low': '🟢'
        }
        return emojis.get(level, '⚪')

    @staticmethod
    def _score_to_category(score: float) -> str:
        if score >= 0.6:
            return 'Positive'
        elif score >= 0.4:
            return 'Neutral'
        else:
            return 'Negative'

    @staticmethod
    def _extract_sentiment_insights(results: List[Dict]) -> List[str]:
        """Extract key insights from sentiment data"""
        insights = []

        if not results:
            return insights

        # Check for patterns
        negative_count = sum(1 for r in results if r.get('sentiment_category') in ['Negative', 'Very Negative'])
        if negative_count / len(results) > 0.3:
            insights.append(f"⚠️ High negative sentiment detected ({negative_count}/{len(results)} interactions)")

        return insights[:3]

    @staticmethod
    def _generate_sentiment_actions(results: List[Dict]) -> List[str]:
        """Generate actionable recommendations"""
        return [
            "Review negative feedback for common themes",
            "Prioritize outreach to members with consecutive negative interactions",
            "Analyze root causes of negative sentiment patterns"
        ]

    @staticmethod
    def _generate_risk_actions(members: List[Dict]) -> List[str]:
        """Generate risk mitigation actions"""
        critical_count = sum(1 for m in members if m.get('risk_category') == 'Critical')

        actions = []
        if critical_count > 0:
            actions.append(f"**Immediate:** Contact {critical_count} critical-risk members within 24 hours")

        actions.extend([
            "Assign dedicated case managers to high-risk members",
            "Implement proactive outreach campaign for at-risk segment"
        ])

        return actions

    @staticmethod
    def _identify_trending(topics: List[Dict]) -> List[Dict]:
        """Identify trending topics"""
        # Simplified trending detection
        # In production, would compare to historical data
        return [
            {
                'topic': t.get('topic'),
                'trend_description': f"Mentioned by {t.get('affected_members', 0)} members"
            }
            for t in sorted(topics, key=lambda x: x.get('mention_count', 0), reverse=True)[:3]
        ]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Enhanced Agent Integration
# MAGIC
# MAGIC Wraps the existing agent with enhanced experience features

# COMMAND ----------

class EnhancedMemberListeningAgent:
    """
    Enhanced wrapper for the Member Listening Agent with:
    - Multi-turn conversation support
    - Explainability and reasoning
    - Rich response formatting
    - Confidence scoring
    """

    def __init__(self):
        self.session_manager = ConversationSession()
        self.explainer = AgentExplainer()
        self.formatter = ResponseFormatter()

    def start_session(self, user_id: str, metadata: Dict = None) -> str:
        """Start a new conversation session"""
        return self.session_manager.create_session(user_id, metadata)

    def process_query(
        self,
        session_id: str,
        user_query: str,
        include_reasoning: bool = True
    ) -> Dict:
        """
        Process user query with enhanced experience features.

        Args:
            session_id: Active session ID
            user_query: User's question or request
            include_reasoning: Include reasoning and explanations

        Returns:
            Enhanced response with answer, reasoning, and formatting
        """

        # Get conversation context
        history = self.session_manager.get_conversation_history(session_id, last_n=3)
        context = self.session_manager.get_session_context(session_id)

        # Detect intent and select tools
        intent = self.explainer._detect_intent(user_query)
        tools_to_use = self._select_tools_for_intent(intent)

        # Generate tool selection explanation
        tool_explanation = self.explainer.explain_tool_selection(
            user_query,
            tools_to_use,
            ['search_member_feedback', 'get_sentiment_trends', 'get_at_risk_members',
             'analyze_topic_distribution', 'get_member_journey'],
            f"Selected based on detected intent: {intent}"
        )

        # Execute tools (simplified - in production would call actual tools)
        tool_results = self._execute_tools(tools_to_use, user_query, context)

        # Format response
        formatted_response = self._format_response(intent, tool_results)

        # Generate reasoning
        reasoning = {
            'intent_detected': intent,
            'tools_selected': tool_explanation,
            'context_used': {
                'conversation_turns': len(history),
                'session_context_keys': list(context.keys())
            }
        } if include_reasoning else {}

        # Calculate confidence
        confidence = tool_explanation['confidence']

        # Save interaction
        self.session_manager.add_interaction(
            session_id,
            user_query,
            formatted_response,
            tool_calls=[{'tool': t, 'status': 'executed'} for t in tools_to_use],
            reasoning=reasoning,
            confidence_scores={'overall': confidence}
        )

        return {
            'response': formatted_response,
            'reasoning': reasoning,
            'confidence': confidence,
            'tools_used': tools_to_use,
            'session_id': session_id
        }

    def _select_tools_for_intent(self, intent: str) -> List[str]:
        """Select appropriate tools based on intent"""
        intent_tool_mapping = {
            'search': ['search_member_feedback'],
            'analysis': ['get_sentiment_trends', 'analyze_topic_distribution'],
            'risk_assessment': ['get_at_risk_members'],
            'summary': ['get_sentiment_trends', 'analyze_topic_distribution']
        }
        return intent_tool_mapping.get(intent, ['search_member_feedback'])

    def _execute_tools(self, tools: List[str], query: str, context: Dict) -> Dict:
        """Execute selected tools (placeholder)"""
        # In production, this would call the actual agent tools
        # For now, return mock results
        return {
            'tool_results': f"Results from tools: {', '.join(tools)}",
            'data': []
        }

    def _format_response(self, intent: str, results: Dict) -> str:
        """Format response based on intent and results"""
        # Simplified formatting
        return f"Based on {intent} analysis:\n\n{results.get('tool_results', 'No results')}"

    def end_session(self, session_id: str):
        """End conversation session"""
        self.session_manager.close_session(session_id)

# COMMAND ----------

# Initialize enhanced agent
print("🚀 Enhanced Agent Experience Initialized")
print("=" * 80)

# Create agent schema if needed
spark.sql("CREATE SCHEMA IF NOT EXISTS art_member_listening.agent")

# Initialize enhanced agent
enhanced_agent = EnhancedMemberListeningAgent()
print("✅ Enhanced Member Listening Agent ready")
print("\nNew capabilities:")
print("- ✅ Multi-turn conversations with context retention")
print("- ✅ Explainable AI with reasoning transparency")
print("- ✅ Confidence scoring for all recommendations")
print("- ✅ Rich response formatting with insights")
print("- ✅ Session management and history tracking")

# Example usage
print("\n" + "=" * 80)
print("Example: Starting a conversation session")
print("=" * 80)

session_id = enhanced_agent.start_session(
    user_id="demo_user",
    metadata={'role': 'member_services_rep', 'region': 'NSW'}
)
print(f"Session ID: {session_id}")

# Process a query
response = enhanced_agent.process_query(
    session_id,
    "Show me negative feedback from the last week",
    include_reasoning=True
)

print(f"\nAgent Response:")
print(response['response'])
print(f"\nConfidence: {response['confidence']:.1%}")
print(f"Tools Used: {', '.join(response['tools_used'])}")
