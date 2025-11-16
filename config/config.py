"""
Configuration for ART Member Listening Intelligence Hub
Contains Databricks workspace-specific settings
"""

# ============================================================================
# Databricks Workspace Configuration
# ============================================================================

# SQL Warehouse for running queries and AI functions
SQL_WAREHOUSE_ID = "4b9b953939869799"

# Vector Search Endpoint
VECTOR_SEARCH_ENDPOINT = "one-env-shared-endpoint-10"

# ============================================================================
# Foundation Model Configuration
# ============================================================================

# Embedding model for Vector Search
EMBEDDING_MODEL = "databricks-bge-large-en"  # BGE embeddings

# LLM endpoints for various tasks
LLM_ENDPOINTS = {
    "chat": "databricks-meta-llama-3-1-405b-instruct",  # For AI Agent
    "fast": "databricks-dbrx-instruct",  # For real-time processing
    "analysis": "databricks-meta-llama-3-1-70b-instruct"  # For analysis tasks
}

# ============================================================================
# Unity Catalog Configuration
# ============================================================================

CATALOG = "art_member_listening"
SCHEMAS = {
    "bronze": f"{CATALOG}.bronze",
    "silver": f"{CATALOG}.silver",
    "gold": f"{CATALOG}.gold",
    "ml_models": f"{CATALOG}.ml_models"
}

# Tables
TABLES = {
    # Bronze layer
    "portal_events_bronze": f"{SCHEMAS['bronze']}.portal_events",
    "calls_bronze": f"{SCHEMAS['bronze']}.call_transcripts",
    "emails_bronze": f"{SCHEMAS['bronze']}.emails",
    "chats_bronze": f"{SCHEMAS['bronze']}.chats",
    "surveys_bronze": f"{SCHEMAS['bronze']}.survey_responses",
    "members_bronze": f"{SCHEMAS['bronze']}.member_profiles",

    # Silver layer
    "interactions_analyzed": f"{SCHEMAS['silver']}.interactions_analyzed",
    "topic_extraction": f"{SCHEMAS['silver']}.topic_extraction",

    # Gold layer
    "member_360": f"{SCHEMAS['gold']}.member_360_view",
    "topic_trends": f"{SCHEMAS['gold']}.topic_trends_daily",
    "sentiment_by_channel": f"{SCHEMAS['gold']}.sentiment_by_channel",
    "at_risk_members": f"{SCHEMAS['gold']}.at_risk_members",
    "executive_kpis": f"{SCHEMAS['gold']}.executive_kpis"
}

# Vector Search Index
VECTOR_SEARCH_INDEX = f"{SCHEMAS['gold']}.member_feedback_vector_index"

# ============================================================================
# Processing Configuration
# ============================================================================

# Real-Time Mode settings
REALTIME_MODE = {
    "enabled": True,
    "async_checkpoint": True,
    "processing_time": "5 seconds",  # Trigger interval
    "target_latency_ms": 300  # Target processing latency
}

# Checkpoint locations
CHECKPOINT_LOCATIONS = {
    "portal_events": "/tmp/checkpoints/portal_events",
    "bronze_to_silver": "/tmp/checkpoints/bronze_to_silver",
    "silver_to_gold": "/tmp/checkpoints/silver_to_gold"
}

# ============================================================================
# Data Generation Configuration (for demo)
# ============================================================================

DATA_GENERATION = {
    "landing_zone": "/dbfs/mnt/landing",
    "num_calls": 10000,
    "num_email_threads": 5000,
    "num_surveys": 3000,
    "num_chat_sessions": 7500,
    "num_members": 10000
}

# ============================================================================
# Dashboard Configuration
# ============================================================================

DASHBOARD = {
    "host": "0.0.0.0",
    "port": 8050,
    "refresh_interval_seconds": 10,
    "debug": True
}

# ============================================================================
# AI Agent Configuration
# ============================================================================

AGENT = {
    "model_endpoint": LLM_ENDPOINTS["chat"],
    "temperature": 0.3,
    "max_tokens": 1000,
    "vector_search_limit": 10  # Number of results for semantic search
}
