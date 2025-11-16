-- ============================================================================
-- ART Member Listening Intelligence Hub - Unity Catalog Setup
-- ============================================================================
-- Purpose: Create catalog, schemas, and governance structure for the demo
-- Run in: Databricks SQL Warehouse or Notebook with SQL
-- ============================================================================

-- Create main catalog
CREATE CATALOG IF NOT EXISTS art_member_listening
COMMENT 'Member listening intelligence hub for Australian Retirement Trust';

USE CATALOG art_member_listening;

-- ============================================================================
-- BRONZE LAYER - Raw data from all sources
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS bronze
COMMENT 'Raw data ingested from all member touchpoints';

-- Portal events (Zerobus streaming)
CREATE TABLE IF NOT EXISTS bronze.portal_events (
  event_id STRING NOT NULL,
  member_id STRING,
  session_id STRING,
  timestamp TIMESTAMP NOT NULL,
  event_type STRING,
  page_url STRING,
  search_query STRING,
  time_on_page_seconds INT,
  referrer_url STRING,
  device_type STRING,
  ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
CLUSTER BY (timestamp, member_id)
COMMENT 'Real-time portal activity events from Zerobus';

-- Call center interactions
CREATE TABLE IF NOT EXISTS bronze.call_transcripts (
  call_id STRING NOT NULL,
  member_id STRING,
  agent_id STRING,
  timestamp TIMESTAMP NOT NULL,
  duration_seconds INT,
  call_type STRING,
  resolution_status STRING,
  transcript STRING,
  call_metadata MAP<STRING, STRING>,
  ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
CLUSTER BY (timestamp, member_id)
COMMENT 'Call center interaction transcripts';

-- Email interactions
CREATE TABLE IF NOT EXISTS bronze.emails (
  email_id STRING NOT NULL,
  member_id STRING,
  thread_id STRING,
  timestamp TIMESTAMP NOT NULL,
  direction STRING, -- 'inbound' or 'outbound'
  subject STRING,
  body STRING,
  from_address STRING,
  to_address STRING,
  cc_addresses ARRAY<STRING>,
  attachments ARRAY<STRING>,
  ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
CLUSTER BY (timestamp, member_id)
COMMENT 'Email correspondence with members';

-- Chat conversations
CREATE TABLE IF NOT EXISTS bronze.chats (
  session_id STRING NOT NULL,
  member_id STRING,
  timestamp TIMESTAMP NOT NULL,
  messages ARRAY<STRUCT<role:STRING, content:STRING, timestamp:TIMESTAMP>>,
  intent_detected STRING,
  resolved BOOLEAN,
  escalated_to_human BOOLEAN,
  chat_duration_seconds INT,
  ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
CLUSTER BY (timestamp, member_id)
COMMENT 'Chatbot conversation logs';

-- Survey responses
CREATE TABLE IF NOT EXISTS bronze.survey_responses (
  response_id STRING NOT NULL,
  member_id STRING,
  survey_type STRING,
  timestamp TIMESTAMP NOT NULL,
  nps_score INT,
  satisfaction_score INT,
  open_text_feedback STRING,
  survey_category STRING,
  survey_metadata MAP<STRING, STRING>,
  ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
CLUSTER BY (timestamp, member_id)
COMMENT 'Qualtrics survey responses';

-- Member profiles
CREATE TABLE IF NOT EXISTS bronze.member_profiles (
  member_id STRING NOT NULL PRIMARY KEY,
  age_bracket STRING,
  balance_bracket STRING,
  years_as_member INT,
  insurance_opted_in BOOLEAN,
  contribution_rate_pct DOUBLE,
  employment_status STRING,
  state STRING,
  last_login_date DATE,
  registration_date DATE,
  updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
COMMENT 'Member demographic and profile data';

-- ============================================================================
-- SILVER LAYER - Cleaned and enriched data
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS silver
COMMENT 'Cleaned and enriched interaction data with ML predictions';

-- Unified interactions with sentiment analysis
CREATE TABLE IF NOT EXISTS silver.interactions_analyzed (
  interaction_id STRING NOT NULL,
  member_id STRING,
  timestamp TIMESTAMP NOT NULL,
  channel STRING, -- 'call', 'email', 'chat', 'survey', 'portal'
  text STRING, -- Unified text field for analysis

  -- ML predictions
  sentiment_score DOUBLE, -- -1 to 1
  sentiment_label STRING, -- 'Positive', 'Neutral', 'Negative'
  primary_topic STRING,
  secondary_topics ARRAY<STRING>,
  intent STRING,
  urgency_score DOUBLE, -- 0 to 1

  -- Metadata
  resolution_status STRING,
  agent_id STRING,
  processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  model_version STRING
)
USING DELTA
CLUSTER BY (timestamp, member_id, channel)
COMMENT 'Unified interactions with sentiment and topic analysis';

-- Topic trends
CREATE TABLE IF NOT EXISTS silver.topic_extraction (
  interaction_id STRING NOT NULL,
  topic STRING,
  confidence_score DOUBLE,
  topic_category STRING, -- 'Insurance', 'Contribution', 'Investment', 'Account', 'Claims'
  keywords ARRAY<STRING>,
  processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
COMMENT 'Detailed topic extraction results';

-- ============================================================================
-- GOLD LAYER - Business aggregations and analytics
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS gold
COMMENT 'Business-ready analytics and member insights';

-- Member 360 view
CREATE TABLE IF NOT EXISTS gold.member_360_view (
  member_id STRING NOT NULL PRIMARY KEY,

  -- Profile
  age_bracket STRING,
  balance_bracket STRING,
  years_as_member INT,

  -- Sentiment metrics
  avg_sentiment DOUBLE,
  min_sentiment DOUBLE,
  max_sentiment DOUBLE,
  sentiment_trend STRING, -- 'improving', 'stable', 'declining'
  negative_interaction_count INT,
  positive_interaction_count INT,

  -- Volume metrics
  total_interactions INT,
  interactions_last_30_days INT,
  interactions_last_90_days INT,
  channels_used INT,
  preferred_channel STRING,

  -- Recency
  last_interaction_date TIMESTAMP,
  days_since_last_interaction INT,
  last_interaction_channel STRING,

  -- Topics
  all_topics_discussed ARRAY<STRING>,
  most_common_topic STRING,
  topic_diversity_score DOUBLE,

  -- Risk scoring
  at_risk_score DOUBLE, -- 0 to 1
  at_risk_flag BOOLEAN,
  at_risk_reasons ARRAY<STRING>,
  churn_probability DOUBLE,

  -- Lifecycle
  member_lifetime_value_estimate DOUBLE,
  engagement_score DOUBLE,

  updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
COMMENT 'Comprehensive member 360-degree view';

-- Daily topic trends
CREATE TABLE IF NOT EXISTS gold.topic_trends_daily (
  date DATE NOT NULL,
  topic STRING NOT NULL,
  channel STRING,

  -- Volume
  mention_count INT,
  unique_members INT,

  -- Sentiment
  avg_sentiment DOUBLE,
  negative_percentage DOUBLE,

  -- Change detection
  mention_count_change_pct DOUBLE, -- vs previous week
  sentiment_change DOUBLE, -- vs previous week

  -- Flags
  trending_up BOOLEAN,
  emerging_issue BOOLEAN,

  updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),

  PRIMARY KEY (date, topic, channel)
)
USING DELTA
COMMENT 'Daily topic trend analysis';

-- Sentiment by channel
CREATE TABLE IF NOT EXISTS gold.sentiment_by_channel (
  date DATE NOT NULL,
  channel STRING NOT NULL,

  -- Volume
  interaction_count INT,
  unique_members INT,

  -- Sentiment
  avg_sentiment DOUBLE,
  sentiment_std_dev DOUBLE,
  negative_percentage DOUBLE,
  neutral_percentage DOUBLE,
  positive_percentage DOUBLE,

  -- Change
  sentiment_change_7d DOUBLE,
  volume_change_7d_pct DOUBLE,

  updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),

  PRIMARY KEY (date, channel)
)
USING DELTA
COMMENT 'Sentiment trends by communication channel';

-- At-risk members for intervention
CREATE TABLE IF NOT EXISTS gold.at_risk_members (
  member_id STRING NOT NULL,
  identified_date DATE NOT NULL,

  -- Risk details
  at_risk_score DOUBLE,
  risk_category STRING, -- 'high', 'medium', 'low'
  primary_risk_factors ARRAY<STRING>,

  -- Context
  recent_sentiment DOUBLE,
  recent_topics ARRAY<STRING>,
  last_negative_interaction_date TIMESTAMP,
  consecutive_negative_interactions INT,

  -- Intervention
  recommended_action STRING,
  priority_rank INT,
  assigned_to STRING,
  intervention_status STRING, -- 'pending', 'in_progress', 'completed'
  intervention_date TIMESTAMP,
  intervention_notes STRING,
  outcome STRING,

  updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),

  PRIMARY KEY (member_id, identified_date)
)
USING DELTA
COMMENT 'At-risk members requiring proactive intervention';

-- Executive KPIs
CREATE TABLE IF NOT EXISTS gold.executive_kpis (
  date DATE NOT NULL PRIMARY KEY,

  -- Volume metrics
  total_interactions INT,
  unique_members_contacted INT,

  -- Sentiment
  avg_sentiment DOUBLE,
  negative_interaction_pct DOUBLE,

  -- Risk
  at_risk_member_count INT,
  at_risk_member_pct DOUBLE,

  -- Resolution
  resolution_rate DOUBLE,
  avg_resolution_time_hours DOUBLE,

  -- Trends
  sentiment_trend_7d DOUBLE,
  volume_trend_7d_pct DOUBLE,
  top_emerging_issue STRING,

  updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
COMMENT 'Executive dashboard KPIs';

-- ============================================================================
-- ML MODELS SCHEMA
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS ml_models
COMMENT 'Machine learning model artifacts and metadata';

-- Model performance tracking
CREATE TABLE IF NOT EXISTS ml_models.model_performance (
  model_name STRING NOT NULL,
  model_version STRING NOT NULL,
  deployment_date TIMESTAMP,

  -- Performance metrics
  accuracy DOUBLE,
  precision_score DOUBLE,
  recall_score DOUBLE,
  f1_score DOUBLE,

  -- Production stats
  predictions_count BIGINT,
  avg_inference_time_ms DOUBLE,

  -- Monitoring
  drift_detected BOOLEAN,
  last_retrain_date TIMESTAMP,

  updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),

  PRIMARY KEY (model_name, model_version)
)
USING DELTA
COMMENT 'ML model performance tracking';

-- ============================================================================
-- GRANTS - Set up access control
-- ============================================================================

-- Member Services Team - Full access to gold layer
GRANT USE CATALOG ON CATALOG art_member_listening TO `member_services_team`;
GRANT USE SCHEMA ON SCHEMA art_member_listening.gold TO `member_services_team`;
GRANT SELECT ON SCHEMA art_member_listening.gold TO `member_services_team`;

-- Executives - Read-only access to gold layer
GRANT USE CATALOG ON CATALOG art_member_listening TO `executives`;
GRANT USE SCHEMA ON SCHEMA art_member_listening.gold TO `executives`;
GRANT SELECT ON SCHEMA art_member_listening.gold TO `executives`;

-- Data Engineers - Full access to bronze and silver
GRANT USE CATALOG ON CATALOG art_member_listening TO `data_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA art_member_listening.bronze TO `data_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA art_member_listening.silver TO `data_engineers`;
GRANT SELECT ON SCHEMA art_member_listening.gold TO `data_engineers`;

-- ML Engineers - Access to ml_models and silver/gold
GRANT USE CATALOG ON CATALOG art_member_listening TO `ml_engineers`;
GRANT SELECT ON SCHEMA art_member_listening.silver TO `ml_engineers`;
GRANT SELECT ON SCHEMA art_member_listening.gold TO `ml_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA art_member_listening.ml_models TO `ml_engineers`;

-- ============================================================================
-- VACUUM AND OPTIMIZE SETTINGS
-- ============================================================================

-- Enable auto-optimize for all tables
ALTER TABLE bronze.portal_events SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

ALTER TABLE silver.interactions_analyzed SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

ALTER TABLE gold.member_360_view SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

-- Set retention for GDPR compliance (7 years for financial services)
ALTER TABLE bronze.portal_events SET TBLPROPERTIES (
  'delta.deletedFileRetentionDuration' = 'interval 7 years'
);

-- ============================================================================
-- VIEWS - Convenience views for common queries
-- ============================================================================

USE SCHEMA gold;

-- Recent interactions view
CREATE OR REPLACE VIEW recent_interactions AS
SELECT
  i.interaction_id,
  i.member_id,
  i.timestamp,
  i.channel,
  i.text,
  i.sentiment_label,
  i.primary_topic,
  m.age_bracket,
  m.balance_bracket,
  m.at_risk_flag
FROM silver.interactions_analyzed i
LEFT JOIN gold.member_360_view m ON i.member_id = m.member_id
WHERE i.timestamp >= CURRENT_TIMESTAMP - INTERVAL '7 days';

-- Emerging issues view
CREATE OR REPLACE VIEW emerging_issues AS
SELECT
  topic,
  SUM(mention_count) as total_mentions,
  AVG(avg_sentiment) as overall_sentiment,
  AVG(mention_count_change_pct) as avg_growth_rate,
  MAX(CASE WHEN emerging_issue THEN 1 ELSE 0 END) as is_emerging
FROM gold.topic_trends_daily
WHERE date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY topic
HAVING is_emerging = 1 OR avg_growth_rate > 50
ORDER BY avg_growth_rate DESC;

-- Member intervention queue
CREATE OR REPLACE VIEW intervention_queue AS
SELECT
  a.member_id,
  a.at_risk_score,
  a.risk_category,
  a.primary_risk_factors,
  a.recommended_action,
  a.priority_rank,
  m.avg_sentiment,
  m.last_interaction_date,
  m.total_interactions,
  m.preferred_channel
FROM gold.at_risk_members a
LEFT JOIN gold.member_360_view m ON a.member_id = m.member_id
WHERE a.intervention_status = 'pending'
ORDER BY a.priority_rank;

-- ============================================================================
-- SAMPLE QUERIES
-- ============================================================================

-- Query 1: Get sentiment trends for last 30 days
-- SELECT date, channel, avg_sentiment, interaction_count
-- FROM gold.sentiment_by_channel
-- WHERE date >= CURRENT_DATE - INTERVAL '30 days'
-- ORDER BY date DESC, channel;

-- Query 2: Find members with declining sentiment
-- SELECT member_id, sentiment_trend, avg_sentiment, negative_interaction_count
-- FROM gold.member_360_view
-- WHERE sentiment_trend = 'declining'
-- AND avg_sentiment < -0.3
-- ORDER BY avg_sentiment ASC;

-- Query 3: Top emerging issues
-- SELECT * FROM emerging_issues LIMIT 10;

-- Query 4: Member intervention queue
-- SELECT * FROM intervention_queue LIMIT 50;

-- ============================================================================
DESCRIBE CATALOG art_member_listening;
SHOW SCHEMAS IN art_member_listening;
