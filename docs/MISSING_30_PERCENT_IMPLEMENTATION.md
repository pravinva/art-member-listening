# The Missing 30%: Enterprise VoC Features Implementation Plan

**Purpose:** Identify and prioritize the enterprise VoC platform features missing from the current demo and provide implementation roadmap.

---

## Current State: The 70% You Have

✅ **Multi-channel Data Ingestion** (Calls, Emails, Chats, Surveys, Portal)
✅ **Real-time Sentiment Analysis** (ai_analyze_sentiment)
✅ **Semantic Search** (Vector Search with BGE embeddings)
✅ **Real-Time Processing** (<300ms latency with Real-Time Mode)
✅ **AI Agent** (Natural language queries using Llama 3.1)
✅ **Basic Member 360 View** (Aggregated feedback per member)
✅ **Data Governance** (Unity Catalog with row/column security)
✅ **Basic Dashboard** (Streamlit app showing insights)

---

## The Missing 30%: Enterprise VoC Features

### Priority 1: Mission-Critical for Production

#### 1. **Closed-Loop Feedback Management** 🔴
**What it is:** Track feedback from identification → assignment → resolution → verification

**Why it matters:** Member services needs to know "who's working on what" and "is this resolved?"

**What commercial platforms provide:**
- Assign feedback to teams/individuals
- Track resolution status (New → In Progress → Resolved → Verified)
- SLA tracking (time to first response, time to resolution)
- Automated reminders for overdue items
- Resolution notes and action history

**How to build it:**

```sql
-- New tables needed
CREATE TABLE gold.feedback_cases (
  case_id STRING PRIMARY KEY,
  interaction_id STRING,  -- Link to original feedback
  member_id STRING,
  issue_type STRING,      -- Insurance, Contribution, Balance, etc.
  severity STRING,        -- Low, Medium, High, Critical
  status STRING,          -- New, Assigned, In Progress, Resolved, Verified, Closed
  assigned_to STRING,     -- User/team
  assigned_at TIMESTAMP,
  due_date TIMESTAMP,     -- Based on SLA
  resolved_at TIMESTAMP,
  resolution_notes STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);

CREATE TABLE gold.case_actions (
  action_id STRING PRIMARY KEY,
  case_id STRING,
  action_type STRING,    -- Assigned, Status Change, Note Added, Escalated
  action_by STRING,
  action_at TIMESTAMP,
  details STRING
);
```

**Implementation steps:**
1. Create case management tables (2 days)
2. Build case creation logic - auto-create cases for at-risk members or negative feedback (3 days)
3. Add assignment workflow to Streamlit dashboard (4 days)
4. Implement SLA tracking and alerts (3 days)
5. Add case analytics (resolution time, backlog, etc.) (2 days)

**Total effort:** 2 weeks (1 engineer)

---

#### 2. **Automated Alerting & Escalation** 🔴
**What it is:** Real-time alerts when critical conditions occur

**Why it matters:** Proactive intervention for VIP members, urgent issues, trending problems

**What commercial platforms provide:**
- Configurable alert rules (e.g., "VIP member + negative sentiment → Alert GM")
- Multi-channel alerts (Email, Slack, SMS, in-app)
- Alert routing based on issue type
- Escalation paths (if not resolved in X hours, escalate to Y)
- Alert fatigue prevention (throttling, grouping)

**How to build it:**

```python
# analytics/07_alert_engine.py

from pyspark.sql import functions as F
from databricks.sdk import WorkspaceClient

class AlertEngine:
    """Real-time alert engine for member feedback"""

    ALERT_RULES = {
        "vip_negative": {
            "condition": "member_tier = 'VIP' AND sentiment_score < -0.6",
            "severity": "Critical",
            "notify": ["gm@art.com.au", "member_services_lead@art.com.au"],
            "channel": ["email", "slack"]
        },
        "repeated_contact": {
            "condition": "contact_count_7d >= 3 AND sentiment_score < 0",
            "severity": "High",
            "notify": ["operations_manager@art.com.au"],
            "channel": ["email"]
        },
        "urgent_topic_spike": {
            "condition": "topic_mention_increase_24h > 300%",
            "severity": "Medium",
            "notify": ["insights_team@art.com.au"],
            "channel": ["slack"]
        }
    }

    def process_alerts(self, df):
        """Evaluate alert rules against incoming data"""
        alerts = []

        for rule_name, rule in self.ALERT_RULES.items():
            # Find matching records
            matches = df.filter(rule["condition"])

            if matches.count() > 0:
                alerts.append({
                    "rule": rule_name,
                    "severity": rule["severity"],
                    "count": matches.count(),
                    "members": matches.select("member_id").collect(),
                    "notify": rule["notify"],
                    "channel": rule["channel"]
                })

        return alerts

    def send_alerts(self, alerts):
        """Send alerts via configured channels"""
        for alert in alerts:
            if "email" in alert["channel"]:
                self._send_email(alert)
            if "slack" in alert["channel"]:
                self._send_slack(alert)

    def _send_slack(self, alert):
        """Send Slack notification"""
        # Use Slack webhook or API
        import requests
        webhook_url = "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"

        message = {
            "text": f"🚨 *{alert['severity']} Alert*: {alert['rule']}",
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*{alert['count']}* members matched alert rule `{alert['rule']}`"
                    }
                }
            ]
        }
        requests.post(webhook_url, json=message)
```

**Integration with Real-Time Processing:**

```python
# In processing/03_realtime_sentiment_processing_v2.py

def process_with_alerts(batch_df, batch_id):
    """Process batch and evaluate alerts"""

    # Existing sentiment processing
    processed_df = process_sentiment(batch_df)

    # Check for alert conditions
    alert_engine = AlertEngine()
    alerts = alert_engine.process_alerts(processed_df)

    # Send alerts
    if alerts:
        alert_engine.send_alerts(alerts)

    # Write to Silver table
    processed_df.write.mode("append").saveAsTable("silver.interactions_analyzed")
```

**Implementation steps:**
1. Create alert configuration system (2 days)
2. Build alert evaluation engine (3 days)
3. Integrate Slack/Email notifications (2 days)
4. Add alert dashboard to Streamlit (2 days)
5. Implement alert history and analytics (2 days)

**Total effort:** 2 weeks (1 engineer)

---

#### 3. **Predictive At-Risk Scoring (ML-based)** 🟡
**What it is:** ML model to predict member churn/dissatisfaction before it happens

**Why it matters:** Current rule-based scoring is simplistic; ML can find complex patterns

**What commercial platforms provide:**
- Churn prediction models
- Propensity to act scores
- NPS prediction
- Driver analysis (what factors predict outcomes)

**How to build it:**

```python
# analytics/08_predictive_at_risk_model.py

from databricks import feature_engineering
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import BinaryClassificationEvaluator

class AtRiskPredictionModel:
    """Predict member churn/at-risk status"""

    def prepare_training_data(self):
        """Create labeled training dataset"""

        # Define "at-risk" label (historical data)
        # Examples: Member who churned, switched to competitor, lodged complaint, etc.

        query = """
        SELECT
            member_id,

            -- Engagement features
            days_since_last_login,
            login_frequency_30d,
            page_views_30d,

            -- Sentiment features
            avg_sentiment_score_30d,
            negative_interaction_count_30d,
            sentiment_trend_30d,  -- Improving or declining?

            -- Contact behavior
            contact_frequency_30d,
            repeat_contact_same_issue,
            channel_diversity,  -- Using multiple channels = sign of frustration

            -- Topic features
            insurance_topic_mentions,
            contribution_topic_mentions,
            technical_issue_mentions,

            -- Member attributes
            member_tier,
            account_balance,
            years_as_member,
            age_group,

            -- Outcome label
            CASE
                WHEN churned_within_90d = 1 OR complaint_filed = 1 OR nps_score < 6
                THEN 1
                ELSE 0
            END as at_risk_label

        FROM gold.member_360_view
        WHERE interaction_date >= DATE_SUB(CURRENT_DATE(), 365)
        """

        return spark.sql(query)

    def train_model(self):
        """Train gradient boosted tree classifier"""

        # Get training data
        train_df = self.prepare_training_data()

        # Feature assembly
        feature_cols = [col for col in train_df.columns if col not in ['member_id', 'at_risk_label']]
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

        # Split train/test
        train, test = train_df.randomSplit([0.8, 0.2], seed=42)

        # Train GBT classifier
        gbt = GBTClassifier(
            featuresCol="features",
            labelCol="at_risk_label",
            maxDepth=6,
            maxIter=20
        )

        model = gbt.fit(assembler.transform(train))

        # Evaluate
        predictions = model.transform(assembler.transform(test))
        evaluator = BinaryClassificationEvaluator(labelCol="at_risk_label")
        auc = evaluator.evaluate(predictions)

        print(f"Model AUC: {auc:.3f}")

        # Feature importance
        self._print_feature_importance(model, feature_cols)

        return model

    def score_members(self, model):
        """Score all active members"""

        current_members = self.prepare_training_data().drop("at_risk_label")
        predictions = model.transform(current_members)

        # Extract probability of being at-risk
        predictions = predictions.withColumn(
            "at_risk_probability",
            F.col("probability")[1]  # Probability of class 1 (at-risk)
        )

        # Write to Gold table
        predictions.select(
            "member_id",
            "at_risk_probability",
            F.current_timestamp().alias("scored_at")
        ).write.mode("overwrite").saveAsTable("gold.member_at_risk_scores")
```

**Implementation steps:**
1. Define "at-risk" label from historical data (3 days)
2. Engineer features from Member 360 view (4 days)
3. Train and evaluate ML model (3 days)
4. Deploy model as batch job (daily scoring) (2 days)
5. Update dashboard to show ML-based scores (2 days)
6. A/B test vs rule-based scoring (ongoing)

**Total effort:** 2-3 weeks (1 data scientist)

---

### Priority 2: Important for Differentiation

#### 4. **Text Analytics Dashboard (Topic Modeling over Time)** 🟡
**What it is:** Visualize trending topics, emerging themes, topic evolution

**Why it matters:** Understand "what's changing" in member concerns

**What commercial platforms provide:**
- Topic extraction and clustering
- Topic trend over time
- Emerging topic detection
- Topic correlation with sentiment
- Custom topic dictionaries

**How to build it:**

```python
# analytics/09_topic_modeling.py

from pyspark.ml.feature import CountVectorizer, IDF
from pyspark.ml.clustering import LDA

class TopicModelingPipeline:
    """Extract and track topics over time"""

    def extract_topics(self, text_df, num_topics=20):
        """Use LDA to discover topics"""

        # Tokenize and vectorize
        vectorizer = CountVectorizer(
            inputCol="text_cleaned",
            outputCol="features",
            minDF=5,
            maxDF=0.8
        )

        # TF-IDF
        idf = IDF(inputCol="features", outputCol="tfidf_features")

        # LDA
        lda = LDA(
            featuresCol="tfidf_features",
            k=num_topics,
            maxIter=20
        )

        model = lda.fit(text_df)

        # Get topics
        topics = model.describeTopics(maxTermsPerTopic=10)
        vocab = vectorizer.vocabulary

        # Print topics
        for topic_id, topic in enumerate(topics.collect()):
            print(f"\nTopic {topic_id}:")
            terms = [vocab[idx] for idx in topic.termIndices]
            weights = topic.termWeights
            for term, weight in zip(terms, weights):
                print(f"  {term}: {weight:.3f}")

        return model

    def track_topic_trends(self):
        """Track topic mentions over time"""

        query = """
        WITH topic_counts AS (
            SELECT
                DATE_TRUNC('day', interaction_date) as date,
                primary_topic,
                COUNT(*) as mention_count,
                AVG(sentiment_score) as avg_sentiment
            FROM silver.interactions_analyzed
            WHERE interaction_date >= DATE_SUB(CURRENT_DATE(), 90)
            GROUP BY 1, 2
        ),
        topic_trends AS (
            SELECT
                date,
                primary_topic,
                mention_count,
                avg_sentiment,
                LAG(mention_count, 7) OVER (
                    PARTITION BY primary_topic ORDER BY date
                ) as mention_count_7d_ago,
                (mention_count - LAG(mention_count, 7) OVER (
                    PARTITION BY primary_topic ORDER BY date
                )) / NULLIF(LAG(mention_count, 7) OVER (
                    PARTITION BY primary_topic ORDER BY date
                ), 0) * 100 as pct_change_7d
            FROM topic_counts
        )
        SELECT * FROM topic_trends
        WHERE ABS(pct_change_7d) > 50  -- Emerging or declining topics
        ORDER BY date DESC, ABS(pct_change_7d) DESC
        """

        return spark.sql(query)
```

**Dashboard visualization (Streamlit):**

```python
# dashboard/pages/topic_trends.py

import streamlit as st
import plotly.express as px

def show_topic_trends():
    """Topic analytics dashboard"""

    st.title("📊 Topic Trends & Emerging Issues")

    # Trending topics
    trending = spark.sql("""
        SELECT primary_topic, COUNT(*) as mentions, AVG(sentiment_score) as sentiment
        FROM silver.interactions_analyzed
        WHERE interaction_date >= DATE_SUB(CURRENT_DATE(), 7)
        GROUP BY primary_topic
        ORDER BY mentions DESC
        LIMIT 10
    """).toPandas()

    col1, col2 = st.columns(2)

    with col1:
        fig = px.bar(trending, x='primary_topic', y='mentions', title="Top Topics (Last 7 Days)")
        st.plotly_chart(fig)

    with col2:
        fig = px.scatter(trending, x='mentions', y='sentiment', text='primary_topic',
                        title="Topic Volume vs Sentiment")
        st.plotly_chart(fig)

    # Topic evolution over time
    topic_history = spark.sql("""
        SELECT
            DATE_TRUNC('week', interaction_date) as week,
            primary_topic,
            COUNT(*) as mentions
        FROM silver.interactions_analyzed
        WHERE interaction_date >= DATE_SUB(CURRENT_DATE(), 90)
        GROUP BY 1, 2
    """).toPandas()

    fig = px.line(topic_history, x='week', y='mentions', color='primary_topic',
                 title="Topic Evolution (Last 90 Days)")
    st.plotly_chart(fig)
```

**Implementation steps:**
1. Implement LDA topic modeling (3 days)
2. Create topic tracking queries (2 days)
3. Build topic trends dashboard (4 days)
4. Add emerging topic detection (2 days)
5. Integrate with alerting (1 day)

**Total effort:** 2 weeks (1 engineer)

---

#### 5. **CRM/Ticketing System Integration** 🟡
**What it is:** Auto-create cases in Salesforce/ServiceNow when at-risk members identified

**Why it matters:** Seamless handoff from insights to action

**What commercial platforms provide:**
- Pre-built connectors to major CRMs
- Bi-directional sync (feedback → CRM, resolution → VoC)
- Custom field mapping
- Deduplication logic

**How to build it:**

```python
# integrations/salesforce_integration.py

from simple_salesforce import Salesforce
from pyspark.sql import functions as F

class SalesforceIntegration:
    """Sync at-risk members to Salesforce cases"""

    def __init__(self):
        self.sf = Salesforce(
            username='your-sf-user@art.com.au',
            password='your-password',
            security_token='your-token'
        )

    def create_case_for_at_risk_member(self, member_row):
        """Create Salesforce case for at-risk member"""

        # Check if case already exists (deduplication)
        existing = self.sf.query(f"""
            SELECT Id FROM Case
            WHERE Member_ID__c = '{member_row['member_id']}'
            AND Status != 'Closed'
            AND CreatedDate = LAST_N_DAYS:7
        """)

        if existing['totalSize'] > 0:
            print(f"Case already exists for {member_row['member_id']}")
            return

        # Create new case
        case = {
            'Subject': f"At-Risk Member: {member_row['member_id']}",
            'Description': f"""
Member identified as at-risk by AI analysis.

At-Risk Score: {member_row['at_risk_score']:.2f}
Recent Sentiment: {member_row['avg_sentiment_30d']:.2f}
Recent Interactions: {member_row['contact_count_30d']}
Primary Concern: {member_row['primary_topic']}

Latest Feedback:
{member_row['latest_feedback_text']}

Recommended Action: Proactive outreach within 48 hours
""",
            'Member_ID__c': member_row['member_id'],
            'Priority': self._get_priority(member_row['at_risk_score']),
            'Origin': 'AI - Member Listening Platform',
            'Type': 'Member Retention',
            'Status': 'New'
        }

        result = self.sf.Case.create(case)
        print(f"Created case {result['id']} for member {member_row['member_id']}")

        return result['id']

    def _get_priority(self, at_risk_score):
        if at_risk_score >= 0.8:
            return 'High'
        elif at_risk_score >= 0.6:
            return 'Medium'
        else:
            return 'Low'

    def sync_batch(self):
        """Sync all at-risk members to Salesforce (daily job)"""

        at_risk_members = spark.sql("""
            SELECT
                m.member_id,
                m.at_risk_score,
                m.avg_sentiment_30d,
                m.contact_count_30d,
                m.primary_topic,
                i.text as latest_feedback_text
            FROM gold.member_360_view m
            JOIN silver.interactions_analyzed i
                ON m.member_id = i.member_id
                AND i.interaction_date = m.last_interaction_date
            WHERE m.at_risk_score >= 0.7
            AND m.last_interaction_date >= DATE_SUB(CURRENT_DATE(), 7)
        """).collect()

        for member in at_risk_members:
            self.create_case_for_at_risk_member(member)
```

**Scheduled job:**

```python
# Create Databricks job that runs daily at 9 AM
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

job = w.jobs.create(
    name="Sync At-Risk Members to Salesforce",
    tasks=[{
        "task_key": "sync_salesforce",
        "python_wheel_task": {
            "package_name": "art_member_listening",
            "entry_point": "sync_at_risk_to_crm"
        }
    }],
    schedule={"quartz_cron_expression": "0 0 9 * * ?", "timezone_id": "Australia/Sydney"}
)
```

**Implementation steps:**
1. Set up Salesforce API credentials and test connection (1 day)
2. Build case creation logic with deduplication (3 days)
3. Create scheduled sync job (1 day)
4. Add bi-directional sync (case resolution → VoC) (3 days)
5. Build sync monitoring dashboard (2 days)

**Total effort:** 2 weeks (1 engineer)

---

### Priority 3: Nice-to-Have

#### 6. **Root Cause Driver Analysis** 🟢
**What it is:** Statistical analysis to identify what drives NPS, satisfaction, churn

**Why it matters:** Understand *why* members are happy/unhappy

**Implementation:** Use correlation analysis, regression, or SHAP values on ML model

**Effort:** 1-2 weeks

---

#### 7. **Journey Mapping Visualization** 🟢
**What it is:** Visual representation of member journey across touchpoints

**Why it matters:** Understand member experience holistically

**Implementation:** Use Sankey diagrams in Streamlit, track sequences of interactions

**Effort:** 1-2 weeks

---

#### 8. **Benchmarking Against Industry** 🟢
**What it is:** Compare ART metrics to industry averages

**Why it matters:** Understand competitive positioning

**Implementation:** Partner with industry associations, manual data entry initially

**Effort:** 1 week (setup), ongoing maintenance

---

## Implementation Prioritization

### Phase 1 (Weeks 1-4): Mission-Critical
**Goal:** Production-ready closed-loop feedback management

| Feature | Effort | Value | Risk |
|---------|--------|-------|------|
| Closed-Loop Feedback | 2 weeks | High | Low |
| Automated Alerting | 2 weeks | High | Low |

**Team:** 2 engineers
**Outcome:** Member services can assign, track, resolve feedback

---

### Phase 2 (Weeks 5-8): AI/ML Enhancement
**Goal:** Smarter predictions and insights

| Feature | Effort | Value | Risk |
|---------|--------|-------|------|
| Predictive At-Risk Model | 3 weeks | High | Medium |
| Topic Modeling Dashboard | 2 weeks | Medium | Low |

**Team:** 1 data scientist + 1 engineer
**Outcome:** ML-driven at-risk scoring, trending topic detection

---

### Phase 3 (Weeks 9-12): Integration & Automation
**Goal:** Seamless workflows with existing systems

| Feature | Effort | Value | Risk |
|---------|--------|-------|------|
| CRM Integration (Salesforce) | 2 weeks | High | Medium |
| Driver Analysis | 2 weeks | Medium | Low |

**Team:** 1 engineer
**Outcome:** Auto-create cases, understand drivers of satisfaction

---

## Total Effort Summary

| Phase | Features | Duration | Team Size | Cost |
|-------|----------|----------|-----------|------|
| **Phase 1** | Closed-loop + Alerts | 4 weeks | 2 engineers | $40K |
| **Phase 2** | ML + Topics | 4 weeks | 1 DS + 1 eng | $40K |
| **Phase 3** | CRM + Drivers | 4 weeks | 1 engineer | $20K |
| **Total** | **6 features** | **12 weeks** | **2-3 people** | **$100K** |

**Overall Timeline:** 3 months to complete all mission-critical and important features

**Revised Build Cost (with missing 30%):**
- Original build: $285K (3 years)
- Add missing features: $100K (one-time)
- **Total Build Cost:** $385K (3 years)
- **Buy Cost:** $475K (3 years)
- **Savings:** Still $90K cheaper + full customization

---

## Comparison: Build vs Buy (Updated)

| Feature Category | Build (After 30%) | Buy (VoC Platform) |
|------------------|-------------------|-------------------|
| **Core Analytics** | ✅ Built | ✅ Included |
| **Closed-Loop Feedback** | ✅ Built (Phase 1) | ✅ Included |
| **Automated Alerts** | ✅ Built (Phase 1) | ✅ Included |
| **Predictive ML** | ✅ Built (Phase 2) | 🟡 Limited/Basic |
| **Topic Modeling** | ✅ Built (Phase 2) | ✅ Included |
| **CRM Integration** | ✅ Built (Phase 3) | ✅ Pre-built connectors |
| **Driver Analysis** | ✅ Built (Phase 3) | ✅ Included |
| **Journey Mapping** | 🟡 Future | ✅ Included |
| **Benchmarking** | 🟡 Future | ✅ Included |
| **Cost (3 years)** | **$385K** | **$475K** |

**Build still wins** even with the missing 30% implemented!

---

## Recommendation

**Build the missing 30% over 3 months**

**Why:**
1. You already have 70% working with real Databricks features
2. The remaining 30% is well-defined and achievable
3. Still $90K cheaper than buying even after building everything
4. Superior AI/ML capabilities (foundation models vs vendor black box)
5. Full control and customization
6. No vendor lock-in

**Risks:**
- 3-month development timeline could slip
- Internal team needs to maintain features long-term
- Missing some nice-to-have features (journey mapping, benchmarking)

**Mitigations:**
- Start with Phase 1 (closed-loop + alerts) - delivers immediate value
- Iterate based on user feedback
- Deprioritize Phase 3 if resources constrained

---

## Next Steps

1. **Week 1:** Form project team (2 engineers, 1 data scientist, 1 product owner)
2. **Week 2:** Detailed design for closed-loop feedback system
3. **Week 3-4:** Build and test Phase 1 features
4. **Week 5:** Pilot with 10 member services users
5. **Week 6-12:** Continue with Phase 2 and 3 based on feedback

**Success Metrics:**
- 80% of at-risk members have assigned cases (Closed-loop)
- <2 hour alert-to-action time for critical issues (Alerts)
- ML model AUC >0.75 for at-risk prediction (Predictive)
- 90% of cases auto-created in CRM (Integration)

---

**Questions? Need help prioritizing?** Let's discuss which features are most critical for your member services team.
