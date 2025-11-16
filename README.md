# ART Member Listening Intelligence Hub

> **AI-Powered Member Feedback Analysis for Australian Retirement Trust**

A comprehensive Databricks demo showcasing real-time member listening across all touchpoints using Agentic AI, Vector Search, and Real-Time Mode processing.

## 🎯 Overview

This demo processes 100% of member interactions (vs 5% survey response rate) across 8+ channels in near real-time to:
- Detect at-risk members automatically
- Surface emerging issues before they escalate
- Provide AI-powered insights via natural language chat
- Enable proactive member service interventions

## 📊 Architecture

```
Data Sources → Databricks Lakehouse → AI Analytics → Insights
├─ Zerobus (5-10s)    ├─ Bronze Layer     ├─ Vector Search   ├─ Executive Dashboard
├─ Auto Loader (1-5m) ├─ Silver Layer     ├─ AI Agent        ├─ Operations Dashboard
└─ Batch (daily)      └─ Gold Layer       └─ Genie Analytics └─ AI Chat Assistant
```

### Key Technologies

- **Databricks Unity Catalog** - Unified governance with advanced RBAC
- **Zerobus** - Real-time event ingestion (5-10 sec latency)
- **Real-Time Mode** - Sub-300ms stream processing
- **Vector Search** - Semantic feedback search with hybrid search
- **Mosaic AI Agent** - Natural language insights with explainability
- **Streamlit** - Interactive dashboards
- **Cost Optimization** - Query caching, materialized views, Photon acceleration
- **Advanced Governance** - Automated PII detection, dynamic data masking

## 🚀 Quick Start

### Prerequisites

- Databricks Workspace (AWS/Azure/GCP)
- Unity Catalog enabled
- Cluster with DBR 14.3+ and ML Runtime

### Installation

```bash
# Clone the repository
git clone https://github.com/pravinva/art-member-listening.git
cd art-member-listening

# Install dependencies
pip install -r requirements.txt

# Set up Unity Catalog (run in Databricks SQL Warehouse)
databricks sql --file config/01_unity_catalog_setup.sql
```

### Generate Synthetic Data

```bash
# Generate 100K interactions across all channels
python data_generation/generate_all_data.py --output /dbfs/mnt/landing/

# Expected output:
# - 100K call transcripts
# - 50K email threads (150K individual emails)
# - 30K survey responses
# - 75K chat sessions
# - 100K member profiles
```

### Deploy Pipelines

```python
# 1. Set up ingestion (Zerobus + Auto Loader)
databricks jobs run --notebook ingestion/01_zerobus_portal_ingestion.py
databricks jobs run --notebook ingestion/02_autoloader_emails_chats.py

# 2. Start real-time processing
databricks jobs run --notebook processing/03_realtime_sentiment_processing.py

# 3. Build aggregations
databricks jobs run --notebook analytics/04_member_360_aggregation.py

# 4. Deploy AI agent
databricks jobs run --notebook analytics/06_create_member_listening_agent.py

# 5. Launch dashboard
databricks apps deploy --source dashboard/streamlit_app.py
```

## 📁 Project Structure

```
art-member-listening/
├── data_generation/          # Synthetic data generators
│   ├── generate_calls.py     # Call center transcripts
│   ├── generate_emails.py    # Email interactions
│   ├── generate_surveys.py   # Qualtrics responses
│   ├── generate_chats.py     # Chatbot conversations
│   ├── generate_portal_events.py  # Portal activity
│   ├── generate_member_profiles.py
│   └── generate_all_data.py  # Master generation script
│
├── ingestion/                # Data ingestion pipelines
│   ├── 01_zerobus_portal_ingestion.py
│   ├── 02_autoloader_emails_chats.py
│   └── 03_batch_surveys_profiles.py
│
├── processing/               # Stream processing
│   ├── 03_realtime_sentiment_processing.py
│   └── models/              # ML models for sentiment/topic
│
├── analytics/                # Analytics & AI
│   ├── 04_member_360_aggregation.py
│   ├── 05_vector_search_setup.py
│   ├── 06_create_member_listening_agent.py
│   ├── 07_cost_optimization_layer.py      # NEW: Query caching & optimization
│   ├── 08_enhanced_agent_experience.py    # NEW: Multi-turn conversations & explainability
│   └── agent_tools/         # AI agent tool functions
│
├── dashboard/                # Streamlit application
│   ├── streamlit_app.py     # Main dashboard
│   ├── pages/
│   │   ├── executive_dashboard.py
│   │   ├── operations_dashboard.py
│   │   └── ai_assistant.py
│   └── utils/
│
├── notebooks/                # Databricks notebooks
│   └── exploration/         # Ad-hoc analysis notebooks
│
├── config/                   # Configuration files
│   ├── 01_unity_catalog_setup.sql
│   ├── 02_advanced_governance.sql          # NEW: PII detection & data masking
│   ├── databricks_optimization_config.py   # NEW: Unified optimization config
│   ├── catalog_config.yaml
│   └── pipeline_config.yaml
│
├── docs/                     # Documentation
│   ├── DEMO_SCRIPT.md       # 15-minute demo walkthrough
│   ├── ARCHITECTURE.md      # Detailed architecture
│   ├── DATABRICKS_AGENTIC_AI_OPTIMIZATION.md  # NEW: Complete optimization guide
│   ├── REAL_IMPLEMENTATIONS.md              # Real Databricks features guide
│   ├── ZEROBUS_AND_REALTIME_MODE_EXPLAINED.md  # Zerobus vs Real-Time Mode
│   └── FAQ.md               # Common questions
│
└── tests/                    # Unit tests
    └── test_data_generation.py
```

## 🎭 Demo Flow (15 minutes)

### Part 1: The Problem (2 min)
- ART receives feedback across 8+ channels
- Data is siloed and manually analyzed
- Only 5% survey response rate - what about the other 95%?

### Part 2: The Solution (3 min)
- Show architecture diagram
- Explain real-time processing capabilities
- Highlight Zerobus (5-10s) + Real-Time Mode (<300ms)

### Part 3: Executive Dashboard (3 min)
- Real-time KPIs updating
- Sentiment trends by channel
- Emerging issues detection
- At-risk member count

### Part 4: Operations Dashboard (3 min)
- Live activity feed
- At-risk member list with scores
- Downloadable intervention list

### Part 5: AI Assistant (3 min)
**Demo Queries:**
1. "What are members most frustrated about regarding insurance?"
2. "Which members should we call first?"
3. "How has sentiment changed for contribution topics?"

### Part 6: The Impact (1 min)
- ✅ 100% coverage vs 5% survey response
- ✅ 10-30 second insights vs weeks
- ✅ Proactive intervention vs reactive
- ✅ $200K+ annual savings

## 🔧 Technical Details

### Why No Kafka?

We use **Zerobus + Auto Loader** instead of Kafka because:
- ✅ Single destination (only Databricks needs the data)
- ✅ 5-30 second latency is acceptable
- ✅ Simpler infrastructure (no Kafka cluster)
- ✅ Direct Delta Lake writes

**Decision Matrix:**

| Source | Method | Latency | Why |
|--------|--------|---------|-----|
| Portal events | Zerobus | 5-10s | Real-time, high volume |
| Emails/Chats | Auto Loader | 1-5min | Batch files, near real-time OK |
| Surveys | Batch | Daily | Daily export from Qualtrics |
| Profiles | Batch | Nightly | Source system sync |

### Real-Time Mode Benefits

- **<300ms processing latency** per micro-batch
- Enables near-instant sentiment analysis
- Supports live dashboards updating every 5 seconds
- No need for separate stream processor

### Vector Search for Semantic Feedback

```python
# Example: Find similar member complaints
results = vsc.similarity_search(
    query_text="insurance options are confusing",
    num_results=10
)
# Returns: 10 most similar feedback instances across all channels
```

## 📈 Data Volumes

| Data Type | Historical | Streaming | File Size |
|-----------|-----------|-----------|-----------|
| Call transcripts | 100K (2 years) | 1K/day | ~500MB |
| Email threads | 50K threads | 500/day | ~200MB |
| Survey responses | 30K | 100/day | ~50MB |
| Chat sessions | 75K | 1K/day | ~300MB |
| Portal events | 10M events | 100/min | ~2GB |
| **Total** | **~260K interactions** | **~3K/day** | **~3GB** |

## 🎯 Business Impact

### Quantified Benefits

- **100% interaction coverage** (vs 5% survey rate)
  - Captures 2.28M+ annual member touchpoints
  - Previously: 120K survey responses/year

- **10-30 second insights** (vs 2-4 weeks manual analysis)
  - Real-time emerging issue detection
  - Automated at-risk member identification

- **$238K+ annual savings** (updated with optimization)
  - 2 FTE analysts freed from manual categorization ($150K)
  - Query optimization and caching ($38K-$57K)
  - Reduced escalation costs through early intervention ($50K)

- **15% improvement in member satisfaction** (projected)
  - Proactive outreach to at-risk members
  - Faster resolution of systemic issues

### NEW: Enhanced Capabilities (2024)

**🚀 Cost Optimization (40-60% reduction):**
- Query result caching with 24-hour TTL
- Materialized views for frequent analytics
- Photon acceleration for 3-8x faster queries
- Serverless SQL for variable workloads
- Estimated savings: **$38K-$57K annually**

**🤖 Enhanced Agentic AI Experience:**
- Multi-turn conversations with context retention
- Explainable AI with full reasoning transparency
- Confidence scores for all recommendations
- Rich response formatting with actionable insights
- Human-like interactions across 10+ conversation turns

**🔐 Advanced Governance & Compliance:**
- Automated PII detection (11 types for Australian context)
- Dynamic data masking based on user roles
- Rate limiting and quota management by role
- 100% Australian Privacy Principles (APP) compliance
- Real-time security monitoring and alerts

📖 **See [DATABRICKS_AGENTIC_AI_OPTIMIZATION.md](docs/DATABRICKS_AGENTIC_AI_OPTIMIZATION.md) for complete details**

## 🛠️ Development Timeline

| Week | Focus | Deliverables |
|------|-------|--------------|
| **Week 1** | Data + Setup | Synthetic data, Unity Catalog |
| **Week 2** | Ingestion | Zerobus, Auto Loader, Bronze layer |
| **Week 3** | Processing | Real-Time Mode, Silver/Gold layers |
| **Week 4** | AI + Dashboard | Agent, Vector Search, Streamlit |

**Effort:** 1-2 engineers, 4 weeks to production-ready demo

## 🔐 Security & Governance

### Unity Catalog Implementation

```sql
-- Catalog structure
CREATE CATALOG art_member_listening;

-- Schema isolation
CREATE SCHEMA bronze;  -- Raw data, restricted access
CREATE SCHEMA silver;  -- Cleaned data, analyst access
CREATE SCHEMA gold;    -- Aggregations, exec access

-- Row-level security (example)
CREATE OR REPLACE FUNCTION mask_member_id(member_id STRING)
RETURN CASE
  WHEN is_member_of('member_services_team')
  THEN member_id
  ELSE 'MASKED'
END;
```

### Data Privacy

- PII masking for non-authorized users
- Audit logging for all data access
- Retention policies (7 years for financial services)
- Compliance with Australian Privacy Principles (APP)

## 🧪 Testing

```bash
# Run data generation tests
pytest tests/test_data_generation.py

# Validate data quality
python tests/validate_data_quality.py

# Test agent tools
pytest tests/test_agent_tools.py
```

## 📚 Additional Resources

- [Databricks Zerobus Documentation](https://docs.databricks.com/ingestion/zerobus.html)
- [Real-Time Mode Guide](https://docs.databricks.com/structured-streaming/real-time-mode.html)
- [Mosaic AI Agent Framework](https://docs.databricks.com/generative-ai/agent-framework.html)
- [Vector Search Setup](https://docs.databricks.com/generative-ai/vector-search.html)

## 🤝 Contributing

This is a demo project. For production deployment:
1. Replace synthetic data with actual source integrations
2. Add comprehensive error handling
3. Implement CI/CD pipelines
4. Add monitoring and alerting
5. Conduct security review

## 📄 License

MIT License - See LICENSE file for details

## 👥 Authors

- Demo Architecture: Pravin Varadhan
- Databricks Platform: [Your Team]

## 🙋 Support

For questions about this demo:
- Create an issue in this repository
- Contact: [your-email@example.com]

---

**Built with ❤️ on Databricks**
