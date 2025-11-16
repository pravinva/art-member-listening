# Quick Start Guide - ART Member Listening Demo

Get the demo running in **30 minutes** on your local machine or Databricks workspace.

## Prerequisites

- Databricks Workspace (Community Edition works for testing)
- Databricks CLI configured
- Python 3.9+
- 8GB+ RAM for local development

## Setup Steps

### 1. Clone and Install Dependencies (5 minutes)

```bash
# Clone repository
git clone https://github.com/pravinva/art-member-listening.git
cd art-member-listening

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure Databricks (5 minutes)

```bash
# Authenticate with Databricks CLI
databricks configure --token

# You'll be prompted for:
# - Databricks workspace URL: https://your-workspace.cloud.databricks.com
# - Access token: (generate from User Settings > Access Tokens)
```

### 3. Setup Unity Catalog (5 minutes)

```bash
# Upload and run Unity Catalog setup SQL
databricks sql --file config/01_unity_catalog_setup.sql

# This creates:
# - Catalog: art_member_listening
# - Schemas: bronze, silver, gold, ml_models
# - Tables: portal_events, call_transcripts, emails, chats, surveys, etc.
```

**Verify:**
```sql
-- Run in Databricks SQL Warehouse
USE CATALOG art_member_listening;
SHOW SCHEMAS;
```

### 4. Generate Synthetic Data (10 minutes)

```bash
# Generate all synthetic data
python data_generation/generate_all_data.py \
  --num-calls 10000 \          # Reduce for faster generation
  --num-email-threads 5000 \
  --num-surveys 3000 \
  --num-chat-sessions 7500 \
  --num-members 10000 \
  --output-dir /dbfs/mnt/landing

# This generates:
# - 10K call transcripts (~100MB)
# - 5K email threads (~50MB)
# - 3K survey responses (~10MB)
# - 7.5K chat sessions (~30MB)
# - 10K member profiles (~5MB)
```

**Quick test:** Use smaller dataset for testing
```bash
python data_generation/generate_calls.py \
  --num-calls 1000 \
  --output-dir /dbfs/mnt/landing/calls \
  --format json
```

### 5. Load Data into Bronze Tables (5 minutes)

Option A: **Using Databricks Notebook**

```python
# In Databricks notebook

# Load calls
calls_df = spark.read.json("/dbfs/mnt/landing/calls/calls.jsonl")
calls_df.write.format("delta").mode("append").saveAsTable("art_member_listening.bronze.call_transcripts")

# Load emails
emails_df = spark.read.json("/dbfs/mnt/landing/emails/emails.jsonl")
emails_df.write.format("delta").mode("append").saveAsTable("art_member_listening.bronze.emails")

# Verify
spark.sql("SELECT COUNT(*) FROM art_member_listening.bronze.call_transcripts").show()
```

Option B: **Using Python script**

```bash
databricks workspace import-dir ./notebooks /Users/your-email@company.com/art-demo
```

### 6. Start Real-Time Processing Pipeline

```python
# In Databricks notebook: processing/03_realtime_sentiment_processing.py

from processing.realtime_sentiment_processing import RealtimeSentimentProcessor
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Enable Real-Time Mode
spark.conf.set("spark.sql.streaming.realTimeMode.enabled", "true")

processor = RealtimeSentimentProcessor(spark)
query = processor.start_realtime_processing()

# Monitor
processor.monitor_stream(query)
```

**Note:** This will start processing interactions with <300ms latency.

### 7. Launch Dashboard (5 minutes)

```bash
# From project root
cd dashboard
python app.py

# Dashboard will be available at:
# http://localhost:8050
```

**Navigate to:**
- Executive Dashboard: http://localhost:8050/
- Operations Dashboard: http://localhost:8050/operations
- AI Assistant: http://localhost:8050/ai-assistant

---

## Quick Demo Flow (5 minutes)

### 1. Executive Dashboard
- View KPI metrics updating in real-time
- Check sentiment trends chart
- Look for emerging issues

### 2. Operations Dashboard
- See live activity feed
- Browse at-risk members table
- Download intervention list

### 3. AI Assistant
- Ask: "What are members most frustrated about?"
- Ask: "Show me at-risk members"
- Ask: "Analyze insurance topic sentiment"

---

## Troubleshooting

### Issue: "Catalog not found"

**Solution:**
```sql
-- Verify catalog exists
SHOW CATALOGS;

-- If missing, re-run setup
databricks sql --file config/01_unity_catalog_setup.sql
```

### Issue: "No data in tables"

**Solution:**
```sql
-- Check data
SELECT COUNT(*) FROM art_member_listening.bronze.call_transcripts;

-- If empty, re-load data
spark.read.json("/dbfs/mnt/landing/calls/calls.jsonl").write.format("delta").mode("append").saveAsTable("art_member_listening.bronze.call_transcripts")
```

### Issue: "Dashboard shows no data"

**Solution:**
- Ensure processing pipeline is running
- Check that data exists in silver/gold tables
- Restart dashboard: `Ctrl+C` then `python app.py`

### Issue: "Real-Time Mode not enabled"

**Solution:**
```python
# Explicitly enable
spark.conf.set("spark.sql.streaming.realTimeMode.enabled", "true")

# Verify
spark.conf.get("spark.sql.streaming.realTimeMode.enabled")
# Should return 'true'
```

### Issue: "Databricks Foundation Model not available"

**Solution:**
- Use alternative model endpoint:
  ```python
  # In agent code, change:
  model_endpoint = "databricks-dbrx-instruct"  # Instead of Sonnet 4.5
  ```
- Or use fallback keyword-based sentiment (already implemented)

---

## Architecture Verification Checklist

After setup, verify each layer:

- [ ] **Bronze Layer**: Raw data loaded
  ```sql
  SELECT COUNT(*) FROM art_member_listening.bronze.call_transcripts;
  SELECT COUNT(*) FROM art_member_listening.bronze.emails;
  ```

- [ ] **Silver Layer**: Processed with sentiment
  ```sql
  SELECT COUNT(*) FROM art_member_listening.silver.interactions_analyzed;
  SELECT sentiment_label, COUNT(*) FROM art_member_listening.silver.interactions_analyzed GROUP BY sentiment_label;
  ```

- [ ] **Gold Layer**: Aggregated views
  ```sql
  SELECT COUNT(*) FROM art_member_listening.gold.member_360_view WHERE at_risk_flag = true;
  ```

- [ ] **Dashboard**: All three views load without errors

- [ ] **AI Agent**: Responds to queries with data

---

## Optional: Production Deployment

### Use Databricks Jobs

```python
# Create job for processing pipeline
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

job = w.jobs.create(
    name="ART Member Listening - Real-Time Processing",
    tasks=[{
        "task_key": "realtime_processing",
        "notebook_task": {
            "notebook_path": "/Users/your-email/art-demo/processing/03_realtime_sentiment_processing",
            "source": "WORKSPACE"
        },
        "new_cluster": {
            "spark_version": "14.3.x-scala2.12",
            "node_type_id": "i3.xlarge",
            "num_workers": 2
        }
    }],
    schedule={"quartz_cron_expression": "0 0 * * * ?", "timezone_id": "UTC"}
)

print(f"Job created: {job.job_id}")
```

### Deploy Dashboard to Databricks Apps

```bash
# Package dashboard
databricks apps create \
  --name "ART Member Listening Hub" \
  --source dashboard/ \
  --port 8050

# Get public URL
databricks apps get --name "ART Member Listening Hub"
```

---

## Next Steps

1. **Customize data generation** to match your actual data patterns
2. **Connect real data sources** (replace synthetic data)
3. **Fine-tune ML models** with your historical data
4. **Configure Unity Catalog access controls** for your teams
5. **Schedule regular data refreshes** using Databricks Jobs
6. **Set up monitoring and alerts** for pipeline health

---

## Support

- **Documentation**: See `/docs` folder
- **Issues**: GitHub Issues
- **Questions**: [your-email@company.com]

---

**Estimated Total Setup Time: 30-40 minutes**

Enjoy your demo! 🚀
