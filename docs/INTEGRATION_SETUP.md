# Integration Setup Guide

Configuration guide for Slack and Salesforce integrations.

---

## 🔔 Slack Integration Setup

### Step 1: Set Environment Variable

The Slack webhook URL is configured via environment variable to avoid committing secrets to Git.

**Your Webhook URL:** [PROVIDED SEPARATELY - DO NOT COMMIT TO GIT]

**Channel:** #all-mcp-testers

### Step 2: Configure in Databricks

#### Option A: Set as Environment Variable (Cluster)
1. Go to your Databricks cluster configuration
2. Click **Edit** → **Advanced Options** → **Environment Variables**
3. Add:
   ```
   SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
   ```
4. Restart cluster

#### Option B: Use Databricks Secrets (Recommended)
```bash
# Create secrets scope
databricks secrets create-scope slack

# Add webhook URL (use your actual webhook URL)
databricks secrets put slack webhook_url --string-value "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
```

Then update `analytics/08_alert_engine.py`:
```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
slack_webhook = w.secrets.get_secret(scope="slack", key="webhook_url")

self.notification_config = {
    "slack_webhook": slack_webhook,
    # ...
}
```

### Step 3: Test Slack Alerts

Run test script:
```python
from analytics.alert_engine import AlertEngine
import os

# Set webhook for this session (use your actual webhook URL)
os.environ['SLACK_WEBHOOK_URL'] = 'https://hooks.slack.com/services/YOUR/WEBHOOK/URL'

# Test alert
engine = AlertEngine()

# Create test data
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

test_data = [{
    "member_id": "TEST123",
    "interaction_id": "INT_TEST",
    "interaction_date": "2025-11-16",
    "channel": "Test",
    "text": "This is a test alert for Slack integration",
    "sentiment_score": -0.85,
    "primary_topic": "Test",
    "at_risk_score": 0.9,
    "member_tier": "VIP"
}]

test_df = spark.createDataFrame(test_data)

# Trigger alerts
alerts = engine.evaluate_alerts(test_df)
if alerts:
    engine.send_alerts(alerts)
    print(f"✅ Sent {len(alerts)} test alerts to #all-mcp-testers")
```

Expected result: You should see a message in #all-mcp-testers with the test alert.

---

## 🏢 Salesforce Integration Setup

### Your Salesforce Developer Edition

**Login URL:** https://orgfarm-ea6d9e5047-dev-ed.develop.my.salesforce.com

**Username:** pravin.varmadbrx898@agentforce.com

**Password:** [Your password]

### Step 1: Get Security Token

1. Log in to Salesforce
2. Click your profile icon → **Settings**
3. In Quick Find, search: "Reset My Security Token"
4. Click **Reset Security Token**
5. Check your email (pravin.varmadbrx898@agentforce.com) for the token

### Step 2: Create Custom Fields in Salesforce

Navigate to **Setup** → **Object Manager** → **Case** → **Fields & Relationships**

Click **New** and create these custom fields:

| Field Label | Field Name | Data Type | Length/Precision |
|-------------|-----------|-----------|------------------|
| Member ID | `Member_ID__c` | Text | 50 |
| At Risk Score | `At_Risk_Score__c` | Number | 3, 2 (e.g., 0.85) |
| Sentiment Score | `Sentiment_Score__c` | Number | 3, 2 (e.g., -0.75) |
| Primary Topic | `Primary_Topic__c` | Text | 100 |
| Contact Count 30d | `Contact_Count_30d__c` | Number | 0 decimal places |
| Latest Feedback | `Latest_Feedback__c` | Text Area | 32,768 |
| Preferred Contact Channel | `Preferred_Contact_Channel__c` | Text | 50 |
| Resolution Notes | `Resolution_Notes__c` | Text Area (Long) | 32,768 |
| Member Satisfaction | `Member_Satisfaction__c` | Number | 1, 0 (e.g., 4.0) |

**Note:** The `__c` suffix is added automatically by Salesforce for custom fields.

### Step 3: Configure Databricks Secrets

```bash
# Create secrets scope
databricks secrets create-scope salesforce

# Add credentials
databricks secrets put salesforce salesforce_username --string-value "pravin.varmadbrx898@agentforce.com"
databricks secrets put salesforce salesforce_password --string-value "YOUR_PASSWORD"
databricks secrets put salesforce salesforce_security_token --string-value "YOUR_TOKEN_FROM_EMAIL"

# Optional: OAuth credentials (if you set up Connected App)
databricks secrets put salesforce salesforce_client_id --string-value "YOUR_CLIENT_ID"
databricks secrets put salesforce salesforce_client_secret --string-value "YOUR_CLIENT_SECRET"

# Set domain (use 'test' for dev orgs)
databricks secrets put salesforce salesforce_domain --string-value "test"
```

### Step 4: Test Salesforce Integration

```python
from integrations.salesforce_integration import SalesforceSync

# Initialize sync (will use Databricks secrets)
sync = SalesforceSync()

# Test connection
print(f"Connected to: {sync.sf.sf_instance}")

# Sync a test member
test_results = sync.sync_at_risk_members(min_risk_score=0.7, lookback_days=7)

print(f"✅ Created {len(test_results)} cases in Salesforce")

# Check in Salesforce
# Go to: Salesforce → Cases → View All → Should see auto-created cases
```

### Step 5: Verify in Salesforce UI

1. Log in to your Salesforce dev org
2. Go to **Cases** tab
3. Click **View All**
4. Look for cases with:
   - Origin: "AI - Member Listening Platform"
   - Member ID populated
   - At-Risk Score populated

### Step 6: Test Bi-directional Sync

1. In Salesforce, update a case to "Resolved" status
2. Add resolution notes
3. Run pull sync:
```python
from integrations.salesforce_integration import SalesforceSync

sync = SalesforceSync()
resolutions = sync.sync_case_resolutions()

print(f"✅ Synced {len(resolutions)} resolutions from Salesforce")

# Check in Databricks
spark.sql("SELECT * FROM art_member_listening.gold.case_resolutions_from_sf").show()
```

---

## 🔄 Scheduled Jobs Setup

### Job 1: Salesforce Sync (Daily)

**Databricks Job Configuration:**
```yaml
name: Salesforce Bi-directional Sync
schedule: Daily at 9:00 AM Australia/Sydney
cluster: Existing cluster or new job cluster
tasks:
  - task_key: salesforce_sync
    python_script:
      script: integrations/salesforce_integration.py
      entry_point: scheduled_salesforce_sync
```

Create job:
```bash
databricks jobs create --json '{
  "name": "Salesforce Bi-directional Sync",
  "tasks": [{
    "task_key": "salesforce_sync",
    "python_wheel_task": {
      "package_name": "art_member_listening",
      "entry_point": "scheduled_salesforce_sync"
    }
  }],
  "schedule": {
    "quartz_cron_expression": "0 0 9 * * ?",
    "timezone_id": "Australia/Sydney"
  }
}'
```

### Job 2: Alert Engine with Slack Notifications (Integrated with Streaming)

Update your existing streaming job to include alert evaluation:

```python
# In processing/03_realtime_sentiment_processing_v2.py

from analytics.alert_engine import process_batch_with_alerts

# Replace existing writeStream with:
query = (
    stream
    .writeStream
    .foreachBatch(process_batch_with_alerts)  # <-- Includes alert evaluation
    .trigger(processingTime='5 seconds')
    .start()
)
```

---

## 📊 Dashboard Integration

### Run the Multi-Page Dashboard

```bash
cd dashboard

# Set environment variable (use your actual webhook URL)
export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/YOUR/WEBHOOK/URL"

# Run dashboard
python app_multipage.py
```

Access at: `http://localhost:8050`

**Pages:**
- `/` - Executive Dashboard
- `/cases` - Case Management
- `/alerts` - Alert Monitoring (will show Slack alert history)
- `/topics` - Topic Trends

---

## ✅ Verification Checklist

### Slack Integration
- [ ] Webhook URL configured (env var or secrets)
- [ ] Test alert sent successfully
- [ ] Message appears in #all-mcp-testers
- [ ] Alert includes proper formatting and data

### Salesforce Integration
- [ ] Connected to Salesforce dev org
- [ ] Custom fields created in Case object
- [ ] Test case created via API
- [ ] Case visible in Salesforce UI
- [ ] Bi-directional sync working
- [ ] Resolution data flowing back to Databricks

### Dashboards
- [ ] Multi-page dashboard launches
- [ ] All pages load without errors
- [ ] Data displays in tables and charts
- [ ] Auto-refresh working
- [ ] Navigation between pages works

---

## 🔧 Troubleshooting

### Slack Alerts Not Sending

**Check:**
1. Environment variable is set correctly
2. Webhook URL is correct and active
3. Network connectivity from Databricks to Slack
4. Alert rules are triggering (check `gold.alert_history` table)

**Debug:**
```python
import os
print(f"Webhook URL: {os.getenv('SLACK_WEBHOOK_URL')}")
```

### Salesforce Connection Failed

**Common Issues:**
1. **Invalid username/password** - Verify credentials
2. **Security token needed** - Append security token to password
3. **IP restricted** - Add Databricks IP to Trusted IP Ranges in Salesforce Setup
4. **Wrong domain** - Use "test" for dev orgs, "" for production

**Debug:**
```python
from simple_salesforce import Salesforce

sf = Salesforce(
    username='pravin.varmadbrx898@agentforce.com',
    password='YOUR_PASSWORD' + 'YOUR_SECURITY_TOKEN',
    domain='test'  # For dev org
)

print(f"Connected: {sf.sf_instance}")
```

### Custom Fields Not Found

**Error:** `INVALID_FIELD: No such column 'Member_ID__c'`

**Solution:**
1. Verify fields created in Salesforce
2. Check field API names end with `__c`
3. Wait a few minutes for metadata to propagate

---

## 📞 Support

**Integration Questions:**
- Slack: Check webhook URL in Slack workspace settings
- Salesforce: Verify field creation and credentials
- Dashboards: Check `dashboard/README.md`

**Code References:**
- Slack: `analytics/08_alert_engine.py:57`
- Salesforce: `integrations/salesforce_integration.py`
- Dashboards: `dashboard/app_multipage.py`
