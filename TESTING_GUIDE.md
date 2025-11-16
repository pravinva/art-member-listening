# Testing Guide - ART VoC Platform

Complete guide to test all enterprise features with real integrations.

## Quick Start - Test Everything

### 1. Set Environment Variables

```bash
# Export all credentials (get actual values from CREDENTIALS.md)
export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/YOUR/WEBHOOK/URL"

export SMS_ENABLED="true"
export TWILIO_ACCOUNT_SID="ACxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
export TWILIO_AUTH_TOKEN="your-twilio-auth-token"
export TWILIO_PHONE_NUMBER="+1234567890"

export SMTP_ENABLED="false"  # Set to "true" if you have SMTP configured
# export SMTP_SERVER="smtp.gmail.com"
# export SMTP_PORT="587"
# export SMTP_USERNAME="your-email@example.com"
# export SMTP_PASSWORD="your-app-password"
```

**Note:** Get actual credential values from CREDENTIALS.md (not committed to git)

Or create a `.env` file (never commit this!):
```bash
cp .env.example .env
# Edit .env with your actual credentials from CREDENTIALS.md
```

### 2. Set Databricks Secrets (For Production)

```bash
# Slack
databricks secrets create-scope art_integrations
databricks secrets put art_integrations slack_webhook_url --string-value "YOUR-WEBHOOK-URL"

# Twilio
databricks secrets put art_integrations twilio_account_sid --string-value "YOUR-ACCOUNT-SID"
databricks secrets put art_integrations twilio_auth_token --string-value "YOUR-AUTH-TOKEN"
databricks secrets put art_integrations twilio_phone_number --string-value "YOUR-PHONE-NUMBER"

# Salesforce
databricks secrets put art_integrations salesforce_username --string-value "YOUR-SF-USERNAME"
databricks secrets put art_integrations salesforce_password --string-value "YOUR-SF-PASSWORD"
databricks secrets put art_integrations salesforce_security_token --string-value "YOUR-SF-TOKEN"
databricks secrets put art_integrations salesforce_domain --string-value "test"
```

**Get actual values from CREDENTIALS.md**

---

## Testing Individual Features

### Test 1: Slack Alerts

```python
# Run in Databricks notebook or Python script
# NOTE: Credentials will be read from Databricks secrets automatically
# No need to set environment variables when using Databricks

from analytics.alert_engine import AlertEngine
from datetime import datetime

engine = AlertEngine()

# Create test alert
test_alert = {
    'rule_id': 'test_alert',
    'rule_description': 'Testing Slack integration from VoC Platform',
    'severity': 'High',
    'count': 5,
    'triggered_at': datetime.now(),
    'matches': [
        {
            'member_id': 'M123456',
            'text_preview': 'Test member feedback for Slack notification',
            'sentiment_score': -0.8
        }
    ]
}

engine._send_slack_alert(test_alert)
print("✅ Check #all-mcp-testers channel for alert!")
```

**Expected Result:** Message appears in #all-mcp-testers Slack channel

---

### Test 2: SMS Alerts (Twilio)

```python
# Run in Databricks notebook or Python script
# NOTE: Credentials will be read from Databricks secrets automatically
# No need to set environment variables when using Databricks

from analytics.alert_engine import AlertEngine
from datetime import datetime

engine = AlertEngine()

# Create test SMS alert
test_alert = {
    'rule_id': 'test_sms',
    'rule_description': 'Critical VIP member issue detected',
    'severity': 'Critical',
    'count': 1,
    'triggered_at': datetime.now(),
    'notify': ['+61XXXXXXXXX'],  # Your verified number from CREDENTIALS.md
    'matches': []
}

engine._send_sms_alert(test_alert)
print("✅ Check your phone for SMS!")
```

**Expected Result:** SMS received at your verified number

---

### Test 3: Email Alerts (SMTP)

**Option A: Gmail (Recommended for testing)**

1. Enable 2FA on your Gmail account
2. Generate App Password: https://myaccount.google.com/apppasswords
3. Configure SMTP secrets in Databricks (see SETUP_DATABRICKS_SECRETS.md)

```python
# Run in Databricks notebook or Python script
# NOTE: SMTP credentials will be read from Databricks secrets automatically
# Make sure smtp_enabled is set to "true" in secrets

from analytics.alert_engine import AlertEngine
from datetime import datetime

engine = AlertEngine()

test_alert = {
    'rule_id': 'test_email',
    'rule_description': 'Testing email integration',
    'severity': 'High',
    'count': 3,
    'triggered_at': datetime.now(),
    'notify': ['your-email@gmail.com'],
    'matches': [
        {'member_id': 'M123', 'text_preview': 'Test feedback'}
    ]
}

engine._send_email_alert(test_alert)
print("✅ Check your email inbox!")
```

**Option B: Skip email testing**
Don't configure SMTP secrets - system will simulate email sending

---

### Test 4: Salesforce Integration

```python
from integrations.salesforce_integration import SalesforceSync

# Initialize (uses credentials from CREDENTIALS.md)
sf_sync = SalesforceSync()

# Test 1: Sync at-risk members (creates Cases in Salesforce)
print("📤 Syncing at-risk members to Salesforce...")
result = sf_sync.sync_at_risk_members(min_risk_score=0.7)
print(f"✅ Created {len(result)} Salesforce cases")

# Test 2: Check cases in Salesforce
cases = sf_sync.sf.query("SELECT Id, Subject, Status FROM Case WHERE Origin = 'VoC Platform' ORDER BY CreatedDate DESC LIMIT 5")
print("\n📋 Recent cases in Salesforce:")
for case in cases['records']:
    print(f"  - {case['Subject']} (Status: {case['Status']})")

# Test 3: Bi-directional sync (pull resolutions back)
print("\n🔄 Syncing case resolutions back to VoC platform...")
sf_sync.sync_case_resolutions()
print("✅ Sync complete")
```

**Expected Result:** Cases appear in your Salesforce org (URL from CREDENTIALS.md)

---

### Test 5: Closed-Loop Case Management

```python
from analytics.case_manager import CaseManager

manager = CaseManager()

# Test 1: Auto-create cases for at-risk members
print("🔍 Auto-creating cases for at-risk members...")
new_cases = manager.auto_create_cases(lookback_hours=24)
print(f"✅ Created {len(new_cases)} new cases")

# Test 2: Check case statistics
stats = manager.get_case_stats()
print("\n📊 Case Statistics:")
print(f"  Open Cases: {stats['open_cases']}")
print(f"  SLA Breached: {stats['sla_breached']}")
print(f"  SLA At Risk: {stats['sla_at_risk']}")

# Test 3: Check SLA breaches (triggers auto-escalation)
print("\n⏰ Checking SLA status...")
sla_status = manager.check_sla_breaches()
print(f"  Breached: {len(sla_status['breached'])}")
print(f"  At Risk: {len(sla_status['at_risk'])}")

# Test 4: Manual case creation
print("\n➕ Creating test case...")
case_id = manager.create_case(
    member_id="M999999",
    issue_type="Insurance - Claims",
    severity="High",
    description="Test case for VoC platform demonstration"
)
print(f"✅ Created case: {case_id}")

# Test 5: Resolve the case
print(f"\n✅ Resolving case {case_id}...")
manager.resolve_case(
    case_id=case_id,
    resolution_notes="Successfully tested case creation and resolution workflow"
)
print("✅ Case resolved")
```

---

### Test 6: AI-Powered Topic Modeling

```python
from analytics.topic_modeling import TopicModeler

modeler = TopicModeler()

# Extract topics using Databricks Foundation Models (Llama 3.1 70B)
print("🤖 Extracting topics with AI...")
topics = modeler.extract_topics_ai(batch_size=100)

# View topic distribution
print("\n📊 Topic Distribution:")
distribution = modeler.get_topic_trends(lookback_days=30)
for topic in distribution.head(10):
    print(f"  {topic['topic']}: {topic['count']} mentions")

# Detect emerging topics
print("\n🔥 Emerging Topics (trending up):")
emerging = modeler.detect_emerging_topics(lookback_days=30)
for topic in emerging:
    print(f"  {topic['topic']}: +{topic['growth_rate']:.1f}% growth")
```

---

### Test 7: Predictive At-Risk Model

```python
from analytics.predictive_at_risk_model import AtRiskModel

model = AtRiskModel()

# Train model (if not already trained)
print("🎯 Training at-risk prediction model...")
metrics = model.train(test_split=0.2)
print(f"  AUC: {metrics['auc']:.3f}")
print(f"  Precision: {metrics['precision']:.3f}")
print(f"  Recall: {metrics['recall']:.3f}")

# Score all active members
print("\n📊 Scoring members for churn risk...")
model_path = model.get_latest_model()
scores = model.score_members(model_path)

# View high-risk members
high_risk = scores.filter("at_risk_probability >= 0.7").limit(10)
print("\n⚠️  Top 10 At-Risk Members:")
high_risk.select("member_id", "at_risk_probability", "risk_factors").show(truncate=False)
```

---

### Test 8: SHAP Driver Analysis

```python
from analytics.driver_analysis import DriverAnalyzer

analyzer = DriverAnalyzer()

# Analyze NPS drivers
print("📈 Analyzing NPS drivers...")
nps_drivers = analyzer.analyze_nps_drivers()
print("\nTop NPS Drivers:")
for _, row in nps_drivers.head(5).iterrows():
    print(f"  {row['feature']}: {row['importance']:.3f} ({row['direction']})")

# Analyze churn/at-risk drivers
print("\n📉 Analyzing churn drivers...")
churn_drivers = analyzer.analyze_churn_drivers()

# Get SHAP explanations (requires model path)
print("\n🔍 Generating SHAP explanations...")
from analytics.predictive_at_risk_model import AtRiskModel
model = AtRiskModel()
model_path = model.get_latest_model()

shap_analysis = analyzer.get_shap_explanations(model_path=model_path)
# SHAP plot saved to /tmp/shap_summary.png
```

---

### Test 9: Real-Time Alert Engine

```python
from analytics.alert_engine import AlertEngine

engine = AlertEngine()

# Run all alert rules
print("🚨 Running alert engine...")
alerts = engine.run_alert_rules()

print(f"\n📊 Alert Summary:")
print(f"  Total alerts triggered: {len(alerts)}")

for alert in alerts:
    print(f"\n  🔔 {alert['rule_description']}")
    print(f"     Severity: {alert['severity']}")
    print(f"     Members affected: {alert['count']}")
    print(f"     Channels: {', '.join(alert.get('channels', ['email']))}")

    # Shows what notifications were sent:
    # - Slack message to #all-mcp-testers ✅
    # - SMS to verified number ✅
    # - Email (if SMTP enabled) ✅
```

**Expected Result:** Multi-channel alerts (Slack + SMS + Email)

---

### Test 10: Complete Dashboard Suite

```bash
# Run multi-page Dash dashboard
cd dashboard
python app_multipage.py
```

Then visit:
- **Executive Dashboard:** http://localhost:8050/
- **Case Management:** http://localhost:8050/cases
- **Alert Monitoring:** http://localhost:8050/alerts
- **Topic Trends:** http://localhost:8050/topics

**Expected Result:** Interactive dashboards with:
- Real-time metrics (auto-refresh)
- Case queue with SLA tracking
- Alert timeline and statistics
- Topic evolution and sentiment analysis

---

## Full End-to-End Test

Run the complete scheduled job that exercises all features:

```python
from analytics.case_manager import scheduled_case_management

# This will:
# 1. Auto-create cases for at-risk members
# 2. Check SLA breaches
# 3. Auto-escalate breached Critical/High cases
# 4. Send email alerts for escalations
# 5. Update case statistics

scheduled_case_management()
```

**What happens:**
1. ✅ Cases created in `gold.feedback_cases` table
2. ✅ SLA breaches detected
3. ✅ Critical cases auto-escalated to gm@art.com.au
4. ✅ Email alerts sent (if SMTP enabled)
5. ✅ Audit trail logged in `gold.case_actions`

---

## Verify Results

### Check Slack
Visit: Your Slack workspace
Channel: #all-mcp-testers
**Expected:** Alert messages with member details

### Check SMS
Your verified phone number (from CREDENTIALS.md)
**Expected:** SMS with "ART Alert (Critical)" messages

### Check Salesforce
Visit: Your Salesforce org URL (from CREDENTIALS.md)
Navigate to: Cases tab
**Expected:** Cases from VoC Platform with member IDs

### Check Databricks Tables
```sql
-- Check cases
SELECT * FROM art_member_listening.gold.feedback_cases
ORDER BY created_at DESC LIMIT 10;

-- Check SLA status
SELECT severity, sla_status, COUNT(*)
FROM art_member_listening.gold.feedback_cases
GROUP BY severity, sla_status;

-- Check at-risk scores
SELECT member_id, at_risk_probability, risk_factors
FROM art_member_listening.gold.member_at_risk_scores_ml
WHERE at_risk_probability >= 0.7
ORDER BY at_risk_probability DESC
LIMIT 20;

-- Check topics
SELECT topic, COUNT(*) as count
FROM art_member_listening.gold.feedback_topics_ai
WHERE topic_date >= CURRENT_DATE() - 30
GROUP BY topic
ORDER BY count DESC;
```

---

## Troubleshooting

### Slack not working
- ✅ Verify webhook URL is correct
- ✅ Check channel exists: #all-mcp-testers
- ✅ Ensure environment variable is set: `echo $SLACK_WEBHOOK_URL`

### SMS not working
- ✅ Trial account: recipient must be verified in Twilio Console
- ✅ Check credentials: Account SID, Auth Token, Phone Number
- ✅ Verify SMS_ENABLED=true
- ✅ Check Twilio console for error logs

### Email not working
- ✅ Check SMTP_ENABLED=true
- ✅ For Gmail: use App Password, not regular password
- ✅ Check firewall allows port 587
- ✅ Try simulation mode first (SMTP_ENABLED=false)

### Salesforce not working
- ✅ Verify credentials in CREDENTIALS.md
- ✅ Check security token is current
- ✅ Use domain='test' for developer edition
- ✅ Run: `pip install simple-salesforce`

### Tables don't exist
```sql
-- Run schema creation first
%run ./config/01_member_360_schema.sql
%run ./config/02_closed_loop_feedback_schema.sql
```

### Import errors
```bash
# Install required packages
pip install twilio simple-salesforce plotly dash dash-bootstrap-components shap
```

---

## Next Steps

1. **Generate Synthetic Data** - Run data generation scripts to populate tables
2. **Schedule Jobs** - Set up Databricks workflows for automated execution
3. **Configure Dashboards** - Deploy Dash apps to production
4. **Train Models** - Run ML model training on historical data
5. **Monitor Alerts** - Set up 24/7 monitoring with real integrations

**Platform is ready for production deployment!** 🚀
