# Setup Databricks Secrets - Quick Start Guide

This guide shows you how to securely configure all VoC platform credentials using Databricks secrets (no credentials in repo).

## Prerequisites

1. **Install Databricks CLI** on your Mac:
```bash
pip install databricks-cli
```

2. **Configure Authentication**:
```bash
databricks configure --token
```

When prompted:
- **Databricks Host**: Your workspace URL (e.g., `https://your-workspace.cloud.databricks.com`)
- **Token**: Your personal access token
  - Get from: Workspace → Settings → User Settings → Access Tokens → Generate New Token

## One-Command Setup

Run the interactive setup script (recommended):

```bash
./setup_secrets.sh
```

This will:
1. Create the `art_integrations` secret scope
2. Prompt you for each credential
3. Store everything securely in Databricks secrets

**Credentials you'll need:**
- Slack webhook URL
- Twilio Account SID, Auth Token, Phone Number
- Salesforce Username, Password, Security Token, Domain
- (Optional) SMTP server details for email alerts

## Manual Setup (Alternative)

If you prefer to set secrets manually:

### 1. Create Secret Scope

```bash
databricks secrets create-scope art_integrations
```

### 2. Add Slack Credentials

```bash
databricks secrets put-secret art_integrations slack_webhook_url \
  --string-value "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
```

### 3. Add Twilio Credentials

```bash
databricks secrets put-secret art_integrations sms_enabled --string-value "true"

databricks secrets put-secret art_integrations twilio_account_sid \
  --string-value "ACxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

databricks secrets put-secret art_integrations twilio_auth_token \
  --string-value "your-twilio-auth-token"

databricks secrets put-secret art_integrations twilio_phone_number \
  --string-value "+1234567890"
```

### 4. Add Salesforce Credentials

```bash
databricks secrets put-secret art_integrations salesforce_username \
  --string-value "your-username@salesforce.com"

databricks secrets put-secret art_integrations salesforce_password \
  --string-value "your-password"

databricks secrets put-secret art_integrations salesforce_security_token \
  --string-value "your-security-token"

databricks secrets put-secret art_integrations salesforce_domain \
  --string-value "test"
```

**Note:** Get actual values from CREDENTIALS.md (not committed to git)

### 5. (Optional) Add SMTP Email Credentials

```bash
databricks secrets put-secret art_integrations smtp_enabled --string-value "true"
databricks secrets put-secret art_integrations smtp_server --string-value "smtp.gmail.com"
databricks secrets put-secret art_integrations smtp_port --string-value "587"
databricks secrets put-secret art_integrations smtp_username --string-value "your-email@gmail.com"
databricks secrets put-secret art_integrations smtp_password --string-value "your-app-password"
```

## Verify Secrets

List all secrets in the scope (values are redacted):

```bash
databricks secrets list-secrets art_integrations
```

Expected output:
```
key                          last_updated_timestamp
slack_webhook_url            1234567890000
sms_enabled                  1234567890000
twilio_account_sid           1234567890000
twilio_auth_token            1234567890000
twilio_phone_number          1234567890000
salesforce_username          1234567890000
salesforce_password          1234567890000
salesforce_security_token    1234567890000
salesforce_domain            1234567890000
smtp_enabled                 1234567890000
...
```

## How the Code Uses Secrets

All integration code automatically uses Databricks secrets via the `utils/secrets_manager.py` module:

```python
from utils.secrets_manager import (
    get_slack_webhook,
    get_twilio_credentials,
    get_salesforce_credentials,
    get_smtp_credentials
)

# Example: Alert Engine
webhook = get_slack_webhook()  # Reads from Databricks secrets
```

**Fallback for Local Testing:**
If Databricks secrets aren't available (e.g., local development), the code falls back to environment variables:

```bash
export SLACK_WEBHOOK_URL="https://..."
export TWILIO_ACCOUNT_SID="AC0bf..."
# etc.
```

## Security Benefits

✅ **No credentials in git repository** - CREDENTIALS.md is in .gitignore
✅ **No credentials in code** - All values read from secrets at runtime
✅ **No credentials in notebooks** - Secrets are never displayed
✅ **Audit trail** - Databricks logs all secret access
✅ **Access control** - Secrets scope has permissions management

## Testing the Setup

After configuring secrets, test each integration:

### Test Slack

```python
from analytics.alert_engine import AlertEngine
from datetime import datetime

engine = AlertEngine()

test_alert = {
    'rule_id': 'test',
    'rule_description': 'Testing Databricks secrets integration',
    'severity': 'High',
    'count': 1,
    'triggered_at': datetime.now(),
    'matches': []
}

engine._send_slack_alert(test_alert)
# ✅ Check #all-mcp-testers for message
```

### Test SMS

```python
test_alert = {
    'rule_id': 'test_sms',
    'rule_description': 'Testing Twilio integration',
    'severity': 'Critical',
    'count': 1,
    'triggered_at': datetime.now(),
    'notify': ['+61416099849'],  # Your verified number
    'matches': []
}

engine._send_sms_alert(test_alert)
# ✅ Check your phone for SMS
```

### Test Salesforce

```python
from integrations.salesforce_integration import SalesforceSync

sync = SalesforceSync()
# ✅ Should connect successfully and print: "Connected to Salesforce (orgfarm-ea6d9e5047-dev-ed.develop.my.salesforce.com)"

# Query Salesforce
cases = sync.sf.query("SELECT Id, Subject FROM Case LIMIT 5")
print(f"Found {len(cases['records'])} cases")
```

## Updating Secrets

To update a secret value:

```bash
databricks secrets delete-secret art_integrations slack_webhook_url
databricks secrets put-secret art_integrations slack_webhook_url --string-value "new-value"
```

Or delete and re-run the setup script:

```bash
databricks secrets delete-scope art_integrations
./setup_secrets.sh
```

## Troubleshooting

### "Secret scope art_integrations does not exist"

Create the scope:
```bash
databricks secrets create-scope art_integrations
```

### "Secret [key] does not exist"

Add the missing secret:
```bash
databricks secrets put-secret art_integrations [key] --string-value "[value]"
```

### "Authentication failed"

Reconfigure Databricks CLI:
```bash
databricks configure --token
```

### "Permission denied"

Ensure your user has permissions on the secret scope:
```bash
databricks secrets put-acl art_integrations --principal <your-email> --permission MANAGE
```

## Next Steps

Once secrets are configured:

1. ✅ Run the full platform with real integrations
2. ✅ Deploy to Databricks workflows
3. ✅ Set up scheduled jobs
4. ✅ Monitor multi-channel alerts

See **TESTING_GUIDE.md** for comprehensive testing instructions.
