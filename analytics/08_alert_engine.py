"""
Real-Time Alerting & Escalation Engine
=======================================
Monitors member feedback and sends real-time alerts when critical conditions occur.

Features:
- Configurable alert rules
- Multi-channel notifications (Email, Slack, SMS)
- Alert throttling to prevent fatigue
- Alert history and analytics
- Integration with case management

Usage:
    from analytics.alert_engine import AlertEngine

    engine = AlertEngine()

    # Process new interactions and check for alerts
    df = spark.table("silver.interactions_analyzed")
    alerts = engine.evaluate_alerts(df)

    # Send alerts
    engine.send_alerts(alerts)

    # Add custom alert rule
    engine.add_rule(
        name="vip_negative_insurance",
        condition="member_tier = 'VIP' AND sentiment_score < -0.6 AND primary_topic = 'Insurance'",
        severity="Critical",
        notify=["gm@art.com.au"],
        channels=["email", "slack"]
    )
"""

from pyspark.sql import SparkSession, functions as F
from datetime import datetime, timedelta
import json
import requests
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import uuid


class AlertEngine:
    """Real-time alert engine for member feedback"""

    def __init__(self):
        self.spark = SparkSession.builder.getOrCreate()
        self.catalog = "art_member_listening"

        # Alert rules configuration
        self.alert_rules = self._load_alert_rules()

        # Notification settings
        self.notification_config = {
            "slack_webhook": os.getenv("SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"),
            "email_from": "member-listening@art.com.au",
            "smtp_server": "smtp.art.com.au",
            "smtp_port": 587
        }

        # Alert throttling (prevent alert fatigue)
        self.throttle_config = {
            "vip_negative": {"window_hours": 24, "max_alerts": 5},
            "repeated_contact": {"window_hours": 12, "max_alerts": 3},
            "topic_spike": {"window_hours": 6, "max_alerts": 2}
        }

    def _load_alert_rules(self):
        """Load alert rules from configuration"""

        return {
            "vip_negative": {
                "name": "VIP Member Negative Feedback",
                "condition": "member_tier IN ('VIP', 'Platinum') AND sentiment_score < -0.6",
                "severity": "Critical",
                "notify": ["gm@art.com.au", "member_services_lead@art.com.au"],
                "channels": ["email", "slack"],
                "description": "VIP or Platinum member with strongly negative feedback",
                "action": "immediate_escalation"
            },
            "repeated_contact": {
                "name": "Repeated Contact - Same Issue",
                "condition": "contact_count_7d >= 3 AND sentiment_score < 0 AND sentiment_trend_30d < -0.2",
                "severity": "High",
                "notify": ["operations_manager@art.com.au", "team_lead@art.com.au"],
                "channels": ["email", "slack"],
                "description": "Member contacted 3+ times in 7 days with declining sentiment",
                "action": "assign_case"
            },
            "topic_spike": {
                "name": "Topic Spike Detection",
                "condition": "topic_mention_increase_24h > 3.0",  # 300% increase
                "severity": "Medium",
                "notify": ["insights_team@art.com.au", "product_manager@art.com.au"],
                "channels": ["slack"],
                "description": "Sudden spike in mentions of specific topic",
                "action": "investigate"
            },
            "critical_sentiment_drop": {
                "name": "Critical Sentiment Drop",
                "condition": "avg_sentiment_7d < -0.7 AND sentiment_trend_30d < -0.3",
                "severity": "Critical",
                "notify": ["executive_team@art.com.au"],
                "channels": ["email", "slack"],
                "description": "Overall sentiment has dropped critically",
                "action": "executive_review"
            },
            "at_risk_member": {
                "name": "At-Risk Member Detected",
                "condition": "at_risk_score >= 0.8",
                "severity": "High",
                "notify": ["retention_team@art.com.au"],
                "channels": ["email"],
                "description": "Member flagged as high risk for churn",
                "action": "proactive_outreach"
            },
            "urgent_issue": {
                "name": "Urgent Issue Detected",
                "condition": "urgency_score >= 0.9 AND sentiment_score < -0.5",
                "severity": "High",
                "notify": ["operations_manager@art.com.au"],
                "channels": ["email", "slack"],
                "description": "Urgent issue requiring immediate attention",
                "action": "assign_case"
            },
            "compliance_keyword": {
                "name": "Compliance/Legal Keyword",
                "condition": "text_lower LIKE '%lawsuit%' OR text_lower LIKE '%ombudsman%' OR text_lower LIKE '%complaint%' OR text_lower LIKE '%legal action%'",
                "severity": "Critical",
                "notify": ["legal@art.com.au", "compliance@art.com.au", "gm@art.com.au"],
                "channels": ["email"],
                "description": "Member mentioned legal/compliance keywords",
                "action": "legal_review"
            }
        }

    def evaluate_alerts(self, df, rule_filter=None):
        """
        Evaluate alert rules against incoming data

        Args:
            df: DataFrame with member interactions/metrics
            rule_filter: Optional list of rule names to evaluate (default: all)

        Returns:
            List of alerts triggered
        """

        # Add computed columns for alert rules
        df_prepared = df.withColumn("text_lower", F.lower(F.col("text")))

        # Enrich with member 360 data
        df_enriched = df_prepared.alias("i").join(
            self.spark.table(f"{self.catalog}.gold.member_360_view").alias("m"),
            on="member_id",
            how="left"
        )

        alerts = []

        # Evaluate each rule
        rules_to_check = (
            {k: v for k, v in self.alert_rules.items() if k in rule_filter}
            if rule_filter
            else self.alert_rules
        )

        for rule_name, rule in rules_to_check.items():
            # Find matching records
            matches_df = df_enriched.filter(rule["condition"])
            match_count = matches_df.count()

            if match_count > 0:
                # Check throttling
                if self._should_throttle(rule_name, match_count):
                    print(f"⏸️  Alert '{rule_name}' throttled (too many recent alerts)")
                    continue

                # Collect matching records
                matches = matches_df.limit(100).collect()  # Limit to prevent memory issues

                alert = {
                    "alert_id": f"ALERT_{uuid.uuid4().hex[:12].upper()}",
                    "rule_name": rule_name,
                    "rule_description": rule["description"],
                    "severity": rule["severity"],
                    "count": match_count,
                    "matches": [self._format_match(row) for row in matches],
                    "notify": rule["notify"],
                    "channels": rule["channels"],
                    "action": rule.get("action"),
                    "triggered_at": datetime.now()
                }

                alerts.append(alert)

                # Log alert to database
                self._log_alert(alert)

                print(f"🚨 Alert triggered: {rule['name']} ({match_count} matches)")

        return alerts

    def _should_throttle(self, rule_name, current_count):
        """Check if alert should be throttled based on recent history"""

        throttle = self.throttle_config.get(rule_name)
        if not throttle:
            return False

        # Query recent alerts
        window_start = datetime.now() - timedelta(hours=throttle["window_hours"])

        recent_alerts = self.spark.sql(f"""
            SELECT COUNT(*) as alert_count
            FROM {self.catalog}.gold.alert_history
            WHERE rule_name = '{rule_name}'
              AND triggered_at >= TIMESTAMP'{window_start}'
        """).first()

        if recent_alerts and recent_alerts['alert_count'] >= throttle["max_alerts"]:
            return True

        return False

    def _format_match(self, row):
        """Format a matching row for alert message"""

        return {
            "member_id": row.get("member_id"),
            "interaction_id": row.get("interaction_id"),
            "date": str(row.get("interaction_date")),
            "channel": row.get("channel"),
            "sentiment_score": round(row.get("sentiment_score", 0), 2) if row.get("sentiment_score") else None,
            "at_risk_score": round(row.get("at_risk_score", 0), 2) if row.get("at_risk_score") else None,
            "topic": row.get("primary_topic"),
            "text_preview": row.get("text", "")[:200] + "..." if row.get("text") and len(row.get("text", "")) > 200 else row.get("text", "")
        }

    def send_alerts(self, alerts):
        """Send alerts via configured channels"""

        for alert in alerts:
            print(f"\n📤 Sending alert: {alert['rule_name']}")

            # Send to each channel
            for channel in alert["channels"]:
                if channel == "email":
                    self._send_email_alert(alert)
                elif channel == "slack":
                    self._send_slack_alert(alert)
                elif channel == "sms":
                    self._send_sms_alert(alert)

            # Take automated action if configured
            if alert.get("action"):
                self._execute_action(alert)

    def _send_email_alert(self, alert):
        """Send email notification"""

        subject = f"🚨 {alert['severity']} Alert: {alert['rule_description']}"

        # Build email body
        body_html = f"""
        <html>
        <body style="font-family: Arial, sans-serif;">
            <h2 style="color: {'#d9534f' if alert['severity'] == 'Critical' else '#f0ad4e'};">
                {alert['severity']} Alert Triggered
            </h2>

            <p><strong>Rule:</strong> {alert['rule_description']}</p>
            <p><strong>Count:</strong> {alert['count']} member(s) affected</p>
            <p><strong>Time:</strong> {alert['triggered_at'].strftime('%Y-%m-%d %H:%M:%S')}</p>

            <h3>Affected Members:</h3>
            <table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse;">
                <tr style="background-color: #f0f0f0;">
                    <th>Member ID</th>
                    <th>Sentiment</th>
                    <th>At-Risk</th>
                    <th>Topic</th>
                    <th>Preview</th>
                </tr>
        """

        for match in alert["matches"][:20]:  # Show max 20 in email
            body_html += f"""
                <tr>
                    <td>{match['member_id']}</td>
                    <td>{match['sentiment_score']}</td>
                    <td>{match['at_risk_score']}</td>
                    <td>{match['topic']}</td>
                    <td>{match['text_preview']}</td>
                </tr>
            """

        body_html += """
            </table>

            <p style="margin-top: 20px;">
                <a href="https://your-databricks-workspace.com/dashboard" style="
                    background-color: #5cb85c;
                    color: white;
                    padding: 10px 20px;
                    text-decoration: none;
                    border-radius: 4px;
                ">View in Dashboard</a>
            </p>
        </body>
        </html>
        """

        # Send email
        try:
            # Check if SMTP is configured
            smtp_enabled = os.getenv('SMTP_ENABLED', 'false').lower() == 'true'

            if smtp_enabled:
                # Real SMTP sending
                msg = MIMEMultipart('alternative')
                msg['Subject'] = subject
                msg['From'] = self.notification_config['email_from']
                msg['To'] = ', '.join(alert['notify'])
                msg.attach(MIMEText(body_html, 'html'))

                # Get SMTP credentials from environment
                smtp_server = os.getenv('SMTP_SERVER', self.notification_config['smtp_server'])
                smtp_port = int(os.getenv('SMTP_PORT', self.notification_config['smtp_port']))
                smtp_username = os.getenv('SMTP_USERNAME')
                smtp_password = os.getenv('SMTP_PASSWORD')
                use_tls = os.getenv('SMTP_USE_TLS', 'true').lower() == 'true'

                # Send email
                server = smtplib.SMTP(smtp_server, smtp_port)

                if use_tls:
                    server.starttls()

                if smtp_username and smtp_password:
                    server.login(smtp_username, smtp_password)

                server.send_message(msg)
                server.quit()

                print(f"   ✅ Email sent to {len(alert['notify'])} recipients via {smtp_server}")
            else:
                # Simulated mode (default)
                print(f"   ✅ Email notification ready for {len(alert['notify'])} recipients")
                print(f"      Subject: {subject}")
                print(f"      To: {', '.join(alert['notify'])}")
                print(f"      💡 Set SMTP_ENABLED=true to send real emails")

        except Exception as e:
            print(f"   ❌ Email sending failed: {e}")
            print(f"      💡 Check SMTP configuration in environment variables")

    def _send_slack_alert(self, alert):
        """Send Slack notification"""

        # Determine color based on severity
        color = {
            "Critical": "#d9534f",
            "High": "#f0ad4e",
            "Medium": "#5bc0de",
            "Low": "#5cb85c"
        }.get(alert['severity'], "#5cb85c")

        # Build Slack message
        message = {
            "text": f"🚨 *{alert['severity']} Alert*: {alert['rule_description']}",
            "attachments": [
                {
                    "color": color,
                    "fields": [
                        {
                            "title": "Rule",
                            "value": alert['rule_description'],
                            "short": False
                        },
                        {
                            "title": "Members Affected",
                            "value": str(alert['count']),
                            "short": True
                        },
                        {
                            "title": "Severity",
                            "value": alert['severity'],
                            "short": True
                        }
                    ],
                    "footer": "ART Member Listening Platform",
                    "ts": int(alert['triggered_at'].timestamp())
                }
            ]
        }

        # Add sample members
        if alert['matches']:
            sample_text = "\n".join([
                f"• Member {m['member_id']}: {m['text_preview']}"
                for m in alert['matches'][:5]
            ])
            message["attachments"][0]["fields"].append({
                "title": "Sample Feedback",
                "value": sample_text,
                "short": False
            })

        # Send to Slack
        try:
            webhook_url = self.notification_config['slack_webhook']
            response = requests.post(webhook_url, json=message, timeout=10)

            if response.status_code == 200:
                print(f"   ✅ Slack alert sent")
            else:
                print(f"   ❌ Slack alert failed: {response.status_code}")

        except Exception as e:
            print(f"   ❌ Slack sending failed: {e}")

    def _send_sms_alert(self, alert):
        """Send SMS notification using Twilio"""

        # Check if SMS is enabled via environment variable
        sms_enabled = os.getenv('SMS_ENABLED', 'false').lower() == 'true'

        # Get recipients (phone numbers)
        recipients = alert.get('notify', [])
        if not recipients:
            print(f"   ℹ️  No SMS recipients configured for this alert")
            return

        # Prepare message content
        message_body = (
            f"ART Alert ({alert['severity']})\n"
            f"{alert['rule_description']}\n"
            f"{alert['count']} members affected\n"
            f"Time: {alert['triggered_at']}"
        )

        if sms_enabled:
            # Real SMS sending using Twilio
            try:
                from twilio.rest import Client

                # Get Twilio credentials from environment
                account_sid = os.getenv('TWILIO_ACCOUNT_SID')
                auth_token = os.getenv('TWILIO_AUTH_TOKEN')
                from_phone = os.getenv('TWILIO_PHONE_NUMBER')

                if not all([account_sid, auth_token, from_phone]):
                    print(f"   ⚠️  SMS enabled but Twilio credentials incomplete")
                    print(f"   💡 Set TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER")
                    return

                # Initialize Twilio client
                client = Client(account_sid, auth_token)

                # Send SMS to each recipient
                sent_count = 0
                for to_phone in recipients:
                    try:
                        message = client.messages.create(
                            body=message_body,
                            from_=from_phone,
                            to=to_phone
                        )
                        sent_count += 1
                        print(f"   ✅ SMS sent to {to_phone} (SID: {message.sid})")
                    except Exception as e:
                        print(f"   ❌ Failed to send SMS to {to_phone}: {str(e)}")

                if sent_count > 0:
                    print(f"   ✅ {sent_count}/{len(recipients)} SMS alerts sent successfully")

            except ImportError:
                print(f"   ⚠️  Twilio library not installed. Run: pip install twilio")
                print(f"   📱 Would send SMS: {message_body[:50]}...")
            except Exception as e:
                print(f"   ❌ SMS sending failed: {str(e)}")
                print(f"   💡 Check your Twilio configuration")
        else:
            # Simulation mode - show what would be sent
            print(f"   📱 SMS alert (simulated)")
            print(f"      Recipients: {', '.join(recipients)}")
            print(f"      Message: {message_body}")
            print(f"   💡 Set SMS_ENABLED=true to send real SMS via Twilio")

    def _execute_action(self, alert):
        """Execute automated action based on alert"""

        action = alert.get("action")

        if action == "assign_case":
            # Auto-create case
            from analytics.case_manager import CaseManager
            manager = CaseManager()

            for match in alert['matches'][:10]:  # Create cases for top 10
                manager.create_case(
                    member_id=match['member_id'],
                    interaction_id=match.get('interaction_id'),
                    issue_type=match.get('topic', 'Unknown'),
                    severity="High",
                    description=f"Auto-created from alert: {alert['rule_description']}",
                    auto_created=True
                )

            print(f"   ✅ Created {min(len(alert['matches']), 10)} cases")

        elif action == "immediate_escalation":
            # Send to GM/executive team
            print(f"   ⚠️  Immediate escalation required")

        elif action == "proactive_outreach":
            # Add to CRM for proactive contact
            print(f"   📞 Queued for proactive outreach")

    def _log_alert(self, alert):
        """Log alert to database for history and analytics"""

        alert_record = {
            "alert_id": alert['alert_id'],
            "rule_name": alert['rule_name'],
            "severity": alert['severity'],
            "member_count": alert['count'],
            "triggered_at": alert['triggered_at'],
            "notification_channels": ",".join(alert['channels']),
            "notification_recipients": ",".join(alert['notify']),
            "action_taken": alert.get('action'),
            "match_details": json.dumps([
                {"member_id": m['member_id'], "sentiment": m['sentiment_score']}
                for m in alert['matches'][:100]
            ])
        }

        # Create table if not exists
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.catalog}.gold.alert_history (
                alert_id STRING,
                rule_name STRING,
                severity STRING,
                member_count INT,
                triggered_at TIMESTAMP,
                notification_channels STRING,
                notification_recipients STRING,
                action_taken STRING,
                match_details STRING
            )
        """)

        # Insert alert
        alert_df = self.spark.createDataFrame([alert_record])
        alert_df.write.mode("append").saveAsTable(f"{self.catalog}.gold.alert_history")

    def add_rule(self, name, condition, severity, notify, channels, description=None, action=None):
        """Add a custom alert rule"""

        self.alert_rules[name] = {
            "name": name,
            "condition": condition,
            "severity": severity,
            "notify": notify,
            "channels": channels,
            "description": description or name,
            "action": action
        }

        print(f"✅ Added alert rule: {name}")

    def get_alert_stats(self):
        """Get alert statistics"""

        stats = self.spark.sql(f"""
            SELECT
                COUNT(*) as total_alerts,
                COUNT(CASE WHEN DATE(triggered_at) = CURRENT_DATE() THEN 1 END) as alerts_today,
                COUNT(CASE WHEN severity = 'Critical' THEN 1 END) as critical_alerts,
                COUNT(CASE WHEN severity = 'High' THEN 1 END) as high_alerts,
                COUNT(DISTINCT rule_name) as unique_rules_triggered
            FROM {self.catalog}.gold.alert_history
            WHERE triggered_at >= DATE_SUB(CURRENT_DATE(), 30)
        """).first()

        return stats.asDict() if stats else {}


# ============================================================================
# INTEGRATION WITH STREAMING PIPELINE
# ============================================================================

def process_batch_with_alerts(batch_df, batch_id):
    """
    Process streaming batch and evaluate alerts
    Use this in structured streaming foreachBatch
    """

    print(f"\n⚡ Processing batch {batch_id} with alert evaluation...")

    # Initialize alert engine
    alert_engine = AlertEngine()

    # Evaluate alerts on this batch
    alerts = alert_engine.evaluate_alerts(batch_df)

    # Send alerts
    if alerts:
        alert_engine.send_alerts(alerts)
        print(f"   🚨 {len(alerts)} alerts triggered and sent")
    else:
        print(f"   ✅ No alerts triggered")

    # Continue with normal processing (write to Silver table, etc.)
    batch_df.write.mode("append").saveAsTable("art_member_listening.silver.interactions_analyzed")


if __name__ == "__main__":
    # Test alert engine with sample data
    spark = SparkSession.builder.getOrCreate()

    # Create sample test data
    test_data = [
        {
            "member_id": "M123456",
            "interaction_id": "INT_001",
            "interaction_date": datetime.now(),
            "channel": "Call",
            "text": "I'm extremely frustrated with the insurance options. This is unacceptable.",
            "sentiment_score": -0.85,
            "primary_topic": "Insurance",
            "at_risk_score": 0.75,
            "member_tier": "VIP"
        }
    ]

    test_df = spark.createDataFrame(test_data)

    # Evaluate alerts
    engine = AlertEngine()
    alerts = engine.evaluate_alerts(test_df)

    # Send alerts (will be simulated)
    if alerts:
        engine.send_alerts(alerts)

    print(f"\n✅ Alert engine test complete. {len(alerts)} alerts triggered.")
