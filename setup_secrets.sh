#!/bin/bash
# Setup Databricks Secrets for ART VoC Platform
# Run this script on your Mac with Databricks CLI configured
#
# Prerequisites:
# 1. Install Databricks CLI: pip install databricks-cli
# 2. Configure auth: databricks configure --token
# 3. Enter your workspace URL and personal access token

set -e

echo "🔐 Setting up Databricks Secrets for ART VoC Platform"
echo "======================================================"

# Create secret scope
echo ""
echo "📦 Creating secret scope 'art_integrations'..."
databricks secrets create-scope art_integrations --initial-manage-principal users || echo "   ℹ️  Scope already exists"

# Slack Integration
echo ""
echo "💬 Setting up Slack integration..."
read -p "Enter Slack Webhook URL: " slack_webhook
databricks secrets put-secret art_integrations slack_webhook_url --string-value "$slack_webhook"
echo "   ✅ Slack webhook configured"

# Twilio Integration
echo ""
echo "📱 Setting up Twilio (SMS) integration..."
read -p "Enter Twilio Account SID: " twilio_sid
read -p "Enter Twilio Auth Token: " twilio_token
read -p "Enter Twilio Phone Number (e.g., +14409702306): " twilio_phone
databricks secrets put-secret art_integrations twilio_account_sid --string-value "$twilio_sid"
databricks secrets put-secret art_integrations twilio_auth_token --string-value "$twilio_token"
databricks secrets put-secret art_integrations twilio_phone_number --string-value "$twilio_phone"
echo "   ✅ Twilio credentials configured"

# Salesforce Integration
echo ""
echo "☁️  Setting up Salesforce integration..."
read -p "Enter Salesforce Username: " sf_username
read -p "Enter Salesforce Password: " sf_password
read -p "Enter Salesforce Security Token: " sf_token
read -p "Enter Salesforce Domain (test for dev orgs, login for production): " sf_domain
databricks secrets put-secret art_integrations salesforce_username --string-value "$sf_username"
databricks secrets put-secret art_integrations salesforce_password --string-value "$sf_password"
databricks secrets put-secret art_integrations salesforce_security_token --string-value "$sf_token"
databricks secrets put-secret art_integrations salesforce_domain --string-value "$sf_domain"
echo "   ✅ Salesforce credentials configured"

# SMTP Configuration (Optional)
echo ""
read -p "Do you want to configure SMTP for email alerts? (y/n): " configure_smtp
if [ "$configure_smtp" = "y" ]; then
    echo "📧 Setting up SMTP integration..."
    read -p "Enter SMTP Server (e.g., smtp.gmail.com): " smtp_server
    read -p "Enter SMTP Port (e.g., 587): " smtp_port
    read -p "Enter SMTP Username: " smtp_user
    read -p "Enter SMTP Password/App Password: " smtp_pass
    databricks secrets put-secret art_integrations smtp_server --string-value "$smtp_server"
    databricks secrets put-secret art_integrations smtp_port --string-value "$smtp_port"
    databricks secrets put-secret art_integrations smtp_username --string-value "$smtp_user"
    databricks secrets put-secret art_integrations smtp_password --string-value "$smtp_pass"
    databricks secrets put-secret art_integrations smtp_enabled --string-value "true"
    echo "   ✅ SMTP credentials configured"
else
    databricks secrets put-secret art_integrations smtp_enabled --string-value "false"
    echo "   ℹ️  SMTP disabled (email alerts will be simulated)"
fi

# SMS Configuration
databricks secrets put-secret art_integrations sms_enabled --string-value "true"

echo ""
echo "✅ All secrets configured successfully!"
echo ""
echo "📋 Verify secrets were created:"
echo "   databricks secrets list-secrets art_integrations"
echo ""
echo "🚀 You can now run the VoC platform with all integrations enabled"
