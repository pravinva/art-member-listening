"""
Secrets Manager for ART VoC Platform
=====================================
Centralized secrets management using Databricks secrets.
Falls back to environment variables for local testing.

Usage:
    from utils.secrets_manager import get_secret

    slack_webhook = get_secret("slack_webhook_url")
"""

import os


def get_secret(key, scope="art_integrations", default=None):
    """
    Get secret from Databricks secrets or environment variables.

    Priority:
    1. Databricks secrets (production)
    2. Environment variables (local testing)
    3. Default value (fallback)

    Args:
        key: Secret key name
        scope: Databricks secret scope (default: art_integrations)
        default: Default value if secret not found

    Returns:
        Secret value as string
    """

    # Try Databricks secrets first (production)
    try:
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        dbutils = DBUtils(spark)

        value = dbutils.secrets.get(scope=scope, key=key)
        if value:
            return value
    except Exception:
        pass  # Databricks not available, try environment variables

    # Fall back to environment variables (local testing)
    env_var = os.getenv(key.upper())
    if env_var:
        return env_var

    # Return default
    return default


def is_enabled(feature):
    """
    Check if a feature is enabled via secrets.

    Args:
        feature: Feature name (e.g., 'sms', 'smtp')

    Returns:
        Boolean indicating if feature is enabled
    """
    value = get_secret(f"{feature}_enabled", default="false")
    return value.lower() in ['true', '1', 'yes', 'enabled']


# Pre-defined secret getters for common integrations
def get_slack_webhook():
    """Get Slack webhook URL"""
    return get_secret("slack_webhook_url")


def get_twilio_credentials():
    """Get Twilio credentials as dict"""
    return {
        'account_sid': get_secret("twilio_account_sid"),
        'auth_token': get_secret("twilio_auth_token"),
        'phone_number': get_secret("twilio_phone_number"),
        'enabled': is_enabled('sms')
    }


def get_salesforce_credentials():
    """Get Salesforce credentials as dict"""
    return {
        'username': get_secret("salesforce_username"),
        'password': get_secret("salesforce_password"),
        'security_token': get_secret("salesforce_security_token"),
        'domain': get_secret("salesforce_domain", default="test")
    }


def get_smtp_credentials():
    """Get SMTP credentials as dict"""
    return {
        'server': get_secret("smtp_server", default="smtp.gmail.com"),
        'port': int(get_secret("smtp_port", default="587")),
        'username': get_secret("smtp_username"),
        'password': get_secret("smtp_password"),
        'use_tls': get_secret("smtp_use_tls", default="true").lower() == 'true',
        'from_email': get_secret("smtp_from_email", default="alerts@art.com.au"),
        'from_name': get_secret("smtp_from_name", default="ART Member Intelligence"),
        'enabled': is_enabled('smtp')
    }
