"""
Salesforce CRM Integration
===========================
Real integration with Salesforce to sync at-risk members and cases.

Features:
- Auto-create Salesforce cases for at-risk members
- Bi-directional sync (feedback → SF, resolution → VoC)
- Deduplication logic
- Bulk API for scale
- Custom field mapping

Setup:
    1. Create a Salesforce Connected App
    2. Get OAuth credentials
    3. Set environment variables or use Databricks secrets:
       - SALESFORCE_USERNAME
       - SALESFORCE_PASSWORD
       - SALESFORCE_SECURITY_TOKEN
       - SALESFORCE_CLIENT_ID
       - SALESFORCE_CLIENT_SECRET

Usage:
    from integrations.salesforce_integration import SalesforceSync

    sync = SalesforceSync()

    # Sync at-risk members to Salesforce
    sync.sync_at_risk_members()

    # Sync case resolutions back to VoC platform
    sync.sync_case_resolutions()

    # Two-way sync (scheduled job)
    sync.bidirectional_sync()
"""

from simple_salesforce import Salesforce, SalesforceLogin, SFBulkHandler
from pyspark.sql import SparkSession, functions as F
from datetime import datetime, timedelta
import os


class SalesforceSync:
    """Salesforce CRM integration for member feedback"""

    def __init__(self):
        self.spark = SparkSession.builder.getOrCreate()
        self.catalog = "art_member_listening"

        # Salesforce credentials (from Databricks secrets or environment)
        self.sf_username = self._get_secret("salesforce_username")
        self.sf_password = self._get_secret("salesforce_password")
        self.sf_security_token = self._get_secret("salesforce_security_token")
        self.sf_client_id = self._get_secret("salesforce_client_id", required=False)
        self.sf_client_secret = self._get_secret("salesforce_client_secret", required=False)

        # Salesforce domain (use 'test' for sandbox, leave blank for production)
        self.sf_domain = os.getenv("SALESFORCE_DOMAIN", "test")  # Change to '' for production

        # Connect to Salesforce
        self.sf = self._connect()

        # Field mapping: VoC platform → Salesforce
        self.case_field_mapping = {
            'member_id': 'Member_ID__c',  # Custom field in Salesforce
            'at_risk_score': 'At_Risk_Score__c',
            'sentiment_score': 'Sentiment_Score__c',
            'primary_topic': 'Primary_Topic__c',
            'contact_count_30d': 'Contact_Count_30d__c',
            'latest_feedback': 'Latest_Feedback__c'
        }

    def _get_secret(self, secret_name, required=True):
        """Get secret from Databricks secrets or environment variable"""

        try:
            # Try Databricks secrets first
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient()
            return w.secrets.get_secret(scope="salesforce", key=secret_name)
        except:
            # Fall back to environment variable
            value = os.getenv(secret_name.upper())
            if not value and required:
                raise ValueError(f"Secret '{secret_name}' not found in Databricks secrets or environment")
            return value

    def _connect(self):
        """Connect to Salesforce"""

        print("🔗 Connecting to Salesforce...")

        try:
            if self.sf_client_id and self.sf_client_secret:
                # OAuth 2.0 flow
                session_id, instance = SalesforceLogin(
                    username=self.sf_username,
                    password=self.sf_password,
                    security_token=self.sf_security_token,
                    client_id=self.sf_client_id,
                    domain=self.sf_domain if self.sf_domain else None
                )

                sf = Salesforce(instance=instance, session_id=session_id)
            else:
                # Simple username/password login
                sf = Salesforce(
                    username=self.sf_username,
                    password=self.sf_password,
                    security_token=self.sf_security_token,
                    domain=self.sf_domain if self.sf_domain else None
                )

            print(f"✅ Connected to Salesforce ({sf.sf_instance})")
            return sf

        except Exception as e:
            print(f"❌ Salesforce connection failed: {e}")
            raise

    def sync_at_risk_members(self, min_risk_score=0.7, lookback_days=7):
        """
        Sync at-risk members to Salesforce as cases

        Args:
            min_risk_score: Minimum at-risk score to sync (default 0.7)
            lookback_days: Only sync members with recent activity (default 7 days)
        """

        print(f"📤 Syncing at-risk members to Salesforce (risk >= {min_risk_score})...")

        # Get at-risk members from VoC platform
        query = f"""
        SELECT
            m.member_id,
            m.at_risk_score,
            m.avg_sentiment_30d as sentiment_score,
            m.primary_topic,
            m.contact_count_30d,
            m.contact_count_7d,
            m.last_interaction_date,
            m.preferred_contact_channel,
            m.member_tier,
            i.text as latest_feedback,
            i.interaction_id,
            i.channel as latest_channel

        FROM {self.catalog}.gold.member_360_view m
        JOIN {self.catalog}.silver.interactions_analyzed i
            ON m.member_id = i.member_id
            AND i.interaction_date = m.last_interaction_date

        WHERE m.at_risk_score >= {min_risk_score}
          AND m.last_interaction_date >= DATE_SUB(CURRENT_DATE(), {lookback_days})
          AND m.member_status = 'Active'

        ORDER BY m.at_risk_score DESC
        """

        at_risk_df = self.spark.sql(query)
        at_risk_members = at_risk_df.collect()

        if not at_risk_members:
            print("✅ No at-risk members to sync")
            return []

        print(f"   Found {len(at_risk_members)} at-risk members")

        synced_cases = []

        for member in at_risk_members:
            # Check if case already exists
            existing_case = self._find_existing_case(member['member_id'])

            if existing_case:
                print(f"   ⏭️  Case already exists for {member['member_id']} (Case ID: {existing_case})")
                continue

            # Create new case
            case_id = self._create_salesforce_case(member)

            if case_id:
                synced_cases.append({
                    'member_id': member['member_id'],
                    'salesforce_case_id': case_id,
                    'synced_at': datetime.now()
                })

        print(f"✅ Synced {len(synced_cases)} new cases to Salesforce")

        # Log sync to database
        if synced_cases:
            sync_df = self.spark.createDataFrame(synced_cases)
            sync_df.write.mode("append").saveAsTable(f"{self.catalog}.gold.salesforce_sync_log")

        return synced_cases

    def _find_existing_case(self, member_id):
        """Check if a case already exists for this member in Salesforce"""

        try:
            # Query Salesforce for existing case
            query = f"""
            SELECT Id, Status
            FROM Case
            WHERE Member_ID__c = '{member_id}'
              AND Status NOT IN ('Closed', 'Resolved')
              AND CreatedDate = LAST_N_DAYS:14
            ORDER BY CreatedDate DESC
            LIMIT 1
            """

            result = self.sf.query(query)

            if result['totalSize'] > 0:
                return result['records'][0]['Id']

            return None

        except Exception as e:
            print(f"   ⚠️  Error checking for existing case: {e}")
            return None

    def _create_salesforce_case(self, member):
        """Create a new case in Salesforce"""

        try:
            # Determine priority based on risk score
            priority = self._get_priority(member['at_risk_score'])

            # Build case description
            description = f"""
At-Risk Member Identified by AI Analysis

Member Details:
- Member ID: {member['member_id']}
- At-Risk Score: {member['at_risk_score']:.2f}
- Member Tier: {member['member_tier']}

Recent Activity:
- Contact Count (30 days): {member['contact_count_30d']}
- Contact Count (7 days): {member['contact_count_7d']}
- Average Sentiment: {member['sentiment_score']:.2f}
- Primary Concern: {member['primary_topic']}
- Last Contact: {member['last_interaction_date']} via {member['latest_channel']}

Latest Feedback:
"{member['latest_feedback'][:500]}{'...' if len(member['latest_feedback']) > 500 else ''}"

Recommended Action:
Proactive outreach via {member['preferred_contact_channel']} within 48 hours to address concerns and prevent potential churn.

---
Auto-created by ART Member Listening Intelligence Hub
"""

            # Create case record
            case = {
                'Subject': f"At-Risk Member: {member['member_id']} - {member['primary_topic']}",
                'Description': description,
                'Priority': priority,
                'Status': 'New',
                'Origin': 'AI - Member Listening Platform',
                'Type': 'Member Retention',

                # Custom fields (ensure these exist in your Salesforce org)
                'Member_ID__c': member['member_id'],
                'At_Risk_Score__c': float(member['at_risk_score']),
                'Sentiment_Score__c': float(member['sentiment_score']) if member['sentiment_score'] else 0.0,
                'Primary_Topic__c': member['primary_topic'],
                'Contact_Count_30d__c': int(member['contact_count_30d']),
                'Latest_Feedback__c': member['latest_feedback'][:255],  # Salesforce field length limit
                'Preferred_Contact_Channel__c': member['preferred_contact_channel']
            }

            # Create in Salesforce
            result = self.sf.Case.create(case)

            if result['success']:
                case_id = result['id']
                print(f"   ✅ Created case {case_id} for member {member['member_id']}")
                return case_id
            else:
                print(f"   ❌ Case creation failed: {result}")
                return None

        except Exception as e:
            print(f"   ❌ Error creating case for {member['member_id']}: {e}")
            return None

    def _get_priority(self, at_risk_score):
        """Determine Salesforce case priority from at-risk score"""

        if at_risk_score >= 0.85:
            return 'High'
        elif at_risk_score >= 0.7:
            return 'Medium'
        else:
            return 'Low'

    def sync_case_resolutions(self):
        """
        Sync case resolutions from Salesforce back to VoC platform
        (Bi-directional sync)
        """

        print("📥 Syncing case resolutions from Salesforce...")

        # Get cases that were created by our system
        query = """
        SELECT
            Id,
            Member_ID__c,
            Status,
            Priority,
            ClosedDate,
            Resolution_Notes__c,
            Member_Satisfaction__c
        FROM Case
        WHERE Origin = 'AI - Member Listening Platform'
          AND Status IN ('Resolved', 'Closed')
          AND ClosedDate = LAST_N_DAYS:7
        """

        result = self.sf.query(query)

        if result['totalSize'] == 0:
            print("✅ No resolved cases to sync")
            return []

        print(f"   Found {result['totalSize']} resolved cases")

        resolved_cases = []

        for case in result['records']:
            # Update VoC platform with resolution
            resolved_cases.append({
                'salesforce_case_id': case['Id'],
                'member_id': case['Member_ID__c'],
                'status': case['Status'],
                'closed_date': case['ClosedDate'],
                'resolution_notes': case.get('Resolution_Notes__c'),
                'member_satisfaction': case.get('Member_Satisfaction__c'),
                'synced_at': datetime.now()
            })

        # Write resolutions to database
        if resolved_cases:
            resolutions_df = self.spark.createDataFrame(resolved_cases)
            resolutions_df.write.mode("append").saveAsTable(f"{self.catalog}.gold.case_resolutions_from_sf")

            print(f"✅ Synced {len(resolved_cases)} case resolutions from Salesforce")

        return resolved_cases

    def bidirectional_sync(self):
        """
        Full bi-directional sync (scheduled job)
        1. Push at-risk members to Salesforce
        2. Pull case resolutions from Salesforce
        """

        print("=" * 100)
        print("SALESFORCE BI-DIRECTIONAL SYNC")
        print(f"Run time: {datetime.now()}")
        print("=" * 100)

        # 1. Push at-risk members to SF
        print("\n1️⃣  Pushing at-risk members to Salesforce...")
        pushed_cases = self.sync_at_risk_members()

        # 2. Pull resolutions from SF
        print("\n2️⃣  Pulling case resolutions from Salesforce...")
        pulled_resolutions = self.sync_case_resolutions()

        # 3. Summary
        print("\n📊 Sync Summary:")
        print(f"   Cases Created in SF: {len(pushed_cases)}")
        print(f"   Resolutions Synced from SF: {len(pulled_resolutions)}")

        print("\n✅ Bi-directional sync complete")

        return {
            'pushed_cases': len(pushed_cases),
            'pulled_resolutions': len(pulled_resolutions)
        }

    def bulk_sync(self, member_ids):
        """
        Bulk sync specific members using Salesforce Bulk API
        (For initial data load or large batches)

        Args:
            member_ids: List of member IDs to sync
        """

        print(f"⚡ Bulk syncing {len(member_ids)} members using Salesforce Bulk API...")

        # Get member data
        member_ids_str = "', '".join(member_ids)

        query = f"""
        SELECT
            m.member_id,
            m.at_risk_score,
            m.avg_sentiment_30d as sentiment_score,
            m.primary_topic,
            m.contact_count_30d,
            i.text as latest_feedback

        FROM {self.catalog}.gold.member_360_view m
        JOIN {self.catalog}.silver.interactions_analyzed i
            ON m.member_id = i.member_id
            AND i.interaction_date = m.last_interaction_date

        WHERE m.member_id IN ('{member_ids_str}')
        """

        members_df = self.spark.sql(query)
        members = members_df.collect()

        # Prepare bulk insert data
        bulk_cases = []

        for member in members:
            case = {
                'Subject': f"At-Risk Member: {member['member_id']}",
                'Description': member['latest_feedback'][:1000],
                'Priority': self._get_priority(member['at_risk_score']),
                'Status': 'New',
                'Origin': 'AI - Member Listening Platform - Bulk',
                'Type': 'Member Retention',
                'Member_ID__c': member['member_id'],
                'At_Risk_Score__c': float(member['at_risk_score']),
                'Sentiment_Score__c': float(member['sentiment_score']) if member['sentiment_score'] else 0.0,
                'Primary_Topic__c': member['primary_topic']
            }

            bulk_cases.append(case)

        # Use Bulk API
        try:
            bulk_handler = SFBulkHandler(
                session_id=self.sf.session_id,
                bulk_url=self.sf.bulk_url
            )

            result = bulk_handler.insert_data('Case', bulk_cases)

            print(f"✅ Bulk sync complete")

            return result

        except Exception as e:
            print(f"❌ Bulk sync failed: {e}")
            return None


# ============================================================================
# SCHEDULED JOB: Daily Salesforce Sync
# ============================================================================

def scheduled_salesforce_sync():
    """Scheduled job to run daily for Salesforce sync"""

    sync = SalesforceSync()
    result = sync.bidirectional_sync()

    return result


if __name__ == "__main__":
    # Test Salesforce integration
    sync = SalesforceSync()

    # Run bi-directional sync
    sync.bidirectional_sync()
