"""
Closed-Loop Feedback Case Management
=====================================
Automatically creates and manages feedback cases from member interactions.

Features:
- Auto-create cases for at-risk members
- SLA tracking and management
- Case assignment and routing
- Resolution tracking
- Audit trail

Usage:
    from analytics.case_manager import CaseManager

    manager = CaseManager()

    # Auto-create cases from at-risk members
    manager.auto_create_cases()

    # Manually create a case
    case_id = manager.create_case(
        member_id="M123456",
        interaction_id="INT_789",
        issue_type="Insurance",
        severity="High",
        description="Member confused about TPD coverage"
    )

    # Assign case
    manager.assign_case(case_id, assigned_to="john.smith@art.com.au", team="Insurance Team")

    # Update case status
    manager.update_status(case_id, "In Progress")

    # Resolve case
    manager.resolve_case(case_id, resolution_notes="Provided TPD explanation call")
"""

from pyspark.sql import SparkSession, functions as F
from datetime import datetime, timedelta
import uuid


class CaseManager:
    """Manages feedback cases for closed-loop tracking"""

    def __init__(self):
        self.spark = SparkSession.builder.getOrCreate()
        self.catalog = "art_member_listening"

        # SLA configuration (loaded from config table)
        self.sla_config = self._load_sla_config()

        # Case severity scoring rules
        self.severity_rules = {
            "critical": {
                "at_risk_score": 0.85,
                "sentiment_threshold": -0.8,
                "contact_count_7d": 4,
                "member_tier": ["VIP", "Platinum"]
            },
            "high": {
                "at_risk_score": 0.7,
                "sentiment_threshold": -0.6,
                "contact_count_7d": 3
            },
            "medium": {
                "at_risk_score": 0.5,
                "sentiment_threshold": -0.4,
                "contact_count_7d": 2
            },
            "low": {
                "at_risk_score": 0.3,
                "sentiment_threshold": -0.2
            }
        }

    def _load_sla_config(self):
        """Load SLA configuration from database"""
        sla_df = self.spark.table(f"{self.catalog}.gold.case_sla_config")
        sla_dict = {}
        for row in sla_df.collect():
            sla_dict[row['severity']] = {
                'first_response_hours': row['first_response_hours'],
                'resolution_hours': row['resolution_hours'],
                'escalation_hours': row['escalation_hours']
            }
        return sla_dict

    def auto_create_cases(self, lookback_hours=24):
        """
        Automatically create cases for at-risk members and negative interactions

        Conditions for auto-case creation:
        1. At-risk score >= 0.7
        2. Sentiment score <= -0.6
        3. 3+ contacts in last 7 days with declining sentiment
        4. VIP member with any negative sentiment

        Args:
            lookback_hours: How many hours back to check for new interactions
        """

        print(f"🔍 Scanning for interactions requiring case creation (last {lookback_hours}h)...")

        # Query to find interactions that need cases
        query = f"""
        WITH recent_interactions AS (
            SELECT
                i.interaction_id,
                i.member_id,
                i.interaction_date,
                i.channel,
                i.text,
                i.sentiment_score,
                i.sentiment_label,
                i.primary_topic,
                i.urgency_score,
                m.at_risk_score,
                m.member_tier,
                m.contact_count_7d,
                m.avg_sentiment_30d,
                m.sentiment_trend_30d,
                m.primary_contact_channel
            FROM {self.catalog}.silver.interactions_analyzed i
            LEFT JOIN {self.catalog}.gold.member_360_view m ON i.member_id = m.member_id
            WHERE i.interaction_date >= TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL -{lookback_hours} HOUR)
        ),
        needs_case AS (
            SELECT
                *,
                CASE
                    WHEN (member_tier IN ('VIP', 'Platinum') AND sentiment_score < -0.3)
                         OR (at_risk_score >= 0.85 AND sentiment_score < -0.6)
                         OR (contact_count_7d >= 4 AND sentiment_score < -0.5)
                    THEN 'Critical'

                    WHEN at_risk_score >= 0.7 OR sentiment_score <= -0.6
                         OR (contact_count_7d >= 3 AND sentiment_score < 0)
                    THEN 'High'

                    WHEN at_risk_score >= 0.5 OR sentiment_score <= -0.4
                    THEN 'Medium'

                    WHEN at_risk_score >= 0.3 OR sentiment_score <= -0.2
                    THEN 'Low'

                    ELSE NULL
                END as severity
            FROM recent_interactions
        ),
        not_already_cased AS (
            SELECT n.*
            FROM needs_case n
            LEFT JOIN {self.catalog}.gold.feedback_cases c
                ON n.interaction_id = c.interaction_id
            WHERE n.severity IS NOT NULL
              AND c.case_id IS NULL  -- No case exists yet
        )
        SELECT * FROM not_already_cased
        """

        interactions_df = self.spark.sql(query)
        count = interactions_df.count()

        print(f"✅ Found {count} interactions requiring cases")

        if count == 0:
            return []

        # Create cases for each interaction
        created_cases = []

        for row in interactions_df.collect():
            case_id = self.create_case(
                member_id=row['member_id'],
                interaction_id=row['interaction_id'],
                issue_type=row['primary_topic'],
                severity=row['severity'],
                description=row['text'][:500],  # Truncate to 500 chars
                auto_created=True
            )
            created_cases.append(case_id)

        print(f"📦 Created {len(created_cases)} new cases")

        return created_cases

    def create_case(
        self,
        member_id,
        issue_type,
        severity,
        description,
        interaction_id=None,
        assigned_to=None,
        assigned_team=None,
        auto_created=False
    ):
        """
        Create a new feedback case

        Args:
            member_id: Member identifier
            issue_type: Type of issue (Insurance, Contribution, Balance, etc.)
            severity: Critical, High, Medium, Low
            description: Description of the issue
            interaction_id: Optional link to specific interaction
            assigned_to: Optional user to assign to
            assigned_team: Optional team to assign to
            auto_created: Whether this was auto-created by system

        Returns:
            case_id: Generated case ID
        """

        case_id = f"CASE_{uuid.uuid4().hex[:12].upper()}"
        now = datetime.now()

        # Get SLA for this severity
        sla = self.sla_config.get(severity, self.sla_config['Medium'])
        due_date = now + timedelta(hours=sla['resolution_hours'])

        # Calculate priority score (1-100)
        priority = {
            'Critical': 100,
            'High': 75,
            'Medium': 50,
            'Low': 25
        }.get(severity, 50)

        # Generate case title from issue type
        case_title = f"{severity} - {issue_type} Issue"

        # Create case record
        case_data = {
            'case_id': case_id,
            'interaction_id': interaction_id,
            'member_id': member_id,
            'issue_type': issue_type,
            'severity': severity,
            'priority': priority,
            'status': 'New' if not assigned_to else 'Assigned',
            'assigned_to': assigned_to,
            'assigned_team': assigned_team,
            'assigned_at': now if assigned_to else None,
            'due_date': due_date,
            'sla_status': 'On Track',
            'case_title': case_title,
            'case_description': description,
            'created_at': now,
            'created_by': 'SYSTEM' if auto_created else 'MANUAL',
            'updated_at': now,
            'updated_by': 'SYSTEM' if auto_created else 'MANUAL',
            'member_contacted': False
        }

        # Insert into database
        case_df = self.spark.createDataFrame([case_data])
        case_df.write.mode("append").saveAsTable(f"{self.catalog}.gold.feedback_cases")

        # Log action
        self._log_action(
            case_id=case_id,
            action_type="Created",
            action_by="SYSTEM" if auto_created else "MANUAL",
            details=f"Case created with severity {severity}",
            system_action=auto_created
        )

        if assigned_to:
            self._log_action(
                case_id=case_id,
                action_type="Assigned",
                action_by="SYSTEM",
                details=f"Auto-assigned to {assigned_to} ({assigned_team})",
                new_value=assigned_to,
                system_action=True
            )

        print(f"✅ Created case {case_id} for member {member_id} (Severity: {severity})")

        return case_id

    def assign_case(self, case_id, assigned_to, assigned_team=None):
        """Assign case to a user/team"""

        now = datetime.now()

        # Update case
        update_query = f"""
        UPDATE {self.catalog}.gold.feedback_cases
        SET
            assigned_to = '{assigned_to}',
            assigned_team = '{assigned_team or ""}',
            assigned_at = TIMESTAMP'{now}',
            status = CASE WHEN status = 'New' THEN 'Assigned' ELSE status END,
            updated_at = TIMESTAMP'{now}',
            updated_by = '{assigned_to}'
        WHERE case_id = '{case_id}'
        """

        self.spark.sql(update_query)

        # Log action
        self._log_action(
            case_id=case_id,
            action_type="Assigned",
            action_by=assigned_to,
            details=f"Assigned to {assigned_to}" + (f" ({assigned_team})" if assigned_team else ""),
            new_value=assigned_to,
            system_action=False
        )

        print(f"✅ Assigned case {case_id} to {assigned_to}")

    def update_status(self, case_id, new_status, notes=None, user="SYSTEM"):
        """Update case status"""

        now = datetime.now()

        # Get current status
        current_case = self.spark.sql(f"""
            SELECT status FROM {self.catalog}.gold.feedback_cases
            WHERE case_id = '{case_id}'
        """).first()

        if not current_case:
            raise ValueError(f"Case {case_id} not found")

        previous_status = current_case['status']

        # Update case
        update_query = f"""
        UPDATE {self.catalog}.gold.feedback_cases
        SET
            status = '{new_status}',
            updated_at = TIMESTAMP'{now}',
            updated_by = '{user}'
        WHERE case_id = '{case_id}'
        """

        self.spark.sql(update_query)

        # Log action
        self._log_action(
            case_id=case_id,
            action_type="Status Change",
            action_by=user,
            details=notes or f"Status changed from {previous_status} to {new_status}",
            previous_value=previous_status,
            new_value=new_status,
            system_action=(user == "SYSTEM")
        )

        print(f"✅ Updated case {case_id} status: {previous_status} → {new_status}")

    def add_note(self, case_id, note, user):
        """Add note to case"""

        now = datetime.now()

        # Log action
        self._log_action(
            case_id=case_id,
            action_type="Note Added",
            action_by=user,
            details=note,
            system_action=False
        )

        # Update case timestamp
        self.spark.sql(f"""
            UPDATE {self.catalog}.gold.feedback_cases
            SET updated_at = TIMESTAMP'{now}', updated_by = '{user}'
            WHERE case_id = '{case_id}'
        """)

        print(f"✅ Added note to case {case_id}")

    def resolve_case(self, case_id, resolution_notes, resolution_type="Resolved", user="SYSTEM"):
        """Mark case as resolved"""

        now = datetime.now()

        # Get case creation time for calculating resolution time
        case = self.spark.sql(f"""
            SELECT created_at FROM {self.catalog}.gold.feedback_cases
            WHERE case_id = '{case_id}'
        """).first()

        if not case:
            raise ValueError(f"Case {case_id} not found")

        resolution_time_hours = (now - case['created_at']).total_seconds() / 3600

        # Update case
        update_query = f"""
        UPDATE {self.catalog}.gold.feedback_cases
        SET
            status = 'Resolved',
            resolved_at = TIMESTAMP'{now}',
            resolution_time_hours = {resolution_time_hours},
            resolution_notes = '{resolution_notes.replace("'", "''")}',
            resolution_type = '{resolution_type}',
            updated_at = TIMESTAMP'{now}',
            updated_by = '{user}'
        WHERE case_id = '{case_id}'
        """

        self.spark.sql(update_query)

        # Log action
        self._log_action(
            case_id=case_id,
            action_type="Resolved",
            action_by=user,
            details=resolution_notes,
            new_value="Resolved",
            system_action=(user == "SYSTEM")
        )

        print(f"✅ Resolved case {case_id} (Resolution time: {resolution_time_hours:.1f}h)")

    def escalate_case(self, case_id, escalate_to, reason, user):
        """Escalate case to senior team/manager"""

        now = datetime.now()

        # Update severity to Critical if not already
        self.spark.sql(f"""
            UPDATE {self.catalog}.gold.feedback_cases
            SET
                severity = CASE WHEN severity != 'Critical' THEN 'High' ELSE severity END,
                status = 'Escalated',
                assigned_to = '{escalate_to}',
                updated_at = TIMESTAMP'{now}',
                updated_by = '{user}'
            WHERE case_id = '{case_id}'
        """)

        # Log action
        self._log_action(
            case_id=case_id,
            action_type="Escalated",
            action_by=user,
            details=f"Escalated to {escalate_to}. Reason: {reason}",
            new_value=escalate_to,
            system_action=False
        )

        print(f"⚠️  Escalated case {case_id} to {escalate_to}")

    def check_sla_breaches(self):
        """Check for SLA breaches and update status"""

        now = datetime.now()

        # Find cases at risk or breached
        query = f"""
        SELECT
            c.case_id,
            c.severity,
            c.created_at,
            c.due_date,
            c.status,
            TIMESTAMPDIFF(HOUR, TIMESTAMP'{now}', c.due_date) as hours_until_due,
            s.escalation_hours
        FROM {self.catalog}.gold.feedback_cases c
        LEFT JOIN {self.catalog}.gold.case_sla_config s ON c.severity = s.severity
        WHERE c.status NOT IN ('Resolved', 'Closed', 'Verified')
          AND c.due_date IS NOT NULL
        """

        cases = self.spark.sql(query)

        breached = []
        at_risk = []

        for case in cases.collect():
            hours_until_due = case['hours_until_due']
            case_id = case['case_id']

            if hours_until_due < 0:
                # Breached
                new_sla_status = 'Breached'
                breached.append(case_id)
            elif hours_until_due < 4:  # Less than 4 hours
                # At risk
                new_sla_status = 'At Risk'
                at_risk.append(case_id)
            else:
                new_sla_status = 'On Track'

            # Update SLA status
            self.spark.sql(f"""
                UPDATE {self.catalog}.gold.feedback_cases
                SET sla_status = '{new_sla_status}'
                WHERE case_id = '{case_id}'
            """)

        print(f"⚠️  SLA Status: {len(breached)} breached, {len(at_risk)} at risk")

        return {'breached': breached, 'at_risk': at_risk}

    def _log_action(
        self,
        case_id,
        action_type,
        action_by,
        details,
        previous_value=None,
        new_value=None,
        system_action=False
    ):
        """Log an action to the audit trail"""

        action_id = f"ACT_{uuid.uuid4().hex[:12].upper()}"
        now = datetime.now()

        action_data = {
            'action_id': action_id,
            'case_id': case_id,
            'action_type': action_type,
            'action_by': action_by,
            'action_at': now,
            'details': details,
            'previous_value': previous_value,
            'new_value': new_value,
            'system_action': system_action
        }

        action_df = self.spark.createDataFrame([action_data])
        action_df.write.mode("append").saveAsTable(f"{self.catalog}.gold.case_actions")

    def get_case_stats(self):
        """Get overall case statistics"""

        stats = self.spark.sql(f"""
            SELECT * FROM {self.catalog}.gold.case_metrics_live
        """).first()

        return stats.asDict() if stats else {}

    def get_overdue_cases(self):
        """Get list of overdue cases"""

        overdue = self.spark.sql(f"""
            SELECT * FROM {self.catalog}.gold.cases_overdue
            WHERE urgency IN ('OVERDUE', 'DUE SOON')
            ORDER BY due_date ASC
        """)

        return overdue.toPandas()


# ============================================================================
# SCHEDULED JOB: Auto-create cases and check SLAs
# ============================================================================

def scheduled_case_management():
    """
    Scheduled job to run every hour:
    1. Auto-create cases for at-risk members
    2. Check for SLA breaches
    3. Auto-escalate breached cases
    """

    manager = CaseManager()

    print("=" * 80)
    print("SCHEDULED CASE MANAGEMENT JOB")
    print(f"Run time: {datetime.now()}")
    print("=" * 80)

    # 1. Auto-create cases
    print("\n1️⃣  Auto-creating cases...")
    new_cases = manager.auto_create_cases(lookback_hours=1)

    # 2. Check SLA breaches
    print("\n2️⃣  Checking SLA status...")
    sla_status = manager.check_sla_breaches()

    # 3. Auto-escalate critical breached cases
    if sla_status['breached']:
        print(f"\n3️⃣  Auto-escalating {len(sla_status['breached'])} breached cases...")

        # Define escalation paths based on severity
        escalation_rules = {
            'Critical': {
                'escalate_to': 'gm@art.com.au',
                'reason': 'Critical SLA breach - immediate attention required'
            },
            'High': {
                'escalate_to': 'senior-care-team@art.com.au',
                'reason': 'High severity SLA breach - escalation required'
            },
            'Medium': {
                'escalate_to': 'team-lead@art.com.au',
                'reason': 'Medium severity SLA breach - review required'
            }
        }

        escalated_count = 0
        for case in sla_status['breached']:
            case_id = case['case_id']
            severity = case['severity']

            # Get escalation rule for this severity
            escalation = escalation_rules.get(severity)

            if escalation and severity in ['Critical', 'High']:
                # Auto-escalate Critical and High severity breached cases
                manager.escalate_case(
                    case_id=case_id,
                    escalate_to=escalation['escalate_to'],
                    reason=escalation['reason'],
                    user='SYSTEM_AUTO_ESCALATION'
                )
                escalated_count += 1

                # Send alert notification for critical escalations
                if severity == 'Critical':
                    try:
                        from analytics.alert_engine import AlertEngine
                        alert_engine = AlertEngine()

                        # Create escalation alert
                        alert_data = {
                            'rule_id': 'auto_escalation_critical',
                            'rule_description': f'Critical case {case_id} auto-escalated due to SLA breach',
                            'severity': 'Critical',
                            'count': 1,
                            'triggered_at': datetime.now(),
                            'notify': [escalation['escalate_to']],
                            'matches': [{
                                'case_id': case_id,
                                'member_id': case.get('member_id', 'Unknown'),
                                'text_preview': f"SLA breached by {case.get('breach_hours', 0):.1f} hours"
                            }]
                        }

                        # Send email alert for critical escalations
                        alert_engine._send_email_alert(alert_data)

                    except Exception as e:
                        print(f"   ⚠️  Failed to send escalation alert: {e}")

        if escalated_count > 0:
            print(f"   ✅ Auto-escalated {escalated_count} cases")
        else:
            print(f"   ℹ️  No cases required auto-escalation (Low severity breaches)")

    # 4. Print summary
    print("\n📊 Summary:")
    stats = manager.get_case_stats()
    print(f"   Total Open Cases: {stats.get('open_cases', 0)}")
    print(f"   Created Today: {stats.get('created_today', 0)}")
    print(f"   Resolved Today: {stats.get('resolved_today', 0)}")
    print(f"   SLA Breached: {stats.get('sla_breached', 0)}")
    print(f"   SLA At Risk: {stats.get('sla_at_risk', 0)}")

    print("\n✅ Scheduled job complete")


if __name__ == "__main__":
    # Run scheduled job
    scheduled_case_management()
