-- ============================================================================
-- Closed-Loop Feedback Management Schema
-- ============================================================================
-- Purpose: Track feedback from identification through resolution
-- Tables: feedback_cases, case_actions, case_sla_config
-- ============================================================================

USE CATALOG art_member_listening;

-- ----------------------------------------------------------------------------
-- 1. FEEDBACK CASES TABLE
-- ----------------------------------------------------------------------------
-- Main table for tracking member feedback cases from creation to closure

CREATE TABLE IF NOT EXISTS gold.feedback_cases (
  -- Primary identifiers
  case_id STRING NOT NULL COMMENT 'Unique case identifier',
  interaction_id STRING COMMENT 'Link to original interaction in silver.interactions_analyzed',
  member_id STRING NOT NULL COMMENT 'Member identifier',

  -- Case classification
  issue_type STRING COMMENT 'Insurance, Contribution, Balance, Claims, Technical, etc.',
  issue_category STRING COMMENT 'Subcategory for detailed tracking',
  severity STRING COMMENT 'Low, Medium, High, Critical',
  priority INT COMMENT 'Computed priority score (1-100)',

  -- Case status and workflow
  status STRING NOT NULL COMMENT 'New, Assigned, In Progress, Pending Member, Resolved, Verified, Closed, Escalated',
  assigned_to STRING COMMENT 'User or team assigned to case',
  assigned_team STRING COMMENT 'Member Services, Insurance Team, Technical Support, etc.',
  assigned_at TIMESTAMP COMMENT 'When case was assigned',

  -- SLA tracking
  due_date TIMESTAMP COMMENT 'Based on SLA for severity level',
  sla_status STRING COMMENT 'On Track, At Risk, Breached',
  first_response_at TIMESTAMP COMMENT 'When first response was provided',
  first_response_sla_met BOOLEAN COMMENT 'Whether first response SLA was met',

  -- Resolution tracking
  resolved_at TIMESTAMP COMMENT 'When case was marked as resolved',
  verified_at TIMESTAMP COMMENT 'When member verified resolution',
  closed_at TIMESTAMP COMMENT 'When case was closed',
  resolution_time_hours DOUBLE COMMENT 'Hours from creation to resolution',
  resolution_notes STRING COMMENT 'Description of how issue was resolved',
  resolution_type STRING COMMENT 'Resolved, Workaround, No Action Needed, Duplicate, etc.',

  -- Member interaction
  member_contacted BOOLEAN COMMENT 'Whether member was contacted about this case',
  member_contact_date TIMESTAMP COMMENT 'When member was contacted',
  member_satisfaction INT COMMENT 'Post-resolution satisfaction score (1-5)',
  member_feedback STRING COMMENT 'Member feedback on resolution',

  -- Case content
  case_title STRING COMMENT 'Short summary of the case',
  case_description STRING COMMENT 'Detailed description of the issue',
  root_cause STRING COMMENT 'Identified root cause of the issue',

  -- Metadata
  created_at TIMESTAMP NOT NULL COMMENT 'Case creation timestamp',
  created_by STRING COMMENT 'System or user who created the case',
  updated_at TIMESTAMP NOT NULL COMMENT 'Last update timestamp',
  updated_by STRING COMMENT 'User who last updated the case',

  -- Tags and references
  tags ARRAY<STRING> COMMENT 'Custom tags for filtering and reporting',
  related_cases ARRAY<STRING> COMMENT 'IDs of related cases',

  CONSTRAINT pk_feedback_cases PRIMARY KEY (case_id)
)
COMMENT 'Cases generated from member feedback for closed-loop tracking'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true'
);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_cases_member ON gold.feedback_cases (member_id);
CREATE INDEX IF NOT EXISTS idx_cases_status ON gold.feedback_cases (status);
CREATE INDEX IF NOT EXISTS idx_cases_assigned ON gold.feedback_cases (assigned_to);
CREATE INDEX IF NOT EXISTS idx_cases_severity ON gold.feedback_cases (severity, status);

-- ----------------------------------------------------------------------------
-- 2. CASE ACTIONS TABLE
-- ----------------------------------------------------------------------------
-- Audit trail of all actions taken on cases

CREATE TABLE IF NOT EXISTS gold.case_actions (
  -- Primary identifiers
  action_id STRING NOT NULL COMMENT 'Unique action identifier',
  case_id STRING NOT NULL COMMENT 'Reference to feedback_cases',

  -- Action details
  action_type STRING NOT NULL COMMENT 'Created, Assigned, Status Change, Note Added, Escalated, Contacted Member, Resolved, etc.',
  action_by STRING NOT NULL COMMENT 'User who performed the action',
  action_at TIMESTAMP NOT NULL COMMENT 'When action was performed',

  -- Action content
  details STRING COMMENT 'Detailed description of the action',
  previous_value STRING COMMENT 'Previous value for change tracking',
  new_value STRING COMMENT 'New value after action',

  -- Metadata
  system_action BOOLEAN COMMENT 'Whether action was automated (true) or manual (false)',

  CONSTRAINT pk_case_actions PRIMARY KEY (action_id)
)
COMMENT 'Audit trail of all actions taken on feedback cases'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true'
);

-- Create index for case lookups
CREATE INDEX IF NOT EXISTS idx_actions_case ON gold.case_actions (case_id);

-- ----------------------------------------------------------------------------
-- 3. CASE SLA CONFIGURATION TABLE
-- ----------------------------------------------------------------------------
-- SLA definitions by severity level

CREATE TABLE IF NOT EXISTS gold.case_sla_config (
  severity STRING NOT NULL COMMENT 'Low, Medium, High, Critical',
  first_response_hours INT COMMENT 'Hours for first response',
  resolution_hours INT COMMENT 'Hours for resolution',
  escalation_hours INT COMMENT 'Hours before auto-escalation',
  active BOOLEAN NOT NULL DEFAULT true COMMENT 'Whether this SLA is active',

  CONSTRAINT pk_case_sla PRIMARY KEY (severity)
)
COMMENT 'SLA configuration for case management';

-- Insert default SLA values
MERGE INTO gold.case_sla_config AS target
USING (
  SELECT 'Critical' as severity, 2 as first_response_hours, 24 as resolution_hours, 12 as escalation_hours, true as active
  UNION ALL
  SELECT 'High', 4, 48, 24, true
  UNION ALL
  SELECT 'Medium', 24, 120, 72, true
  UNION ALL
  SELECT 'Low', 48, 240, 168, true
) AS source
ON target.severity = source.severity
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- ----------------------------------------------------------------------------
-- 4. CASE METRICS VIEW
-- ----------------------------------------------------------------------------
-- Real-time view of case metrics for dashboards

CREATE OR REPLACE VIEW gold.case_metrics_live AS
SELECT
  -- Current state
  COUNT(*) as total_cases,
  COUNT(CASE WHEN status IN ('New', 'Assigned', 'In Progress') THEN 1 END) as open_cases,
  COUNT(CASE WHEN status = 'Resolved' THEN 1 END) as resolved_cases,
  COUNT(CASE WHEN status = 'Closed' THEN 1 END) as closed_cases,

  -- SLA metrics
  COUNT(CASE WHEN sla_status = 'Breached' THEN 1 END) as sla_breached,
  COUNT(CASE WHEN sla_status = 'At Risk' THEN 1 END) as sla_at_risk,

  -- By severity
  COUNT(CASE WHEN severity = 'Critical' AND status NOT IN ('Closed') THEN 1 END) as critical_open,
  COUNT(CASE WHEN severity = 'High' AND status NOT IN ('Closed') THEN 1 END) as high_open,

  -- Performance
  AVG(resolution_time_hours) as avg_resolution_hours,
  PERCENTILE(resolution_time_hours, 0.95) as p95_resolution_hours,
  AVG(CASE WHEN first_response_sla_met THEN 1.0 ELSE 0.0 END) * 100 as first_response_sla_pct,

  -- Today's activity
  COUNT(CASE WHEN DATE(created_at) = CURRENT_DATE() THEN 1 END) as created_today,
  COUNT(CASE WHEN DATE(resolved_at) = CURRENT_DATE() THEN 1 END) as resolved_today,

  -- Member satisfaction
  AVG(member_satisfaction) as avg_satisfaction
FROM gold.feedback_cases
WHERE created_at >= DATE_SUB(CURRENT_DATE(), 90);  -- Last 90 days

-- ----------------------------------------------------------------------------
-- 5. OVERDUE CASES VIEW
-- ----------------------------------------------------------------------------
-- Cases that need immediate attention

CREATE OR REPLACE VIEW gold.cases_overdue AS
SELECT
  c.case_id,
  c.member_id,
  c.case_title,
  c.severity,
  c.status,
  c.assigned_to,
  c.assigned_team,
  c.due_date,
  c.created_at,
  TIMESTAMPDIFF(HOUR, CURRENT_TIMESTAMP(), c.due_date) as hours_until_due,
  CASE
    WHEN c.due_date < CURRENT_TIMESTAMP() THEN 'OVERDUE'
    WHEN TIMESTAMPDIFF(HOUR, CURRENT_TIMESTAMP(), c.due_date) < 4 THEN 'DUE SOON'
    ELSE 'ON TRACK'
  END as urgency,
  sla.escalation_hours,
  TIMESTAMPDIFF(HOUR, c.created_at, CURRENT_TIMESTAMP()) as age_hours
FROM gold.feedback_cases c
LEFT JOIN gold.case_sla_config sla ON c.severity = sla.severity
WHERE c.status NOT IN ('Resolved', 'Verified', 'Closed')
  AND c.due_date IS NOT NULL
ORDER BY c.due_date ASC;

-- ----------------------------------------------------------------------------
-- 6. CASE HISTORY VIEW
-- ----------------------------------------------------------------------------
-- Complete audit trail for a case

CREATE OR REPLACE VIEW gold.case_history AS
SELECT
  c.case_id,
  c.member_id,
  c.case_title,
  c.status,
  c.created_at,
  c.resolved_at,
  c.resolution_time_hours,
  a.action_id,
  a.action_type,
  a.action_by,
  a.action_at,
  a.details,
  a.previous_value,
  a.new_value,
  a.system_action
FROM gold.feedback_cases c
LEFT JOIN gold.case_actions a ON c.case_id = a.case_id
ORDER BY c.case_id, a.action_at;

-- ----------------------------------------------------------------------------
-- 7. TEAM PERFORMANCE VIEW
-- ----------------------------------------------------------------------------
-- Performance metrics by team and assignee

CREATE OR REPLACE VIEW gold.team_performance AS
SELECT
  assigned_team,
  assigned_to,
  COUNT(*) as total_assigned,
  COUNT(CASE WHEN status IN ('Resolved', 'Closed') THEN 1 END) as resolved,
  COUNT(CASE WHEN status IN ('New', 'Assigned', 'In Progress') THEN 1 END) as active,
  AVG(resolution_time_hours) as avg_resolution_hours,
  AVG(CASE WHEN first_response_sla_met THEN 1.0 ELSE 0.0 END) * 100 as first_response_sla_pct,
  COUNT(CASE WHEN sla_status = 'Breached' THEN 1 END) as sla_breached,
  AVG(member_satisfaction) as avg_satisfaction,
  COUNT(CASE WHEN DATE(assigned_at) >= DATE_SUB(CURRENT_DATE(), 7) THEN 1 END) as assigned_last_7d
FROM gold.feedback_cases
WHERE assigned_to IS NOT NULL
  AND created_at >= DATE_SUB(CURRENT_DATE(), 90)
GROUP BY assigned_team, assigned_to
ORDER BY assigned_team, total_assigned DESC;

-- ============================================================================
-- GRANTS AND PERMISSIONS
-- ============================================================================

-- Grant access to member services team
GRANT SELECT ON gold.feedback_cases TO `member_services_team`;
GRANT SELECT ON gold.case_actions TO `member_services_team`;
GRANT SELECT ON gold.case_metrics_live TO `member_services_team`;
GRANT SELECT ON gold.cases_overdue TO `member_services_team`;
GRANT SELECT ON gold.case_history TO `member_services_team`;
GRANT SELECT ON gold.team_performance TO `member_services_team`;

-- Grant write access to case managers
GRANT INSERT, UPDATE ON gold.feedback_cases TO `case_managers`;
GRANT INSERT ON gold.case_actions TO `case_managers`;

-- ============================================================================
-- COMPLETED
-- ============================================================================
-- Schema created for closed-loop feedback management
-- Tables: feedback_cases, case_actions, case_sla_config
-- Views: case_metrics_live, cases_overdue, case_history, team_performance
