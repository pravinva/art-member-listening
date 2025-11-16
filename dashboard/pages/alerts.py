"""
Alert Monitoring Dashboard
===========================
Dashboard for viewing alert history and configuring rules.
"""

import dash
from dash import dcc, html, Input, Output, State, dash_table, callback
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder.getOrCreate()
catalog = "art_member_listening"


# ============================================================================
# Helper Functions
# ============================================================================

def get_alert_stats():
    """Get alert statistics"""

    query = f"""
    SELECT
        COUNT(*) as total_alerts,
        COUNT(CASE WHEN DATE(triggered_at) = CURRENT_DATE() THEN 1 END) as alerts_today,
        COUNT(CASE WHEN severity = 'Critical' THEN 1 END) as critical_alerts,
        COUNT(CASE WHEN severity = 'High' THEN 1 END) as high_alerts,
        COUNT(DISTINCT rule_name) as unique_rules_triggered,
        COUNT(DISTINCT DATE(triggered_at)) as days_with_alerts
    FROM {catalog}.gold.alert_history
    WHERE triggered_at >= DATE_SUB(CURRENT_DATE(), 30)
    """

    result = spark.sql(query).first()
    return result.asDict() if result else {}


def get_recent_alerts(limit=50):
    """Get recent alerts"""

    query = f"""
    SELECT
        alert_id,
        rule_name,
        severity,
        member_count,
        triggered_at,
        notification_channels,
        action_taken
    FROM {catalog}.gold.alert_history
    ORDER BY triggered_at DESC
    LIMIT {limit}
    """

    df = spark.sql(query).toPandas()

    # Format datetime
    if not df.empty:
        df['triggered_at'] = pd.to_datetime(df['triggered_at']).dt.strftime('%Y-%m-%d %H:%M:%S')

    return df


def get_alerts_by_rule():
    """Get alert counts by rule"""

    query = f"""
    SELECT
        rule_name,
        severity,
        COUNT(*) as alert_count,
        SUM(member_count) as total_members_affected,
        MAX(triggered_at) as last_triggered
    FROM {catalog}.gold.alert_history
    WHERE triggered_at >= DATE_SUB(CURRENT_DATE(), 30)
    GROUP BY rule_name, severity
    ORDER BY alert_count DESC
    """

    df = spark.sql(query).toPandas()

    if not df.empty:
        df['last_triggered'] = pd.to_datetime(df['last_triggered']).dt.strftime('%Y-%m-%d %H:%M:%S')

    return df


def get_alerts_timeline():
    """Get alerts over time"""

    query = f"""
    SELECT
        DATE(triggered_at) as date,
        severity,
        COUNT(*) as alert_count
    FROM {catalog}.gold.alert_history
    WHERE triggered_at >= DATE_SUB(CURRENT_DATE(), 30)
    GROUP BY 1, 2
    ORDER BY 1, 2
    """

    df = spark.sql(query).toPandas()
    return df


def get_alerts_by_hour():
    """Get alerts by hour of day"""

    query = f"""
    SELECT
        HOUR(triggered_at) as hour,
        severity,
        COUNT(*) as alert_count
    FROM {catalog}.gold.alert_history
    WHERE triggered_at >= DATE_SUB(CURRENT_DATE(), 7)
    GROUP BY 1, 2
    ORDER BY 1, 2
    """

    df = spark.sql(query).toPandas()
    return df


# ============================================================================
# Alert Rules Configuration Data
# ============================================================================

ALERT_RULES = [
    {
        'rule_name': 'vip_negative',
        'description': 'VIP Member Negative Feedback',
        'severity': 'Critical',
        'condition': "member_tier IN ('VIP', 'Platinum') AND sentiment_score < -0.6",
        'channels': 'email, slack',
        'action': 'immediate_escalation',
        'enabled': True
    },
    {
        'rule_name': 'repeated_contact',
        'description': 'Repeated Contact - Same Issue',
        'severity': 'High',
        'condition': "contact_count_7d >= 3 AND sentiment_score < 0",
        'channels': 'email, slack',
        'action': 'assign_case',
        'enabled': True
    },
    {
        'rule_name': 'topic_spike',
        'description': 'Topic Spike Detection',
        'severity': 'Medium',
        'condition': "topic_mention_increase_24h > 3.0",
        'channels': 'slack',
        'action': 'investigate',
        'enabled': True
    },
    {
        'rule_name': 'critical_sentiment_drop',
        'description': 'Critical Sentiment Drop',
        'severity': 'Critical',
        'condition': "avg_sentiment_7d < -0.7",
        'channels': 'email, slack',
        'action': 'executive_review',
        'enabled': True
    },
    {
        'rule_name': 'at_risk_member',
        'description': 'At-Risk Member Detected',
        'severity': 'High',
        'condition': "at_risk_score >= 0.8",
        'channels': 'email',
        'action': 'proactive_outreach',
        'enabled': True
    },
    {
        'rule_name': 'urgent_issue',
        'description': 'Urgent Issue Detected',
        'severity': 'High',
        'condition': "urgency_score >= 0.9 AND sentiment_score < -0.5",
        'channels': 'email, slack',
        'action': 'assign_case',
        'enabled': True
    },
    {
        'rule_name': 'compliance_keyword',
        'description': 'Compliance/Legal Keyword',
        'severity': 'Critical',
        'condition': "text LIKE '%lawsuit%' OR text LIKE '%ombudsman%'",
        'channels': 'email',
        'action': 'legal_review',
        'enabled': True
    },
]


# ============================================================================
# Layout
# ============================================================================

layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H2("🚨 Alert Monitoring Dashboard", className="text-primary mb-4"),
            html.P("View alert history and configure rules", className="text-muted")
        ])
    ]),

    # KPI Cards
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4(id="total-alerts-30d", children="0", className="text-primary"),
                    html.P("Total Alerts (30d)", className="text-muted mb-0")
                ])
            ])
        ], width=2),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4(id="alerts-today", children="0", className="text-info"),
                    html.P("Alerts Today", className="text-muted mb-0")
                ])
            ])
        ], width=2),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4(id="critical-alerts", children="0", className="text-danger"),
                    html.P("Critical Alerts", className="text-muted mb-0")
                ])
            ])
        ], width=2),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4(id="high-alerts", children="0", className="text-warning"),
                    html.P("High Alerts", className="text-muted mb-0")
                ])
            ])
        ], width=2),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4(id="unique-rules", children="0", className="text-success"),
                    html.P("Rules Triggered", className="text-muted mb-0")
                ])
            ])
        ], width=2),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4(id="days-with-alerts", children="0", className="text-secondary"),
                    html.P("Days with Alerts", className="text-muted mb-0")
                ])
            ])
        ], width=2),
    ], className="mb-4"),

    # Tabs
    dbc.Tabs([
        # Recent Alerts Tab
        dbc.Tab(label="Recent Alerts", children=[
            dbc.Row([
                dbc.Col([
                    html.Div([
                        html.H5("Recent Alerts (Last 50)", className="mt-3 mb-3"),
                        dbc.Button("🔄 Refresh", id="refresh-alerts-btn", color="primary", size="sm", className="mb-2"),
                        dash_table.DataTable(
                            id='recent-alerts-table',
                            columns=[
                                {'name': 'Alert ID', 'id': 'alert_id'},
                                {'name': 'Rule', 'id': 'rule_name'},
                                {'name': 'Severity', 'id': 'severity'},
                                {'name': 'Members Affected', 'id': 'member_count'},
                                {'name': 'Triggered At', 'id': 'triggered_at'},
                                {'name': 'Channels', 'id': 'notification_channels'},
                                {'name': 'Action Taken', 'id': 'action_taken'},
                            ],
                            data=[],
                            style_table={'overflowX': 'auto'},
                            style_cell={
                                'textAlign': 'left',
                                'padding': '10px',
                                'fontSize': '14px'
                            },
                            style_header={
                                'backgroundColor': 'rgb(230, 230, 230)',
                                'fontWeight': 'bold'
                            },
                            style_data_conditional=[
                                {
                                    'if': {'filter_query': '{severity} = "Critical"'},
                                    'backgroundColor': '#ffebee',
                                    'color': '#c62828',
                                    'fontWeight': 'bold'
                                },
                                {
                                    'if': {'filter_query': '{severity} = "High"'},
                                    'backgroundColor': '#fff3e0',
                                    'color': '#e65100'
                                },
                            ],
                            page_size=20,
                            sort_action='native',
                            filter_action='native',
                        )
                    ])
                ])
            ])
        ]),

        # Alert Rules Tab
        dbc.Tab(label="Alert Rules", children=[
            dbc.Row([
                dbc.Col([
                    html.Div([
                        html.H5("Configured Alert Rules", className="mt-3 mb-3"),
                        dash_table.DataTable(
                            id='alert-rules-table',
                            columns=[
                                {'name': 'Rule Name', 'id': 'rule_name'},
                                {'name': 'Description', 'id': 'description'},
                                {'name': 'Severity', 'id': 'severity'},
                                {'name': 'Condition', 'id': 'condition'},
                                {'name': 'Channels', 'id': 'channels'},
                                {'name': 'Action', 'id': 'action'},
                                {'name': 'Enabled', 'id': 'enabled'},
                            ],
                            data=ALERT_RULES,
                            style_table={'overflowX': 'auto'},
                            style_cell={
                                'textAlign': 'left',
                                'padding': '10px',
                                'fontSize': '14px',
                                'whiteSpace': 'normal',
                                'height': 'auto',
                            },
                            style_header={
                                'backgroundColor': 'rgb(230, 230, 230)',
                                'fontWeight': 'bold'
                            },
                            style_data_conditional=[
                                {
                                    'if': {'filter_query': '{severity} = "Critical"'},
                                    'color': '#c62828',
                                    'fontWeight': 'bold'
                                },
                                {
                                    'if': {'filter_query': '{enabled} = false'},
                                    'opacity': '0.5'
                                },
                            ],
                            page_size=20,
                        ),
                        html.P("Note: Alert rules can be modified in analytics/08_alert_engine.py",
                               className="text-muted mt-2")
                    ])
                ])
            ])
        ]),

        # Statistics Tab
        dbc.Tab(label="Statistics", children=[
            dbc.Row([
                dbc.Col([
                    html.H5("Alerts by Rule (Last 30 Days)", className="mt-3 mb-3"),
                    dash_table.DataTable(
                        id='alerts-by-rule-table',
                        columns=[
                            {'name': 'Rule Name', 'id': 'rule_name'},
                            {'name': 'Severity', 'id': 'severity'},
                            {'name': 'Alert Count', 'id': 'alert_count'},
                            {'name': 'Total Members Affected', 'id': 'total_members_affected'},
                            {'name': 'Last Triggered', 'id': 'last_triggered'},
                        ],
                        data=[],
                        style_table={'overflowX': 'auto'},
                        style_cell={
                            'textAlign': 'left',
                            'padding': '10px',
                            'fontSize': '14px'
                        },
                        style_header={
                            'backgroundColor': 'rgb(230, 230, 230)',
                            'fontWeight': 'bold'
                        },
                        page_size=20,
                        sort_action='native',
                    )
                ], width=12)
            ])
        ]),

        # Trends Tab
        dbc.Tab(label="Trends", children=[
            dbc.Row([
                dbc.Col([
                    html.H5("Alert Timeline (Last 30 Days)", className="mt-3 mb-3"),
                    dcc.Graph(id='alerts-timeline-chart')
                ], width=12)
            ]),
            dbc.Row([
                dbc.Col([
                    html.H5("Alerts by Hour of Day (Last 7 Days)", className="mt-3 mb-3"),
                    dcc.Graph(id='alerts-by-hour-chart')
                ], width=12)
            ])
        ]),
    ]),

    # Auto-refresh interval
    dcc.Interval(
        id='alert-refresh-interval',
        interval=30*1000,  # 30 seconds
        n_intervals=0
    ),

], fluid=True)


# ============================================================================
# Callbacks
# ============================================================================

@callback(
    [
        Output('total-alerts-30d', 'children'),
        Output('alerts-today', 'children'),
        Output('critical-alerts', 'children'),
        Output('high-alerts', 'children'),
        Output('unique-rules', 'children'),
        Output('days-with-alerts', 'children'),
        Output('recent-alerts-table', 'data'),
        Output('alerts-by-rule-table', 'data'),
        Output('alerts-timeline-chart', 'figure'),
        Output('alerts-by-hour-chart', 'figure'),
    ],
    [
        Input('alert-refresh-interval', 'n_intervals'),
        Input('refresh-alerts-btn', 'n_clicks')
    ]
)
def update_alert_dashboard(n_intervals, n_clicks):
    """Update all alert monitoring dashboard components"""

    # Get stats
    stats = get_alert_stats()

    # Get tables
    recent_alerts = get_recent_alerts()
    alerts_by_rule = get_alerts_by_rule()
    timeline = get_alerts_timeline()
    by_hour = get_alerts_by_hour()

    # Format KPIs
    total_alerts = stats.get('total_alerts', 0)
    alerts_today = stats.get('alerts_today', 0)
    critical = stats.get('critical_alerts', 0)
    high = stats.get('high_alerts', 0)
    unique_rules = stats.get('unique_rules_triggered', 0)
    days_alerts = stats.get('days_with_alerts', 0)

    # Create timeline chart
    if not timeline.empty:
        fig_timeline = px.area(
            timeline,
            x='date',
            y='alert_count',
            color='severity',
            title='Alert Timeline',
            labels={'alert_count': 'Number of Alerts', 'date': 'Date'},
            color_discrete_map={
                'Critical': '#c62828',
                'High': '#e65100',
                'Medium': '#f57c00',
                'Low': '#fbc02d'
            }
        )
        fig_timeline.update_layout(hovermode='x unified')
    else:
        fig_timeline = go.Figure()
        fig_timeline.add_annotation(text="No alert data available", showarrow=False)

    # Create hour-of-day chart
    if not by_hour.empty:
        fig_hour = px.bar(
            by_hour,
            x='hour',
            y='alert_count',
            color='severity',
            title='Alerts by Hour of Day',
            labels={'alert_count': 'Number of Alerts', 'hour': 'Hour of Day'},
            color_discrete_map={
                'Critical': '#c62828',
                'High': '#e65100',
                'Medium': '#f57c00',
                'Low': '#fbc02d'
            }
        )
        fig_hour.update_xaxes(dtick=1)
    else:
        fig_hour = go.Figure()
        fig_hour.add_annotation(text="No alert data available", showarrow=False)

    return (
        total_alerts,
        alerts_today,
        critical,
        high,
        unique_rules,
        days_alerts,
        recent_alerts.to_dict('records'),
        alerts_by_rule.to_dict('records'),
        fig_timeline,
        fig_hour
    )
