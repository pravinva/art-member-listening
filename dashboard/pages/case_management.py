"""
Case Management Dashboard
=========================
Dashboard for managing feedback cases and tracking SLAs.
"""

import dash
from dash import dcc, html, Input, Output, State, dash_table, callback
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder.getOrCreate()
catalog = "art_member_listening"


# ============================================================================
# Helper Functions
# ============================================================================

def get_case_metrics():
    """Get case management KPIs"""

    query = f"""
    SELECT
        total_cases,
        open_cases,
        resolved_cases,
        closed_cases,
        sla_breached,
        sla_at_risk,
        critical_open,
        high_open,
        avg_resolution_hours,
        p95_resolution_hours,
        first_response_sla_pct,
        created_today,
        resolved_today,
        avg_satisfaction
    FROM {catalog}.gold.case_metrics_live
    """

    result = spark.sql(query).first()
    return result.asDict() if result else {}


def get_open_cases():
    """Get list of open cases"""

    query = f"""
    SELECT
        case_id,
        member_id,
        case_title,
        severity,
        status,
        assigned_to,
        assigned_team,
        due_date,
        created_at,
        sla_status,
        TIMESTAMPDIFF(HOUR, created_at, CURRENT_TIMESTAMP()) as age_hours
    FROM {catalog}.gold.feedback_cases
    WHERE status NOT IN ('Resolved', 'Closed')
    ORDER BY
        CASE severity
            WHEN 'Critical' THEN 1
            WHEN 'High' THEN 2
            WHEN 'Medium' THEN 3
            WHEN 'Low' THEN 4
        END,
        due_date ASC
    LIMIT 100
    """

    df = spark.sql(query).toPandas()
    return df


def get_overdue_cases():
    """Get overdue cases"""

    query = f"""
    SELECT
        case_id,
        member_id,
        case_title,
        severity,
        assigned_to,
        due_date,
        hours_until_due,
        urgency,
        age_hours
    FROM {catalog}.gold.cases_overdue
    WHERE urgency IN ('OVERDUE', 'DUE SOON')
    ORDER BY due_date ASC
    LIMIT 50
    """

    df = spark.sql(query).toPandas()
    return df


def get_team_performance():
    """Get team performance metrics"""

    query = f"""
    SELECT
        assigned_team,
        assigned_to,
        total_assigned,
        resolved,
        active,
        avg_resolution_hours,
        first_response_sla_pct,
        sla_breached,
        avg_satisfaction,
        assigned_last_7d
    FROM {catalog}.gold.team_performance
    WHERE total_assigned > 0
    ORDER BY assigned_team, total_assigned DESC
    """

    df = spark.sql(query).toPandas()
    return df


def get_case_trends():
    """Get case creation and resolution trends"""

    query = f"""
    SELECT
        DATE(created_at) as date,
        severity,
        COUNT(*) as count
    FROM {catalog}.gold.feedback_cases
    WHERE created_at >= DATE_SUB(CURRENT_DATE(), 30)
    GROUP BY 1, 2
    ORDER BY 1, 2
    """

    df = spark.sql(query).toPandas()
    return df


# ============================================================================
# Layout
# ============================================================================

layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H2("📋 Case Management Dashboard", className="text-primary mb-4"),
            html.P("Manage feedback cases and track SLAs", className="text-muted")
        ])
    ]),

    # KPI Cards
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4(id="total-cases", children="0", className="text-primary"),
                    html.P("Total Open Cases", className="text-muted mb-0")
                ])
            ])
        ], width=2),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4(id="critical-cases", children="0", className="text-danger"),
                    html.P("Critical", className="text-muted mb-0")
                ])
            ])
        ], width=2),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4(id="sla-breached", children="0", className="text-danger"),
                    html.P("SLA Breached", className="text-muted mb-0")
                ])
            ])
        ], width=2),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4(id="avg-resolution", children="0h", className="text-info"),
                    html.P("Avg Resolution Time", className="text-muted mb-0")
                ])
            ])
        ], width=2),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4(id="first-response-sla", children="0%", className="text-success"),
                    html.P("First Response SLA", className="text-muted mb-0")
                ])
            ])
        ], width=2),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4(id="satisfaction-score", children="0.0", className="text-warning"),
                    html.P("Avg Satisfaction", className="text-muted mb-0")
                ])
            ])
        ], width=2),
    ], className="mb-4"),

    # Tabs
    dbc.Tabs([
        # Open Cases Tab
        dbc.Tab(label="Open Cases", children=[
            dbc.Row([
                dbc.Col([
                    html.Div([
                        html.H5("Open Cases", className="mt-3 mb-3"),
                        dbc.Button("🔄 Refresh", id="refresh-cases-btn", color="primary", size="sm", className="mb-2"),
                        dash_table.DataTable(
                            id='open-cases-table',
                            columns=[
                                {'name': 'Case ID', 'id': 'case_id'},
                                {'name': 'Member', 'id': 'member_id'},
                                {'name': 'Title', 'id': 'case_title'},
                                {'name': 'Severity', 'id': 'severity'},
                                {'name': 'Status', 'id': 'status'},
                                {'name': 'Assigned To', 'id': 'assigned_to'},
                                {'name': 'Team', 'id': 'assigned_team'},
                                {'name': 'SLA Status', 'id': 'sla_status'},
                                {'name': 'Age (hours)', 'id': 'age_hours'},
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
                                    'color': '#c62828'
                                },
                                {
                                    'if': {'filter_query': '{severity} = "High"'},
                                    'backgroundColor': '#fff3e0',
                                    'color': '#e65100'
                                },
                                {
                                    'if': {'filter_query': '{sla_status} = "Breached"'},
                                    'backgroundColor': '#ffcdd2',
                                    'fontWeight': 'bold'
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

        # Overdue Cases Tab
        dbc.Tab(label="⚠️ Overdue", children=[
            dbc.Row([
                dbc.Col([
                    html.Div([
                        html.H5("Overdue & At-Risk Cases", className="mt-3 mb-3 text-danger"),
                        dash_table.DataTable(
                            id='overdue-cases-table',
                            columns=[
                                {'name': 'Case ID', 'id': 'case_id'},
                                {'name': 'Member', 'id': 'member_id'},
                                {'name': 'Title', 'id': 'case_title'},
                                {'name': 'Severity', 'id': 'severity'},
                                {'name': 'Assigned To', 'id': 'assigned_to'},
                                {'name': 'Urgency', 'id': 'urgency'},
                                {'name': 'Hours Until Due', 'id': 'hours_until_due'},
                            ],
                            data=[],
                            style_table={'overflowX': 'auto'},
                            style_cell={
                                'textAlign': 'left',
                                'padding': '10px',
                                'fontSize': '14px'
                            },
                            style_header={
                                'backgroundColor': '#ffcdd2',
                                'fontWeight': 'bold',
                                'color': '#c62828'
                            },
                            style_data_conditional=[
                                {
                                    'if': {'filter_query': '{urgency} = "OVERDUE"'},
                                    'backgroundColor': '#ffcdd2',
                                    'fontWeight': 'bold'
                                },
                                {
                                    'if': {'filter_query': '{urgency} = "DUE SOON"'},
                                    'backgroundColor': '#fff3e0'
                                },
                            ],
                            page_size=20,
                        )
                    ])
                ])
            ])
        ]),

        # Team Performance Tab
        dbc.Tab(label="Team Performance", children=[
            dbc.Row([
                dbc.Col([
                    html.Div([
                        html.H5("Team Performance Metrics", className="mt-3 mb-3"),
                        dash_table.DataTable(
                            id='team-performance-table',
                            columns=[
                                {'name': 'Team', 'id': 'assigned_team'},
                                {'name': 'Assignee', 'id': 'assigned_to'},
                                {'name': 'Total Assigned', 'id': 'total_assigned'},
                                {'name': 'Resolved', 'id': 'resolved'},
                                {'name': 'Active', 'id': 'active'},
                                {'name': 'Avg Resolution (h)', 'id': 'avg_resolution_hours', 'type': 'numeric', 'format': {'specifier': '.1f'}},
                                {'name': 'First Response SLA %', 'id': 'first_response_sla_pct', 'type': 'numeric', 'format': {'specifier': '.0f'}},
                                {'name': 'SLA Breached', 'id': 'sla_breached'},
                                {'name': 'Satisfaction', 'id': 'avg_satisfaction', 'type': 'numeric', 'format': {'specifier': '.1f'}},
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
                    ])
                ], width=12)
            ])
        ]),

        # Trends Tab
        dbc.Tab(label="Trends", children=[
            dbc.Row([
                dbc.Col([
                    html.H5("Case Creation Trends (Last 30 Days)", className="mt-3 mb-3"),
                    dcc.Graph(id='case-trends-chart')
                ])
            ])
        ]),
    ]),

    # Auto-refresh interval
    dcc.Interval(
        id='case-refresh-interval',
        interval=60*1000,  # 1 minute
        n_intervals=0
    ),

], fluid=True)


# ============================================================================
# Callbacks
# ============================================================================

@callback(
    [
        Output('total-cases', 'children'),
        Output('critical-cases', 'children'),
        Output('sla-breached', 'children'),
        Output('avg-resolution', 'children'),
        Output('first-response-sla', 'children'),
        Output('satisfaction-score', 'children'),
        Output('open-cases-table', 'data'),
        Output('overdue-cases-table', 'data'),
        Output('team-performance-table', 'data'),
        Output('case-trends-chart', 'figure'),
    ],
    [
        Input('case-refresh-interval', 'n_intervals'),
        Input('refresh-cases-btn', 'n_clicks')
    ]
)
def update_case_dashboard(n_intervals, n_clicks):
    """Update all case management dashboard components"""

    # Get metrics
    metrics = get_case_metrics()

    # Get tables
    open_cases = get_open_cases()
    overdue_cases = get_overdue_cases()
    team_perf = get_team_performance()
    trends = get_case_trends()

    # Format KPIs
    total_cases = metrics.get('open_cases', 0)
    critical_cases = f"{metrics.get('critical_open', 0)} + {metrics.get('high_open', 0)}H"
    sla_breached = metrics.get('sla_breached', 0)
    avg_resolution = f"{metrics.get('avg_resolution_hours', 0):.1f}h"
    first_response_sla = f"{metrics.get('first_response_sla_pct', 0):.0f}%"
    satisfaction = f"{metrics.get('avg_satisfaction', 0):.1f}/5.0"

    # Create trends chart
    if not trends.empty:
        fig = px.line(
            trends,
            x='date',
            y='count',
            color='severity',
            title='Case Creation by Severity',
            labels={'count': 'Number of Cases', 'date': 'Date'},
            color_discrete_map={
                'Critical': '#c62828',
                'High': '#e65100',
                'Medium': '#f57c00',
                'Low': '#fbc02d'
            }
        )
        fig.update_layout(hovermode='x unified')
    else:
        fig = go.Figure()
        fig.add_annotation(text="No data available", showarrow=False)

    return (
        total_cases,
        critical_cases,
        sla_breached,
        avg_resolution,
        first_response_sla,
        satisfaction,
        open_cases.to_dict('records'),
        overdue_cases.to_dict('records'),
        team_perf.to_dict('records'),
        fig
    )
