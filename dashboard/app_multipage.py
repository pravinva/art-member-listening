"""
ART Member Listening Intelligence Hub - Multi-Page Dash Dashboard
==================================================================
Main application with navigation to all dashboard pages.

Pages:
1. Executive Dashboard - High-level KPIs and trends
2. Case Management - Manage feedback cases and track SLAs
3. Alert Monitoring - View alert history and configure rules
4. Topic Trends - AI-powered topic analysis and trends
"""

import dash
from dash import dcc, html, Input, Output, callback
import dash_bootstrap_components as dbc

# Initialize App with multi-page support
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.FONT_AWESOME],
    title="ART Member Listening Hub",
    suppress_callback_exceptions=True,
    use_pages=False  # We'll handle pages manually
)

server = app.server  # For deployment

# Import page layouts
from pages import case_management, alerts, topics

# Navigation bar
navbar = dbc.NavbarSimple(
    children=[
        dbc.NavItem(dbc.NavLink("📊 Executive", href="/", id="nav-executive")),
        dbc.NavItem(dbc.NavLink("📋 Cases", href="/cases", id="nav-cases")),
        dbc.NavItem(dbc.NavLink("🚨 Alerts", href="/alerts", id="nav-alerts")),
        dbc.NavItem(dbc.NavLink("📈 Topics", href="/topics", id="nav-topics")),
    ],
    brand="🎧 ART Member Listening Intelligence Hub",
    brand_href="/",
    color="primary",
    dark=True,
    className="mb-4",
)

# App layout with URL routing
app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    navbar,
    html.Div(id='page-content')
])


# ============================================================================
# Executive Dashboard Page (Simplified version)
# ============================================================================

def get_executive_layout():
    """Simple executive dashboard"""

    return dbc.Container([
        dbc.Row([
            dbc.Col([
                html.H2("📊 Executive Dashboard", className="text-primary mb-4"),
                html.P("High-level KPIs and member insights", className="text-muted")
            ])
        ]),

        # KPI Cards
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H4("125,847", className="text-primary"),
                        html.P("Total Interactions (30d)", className="text-muted mb-0")
                    ])
                ])
            ], width=3),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H4("45,231", className="text-info"),
                        html.P("Unique Members", className="text-muted mb-0")
                    ])
                ])
            ], width=3),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H4("0.32", className="text-success"),
                        html.P("Avg Sentiment", className="text-muted mb-0")
                    ])
                ])
            ], width=3),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H4("1,247", className="text-danger"),
                        html.P("At-Risk Members", className="text-muted mb-0")
                    ])
                ])
            ], width=3),
        ], className="mb-4"),

        # Quick Links
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H5("📋 Case Management", className="card-title"),
                        html.P("Manage feedback cases and track SLAs", className="card-text"),
                        dbc.Button("Go to Cases →", color="primary", href="/cases")
                    ])
                ])
            ], width=4),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H5("🚨 Alert Monitoring", className="card-title"),
                        html.P("View alert history and configure rules", className="card-text"),
                        dbc.Button("Go to Alerts →", color="warning", href="/alerts")
                    ])
                ])
            ], width=4),
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H5("📈 Topic Trends", className="card-title"),
                        html.P("AI-powered topic analysis and trends", className="card-text"),
                        dbc.Button("Go to Topics →", color="info", href="/topics")
                    ])
                ])
            ], width=4),
        ], className="mb-4"),

        # Key Insights Section
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("🎯 Key Insights"),
                    dbc.CardBody([
                        html.Ul([
                            html.Li("Insurance-related inquiries up 23% this week"),
                            html.Li("Average response time improved to 3.2 hours"),
                            html.Li("Member satisfaction score: 4.2/5.0"),
                            html.Li("VIP member sentiment trending positive (+0.15)"),
                        ])
                    ])
                ])
            ], width=6),
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("⚠️ Action Items"),
                    dbc.CardBody([
                        html.Ul([
                            html.Li([
                                html.Strong("24 cases "),
                                "breached SLA - requires immediate attention"
                            ]),
                            html.Li([
                                html.Strong("15 critical alerts "),
                                "triggered in last 24 hours"
                            ]),
                            html.Li([
                                html.Strong("3 emerging topics "),
                                "detected - review trending issues"
                            ]),
                            html.Li([
                                html.Strong("127 at-risk VIP members "),
                                "- proactive outreach recommended"
                            ]),
                        ])
                    ])
                ])
            ], width=6),
        ]),

        html.Hr(className="my-4"),

        # System Status
        dbc.Row([
            dbc.Col([
                html.H5("🔧 System Status", className="mb-3"),
                dbc.Alert([
                    html.Strong("✅ All systems operational"),
                    html.Br(),
                    html.Small("Last updated: 2 minutes ago")
                ], color="success"),
                html.Ul([
                    html.Li("Real-time processing: Active (87ms avg latency)"),
                    html.Li("AI topic modeling: Online"),
                    html.Li("Salesforce sync: Connected (last sync: 1 hour ago)"),
                    html.Li("Slack alerts: Configured and active"),
                ], className="text-muted")
            ])
        ])

    ], fluid=True)


# ============================================================================
# Routing Callback
# ============================================================================

@callback(
    Output('page-content', 'children'),
    [Input('url', 'pathname')]
)
def display_page(pathname):
    """Route to appropriate page based on URL"""

    if pathname == '/cases':
        return case_management.layout
    elif pathname == '/alerts':
        return alerts.layout
    elif pathname == '/topics':
        return topics.layout
    else:
        # Default to executive dashboard
        return get_executive_layout()


# ============================================================================
# Run Server
# ============================================================================

if __name__ == '__main__':
    app.run_server(
        debug=True,
        host='0.0.0.0',
        port=8050
    )
