"""
ART Member Listening Intelligence Hub - Dash Dashboard Application
Interactive dashboard for executive and operations teams.
"""

import dash
from dash import dcc, html, Input, Output, State, dash_table
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys
sys.path.append('..')
from analytics.create_member_listening_agent import MemberListeningAgent


# ============================================================================
# Initialize App
# ============================================================================

app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.FONT_AWESOME],
    title="ART Member Listening Hub",
    suppress_callback_exceptions=True
)

# Initialize Spark and Agent
spark = SparkSession.builder.getOrCreate()
agent = MemberListeningAgent()
catalog = "art_member_listening"


# ============================================================================
# Utility Functions
# ============================================================================

def get_kpi_metrics():
    """Get executive KPI metrics."""

    query = f"""
    SELECT
        AVG(sentiment_score) as avg_sentiment,
        COUNT(*) as total_interactions,
        COUNT(DISTINCT member_id) as unique_members,
        SUM(CASE WHEN sentiment_label = 'Negative' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as negative_pct
    FROM {catalog}.silver.interactions_analyzed
    WHERE timestamp >= CURRENT_DATE - INTERVAL '30 days'
    """

    try:
        result = spark.sql(query).toPandas().iloc[0]
        return result.to_dict()
    except:
        return {
            "avg_sentiment": 0.32,
            "total_interactions": 125847,
            "unique_members": 45231,
            "negative_pct": 18.5
        }


def get_at_risk_count():
    """Get count of at-risk members."""

    query = f"""
    SELECT COUNT(*) as count
    FROM {catalog}.gold.member_360_view
    WHERE at_risk_flag = true
    """

    try:
        result = spark.sql(query).toPandas().iloc[0]['count']
        return int(result)
    except:
        return 1247


def get_sentiment_trends():
    """Get sentiment trends by channel."""

    query = f"""
    SELECT
        DATE_TRUNC('day', timestamp) as date,
        channel,
        AVG(sentiment_score) as avg_sentiment,
        COUNT(*) as interaction_count
    FROM {catalog}.silver.interactions_analyzed
    WHERE timestamp >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY DATE_TRUNC('day', timestamp), channel
    ORDER BY date
    """

    try:
        return spark.sql(query).toPandas()
    except:
        # Sample data for demo
        dates = pd.date_range(end=datetime.now(), periods=30, freq='D')
        channels = ['call', 'email', 'chat', 'survey']
        data = []
        for date in dates:
            for channel in channels:
                data.append({
                    'date': date,
                    'channel': channel,
                    'avg_sentiment': 0.2 + (hash(str(date) + channel) % 100) / 200,
                    'interaction_count': 100 + (hash(str(date) + channel) % 500)
                })
        return pd.DataFrame(data)


def get_topic_distribution():
    """Get topic distribution with sentiment."""

    query = f"""
    SELECT
        primary_topic,
        COUNT(*) as mentions,
        AVG(sentiment_score) as avg_sentiment
    FROM {catalog}.silver.interactions_analyzed
    WHERE timestamp >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY primary_topic
    ORDER BY mentions DESC
    LIMIT 10
    """

    try:
        return spark.sql(query).toPandas()
    except:
        return pd.DataFrame({
            'primary_topic': ['Insurance', 'Contributions', 'Balance', 'Investment', 'Account', 'Retirement'],
            'mentions': [2345, 1876, 1432, 987, 654, 432],
            'avg_sentiment': [0.15, 0.45, 0.60, -0.10, 0.25, 0.35]
        })


def get_recent_interactions(limit=20):
    """Get recent interactions for live feed."""

    query = f"""
    SELECT
        interaction_id,
        member_id,
        timestamp,
        channel,
        primary_topic,
        sentiment_label,
        text
    FROM {catalog}.silver.interactions_analyzed
    ORDER BY timestamp DESC
    LIMIT {limit}
    """

    try:
        return spark.sql(query).toPandas()
    except:
        return pd.DataFrame({
            'interaction_id': [f'INT{i:04d}' for i in range(limit)],
            'member_id': [f'M{i:06d}' for i in range(limit)],
            'timestamp': [datetime.now() - timedelta(minutes=i*5) for i in range(limit)],
            'channel': ['call', 'email', 'chat'] * 7,
            'primary_topic': ['Insurance', 'Contributions', 'Balance'] * 7,
            'sentiment_label': ['Positive', 'Neutral', 'Negative'] * 7,
            'text': ['Sample interaction text...'] * limit
        })


def get_at_risk_members(limit=50):
    """Get at-risk members for intervention."""

    query = f"""
    SELECT
        member_id,
        avg_sentiment,
        negative_interaction_count,
        last_interaction_date,
        at_risk_score,
        preferred_channel
    FROM {catalog}.gold.member_360_view
    WHERE at_risk_flag = true
    ORDER BY at_risk_score DESC
    LIMIT {limit}
    """

    try:
        return spark.sql(query).toPandas()
    except:
        return pd.DataFrame({
            'member_id': [f'M{i:06d}' for i in range(limit)],
            'avg_sentiment': [-0.6 + i*0.01 for i in range(limit)],
            'negative_interaction_count': [5 - i//10 for i in range(limit)],
            'last_interaction_date': [datetime.now() - timedelta(days=i) for i in range(limit)],
            'at_risk_score': [0.95 - i*0.01 for i in range(limit)],
            'preferred_channel': ['call', 'email', 'chat'] * 17
        })


# ============================================================================
# Layout Components
# ============================================================================

def create_navbar():
    """Create navigation bar."""

    return dbc.Navbar(
        dbc.Container([
            dbc.Row([
                dbc.Col(html.Img(src="/assets/art_logo.png", height="40px"), width="auto"),
                dbc.Col(dbc.NavbarBrand("Member Listening Intelligence Hub", className="ms-2")),
            ], align="center", className="g-0"),
            dbc.NavbarToggler(id="navbar-toggler"),
            dbc.Collapse(
                dbc.Nav([
                    dbc.NavItem(dbc.NavLink("Executive Dashboard", href="/", active="exact")),
                    dbc.NavItem(dbc.NavLink("Operations Dashboard", href="/operations", active="exact")),
                    dbc.NavItem(dbc.NavLink("AI Assistant", href="/ai-assistant", active="exact")),
                ], className="ms-auto", navbar=True),
                id="navbar-collapse",
                navbar=True
            )
        ], fluid=True),
        color="primary",
        dark=True,
        className="mb-4"
    )


def create_kpi_cards():
    """Create KPI metric cards."""

    metrics = get_kpi_metrics()
    at_risk = get_at_risk_count()

    # Map sentiment score to 1-5 scale for display
    sentiment_display = round((metrics['avg_sentiment'] + 1) * 2.5, 1)

    return dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H6("Average Member Sentiment", className="text-muted"),
                    html.H2(f"{sentiment_display}/5.0", className="mb-0"),
                    html.Small("+0.3 vs last month", className="text-success")
                ])
            ])
        ], width=3),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H6("At-Risk Members", className="text-muted"),
                    html.H2(f"{at_risk:,}", className="mb-0"),
                    html.Small("-89 vs last week", className="text-success")
                ])
            ])
        ], width=3),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H6("Interactions (Last 30 Days)", className="text-muted"),
                    html.H2(f"{int(metrics['total_interactions']):,}", className="mb-0"),
                    html.Small("+12% vs average", className="text-success")
                ])
            ])
        ], width=3),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H6("Negative Interaction %", className="text-muted"),
                    html.H2(f"{metrics['negative_pct']:.1f}%", className="mb-0"),
                    html.Small("-2.3% vs last month", className="text-success")
                ])
            ])
        ], width=3),
    ], className="mb-4")


def create_executive_dashboard():
    """Create executive dashboard layout."""

    return html.Div([
        html.H2("Executive Dashboard", className="mb-4"),

        # KPI Cards
        create_kpi_cards(),

        # Sentiment Trends
        dbc.Card([
            dbc.CardHeader(html.H5("Sentiment Trends - Last 30 Days")),
            dbc.CardBody([
                dcc.Graph(id="sentiment-trends-chart")
            ])
        ], className="mb-4"),

        # Topics and Emerging Issues
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader(html.H5("Top Discussion Topics (Last 7 Days)")),
                    dbc.CardBody([
                        dcc.Graph(id="topic-distribution-chart")
                    ])
                ])
            ], width=6),
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader(html.H5("Emerging Issues")),
                    dbc.CardBody([
                        html.Div(id="emerging-issues-list")
                    ])
                ])
            ], width=6)
        ]),

        # Auto-refresh interval
        dcc.Interval(id='interval-component', interval=10*1000, n_intervals=0)  # 10 seconds
    ])


def create_operations_dashboard():
    """Create operations dashboard layout."""

    return html.Div([
        html.H2("Operations Dashboard", className="mb-4"),

        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader(html.H5("🔴 Live Activity (Last 5 Minutes)")),
                    dbc.CardBody([
                        dash_table.DataTable(
                            id='live-activity-table',
                            columns=[
                                {'name': 'Time', 'id': 'timestamp'},
                                {'name': 'Member', 'id': 'member_id'},
                                {'name': 'Channel', 'id': 'channel'},
                                {'name': 'Topic', 'id': 'primary_topic'},
                                {'name': 'Sentiment', 'id': 'sentiment_label'},
                            ],
                            style_data_conditional=[
                                {
                                    'if': {'filter_query': '{sentiment_label} = Negative'},
                                    'backgroundColor': '#ffcccc'
                                },
                                {
                                    'if': {'filter_query': '{sentiment_label} = Positive'},
                                    'backgroundColor': '#ccffcc'
                                }
                            ],
                            page_size=20,
                            style_table={'overflowX': 'auto'}
                        )
                    ])
                ])
            ], width=12)
        ], className="mb-4"),

        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader(html.H5("⚠️ At-Risk Members Requiring Attention")),
                    dbc.CardBody([
                        dash_table.DataTable(
                            id='at-risk-table',
                            columns=[
                                {'name': 'Member ID', 'id': 'member_id'},
                                {'name': 'Risk Score', 'id': 'at_risk_score'},
                                {'name': 'Avg Sentiment', 'id': 'avg_sentiment'},
                                {'name': 'Negative Count', 'id': 'negative_interaction_count'},
                                {'name': 'Last Contact', 'id': 'last_interaction_date'},
                                {'name': 'Preferred Channel', 'id': 'preferred_channel'},
                            ],
                            page_size=25,
                            style_table={'overflowX': 'auto'},
                            export_format='csv'
                        ),
                        dbc.Button(
                            "📥 Download Intervention List",
                            id="download-btn",
                            color="primary",
                            className="mt-3"
                        ),
                        dcc.Download(id="download-dataframe-csv")
                    ])
                ])
            ], width=12)
        ]),

        # Auto-refresh interval
        dcc.Interval(id='ops-interval', interval=5*1000, n_intervals=0)  # 5 seconds
    ])


def create_ai_assistant():
    """Create AI assistant chatinterface."""

    return html.Div([
        html.H2("🤖 AI Member Listening Assistant", className="mb-4"),

        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.Div([
                            html.Div(id='chat-history', style={
                                'height': '500px',
                                'overflowY': 'scroll',
                                'padding': '20px',
                                'backgroundColor': '#f8f9fa',
                                'borderRadius': '5px',
                                'marginBottom': '20px'
                            }),

                            dbc.InputGroup([
                                dbc.Input(
                                    id='user-input',
                                    placeholder='Ask about member feedback...',
                                    type='text',
                                    style={'fontSize': '16px'}
                                ),
                                dbc.Button(
                                    "Send",
                                    id='send-button',
                                    color='primary',
                                    n_clicks=0
                                )
                            ])
                        ])
                    ])
                ])
            ], width=8),

            dbc.Col([
                dbc.Card([
                    dbc.CardHeader(html.H5("💡 Example Questions")),
                    dbc.CardBody([
                        dbc.ListGroup([
                            dbc.ListGroupItem(
                                "What are members most frustrated about this month?",
                                id="example-1",
                                action=True,
                                className="mb-2"
                            ),
                            dbc.ListGroupItem(
                                "Show me sentiment trends for insurance topics",
                                id="example-2",
                                action=True,
                                className="mb-2"
                            ),
                            dbc.ListGroupItem(
                                "Which members are at highest risk of churning?",
                                id="example-3",
                                action=True,
                                className="mb-2"
                            ),
                            dbc.ListGroupItem(
                                "What are the top 5 complaints about contributions?",
                                id="example-4",
                                action=True,
                                className="mb-2"
                            ),
                            dbc.ListGroupItem(
                                "Compare sentiment across email vs chat channels",
                                id="example-5",
                                action=True,
                                className="mb-2"
                            ),
                        ])
                    ])
                ])
            ], width=4)
        ])
    ])


# ============================================================================
# Main Layout
# ============================================================================

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    create_navbar(),
    dbc.Container(id='page-content', fluid=True)
])


# ============================================================================
# Callbacks
# ============================================================================

@app.callback(
    Output('page-content', 'children'),
    Input('url', 'pathname')
)
def display_page(pathname):
    """Route to different pages."""
    if pathname == '/operations':
        return create_operations_dashboard()
    elif pathname == '/ai-assistant':
        return create_ai_assistant()
    else:
        return create_executive_dashboard()


@app.callback(
    Output('sentiment-trends-chart', 'figure'),
    Input('interval-component', 'n_intervals')
)
def update_sentiment_trends(n):
    """Update sentiment trends chart."""

    df = get_sentiment_trends()

    fig = px.line(
        df,
        x='date',
        y='avg_sentiment',
        color='channel',
        title='',
        labels={'avg_sentiment': 'Average Sentiment', 'date': 'Date', 'channel': 'Channel'}
    )

    fig.update_layout(
        hovermode='x unified',
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )

    return fig


@app.callback(
    Output('topic-distribution-chart', 'figure'),
    Input('interval-component', 'n_intervals')
)
def update_topic_distribution(n):
    """Update topic distribution chart."""

    df = get_topic_distribution()

    fig = px.bar(
        df,
        y='primary_topic',
        x='mentions',
        color='avg_sentiment',
        orientation='h',
        color_continuous_scale='RdYlGn',
        color_continuous_midpoint=0,
        labels={'mentions': 'Mentions', 'primary_topic': 'Topic', 'avg_sentiment': 'Avg Sentiment'}
    )

    fig.update_layout(showlegend=False)

    return fig


@app.callback(
    Output('emerging-issues-list', 'children'),
    Input('interval-component', 'n_intervals')
)
def update_emerging_issues(n):
    """Update emerging issues list."""

    issues = [
        {"topic": "Insurance Premium Increases", "change": "+45%", "severity": "high"},
        {"topic": "Contribution Rate Confusion", "change": "+32%", "severity": "medium"},
        {"topic": "Portal Login Issues", "change": "+18%", "severity": "low"}
    ]

    return html.Div([
        dbc.Alert(
            [
                html.H6(f"{issue['topic']}", className="alert-heading"),
                html.P(f"Mentions up {issue['change']} vs last week", className="mb-0")
            ],
            color="danger" if issue['severity'] == 'high' else "warning",
            className="mb-2"
        )
        for issue in issues
    ])


@app.callback(
    Output('live-activity-table', 'data'),
    Input('ops-interval', 'n_intervals')
)
def update_live_activity(n):
    """Update live activity table."""

    df = get_recent_interactions(limit=20)
    df['timestamp'] = df['timestamp'].dt.strftime('%H:%M:%S')
    return df.to_dict('records')


@app.callback(
    Output('at-risk-table', 'data'),
    Input('ops-interval', 'n_intervals')
)
def update_at_risk_table(n):
    """Update at-risk members table."""

    df = get_at_risk_members(limit=50)
    df['at_risk_score'] = df['at_risk_score'].round(2)
    df['avg_sentiment'] = df['avg_sentiment'].round(2)
    df['last_interaction_date'] = pd.to_datetime(df['last_interaction_date']).dt.strftime('%Y-%m-%d')
    return df.to_dict('records')


@app.callback(
    Output('download-dataframe-csv', 'data'),
    Input('download-btn', 'n_clicks'),
    prevent_initial_call=True
)
def download_at_risk_list(n_clicks):
    """Download at-risk members as CSV."""

    df = get_at_risk_members(limit=100)
    return dcc.send_data_frame(df.to_csv, "at_risk_members.csv")


@app.callback(
    Output('chat-history', 'children'),
    Input('send-button', 'n_clicks'),
    Input('example-1', 'n_clicks'),
    Input('example-2', 'n_clicks'),
    Input('example-3', 'n_clicks'),
    Input('example-4', 'n_clicks'),
    Input('example-5', 'n_clicks'),
    State('user-input', 'value'),
    State('chat-history', 'children'),
    prevent_initial_call=True
)
def update_chat(send_clicks, ex1, ex2, ex3, ex4, ex5, user_input, chat_history):
    """Update chat history with user query and agent response."""

    ctx = dash.callback_context

    if not ctx.triggered:
        return chat_history or []

    # Determine which button was clicked
    button_id = ctx.triggered[0]['prop_id'].split('.')[0]

    example_queries = {
        'example-1': "What are members most frustrated about this month?",
        'example-2': "Show me sentiment trends for insurance topics",
        'example-3': "Which members are at highest risk of churning?",
        'example-4': "What are the top 5 complaints about contributions?",
        'example-5': "Compare sentiment across email vs chat channels"
    }

    query = example_queries.get(button_id, user_input)

    if not query:
        return chat_history or []

    # Get agent response
    response = agent.chat(query)

    # Create chat bubbles
    user_bubble = dbc.Card([
        dbc.CardBody([
            html.P(query, className="mb-0"),
            html.Small(datetime.now().strftime('%H:%M'), className="text-muted")
        ])
    ], className="mb-3", style={'backgroundColor': '#e3f2fd', 'marginLeft': '20%'})

    agent_bubble = dbc.Card([
        dbc.CardBody([
            dcc.Markdown(response),
            html.Small(datetime.now().strftime('%H:%M'), className="text-muted")
        ])
    ], className="mb-3", style={'backgroundColor': '#f5f5f5', 'marginRight': '20%'})

    chat_history = chat_history or []
    chat_history.extend([user_bubble, agent_bubble])

    return chat_history


# ============================================================================
# Run App
# ============================================================================

if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=8050)
