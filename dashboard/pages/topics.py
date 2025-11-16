"""
Topic Trends Visualization Dashboard
======================================
Dashboard for exploring topic evolution and emerging themes.
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

def get_topic_stats():
    """Get topic statistics"""

    query = f"""
    SELECT
        ai_extracted_topic as topic,
        COUNT(*) as mention_count,
        AVG(sentiment_score) as avg_sentiment,
        AVG(topic_confidence) as avg_confidence
    FROM {catalog}.gold.ai_extracted_topics
    WHERE interaction_date >= DATE_SUB(CURRENT_DATE(), 30)
    GROUP BY 1
    ORDER BY 2 DESC
    """

    df = spark.sql(query).toPandas()
    return df


def get_topic_trends(days_back=90):
    """Get topic trends over time"""

    query = f"""
    SELECT
        DATE_TRUNC('day', interaction_date) as date,
        ai_extracted_topic as topic,
        COUNT(*) as mention_count,
        AVG(sentiment_score) as avg_sentiment
    FROM {catalog}.gold.ai_extracted_topics
    WHERE interaction_date >= DATE_SUB(CURRENT_DATE(), {days_back})
    GROUP BY 1, 2
    ORDER BY 1, 2
    """

    df = spark.sql(query).toPandas()

    if not df.empty:
        df['date'] = pd.to_datetime(df['date'])

    return df


def get_emerging_topics():
    """Get emerging topics"""

    try:
        query = f"""
        SELECT
            topic,
            date,
            mention_count,
            mention_count_7d_ago,
            pct_change_7d,
            avg_sentiment
        FROM {catalog}.gold.emerging_topics
        WHERE date >= DATE_SUB(CURRENT_DATE(), 7)
        ORDER BY pct_change_7d DESC
        LIMIT 20
        """

        df = spark.sql(query).toPandas()

        if not df.empty:
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')

        return df
    except:
        # Table might not exist yet
        return pd.DataFrame()


def get_topic_sentiment_analysis():
    """Get sentiment distribution by topic"""

    try:
        query = f"""
        SELECT
            topic,
            total_mentions,
            avg_sentiment,
            positive_count,
            neutral_count,
            negative_count,
            positive_pct,
            negative_pct
        FROM {catalog}.gold.topic_sentiment_analysis
        ORDER BY total_mentions DESC
        LIMIT 20
        """

        df = spark.sql(query).toPandas()
        return df
    except:
        # Fallback to basic query
        query = f"""
        SELECT
            ai_extracted_topic as topic,
            COUNT(*) as total_mentions,
            AVG(sentiment_score) as avg_sentiment,
            SUM(CASE WHEN sentiment_score > 0.2 THEN 1 ELSE 0 END) as positive_count,
            SUM(CASE WHEN sentiment_score BETWEEN -0.2 AND 0.2 THEN 1 ELSE 0 END) as neutral_count,
            SUM(CASE WHEN sentiment_score < -0.2 THEN 1 ELSE 0 END) as negative_count
        FROM {catalog}.gold.ai_extracted_topics
        WHERE interaction_date >= DATE_SUB(CURRENT_DATE(), 30)
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 20
        """

        df = spark.sql(query).toPandas()

        if not df.empty:
            df['positive_pct'] = (df['positive_count'] / df['total_mentions'] * 100).round(1)
            df['negative_pct'] = (df['negative_count'] / df['total_mentions'] * 100).round(1)

        return df


def get_topic_by_channel():
    """Get topic distribution by channel"""

    query = f"""
    SELECT
        t.ai_extracted_topic as topic,
        i.channel,
        COUNT(*) as count
    FROM {catalog}.gold.ai_extracted_topics t
    JOIN {catalog}.silver.interactions_analyzed i ON t.interaction_id = i.interaction_id
    WHERE t.interaction_date >= DATE_SUB(CURRENT_DATE(), 30)
    GROUP BY 1, 2
    ORDER BY 1, 3 DESC
    """

    df = spark.sql(query).toPandas()
    return df


# ============================================================================
# Layout
# ============================================================================

layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H2("📊 Topic Trends Dashboard", className="text-primary mb-4"),
            html.P("AI-powered topic extraction and trend analysis", className="text-muted")
        ])
    ]),

    # KPI Cards
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4(id="unique-topics", children="0", className="text-primary"),
                    html.P("Unique Topics (30d)", className="text-muted mb-0")
                ])
            ])
        ], width=3),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4(id="total-mentions", children="0", className="text-info"),
                    html.P("Total Mentions", className="text-muted mb-0")
                ])
            ])
        ], width=3),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4(id="emerging-count", children="0", className="text-warning"),
                    html.P("Emerging Topics", className="text-muted mb-0")
                ])
            ])
        ], width=3),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4(id="avg-confidence", children="0%", className="text-success"),
                    html.P("Avg Confidence", className="text-muted mb-0")
                ])
            ])
        ], width=3),
    ], className="mb-4"),

    # Tabs
    dbc.Tabs([
        # Top Topics Tab
        dbc.Tab(label="Top Topics", children=[
            dbc.Row([
                dbc.Col([
                    html.Div([
                        html.H5("Top Topics (Last 30 Days)", className="mt-3 mb-3"),
                        dbc.Button("🔄 Refresh", id="refresh-topics-btn", color="primary", size="sm", className="mb-2"),
                        dcc.Graph(id='top-topics-chart')
                    ])
                ], width=6),
                dbc.Col([
                    html.Div([
                        html.H5("Topic Volume vs Sentiment", className="mt-3 mb-3"),
                        dcc.Graph(id='topic-sentiment-scatter')
                    ])
                ], width=6)
            ]),
            dbc.Row([
                dbc.Col([
                    html.H5("Topic Statistics", className="mt-3 mb-3"),
                    dash_table.DataTable(
                        id='topic-stats-table',
                        columns=[
                            {'name': 'Topic', 'id': 'topic'},
                            {'name': 'Mentions', 'id': 'mention_count'},
                            {'name': 'Avg Sentiment', 'id': 'avg_sentiment', 'type': 'numeric', 'format': {'specifier': '.2f'}},
                            {'name': 'Confidence', 'id': 'avg_confidence', 'type': 'numeric', 'format': {'specifier': '.1%'}},
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
                                'if': {
                                    'filter_query': '{avg_sentiment} < -0.3',
                                    'column_id': 'avg_sentiment'
                                },
                                'backgroundColor': '#ffebee',
                                'color': '#c62828'
                            },
                            {
                                'if': {
                                    'filter_query': '{avg_sentiment} > 0.3',
                                    'column_id': 'avg_sentiment'
                                },
                                'backgroundColor': '#e8f5e9',
                                'color': '#2e7d32'
                            },
                        ],
                        page_size=20,
                        sort_action='native',
                    )
                ])
            ])
        ]),

        # Trends Tab
        dbc.Tab(label="Trends", children=[
            dbc.Row([
                dbc.Col([
                    html.Div([
                        html.H5("Topic Evolution (Last 90 Days)", className="mt-3 mb-3"),
                        dbc.Select(
                            id='topic-selector',
                            options=[],
                            value=None,
                            placeholder="Select a topic to highlight..."
                        ),
                        dcc.Graph(id='topic-trends-chart')
                    ])
                ])
            ])
        ]),

        # Emerging Topics Tab
        dbc.Tab(label="🚀 Emerging", children=[
            dbc.Row([
                dbc.Col([
                    html.Div([
                        html.H5("Emerging Topics (Last 7 Days)", className="mt-3 mb-3 text-warning"),
                        html.P("Topics with significant increase in mentions", className="text-muted"),
                        dash_table.DataTable(
                            id='emerging-topics-table',
                            columns=[
                                {'name': 'Topic', 'id': 'topic'},
                                {'name': 'Date', 'id': 'date'},
                                {'name': 'Current Mentions', 'id': 'mention_count'},
                                {'name': 'Previous (7d ago)', 'id': 'mention_count_7d_ago'},
                                {'name': '% Change', 'id': 'pct_change_7d', 'type': 'numeric', 'format': {'specifier': '.0f'}},
                                {'name': 'Sentiment', 'id': 'avg_sentiment', 'type': 'numeric', 'format': {'specifier': '.2f'}},
                            ],
                            data=[],
                            style_table={'overflowX': 'auto'},
                            style_cell={
                                'textAlign': 'left',
                                'padding': '10px',
                                'fontSize': '14px'
                            },
                            style_header={
                                'backgroundColor': '#fff3e0',
                                'fontWeight': 'bold',
                                'color': '#e65100'
                            },
                            style_data_conditional=[
                                {
                                    'if': {'filter_query': '{pct_change_7d} >= 300'},
                                    'backgroundColor': '#ffebee',
                                    'fontWeight': 'bold'
                                },
                                {
                                    'if': {'filter_query': '{pct_change_7d} >= 200'},
                                    'backgroundColor': '#fff3e0'
                                },
                            ],
                            page_size=20,
                        )
                    ])
                ])
            ])
        ]),

        # Sentiment Analysis Tab
        dbc.Tab(label="Sentiment", children=[
            dbc.Row([
                dbc.Col([
                    html.H5("Sentiment Distribution by Topic", className="mt-3 mb-3"),
                    dcc.Graph(id='topic-sentiment-breakdown')
                ], width=12)
            ]),
            dbc.Row([
                dbc.Col([
                    html.H5("Topic Sentiment Details", className="mt-3 mb-3"),
                    dash_table.DataTable(
                        id='topic-sentiment-table',
                        columns=[
                            {'name': 'Topic', 'id': 'topic'},
                            {'name': 'Total', 'id': 'total_mentions'},
                            {'name': 'Avg Sentiment', 'id': 'avg_sentiment', 'type': 'numeric', 'format': {'specifier': '.2f'}},
                            {'name': 'Positive', 'id': 'positive_count'},
                            {'name': 'Neutral', 'id': 'neutral_count'},
                            {'name': 'Negative', 'id': 'negative_count'},
                            {'name': 'Positive %', 'id': 'positive_pct', 'type': 'numeric', 'format': {'specifier': '.1f'}},
                            {'name': 'Negative %', 'id': 'negative_pct', 'type': 'numeric', 'format': {'specifier': '.1f'}},
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
            ])
        ]),

        # Channel Distribution Tab
        dbc.Tab(label="By Channel", children=[
            dbc.Row([
                dbc.Col([
                    html.H5("Topic Distribution by Channel", className="mt-3 mb-3"),
                    dcc.Graph(id='topic-by-channel-chart')
                ])
            ])
        ]),
    ]),

    # Auto-refresh interval
    dcc.Interval(
        id='topic-refresh-interval',
        interval=5*60*1000,  # 5 minutes
        n_intervals=0
    ),

], fluid=True)


# ============================================================================
# Callbacks
# ============================================================================

@callback(
    [
        Output('unique-topics', 'children'),
        Output('total-mentions', 'children'),
        Output('emerging-count', 'children'),
        Output('avg-confidence', 'children'),
        Output('topic-stats-table', 'data'),
        Output('top-topics-chart', 'figure'),
        Output('topic-sentiment-scatter', 'figure'),
        Output('topic-selector', 'options'),
        Output('emerging-topics-table', 'data'),
        Output('topic-sentiment-table', 'data'),
        Output('topic-sentiment-breakdown', 'figure'),
        Output('topic-by-channel-chart', 'figure'),
    ],
    [
        Input('topic-refresh-interval', 'n_intervals'),
        Input('refresh-topics-btn', 'n_clicks')
    ]
)
def update_topic_dashboard(n_intervals, n_clicks):
    """Update all topic dashboard components"""

    # Get data
    topic_stats = get_topic_stats()
    emerging = get_emerging_topics()
    sentiment_analysis = get_topic_sentiment_analysis()
    by_channel = get_topic_by_channel()

    # Calculate KPIs
    unique_topics = len(topic_stats) if not topic_stats.empty else 0
    total_mentions = topic_stats['mention_count'].sum() if not topic_stats.empty else 0
    emerging_count = len(emerging) if not emerging.empty else 0
    avg_confidence = f"{topic_stats['avg_confidence'].mean() * 100:.0f}%" if not topic_stats.empty else "0%"

    # Top topics chart
    if not topic_stats.empty:
        top_10 = topic_stats.head(10)
        fig_top = px.bar(
            top_10,
            x='mention_count',
            y='topic',
            orientation='h',
            title='Top 10 Topics by Mentions',
            labels={'mention_count': 'Mentions', 'topic': 'Topic'},
            color='avg_sentiment',
            color_continuous_scale='RdYlGn',
            color_continuous_midpoint=0
        )
        fig_top.update_layout(yaxis={'categoryorder': 'total ascending'})
    else:
        fig_top = go.Figure()
        fig_top.add_annotation(text="No topic data available", showarrow=False)

    # Sentiment scatter
    if not topic_stats.empty:
        fig_scatter = px.scatter(
            topic_stats,
            x='mention_count',
            y='avg_sentiment',
            text='topic',
            title='Topic Volume vs Sentiment',
            labels={'mention_count': 'Mentions', 'avg_sentiment': 'Avg Sentiment'},
            size='mention_count',
            color='avg_sentiment',
            color_continuous_scale='RdYlGn',
            color_continuous_midpoint=0
        )
        fig_scatter.update_traces(textposition='top center')
    else:
        fig_scatter = go.Figure()
        fig_scatter.add_annotation(text="No data available", showarrow=False)

    # Topic selector options
    topic_options = [{'label': topic, 'value': topic} for topic in topic_stats['topic'].tolist()]

    # Sentiment breakdown chart
    if not sentiment_analysis.empty:
        top_sentiment = sentiment_analysis.head(10)
        fig_sentiment = go.Figure()

        fig_sentiment.add_trace(go.Bar(
            name='Positive',
            y=top_sentiment['topic'],
            x=top_sentiment['positive_count'],
            orientation='h',
            marker_color='#4caf50'
        ))
        fig_sentiment.add_trace(go.Bar(
            name='Neutral',
            y=top_sentiment['topic'],
            x=top_sentiment['neutral_count'],
            orientation='h',
            marker_color='#9e9e9e'
        ))
        fig_sentiment.add_trace(go.Bar(
            name='Negative',
            y=top_sentiment['topic'],
            x=top_sentiment['negative_count'],
            orientation='h',
            marker_color='#f44336'
        ))

        fig_sentiment.update_layout(
            barmode='stack',
            title='Sentiment Distribution by Topic',
            yaxis={'categoryorder': 'total ascending'},
            xaxis_title='Number of Mentions'
        )
    else:
        fig_sentiment = go.Figure()
        fig_sentiment.add_annotation(text="No data available", showarrow=False)

    # Topic by channel chart
    if not by_channel.empty:
        fig_channel = px.bar(
            by_channel,
            x='channel',
            y='count',
            color='topic',
            title='Topic Distribution by Channel',
            labels={'count': 'Mentions', 'channel': 'Channel'}
        )
    else:
        fig_channel = go.Figure()
        fig_channel.add_annotation(text="No data available", showarrow=False)

    return (
        unique_topics,
        f"{total_mentions:,}",
        emerging_count,
        avg_confidence,
        topic_stats.to_dict('records'),
        fig_top,
        fig_scatter,
        topic_options,
        emerging.to_dict('records') if not emerging.empty else [],
        sentiment_analysis.to_dict('records'),
        fig_sentiment,
        fig_channel
    )


@callback(
    Output('topic-trends-chart', 'figure'),
    [Input('topic-selector', 'value')]
)
def update_topic_trends(selected_topic):
    """Update topic trends chart based on selection"""

    trends = get_topic_trends(days_back=90)

    if trends.empty:
        fig = go.Figure()
        fig.add_annotation(text="No trend data available", showarrow=False)
        return fig

    if selected_topic:
        # Highlight selected topic
        fig = go.Figure()

        # Other topics (lighter)
        other_topics = trends[trends['topic'] != selected_topic]
        for topic in other_topics['topic'].unique():
            topic_data = other_topics[other_topics['topic'] == topic]
            fig.add_trace(go.Scatter(
                x=topic_data['date'],
                y=topic_data['mention_count'],
                name=topic,
                mode='lines',
                opacity=0.2,
                showlegend=False
            ))

        # Selected topic (highlighted)
        selected_data = trends[trends['topic'] == selected_topic]
        if not selected_data.empty:
            fig.add_trace(go.Scatter(
                x=selected_data['date'],
                y=selected_data['mention_count'],
                name=selected_topic,
                mode='lines+markers',
                line=dict(width=3),
                marker=dict(size=8)
            ))

        fig.update_layout(
            title=f'Topic Evolution: {selected_topic}',
            xaxis_title='Date',
            yaxis_title='Mentions',
            hovermode='x unified'
        )
    else:
        # Show all topics
        fig = px.line(
            trends,
            x='date',
            y='mention_count',
            color='topic',
            title='Topic Evolution (Select a topic to highlight)',
            labels={'mention_count': 'Mentions', 'date': 'Date'}
        )
        fig.update_layout(hovermode='x unified')

    return fig
