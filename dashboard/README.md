# ART Member Listening Intelligence Hub - Dashboards

Multi-page Dash application for monitoring member feedback, managing cases, viewing alerts, and analyzing topic trends.

## Pages

### 1. Executive Dashboard (`/`)
High-level KPIs and quick links to other dashboards.

**Features:**
- Total interactions and unique members
- Average sentiment score
- At-risk member count
- Key insights and action items
- System status

### 2. Case Management (`/cases`)
Manage feedback cases and track SLAs.

**Features:**
- Open cases table with filtering and sorting
- Overdue and at-risk cases view
- Team performance metrics
- Case creation trends
- Real-time SLA monitoring

### 3. Alert Monitoring (`/alerts`)
View alert history and configured rules.

**Features:**
- Recent alerts feed
- Alert statistics by rule
- Alert timeline visualization
- Hourly pattern analysis
- Configured alert rules reference

### 4. Topic Trends (`/topics`)
AI-powered topic analysis and visualization.

**Features:**
- Top topics by mentions
- Topic volume vs sentiment scatter plot
- Topic evolution trends over time
- Emerging topics detection (>200% increase)
- Sentiment breakdown by topic
- Topic distribution by channel

## Running the Dashboard

### Prerequisites

Install required packages:
```bash
pip install dash dash-bootstrap-components plotly pandas
```

### Start the Dashboard

```bash
cd dashboard
python app_multipage.py
```

Access at: `http://localhost:8050`

### Environment Variables

Optional configuration via environment variables:
- `DASH_DEBUG` - Set to `False` for production
- `DASH_PORT` - Port to run on (default: 8050)
- `DASH_HOST` - Host to bind to (default: 0.0.0.0)

## Dashboard Architecture

```
dashboard/
├── app_multipage.py          # Main app with routing
├── app.py                     # Original single-page app (legacy)
├── pages/
│   ├── __init__.py           # Pages package
│   ├── case_management.py    # Case management dashboard
│   ├── alerts.py             # Alert monitoring dashboard
│   └── topics.py             # Topic trends dashboard
└── README.md                  # This file
```

## Navigation

The dashboard uses URL-based routing:
- `/` - Executive Dashboard
- `/cases` - Case Management
- `/alerts` - Alert Monitoring
- `/topics` - Topic Trends

## Data Sources

All dashboards query data from Databricks tables:
- `gold.feedback_cases` - Case management data
- `gold.alert_history` - Alert data
- `gold.ai_extracted_topics` - AI-powered topic data
- `gold.member_360_view` - Member aggregated data

## Auto-Refresh

Each dashboard auto-refreshes at different intervals:
- **Case Management:** Every 1 minute
- **Alert Monitoring:** Every 30 seconds
- **Topic Trends:** Every 5 minutes

## Customization

### Adding a New Page

1. Create new file in `pages/` directory
2. Define `layout` variable with Dash components
3. Import in `app_multipage.py`
4. Add route in `display_page()` callback
5. Add navigation link in navbar

### Modifying Refresh Intervals

Edit the `dcc.Interval` component in each page:
```python
dcc.Interval(
    id='my-refresh-interval',
    interval=60*1000,  # milliseconds
    n_intervals=0
)
```

## Deployment

### Databricks Apps

Deploy to Databricks Apps:
```bash
databricks apps deploy --source dashboard/app_multipage.py --name art-member-listening
```

### Docker

```dockerfile
FROM python:3.10
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY dashboard/ ./dashboard/
CMD ["python", "dashboard/app_multipage.py"]
```

Build and run:
```bash
docker build -t art-dashboard .
docker run -p 8050:8050 art-dashboard
```

## Troubleshooting

### Issue: "Module not found: pages"
**Solution:** Ensure `pages/__init__.py` exists

### Issue: "No data available" in charts
**Solution:**
1. Verify Spark session is initialized
2. Check that tables exist in catalog
3. Verify data exists in tables

### Issue: Auto-refresh not working
**Solution:** Check browser console for errors, ensure callbacks are properly defined

## Screenshots

### Executive Dashboard
- Overview of key metrics
- Quick links to other dashboards

### Case Management
- Filterable table of open cases
- SLA breach warnings
- Team performance metrics

### Alert Monitoring
- Real-time alert feed
- Alert pattern analysis
- Configured rules

### Topic Trends
- Interactive topic visualization
- Emerging topic detection
- Sentiment analysis

## Support

For issues or questions:
- Check `docs/IMPLEMENTATION_COMPLETE.md` for setup guide
- Review page source code in `dashboard/pages/`
- Verify data exists in Databricks tables
