import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
from datetime import datetime

# Page config
st.set_page_config(
    page_title="Canada's Fintech Pulse",
    page_icon="🇨🇦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Force light mode and clean design
st.markdown("""
    <style>
    .stApp { background-color: #ffffff !important; }
    .main { background-color: #ffffff !important; }
    .block-container { 
        padding-top: 2rem; 
        padding-left: 3rem; 
        padding-right: 3rem;
        background-color: #ffffff !important;
    }
    h1, h2, h3 { color: #1a1a2e !important; font-weight: 700; }
    p, div, span, label { color: #1a1a2e !important; }
    section[data-testid="stSidebar"] { background-color: #f8f9fa !important; }
    section[data-testid="stSidebar"] * { color: #1a1a2e !important; }
    div[data-testid="metric-container"] {
        background-color: #f8f9fa !important;
        border-radius: 12px !important;
        padding: 1rem !important;
        border: 1px solid #e0e0e0 !important;
    }
    div[data-testid="stDataFrame"] { border-radius: 12px !important; }
    </style>
""", unsafe_allow_html=True)

# Database connection
con = duckdb.connect('/Users/rimona/fintech-pulse/ingestion/fintech_pulse.duckdb', read_only=True)

# Sidebar
with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/commons/d/d9/Flag_of_Canada_%28Pantone%29.svg", width=60)
    st.title("Canada's Fintech Pulse")
    st.caption("Powered by Reddit sentiment analysis")
    st.divider()

    companies = con.execute("SELECT DISTINCT company FROM mart_company_sentiment ORDER BY company").df()['company'].tolist()
    selected_companies = st.multiselect("Companies", companies, default=companies)

    content_type = st.radio("Content Type", ["Both", "post", "comment"])

    st.divider()
    total = con.execute("SELECT COUNT(*) as total FROM raw_posts").df()['total'][0]
    st.metric("Total Records", f"{total:,}")
    st.caption(f"Last updated: {datetime.now().strftime('%b %d, %Y %H:%M')}")

# Build filters
if content_type == "Both":
    content_filter = "('post', 'comment')"
else:
    content_filter = f"('{content_type}')"

company_filter = ', '.join([f"'{c}'" for c in selected_companies])

# Header
st.title("🇨🇦 Canada's Fintech Pulse")
st.caption("Real time sentiment tracking of Canadian fintech companies on Reddit")
st.divider()

# Section 1: KPI Metrics
col1, col2, col3, col4 = st.columns(4)

with col1:
    top = con.execute(f"""
        SELECT company, ROUND(AVG(weighted_sentiment), 3) as score
        FROM mart_company_sentiment
        WHERE company IN ({company_filter})
        GROUP BY company ORDER BY score DESC LIMIT 1
    """).df()
    if len(top) > 0:
        st.metric("Most Positive Brand", top['company'][0].title(), f"{top['score'][0]}")

with col2:
    bottom = con.execute(f"""
        SELECT company, ROUND(AVG(weighted_sentiment), 3) as score
        FROM mart_company_sentiment
        WHERE company IN ({company_filter})
        GROUP BY company ORDER BY score ASC LIMIT 1
    """).df()
    if len(bottom) > 0:
        st.metric("Most Negative Brand", bottom['company'][0].title(), f"{bottom['score'][0]}")

with col3:
    total_mentions = con.execute(f"""
        SELECT SUM(mention_count) as total
        FROM mart_company_sentiment
        WHERE company IN ({company_filter})
    """).df()['total'][0]
    st.metric("Total Mentions", f"{int(total_mentions):,}")

with col4:
    anomaly_count = con.execute(f"""
        SELECT COUNT(*) as count
        FROM mart_company_sentiment
        WHERE anomaly_flag = true
        AND company IN ({company_filter})
    """).df()['count'][0]
    st.metric("Anomalies Detected", int(anomaly_count))

st.divider()

# Section 2: Sentiment Leaderboard
st.header("Sentiment Leaderboard")

leaderboard = con.execute(f"""
    SELECT 
        company,
        ROUND(AVG(weighted_sentiment), 3) as avg_sentiment,
        SUM(mention_count) as total_mentions
    FROM mart_company_sentiment
    WHERE company IN ({company_filter})
    GROUP BY company
    ORDER BY avg_sentiment DESC
""").df()

leaderboard['company'] = leaderboard['company'].str.title()

fig_bar = px.bar(
    leaderboard,
    x='company',
    y='avg_sentiment',
    color='avg_sentiment',
    color_continuous_scale=['#ff4b4b', '#ffa500', '#00c875'],
    labels={'avg_sentiment': 'Sentiment Score', 'company': 'Company'},
    text='avg_sentiment'
)
fig_bar.update_traces(textposition='outside')
fig_bar.update_layout(
    plot_bgcolor='white',
    paper_bgcolor='white',
    font=dict(color='#1a1a2e', family='Inter, sans-serif', size=13),
    title_font=dict(color='#1a1a2e'),
    xaxis=dict(color='#1a1a2e', gridcolor='#f0f0f0', title='Company'),
    yaxis=dict(color='#1a1a2e', gridcolor='#f0f0f0', title='Sentiment Score'),
    coloraxis_showscale=False,
    showlegend=False,
    height=400
)
fig_bar.add_hline(y=0, line_dash="dash", line_color="#cccccc")
st.plotly_chart(fig_bar, use_container_width=True)

st.divider()

# Section 3: Sentiment Trend Over Time
st.header("Sentiment Trend Over Time")

df = con.execute(f"""
    SELECT * FROM mart_company_sentiment
    WHERE company IN ({company_filter})
    AND content_type IN {content_filter}
    ORDER BY post_date
""").df()

df['company'] = df['company'].str.title()

fig_line = px.line(
    df,
    x='post_date',
    y='weighted_sentiment',
    color='company',
    labels={'weighted_sentiment': 'Sentiment Score', 'post_date': 'Date', 'company': 'Company'}
)
fig_line.update_layout(
    plot_bgcolor='white',
    paper_bgcolor='white',
    font=dict(color='#1a1a2e', family='Inter, sans-serif', size=13),
    xaxis=dict(color='#1a1a2e', gridcolor='#f0f0f0'),
    yaxis=dict(color='#1a1a2e', gridcolor='#f0f0f0'),
    legend=dict(font=dict(color='#1a1a2e')),
    hovermode='x unified',
    height=450
)
fig_line.add_hline(y=0, line_dash="dash", line_color="#cccccc", annotation_text="Neutral")
st.plotly_chart(fig_line, use_container_width=True)

st.divider()

# Section 4: Mention Volume
st.header("Mention Volume")

volume = con.execute(f"""
    SELECT 
        date_trunc('week', post_date) as week,
        company,
        SUM(mention_count) as weekly_mentions
    FROM mart_company_sentiment
    WHERE company IN ({company_filter})
    GROUP BY date_trunc('week', post_date), company
    ORDER BY week
""").df()

volume['company'] = volume['company'].str.title()

fig_vol = px.bar(
    volume,
    x='week',
    y='weekly_mentions',
    color='company',
    labels={'weekly_mentions': 'Mentions', 'week': 'Week', 'company': 'Company'}
)
fig_vol.update_layout(
    plot_bgcolor='white',
    paper_bgcolor='white',
    font=dict(color='#1a1a2e', family='Inter, sans-serif', size=13),
    xaxis=dict(color='#1a1a2e', gridcolor='#f0f0f0'),
    yaxis=dict(color='#1a1a2e', gridcolor='#f0f0f0'),
    legend=dict(font=dict(color='#1a1a2e')),
    height=400
)
st.plotly_chart(fig_vol, use_container_width=True)

st.divider()

# Section 5: Most Viral Posts
st.header("Most Viral Posts This Week")

viral = con.execute(f"""
    SELECT 
        company,
        title,
        upvote_score,
        ROUND(upvote_velocity, 2) as velocity,
        sentiment_category,
        ROUND(sentiment_score, 3) as sentiment_score,
        CAST(created_utc AS DATE) as date
    FROM int_posts_enriched
    WHERE company IN ({company_filter})
    AND content_type = 'post'
    AND title != ''
    AND created_utc >= current_date - interval '7 days'
    ORDER BY upvote_velocity DESC
    LIMIT 10
""").df()

if len(viral) > 0:
    viral['company'] = viral['company'].str.title()
    st.dataframe(viral, use_container_width=True, hide_index=True)
else:
    st.info("No posts found in the last 7 days.")

st.divider()


# Section 6: Anomaly Alerts
st.header("Anomaly Alerts")

anomalies = con.execute(f"""
    SELECT 
        company,
        CAST(post_date AS DATE) as date,
        ROUND(weighted_sentiment, 3) as sentiment,
        ROUND(rolling_7day_sentiment, 3) as rolling_avg,
        mention_count
    FROM mart_company_sentiment
    WHERE anomaly_flag = true
    AND company IN ({company_filter})
    ORDER BY post_date DESC
    LIMIT 10
""").df()

if len(anomalies) > 0:
    anomalies['company'] = anomalies['company'].str.title()
    st.warning(f"Found {len(anomalies)} days where sentiment dropped significantly below the 7 day average")
    st.dataframe(anomalies, use_container_width=True, hide_index=True)
else:
    st.success("No anomalies detected in the selected time period")