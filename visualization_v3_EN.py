import streamlit as st
import plotly.express as px
import pandas as pd
from datetime import datetime
import logging
from langchain_query_EN import process_question
from data_processing import get_duckdb_connection
import os
import uuid

# Setup logging
log_dir = os.path.join(os.path.dirname(__file__), 'logs')
os.makedirs(log_dir, exist_ok=True)
timestamp = datetime.now().strftime('%Y%m%d_%H%M')
log_file = os.path.join(log_dir, f'visualization_log_{timestamp}.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ],
    force=True
)
logger = logging.getLogger(__name__)
logger.info(f"Initializing log at {log_file}")

# Configure the page
st.set_page_config(page_title="FOCUS.AI - Cloud Consumption Analysis Framework", layout="wide")

# CSS to style cards, charts, and chat
st.markdown(
    """
    <style>
    .response-text {
        font-family: 'Arial', sans-serif;
        font-size: 16px;
        color: #333333;
        margin-bottom: 10px;
    }
    .card {
        background-color: #ffffff;
        border-radius: 8px;
        border: 2px solid #DDDDDD;
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
        padding: 15px;
        margin-bottom: 20px;
        text-align: center;
        height: 95%;
    }
    .card-title {
        font-family: 'Arial', sans-serif;
        font-size: 16px;
        font-weight: bold;
        color: #333333;
        margin-bottom: 8px;
    }
    .card-value {
        font-family: 'Arial', sans-serif;
        font-size: 20px;
        color: #0078d4;
    }
    [data-testid="stChatMessage-user"] {
        display: flex !important;
        flex-direction: row-reverse !important;
        align-items: flex-start !important;
    }
    [data-testid="stChatMessage-user"] > div:first-child {
        margin-left: 10px !important;
        margin-right: 0 !important;
    }
    [data-testid="stChatMessage-user"] .stChatMessageContent {
        font-family: 'Arial', sans-serif !important;
        font-size: 16px !important;
        color: #333333 !important;
    }
    [data-testid="stChatMessage-assistant"] {
        display: flex !important;
        flex-direction: row !important;
        align-items: flex-start !important;
    }
    [data-testid="stChatMessage-assistant"] > div:first-child {
        margin-right: 10px !important;
        margin-left: 0 !important;
    }
    [data-testid="stChatMessage-assistant"] .stChatMessageContent {
        font-family: 'Arial', sans-serif !important;
        font-size: 16px !important;
        color: #333333 !important;
    }
    .plotly-chart {
        margin-bottom: 20px;
        border: 2px solid #DDDDDD;
        border-radius: 8px;
        padding: 10px;
    }
    </style>
    """,
    unsafe_allow_html=True
)

# Function to fetch summary metrics
@st.cache_data
def fetch_summary_metrics():
    con = get_duckdb_connection()
    try:
        query = """
        SELECT
            SUM(BilledCost) AS total_billed_cost,
            COUNT(DISTINCT ProviderName) AS provider_count,
            SUM(CASE WHEN ProviderName = 'AWS' THEN BilledCost ELSE 0 END) AS aws_cost,
            SUM(CASE WHEN ProviderName = 'Microsoft' THEN BilledCost ELSE 0 END) AS azure_cost,
            SUM(CASE WHEN ProviderName = 'Oracle' THEN BilledCost ELSE 0 END) AS oracle_cost
        FROM consolidated_billing
        """
        result = con.execute(query).fetchone()
        total_billed_cost = result[0] if result[0] is not None else 0.0
        provider_count = result[1] if result[1] is not None else 0
        aws_cost = result[2] if result[2] is not None else 0.0
        azure_cost = result[3] if result[3] is not None else 0.0
        oracle_cost = result[4] if result[4] is not None else 0.0
        logger.info(f"Summary metrics: Total=${total_billed_cost:,.2f}, Cloud Providers={provider_count}, "
                    f"AWS=${aws_cost:,.2f}, Azure=${azure_cost:,.2f}, Oracle=${oracle_cost:,.2f}")
        return total_billed_cost, provider_count, aws_cost, azure_cost, oracle_cost
    except Exception as e:
        logger.error(f"Error fetching summary metrics: {str(e)}")
        return 0.0, 0, 0.0, 0.0, 0.0
    finally:
        con.close()

# Function to fetch data for bar and pie charts
@st.cache_data
def fetch_dashboard_data():
    con = get_duckdb_connection()
    try:
        # Query for Top 10 ServiceCategory
        query_service_category = """
        SELECT
            ServiceCategory,
            SUM(BilledCost) AS total_cost
        FROM consolidated_billing
        GROUP BY ServiceCategory
        ORDER BY total_cost DESC
        LIMIT 10
        """
        df_service_category = pd.DataFrame(
            con.execute(query_service_category).fetchall(),
            columns=["ServiceCategory", "total_cost"]
        )
        
        # Query for other charts
        query_others = """
        SELECT
            tag_application,
            tag_environment,
            tag_business_unit,
            ProviderName,
            SUM(BilledCost) AS total_cost
        FROM consolidated_billing
        GROUP BY tag_application, tag_environment, tag_business_unit, ProviderName
        """
        df_others = pd.DataFrame(
            con.execute(query_others).fetchall(),
            columns=["tag_application", "tag_environment", "tag_business_unit", "ProviderName", "total_cost"]
        )
        return df_service_category, df_others
    except Exception as e:
        logger.error(f"Error executing consolidated query: {str(e)}")
        return pd.DataFrame(), pd.DataFrame()
    finally:
        con.close()

# Function to fetch data for the Treemap chart
@st.cache_data
def fetch_treemap_data():
    con = get_duckdb_connection()
    try:
        query_treemap = """
        SELECT
            ServiceCategory,
            ServiceName,
            ResourceID,
            SUM(BilledCost) AS total_cost
        FROM consolidated_billing
        WHERE ServiceCategory IS NOT NULL AND ServiceName IS NOT NULL AND ResourceID IS NOT NULL
        GROUP BY ServiceCategory, ServiceName, ResourceID
        HAVING SUM(BilledCost) > 0
        """
        df_treemap = pd.DataFrame(
            con.execute(query_treemap).fetchall(),
            columns=["ServiceCategory", "ServiceName", "ResourceID", "total_cost"]
        )
        return df_treemap
    except Exception as e:
        logger.error(f"Error fetching data for treemap: {str(e)}")
        return pd.DataFrame()
    finally:
        con.close()

# Function to get unique ProviderName values (for debugging)
@st.cache_data
def load_providers():
    con = get_duckdb_connection()
    try:
        provider_query = "SELECT DISTINCT ProviderName FROM consolidated_billing WHERE ProviderName IS NOT NULL ORDER BY ProviderName"
        providers = [row[0] for row in con.execute(provider_query).fetchall()]
        logger.info(f"Unique ProviderName values: {providers}")
        return providers
    except Exception as e:
        logger.error(f"Error loading providers: {str(e)}")
        return []
    finally:
        con.close()

# Load providers for debugging
load_providers()

# Initialize session_state for the chatbot
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []
if 'processing_message_id' not in st.session_state:
    st.session_state.processing_message_id = None

# Main layout defined at the beginning to align the chatbot to the top
col1, col2 = st.columns([7, 3])

# Column 1: Header, Cards, and Charts
with col1:
    # Header with logo and title
    col_logo, col_title = st.columns([1, 4])
    with col_logo:
        logo_path = "logo.png"
        if os.path.exists(logo_path):
            st.image(logo_path, width=120)
        else:
            st.image("https://via.placeholder.com/120", width=120)
    with col_title:
        st.markdown("<h1 style='margin-top: 25px; font-size: 24px;'>FOCUS.AI - Cloud Consumption Analysis Framework</h1>", unsafe_allow_html=True)

    # Cards for summary metrics
    total_billed_cost, provider_count, aws_cost, azure_cost, oracle_cost = fetch_summary_metrics()
    card_col1, card_col2, card_col3, card_col4, card_col5 = st.columns(5)
    cards_data = {
        "Total Billed Cost": f"${total_billed_cost:,.2f}",
        "Cloud Providers": provider_count,
        "AWS": f"${aws_cost:,.2f}",
        "Azure": f"${azure_cost:,.2f}",
        "Oracle": f"${oracle_cost:,.2f}"
    }
    card_cols = [card_col1, card_col2, card_col3, card_col4, card_col5]
    for i, (title, value) in enumerate(cards_data.items()):
        with card_cols[i]:
            st.markdown(
                f"""
                <div class="card">
                    <div class="card-title">{title}</div>
                    <div class="card-value">{value}</div>
                </div>
                """,
                unsafe_allow_html=True
            )

    # Load data for charts
    df_service_category, df_others = fetch_dashboard_data()
    df_treemap = fetch_treemap_data()
    
    st.markdown("---") # Visual divider
    st.markdown("📊 Visualizations")

    if df_others is not None and not df_others.empty and df_service_category is not None and not df_service_category.empty:
        
        # Columns for the bar charts
        bar_col1, bar_col2 = st.columns(2)

        with bar_col1:
            st.markdown("Costs by Provider")
            df_provider = df_others.groupby("ProviderName")["total_cost"].sum().reset_index().sort_values("total_cost", ascending=True)
            if not df_provider.empty:
                fig_provider = px.bar(df_provider, y="ProviderName", x="total_cost", orientation='h',
                                      labels={"total_cost": "Total Cost (USD)", "ProviderName": "Provider"},
                                      text=[f"${val:,.2f}" for val in df_provider["total_cost"]])
                fig_provider.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)
                fig_provider.update_layout(margin=dict(t=30, l=10, r=10, b=10), showlegend=False, xaxis_title="Cost (USD)", yaxis_title="Provider")
                st.plotly_chart(fig_provider, use_container_width=True)
            else:
                st.warning("No data available for providers.")

        with bar_col2:
            st.markdown("Costs by Service Category (Top 10)")
            if not df_service_category.empty:
                fig_category = px.bar(df_service_category, x="ServiceCategory", y="total_cost",
                                      labels={"total_cost": "Total Cost (USD)", "ServiceCategory": "Category"},
                                      text=[f"${val:,.2f}" for val in df_service_category["total_cost"]])
                fig_category.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)
                fig_category.update_layout(margin=dict(t=30, l=10, r=10, b=10), showlegend=False, xaxis_title="Service Category", yaxis_title="Cost (USD)", xaxis_tickangle=45)
                st.plotly_chart(fig_category, use_container_width=True)
            else:
                st.warning("No data available for service categories.")
        
        st.markdown("---") # Visual divider

        # Pie charts side-by-side (3 columns)
        pie_col1, pie_col2, pie_col3 = st.columns(3)

        with pie_col1:
            st.markdown("<h6>Costs by Application (Top 10)</h6>", unsafe_allow_html=True)
            df_app_full = df_others.groupby("tag_application")["total_cost"].sum().reset_index()
            df_app = df_app_full.sort_values("total_cost", ascending=False).head(10) # Filter Top 10
            if not df_app.empty and df_app['tag_application'].notna().any():
                fig_app = px.pie(df_app.dropna(subset=['tag_application']), names="tag_application", values="total_cost", hole=0.4,
                                 labels={"total_cost": "Cost (USD)", "tag_application": "Application"})
                fig_app.update_traces(textinfo='none', hovertemplate="%{label}: $%{value:,.2f}<extra></extra>")
                fig_app.update_layout(
                    margin=dict(t=25, b=0, l=0, r=0),
                    showlegend=True,
                    legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5)
                )
                st.plotly_chart(fig_app, use_container_width=True)
            else:
                st.warning("No data for applications.")

        with pie_col2:
            st.markdown("<h6>Costs by Environment</h6>", unsafe_allow_html=True)
            df_env = df_others.groupby("tag_environment")["total_cost"].sum().reset_index()
            if not df_env.empty and df_env['tag_environment'].notna().any():
                fig_env = px.pie(df_env.dropna(subset=['tag_environment']), names="tag_environment", values="total_cost", hole=0.4,
                                 labels={"total_cost": "Cost (USD)", "tag_environment": "Environment"})
                fig_env.update_traces(textinfo='none', hovertemplate="%{label}: $%{value:,.2f}<extra></extra>")
                fig_env.update_layout(
                    margin=dict(t=25, b=0, l=0, r=0),
                    showlegend=True,
                    legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5)
                )
                st.plotly_chart(fig_env, use_container_width=True)
            else:
                st.warning("No data for environments.")
        
        with pie_col3:
            st.markdown("<h6>Costs by Business Unit (Top 10)</h6>", unsafe_allow_html=True)
            df_bu_full = df_others.groupby("tag_business_unit")["total_cost"].sum().reset_index()
            df_bu = df_bu_full.sort_values("total_cost", ascending=False).head(10) # Filter Top 10
            if not df_bu.empty and df_bu['tag_business_unit'].notna().any():
                fig_bu = px.pie(df_bu.dropna(subset=['tag_business_unit']), names="tag_business_unit", values="total_cost", hole=0.4,
                                 labels={"total_cost": "Cost (USD)", "tag_business_unit": "Business Unit"})
                fig_bu.update_traces(textinfo='none', hovertemplate="%{label}: $%{value:,.2f}<extra></extra>")
                fig_bu.update_layout(
                    margin=dict(t=25, b=0, l=0, r=0),
                    showlegend=True,
                    legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5)
                )
                st.plotly_chart(fig_bu, use_container_width=True)
            else:
                st.warning("No data for Business Unit.")

        st.markdown("---") # Visual divider
        
        # Treemap chart for Cost Breakdown
        st.markdown("Cost Breakdown by Service and Resource")
        if not df_treemap.empty:
            fig_treemap = px.treemap(
                df_treemap,
                path=[px.Constant("Total Cost"), 'ServiceCategory', 'ServiceName', 'ResourceID'],
                values='total_cost',
                labels={'total_cost': 'Total Cost (USD)'},
                hover_data={'total_cost': ':.2f'}
            )
            fig_treemap.update_layout(margin=dict(t=30, l=10, r=10, b=10))
            fig_treemap.update_traces(textinfo="label+value", hovertemplate='<b>%{label}</b><br>Cost: $%{value:,.2f}')
            st.plotly_chart(fig_treemap, use_container_width=True)
        else:
            st.warning("No data available for the breakdown chart.")

    else:
        st.error("No data available. Please check the dataset.")

# Column 2: Chatbot
with col2:
    st.markdown("❓ **Chatbot**")
    st.markdown("---") # Visual divider

    # Container for chat history to allow scrolling
    chat_container = st.container()
    with chat_container:
        for message in st.session_state.chat_history:
            with st.chat_message(message["role"]):
                st.markdown(f'<div class="response-text">{message["content"]}</div>', unsafe_allow_html=True)
    
    if question := st.chat_input("Enter your question"):
        st.session_state.chat_history.append({"role": "user", "content": question})
        logger.info(f"Question added to history: {question}")
        processing_id = str(uuid.uuid4())
        st.session_state.processing_message_id = processing_id
        st.session_state.chat_history.append({
            "role": "assistant",
            "content": "Processing...",
            "message_id": processing_id
        })
        st.rerun()
    
    if st.session_state.processing_message_id:
        last_message = st.session_state.chat_history[-1]
        if last_message.get("message_id") == st.session_state.processing_message_id:
            question = st.session_state.chat_history[-2]["content"]
            with st.spinner("Processing..."):
                try:
                    sql_query, response = process_question(question)
                    st.session_state.chat_history[-1] = {
                        "role": "assistant",
                        "content": response
                    }
                    logger.info(f"Response added to history: {response}")
                    logger.info(f"SQL query generated for '{question}': {sql_query}")
                except Exception as e:
                    error_message = f"Error processing the question: {str(e)}"
                    st.session_state.chat_history[-1] = {
                        "role": "assistant",
                        "content": error_message
                    }
                    logger.error(f"Error processing question '{question}': {str(e)}")
                st.session_state.processing_message_id = None
                st.rerun()
