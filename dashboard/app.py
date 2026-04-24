import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.graph_objects as go
import os

st.set_page_config(page_title="Crypto Data Warehouse", layout="wide")
st.title("📈 Crypto Real-time Analytics")

engine = create_engine(os.getenv('DB_URL'))


@st.cache_data(ttl=60) 
def get_data():
    try:
        query = "SELECT * FROM public.fct_crypto_trends ORDER BY event_time DESC"
        return pd.read_sql(query, engine)
    except Exception:
        return pd.DataFrame()

df = get_data()

if not df.empty:
    symbol = st.sidebar.selectbox("Обери монету:", df['symbol'].unique())
    filtered_df = df[df['symbol'] == symbol].sort_values('event_time') 

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=filtered_df['event_time'],
        y=filtered_df['price'],
        mode='lines',
        fill='tozeroy', 
        name='Price (USD)',
        line={'color': '#00ffcc', 'width': 3}
    ))

    fig.update_layout(
        title=f"Ціновий тренд для {symbol}",
        xaxis_title="Час події",
        yaxis_title="Ціна (USD)",
        template="plotly_dark",
        hovermode="x unified"
    )

    st.plotly_chart(fig, use_container_width=True)

    # Вивід таблиці під графіком
    st.subheader("Останні 10 записів")
    st.dataframe(filtered_df.tail(10), use_container_width=True)
else:
    st.warning("⚠️ Дані ще не завантажені або таблиця public.fct_crypto_trends не існує. Запусти пайплайн в Airflow та dbt!")
