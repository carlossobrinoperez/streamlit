# Import libraries
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import sum, col
import altair as alt
import streamlit as st

st.markdown("# Credit_consumption.")
st.sidebar.markdown("# Credit_consumption.")

conn = st.connection("snowflake")

df = conn.query("select user_name, count(*) as Queries  from snowflake.account_usage.query_history;", ttl=600)

st.write(df)

