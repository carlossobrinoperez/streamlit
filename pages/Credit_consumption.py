import streamlit as st
from snowflake.snowpark import Session

st.title('❄️ How to connect Streamlit to a Snowflake database')

# Establish Snowflake session
@st.cache_resource
def create_session():
    return Session.builder.configs(st.secrets.snowflake).create()

session = create_session()
st.success("Connected to Snowflake!")

df = conn.query("select user_name, count(*) as Queries  from snowflake.account_usage.query_history;", ttl=600)

st.write(df)

