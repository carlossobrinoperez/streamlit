from snowflake.snowpark import Session
import streamlit as st

st.markdown("# Main page")
st.sidebar.markdown("# Main page")

def create_session():
    connection_parameters = {
        "account": "vpdyjth-ft62604",
        "user": "Carlos",
        "password": "Temp1234"}
    return Session.builder.configs(connection_parameters).create()
  
session = create_session()
st.success("Connected to Snowflake!")

df = session.sql("select user_name, count(*) as Queries  from snowflake.account_usage.query_history;")

for row in df.itertuples():
    st.write(f"{row.user_name} ha realizado{row.Queries} queries")

