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

st.title("Top 10 queris lanzadas por Users")
df = session.sql("select user_name, count(*) as Queries from snowflake.account_usage.query_history group by 1 order by Queries desc limit 10")
st.bar_chart(df, x="USER_NAME", y= "QUERIES")

