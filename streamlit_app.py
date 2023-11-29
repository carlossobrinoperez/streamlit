from snowflake.snowpark import Session
import streamlit as st

st.markdown("# Main page")
st.sidebar.markdown("# Main page")

def create_session():
    connection_parameters = st.secrets["snowflake"]
    return Session.builder.configs(connection_parameters).create()
  
session = create_session()
st.success("Connected to Snowflake!")

