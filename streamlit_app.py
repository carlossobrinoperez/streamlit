from snowflake.snowpark import Session
import streamlit as st

st.markdown("# Main page")
st.sidebar.markdown("# Main page")

def create_session():
    return Session.builder.configs(st.secrets.snowflake).create()
  
session = create_session()
st.success("Connected to Snowflake!")

