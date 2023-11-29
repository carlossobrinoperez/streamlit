import streamlit as st
import snowflake.connector
from snowflake.snowpark import Session


st.markdown("# Credit_consumption.")
st.sidebar.markdown("# Credit_consumption.")

connection_parameters = {
"account": "vpdyjth-ft62604",
"user": "Carlos",
"password": "Temp1234",
 }  

session = Session.builder.configs(connection_parameters).create()


df_sql = snowpark.session.sql("select ROUND(sum(credits_used)) from snowflake.account_usage.metering_history")
df_sql.show()
