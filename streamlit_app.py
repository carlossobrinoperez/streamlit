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
df = session.sql("""WITH ConsultasEsteMes AS (
  SELECT
    TO_CHAR(DATE_TRUNC('MONTH', END_TIME), 'MM/YYYY') AS MES_FORMATO,
    COUNT(*) AS NUMERO_DE_CONSULTAS_EXITOSAS
  FROM
    SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
  WHERE
    EXECUTION_STATUS = 'SUCCESS' -- Solo consultas exitosas
    AND DATE_TRUNC('MONTH', END_TIME) = DATE_TRUNC('MONTH', CURRENT_DATE()) -- Consultas del mes actual
  GROUP BY
    MES_FORMATO
),
ConsultasMesAnterior AS (
  SELECT
    TO_CHAR(DATE_TRUNC('MONTH', END_TIME), 'MM/YYYY') AS MES_FORMATO,
    COUNT(*) AS NUMERO_DE_CONSULTAS_EXITOSAS_MES_ANTERIOR
  FROM
    SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
  WHERE
    EXECUTION_STATUS = 'SUCCESS' -- Solo consultas exitosas
    AND DATE_TRUNC('MONTH', END_TIME) = DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE())) 
	GROUP BY MES_FORMATO)
SELECT
  MES_FORMATO,
  NUMERO_DE_CONSULTAS_EXITOSAS,
  NULL AS NUMERO_DE_CONSULTAS_EXITOSAS_MES_ANTERIOR
FROM
  ConsultasEsteMes
UNION ALL
SELECT
  MES_FORMATO,
  NULL AS NUMERO_DE_CONSULTAS_EXITOSAS,
  NUMERO_DE_CONSULTAS_EXITOSAS_MES_ANTERIOR
FROM
  ConsultasMesAnterior
ORDER BY
  MES_FORMATO DESC""")
st.dataframe(df)
st.metric(df.collect())
