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


#df_queries_exitosas
df_queries_exitosas = session.sql("""WITH ConsultasEsteMes AS (
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
  MES_FORMATO DESC""").collect()


#df_queries_Fallidas
df_queries_fallidas = session.sql("""WITH ConsultasEsteMes AS (
  SELECT
    TO_CHAR(DATE_TRUNC('MONTH', END_TIME), 'MM/YYYY') AS MES_FORMATO,
    COUNT(*) AS NUMERO_DE_CONSULTAS_FALLIDAS
  FROM
    ACCOUNT_USAGE.QUERY_HISTORY
  WHERE
    EXECUTION_STATUS <> 'SUCCESS' -- Solo consultas fallidas
    AND DATE_TRUNC('MONTH', END_TIME) = DATE_TRUNC('MONTH', CURRENT_DATE()) -- Consultas del mes actual
  GROUP BY
    MES_FORMATO
),
ConsultasMesAnterior AS (
  SELECT
    TO_CHAR(DATE_TRUNC('MONTH', END_TIME), 'MM/YYYY') AS MES_FORMATO,
    COUNT(*) AS NUMERO_DE_CONSULTAS_FALLIDAS_MES_ANTERIOR
  FROM
    ACCOUNT_USAGE.QUERY_HISTORY
  WHERE
    EXECUTION_STATUS <> 'SUCCESS' -- Solo consultas fallidas
    AND DATE_TRUNC('MONTH', END_TIME) = DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE())) -- Consultas del mes anterior
  GROUP BY
    MES_FORMATO
)
SELECT
  MES_FORMATO,
  NUMERO_DE_CONSULTAS_FALLIDAS,
  NULL AS NUMERO_DE_CONSULTAS_FALLIDAS_MES_ANTERIOR
FROM
  ConsultasEsteMes
UNION ALL
SELECT
  MES_FORMATO,
  NULL AS NUMERO_DE_CONSULTAS_FALLIDAS,
  NUMERO_DE_CONSULTAS_FALLIDAS_MES_ANTERIOR
FROM
  ConsultasMesAnterior
ORDER BY
  MES_FORMATO DESC
""").collect()



#df_queries
df_queries = session.sql("""SELECT
  TO_CHAR(DATE_TRUNC('MONTH', END_TIME), 'MM/YYYY') AS MES_FORMATO,
  COUNT(*) AS TOTAL_CONSULTAS
FROM
  ACCOUNT_USAGE.QUERY_HISTORY
GROUP BY
  MES_FORMATO
ORDER BY
  MES_FORMATO DESC;


  WITH ConsultasEsteMes AS (
  SELECT
    TO_CHAR(DATE_TRUNC('MONTH', END_TIME), 'MM/YYYY') AS MES_FORMATO,
    COUNT(*) AS NUMERO_DE_CONSULTAS
  FROM
    ACCOUNT_USAGE.QUERY_HISTORY
  WHERE
  DATE_TRUNC('MONTH', END_TIME) = DATE_TRUNC('MONTH', CURRENT_DATE()) -- Consultas del mes actual
  GROUP BY
    MES_FORMATO
),
ConsultasMesAnterior AS (
  SELECT
    TO_CHAR(DATE_TRUNC('MONTH', END_TIME), 'MM/YYYY') AS MES_FORMATO,
    COUNT(*) AS NUMERO_DE_CONSULTAS_MES_ANTERIOR
  FROM
    ACCOUNT_USAGE.QUERY_HISTORY
  WHERE
  DATE_TRUNC('MONTH', END_TIME) = DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE())) -- Consultas del mes anterior
  GROUP BY
    MES_FORMATO
)
SELECT
  MES_FORMATO,
  NUMERO_DE_CONSULTAS,
  NULL AS NUMERO_DE_CONSULTAS_MES_ANTERIOR
FROM
  ConsultasEsteMes
UNION ALL
SELECT
  MES_FORMATO,
  NULL AS NUMERO_DE_CONSULTAS,
  NUMERO_DE_CONSULTAS_MES_ANTERIOR
FROM
  ConsultasMesAnterior
ORDER BY
  MES_FORMATO DESC;
""").collect()

col1, col2, col3 = st.columns(3)
col2.metric("Numero de Queries exitosas ultimo mes",df_queries_exitosas[0][1],df_queries_exitosas[0][2])
col3.metric("Numero de Queries fallidas ultimo mes",df_queries_fallidas[0][1],df_queries_fallidas[0][2])
col1.metric("Numero de Queries total ultimo mes",df_queries[0][1],df_queries[0][2])
