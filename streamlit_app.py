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
    SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
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
    SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
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
df_queries = session.sql("""
  WITH ConsultasEsteMes AS (
  SELECT
    TO_CHAR(DATE_TRUNC('MONTH', END_TIME), 'MM/YYYY') AS MES_FORMATO,
    COUNT(*) AS NUMERO_DE_CONSULTAS
  FROM
    SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
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
    SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
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
col2.metric("Numero de Queries exitosas",df_queries_exitosas[0][1],df_queries_exitosas[0][2])
col3.metric("Numero de Queries fallidas",df_queries_fallidas[0][1],df_queries_fallidas[0][2])
col1.metric("Numero de Queries total",df_queries[0][1],df_queries[0][2])


#Numero de consultas ejecutadas por WH en el ultimo mes
df_wh_q = session.sql("""
  SELECT
  WAREHOUSE_NAME,
  COUNT(*) AS NUMERO_DE_CONSULTAS
FROM
  SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE
  END_TIME >= DATE_TRUNC('MONTH', CURRENT_DATE()) - INTERVAL '1 MONTH' AND WAREHOUSE_NAME IS NOT NULL -- Filtra por el último mes
GROUP BY
  WAREHOUSE_NAME
ORDER BY
  NUMERO_DE_CONSULTAS DESC
""")

#Numero de consultas ejecutadas por WH en el ultimo mes
df_db_q = session.sql("""SELECT
  DATABASE_NAME,
  COUNT(*) AS NUMERO_DE_CONSULTAS
FROM
  SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE
  END_TIME >= DATE_TRUNC('MONTH', CURRENT_DATE()) - INTERVAL '1 MONTH' AND DATABASE_NAME IS NOT NULL -- Filtra por el último mes
GROUP BY
  DATABASE_NAME
ORDER BY
  NUMERO_DE_CONSULTAS DESC
""")

col1, col2= st.columns(2)
col1.write("Numero de consultas ejecutadas por WH")
col1.bar_chart(df_wh_q,x= "WAREHOUSE_NAME" ,y= "NUMERO_DE_CONSULTAS")
col2.write("Numero de consultas ejecutadas por DB")
col2.bar_chart(df_db_q,x= "DATABASE_NAME" ,y= "NUMERO_DE_CONSULTAS")

#Total de consultas ejecutadas el ultimo mes

df_queries = session.sql("""SELECT
  TO_CHAR(DATE_TRUNC('MONTH', END_TIME), 'MM/YYYY') AS MES_FORMATO,
  QUERY_TYPE,
  COUNT(*) AS NUMERO_DE_CONSULTAS
FROM
  SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE
  END_TIME >= DATE_TRUNC('MONTH', CURRENT_DATE()) -- Filtra por el mes actual
GROUP BY
  MES_FORMATO,
  QUERY_TYPE
  HAVING
  COUNT(*) > 10
ORDER BY
  MES_FORMATO DESC, QUERY_TYPE
""")

st.write("Total de consultas ejecutadas el ultimo mes")
st.bar_chart(df_queries, y= "NUMERO_DE_CONSULTAS" , x= "QUERY_TYPE" )


# Duracion media de queries por mes (En segundos)

df_q_sec = session.sql("""SELECT DATE(END_TIME) AS DAY_FORMATO,
round(AVG(EXECUTION_TIME)) AS DURACION_MEDIA_MILISEGUNDOS
FROM
  SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
GROUP BY
  DAY_FORMATO
ORDER BY
  DAY_FORMATO DESC
""")

#st.dataframe(df_q_sec)

st.write("Duracion media de queries por dia (En milisegundos)")
st.line_chart(df_q_sec, y= "DURACION_MEDIA_MILISEGUNDOS" , x="DAY_FORMATO" )


# Consultas falladas y con que frecuencia en el ultimo mes
df_q_fail_frec = session.sql(""" SELECT
  TO_CHAR(DATE_TRUNC('MONTH', END_TIME), 'MM/YYYY') AS MES_FORMATO,
  QUERY_TEXT,
  COUNT(*) AS FRECUENCIA_DE_FALLO
FROM
  SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE
  END_TIME >= DATE_TRUNC('MONTH', CURRENT_DATE()) - INTERVAL '1 MONTH' -- Filtra por el último mes
  AND EXECUTION_STATUS <> 'SUCCESS' -- Filtra por consultas fallidas
GROUP BY
  MES_FORMATO,
  QUERY_TEXT
ORDER BY
  MES_FORMATO DESC, FRECUENCIA_DE_FALLO DESC
  LIMIT 10 """)

# Media de tiempo de ejecución de consultas por usuario (En minutos)
df_q_user = session.sql(""" select
    user_name,
    round((avg(execution_time)) / 60) as AVERAGE_EXECUTION_TIME
from SNOWFLAKE.account_usage.query_history
group by 1
order by 2 desc
""")


st.write("Consultas falladas y con que frecuencia en el ultimo mes")
st.bar_chart(df_q_fail_frec,x= "QUERY_TEXT" ,y= "FRECUENCIA_DE_FALLO")

col1, col2= st.columns(2)
col1.write("Numero de consultas ejecutadas por DB")
col1.line_chart(df_q_user,x= "USER_NAME" ,y= "AVERAGE_EXECUTION_TIME")

