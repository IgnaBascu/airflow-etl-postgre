# Imports

from airflow import DAG
from datetime import timedelta,datetime
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# Argumentos Default

default_args = {
    'owner':'Megunino',
    'depend_on_past':False,
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay': timedelta(minutes=2)
}

# Funciones

def _get_api():
    import requests
    url = 'https://my.api.mockaroo.com/sales_schema.json'
    headers = {'X-API-Key': '50917c00'}
    response = requests.get(url, headers = headers)
    with open('/tmp/sales_db_python.csv','wb') as file:
        file.write(response.content)
        file.close()

def _join_csv():
    '''Une ambos csv y crea uno solo con las columnas date, store y 
    la suma de cada una'''
    import pandas as pd

    df_py = pd.read_csv('/tmp/sales_db_python.csv')
    df_bash = pd.read_csv('/tmp/sales_db_bash.csv')
    df = pd.concat([df_py, df_bash], ignore_index=True)

    # ⚠️ Primero convertir fecha antes de agrupar
    df['date'] = pd.to_datetime(df['date'], format='%m/%d/%Y')
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')

    df = df.groupby(['date', 'store'])['sales'].sum().reset_index()
    df = df.rename(columns={'date': 'ddate'})  # evita conflicto con palabra reservada
    df.to_csv('/tmp/sales_db.csv', sep='\t', index=False, header=False)

def _load_data():
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    pd_hook = PostgresHook(postgres_conn_id = 'postgres_conn_local')
    pd_hook.bulk_load(table ='sales_db', tmp_file = '/tmp/sales_db.csv')

# Armado DAG

with DAG(
    'DAG_ETL_PostgreSQL',
    default_args = default_args,
    description = 'Creación de DAG ETL PostgreSQL con API Mockaro',
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["ETL",'Ingeniería','PostgreSQL'],
) as dag:
    
    # Extracción
    get_api_python = PythonOperator(
        task_id = 'get_api_python',
        python_callable = _get_api
    )

    get_api_bash = BashOperator(
        task_id = 'get_api_bash',
        bash_command = r'curl -H "X-API-Key: 50917c00" https://my.api.mockaroo.com/sales_schema.json > /tmp/sales_db_bash.csv'
    )

    # Transformación
    join_csv = PythonOperator(
        task_id = 'join_csv',
        python_callable= _join_csv
    )

    # Carga

    check_table = SQLExecuteQueryOperator(
        task_id="check_table",
        conn_id="postgres_conn_local",
        sql="sql/create_table.sql"
    )

    load_table = PythonOperator(
        task_id = 'load_table',
        python_callable= _load_data
    )

    # Orden

    [get_api_python,get_api_bash] >> join_csv >> check_table >> load_table



