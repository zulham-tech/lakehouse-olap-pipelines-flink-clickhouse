"""
Airflow DAG — Project 9: IMF WEO ELT Pipeline (DuckDB + BigQuery)
Schedule  : Daily at 04:00 WIB (21:00 UTC)
Author    : Ahmad Zulham Hamdan
"""
from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    'owner': 'zulham-hamdan', 'depends_on_past': False,
    'start_date': datetime(2024, 1, 1), 'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(minutes=30),
}

S3_BUCKET   = Variable.get('IMF_S3_BUCKET', default_var='your-imf-raw-bucket')
GCP_PROJECT = Variable.get('GCP_PROJECT',   default_var='your-gcp-project')

IMF_INDICATORS = {
    'NGDP_RPCH':'gdp_real_growth_pct','PCPIPCH':'inflation_avg_pct',
    'LUR':'unemployment_rate_pct','BCA_NGDPDP':'current_account_pct_gdp',
    'NGDPDPC':'gdp_per_capita_usd','GGXWDG_NGDP':'gross_debt_pct_gdp',
}

def validate_imf_api(**context):
    import requests
    resp = requests.get(
        'https://www.imf.org/external/datamapper/api/v1/NGDP_RPCH',
        timeout=20, headers={'Accept':'application/json'}
    )
    resp.raise_for_status()
    count = sum(len(v) for v in resp.json().get('values',{}).get('NGDP_RPCH',{}).values())
    logger.info(f'✅ IMF API OK — GDP growth data points: {count}')

def extract_to_s3(**context):
    import requests, pandas as pd, boto3
    execution_date = context['ds']
    s3 = boto3.client('s3')
    all_records = []
    for code, name in IMF_INDICATORS.items():
        resp = requests.get(
            f'https://www.imf.org/external/datamapper/api/v1/{code}',
            timeout=30, headers={'Accept':'application/json'}
        )
        resp.raise_for_status()
        values = resp.json().get('values',{}).get(code,{})
        for country, years in values.items():
            for year, value in years.items():
                if value is None: continue
                try:
                    all_records.append({
                        'country_code': country.upper(),
                        'indicator_code': code, 'indicator_name': name,
                        'year': int(year), 'value': float(value),
                        'is_forecast': int(year) > datetime.now().year - 1,
                        'batch_date': execution_date,
                        'ingested_at': datetime.utcnow().isoformat(),
                        'source': 'imf-datamapper-api'
                    })
                except: pass
    df = pd.DataFrame(all_records)
    s3_key = f'imf/raw/indicators/dt={execution_date}/weo_indicators.parquet'
    s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=df.to_parquet(index=False))
    logger.info(f'✅ Extracted {len(df)} IMF records → S3')
    context['ti'].xcom_push(key='extracted_rows', value=len(df))

def transform_with_duckdb(**context):
    import duckdb, pandas as pd
    execution_date = context['ds']
    con = duckdb.connect('/tmp/imf_pipeline.duckdb')
    con.execute('INSTALL httpfs; LOAD httpfs;')
    con.execute("SET s3_region='ap-southeast-1';")
    con.execute(f"""
        CREATE OR REPLACE VIEW raw_indicators AS
        SELECT * FROM read_parquet('s3://{S3_BUCKET}/imf/raw/indicators/dt={execution_date}/*.parquet')
    """)
    result_df = con.execute("""
        SELECT country_code, indicator_name, year, value, is_forecast, batch_date
        FROM raw_indicators
        WHERE year BETWEEN 2000 AND 2030
    """).df()
    pivot_df = result_df.pivot_table(
        index=['country_code','year','is_forecast','batch_date'],
        columns='indicator_name', values='value', aggfunc='first'
    ).reset_index()
    pivot_df.columns = [c if not isinstance(c,str) else c for c in pivot_df.columns]
    con.close()
    pivot_df.to_parquet('/tmp/imf_transformed.parquet', index=False)
    logger.info(f'✅ DuckDB transform: {len(pivot_df)} rows')
    context['ti'].xcom_push(key='transformed_rows', value=len(pivot_df))

def load_to_bigquery(**context):
    import pandas as pd
    from google.cloud import bigquery
    execution_date = context['ds']
    df = pd.read_parquet('/tmp/imf_transformed.parquet')
    bq = bigquery.Client(project=GCP_PROJECT)
    table_id = f'{GCP_PROJECT}.imf_economic_analytics.weo_enriched'
    bq.query(f"DELETE FROM `{table_id}` WHERE batch_date = '{execution_date}'").result()
    job = bq.load_table_from_dataframe(
        df, table_id,
        job_config=bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
    )
    job.result()
    logger.info(f'✅ Loaded {len(df)} rows → BigQuery')

with DAG(
    dag_id='imf_duckdb_bigquery_elt',
    description='Daily IMF WEO ELT: API → S3 (raw) → DuckDB (transform) → BigQuery',
    default_args=DEFAULT_ARGS,
    schedule_interval='0 21 * * *',
    catchup=False, max_active_runs=1,
    tags=['batch','imf','duckdb','bigquery','elt','project9'],
) as dag:
    start     = EmptyOperator(task_id='start')
    validate  = PythonOperator(task_id='validate_imf_api', python_callable=validate_imf_api)
    extract   = PythonOperator(task_id='extract_to_s3',    python_callable=extract_to_s3)
    transform = PythonOperator(task_id='duckdb_transform',  python_callable=transform_with_duckdb)
    load      = PythonOperator(task_id='load_to_bigquery',  python_callable=load_to_bigquery)
    end       = EmptyOperator(task_id='end')
    start >> validate >> extract >> transform >> load >> end
