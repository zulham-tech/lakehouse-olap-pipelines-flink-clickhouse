"""
Airflow DAG — Project 10: Wikimedia Trending Pipeline (Flink + Cassandra + Snowflake)
Two DAGs: speed (hourly SSE) + batch (daily pageviews)
Author    : Ahmad Zulham Hamdan
"""
from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    'owner': 'zulham-hamdan', 'depends_on_past': False,
    'start_date': datetime(2024, 1, 1), 'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

KAFKA_BOOTSTRAP = Variable.get('KAFKA_BOOTSTRAP', default_var='localhost:9092')
CASSANDRA_HOSTS = Variable.get('CASSANDRA_HOSTS', default_var='localhost')
SNOWFLAKE_ACCOUNT = Variable.get('SNOWFLAKE_ACCOUNT', default_var='your_account')
SNOWFLAKE_USER    = Variable.get('SNOWFLAKE_USER',    default_var='your_user')
SNOWFLAKE_PASS    = Variable.get('SNOWFLAKE_PASSWORD', default_var='')
LANGUAGES = ['en','id','de','fr','ja','es','pt','ru','zh','ar']
WM_USER_AGENT = 'DataEngineeringPortfolio/1.0 (zulham.va@gmail.com)'

def fetch_top_articles_kafka(**context):
    import requests, json, time
    from kafka import KafkaProducer
    from datetime import timezone
    execution_date = context['ds']
    dt = datetime.strptime(execution_date,'%Y-%m-%d')
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
        acks='all', compression_type='gzip'
    )
    total = 0
    for lang in LANGUAGES:
        try:
            url = f'https://wikimedia.org/api/rest_v1/metrics/pageviews/top/{lang}.wikipedia/all-access/{dt.year}/{dt.month:02d}/{dt.day:02d}'
            resp = requests.get(url, headers={'User-Agent': WM_USER_AGENT}, timeout=20)
            if resp.status_code == 404: continue
            resp.raise_for_status()
            articles = resp.json().get('items',[{}])[0].get('articles',[])
            for art in articles[:50]:
                title = art.get('article','')
                if title.startswith('Special:') or title.startswith('Wikipedia:'): continue
                record = {
                    'article_id': f'{lang}:{title}', 'language': lang,
                    'article_title': title, 'view_date': execution_date,
                    'rank': art.get('rank',0), 'page_views': int(art.get('views',0)),
                    'ingested_at': datetime.now(timezone.utc).isoformat(),
                    'source': 'wikimedia-pageviews'
                }
                producer.send('wikimedia-pageviews', key=record['article_id'].encode(), value=record)
                total += 1
            time.sleep(0.3)
        except Exception as e:
            logger.warning(f'{lang}: {e}')
    producer.flush(); producer.close()
    logger.info(f'✅ Published {total} pageview records')
    context['ti'].xcom_push(key='pageview_count', value=total)

def load_pageviews_snowflake(**context):
    import pandas as pd, json
    from kafka import KafkaConsumer
    import snowflake.connector
    from snowflake.connector.pandas_tools import write_pandas
    execution_date = context['ds']
    consumer = KafkaConsumer(
        'wikimedia-pageviews', bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset='earliest', group_id='airflow-wiki-batch',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        consumer_timeout_ms=10000
    )
    records = [msg.value for msg in consumer]
    consumer.close()
    if not records:
        logger.info('No records'); return
    df = pd.DataFrame(records)
    df.columns = [c.upper() for c in df.columns]
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT, user=SNOWFLAKE_USER, password=SNOWFLAKE_PASS,
        warehouse='COMPUTE_WH', database='WIKIMEDIA_DW', schema='ANALYTICS'
    )
    conn.cursor().execute(f"DELETE FROM ARTICLE_PAGEVIEWS WHERE VIEW_DATE = '{execution_date}'")
    write_pandas(conn, df, 'ARTICLE_PAGEVIEWS', database='WIKIMEDIA_DW', schema='ANALYTICS')
    conn.close()
    logger.info(f'✅ {len(df)} pageview rows → Snowflake')

# DAG 1: Speed (hourly SSE-based)
with DAG(
    dag_id='wikimedia_speed_layer',
    description='Hourly: Wikimedia pageviews → Kafka → Flink → Cassandra',
    default_args={**DEFAULT_ARGS, 'execution_timeout': timedelta(minutes=20)},
    schedule_interval='0 * * * *',
    catchup=False, max_active_runs=1,
    tags=['streaming','wikimedia','flink','cassandra','project10'],
) as speed_dag:
    s_start   = EmptyOperator(task_id='start')
    s_fetch   = PythonOperator(task_id='fetch_articles_kafka', python_callable=fetch_top_articles_kafka)
    s_end     = EmptyOperator(task_id='end')
    s_start >> s_fetch >> s_end

# DAG 2: Batch (daily Snowflake)
with DAG(
    dag_id='wikimedia_batch_layer',
    description='Daily: Wikimedia pageviews → Kafka → PySpark → Snowflake',
    default_args={**DEFAULT_ARGS, 'execution_timeout': timedelta(minutes=45)},
    schedule_interval='0 22 * * *',
    catchup=False, max_active_runs=1,
    tags=['batch','wikimedia','snowflake','project10'],
) as batch_dag:
    b_start   = EmptyOperator(task_id='start')
    b_fetch   = PythonOperator(task_id='fetch_articles_kafka',   python_callable=fetch_top_articles_kafka)
    b_load    = PythonOperator(task_id='load_pageviews_snowflake', python_callable=load_pageviews_snowflake)
    b_end     = EmptyOperator(task_id='end')
    b_start >> b_fetch >> b_load >> b_end
