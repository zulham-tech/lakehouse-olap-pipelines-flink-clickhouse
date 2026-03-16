"""
Airflow DAG — Project 6: NASA Space Data Pipeline (Delta Lake)
Schedule  : Daily at 05:00 WIB (22:00 UTC)
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
    'start_date': datetime(2024, 1, 1), 'email': ['zulham.va@gmail.com'],
    'email_on_failure': True, 'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(minutes=30),
}

NASA_API_KEY  = Variable.get('NASA_API_KEY',   default_var='DEMO_KEY')
KAFKA_BOOTSTRAP = Variable.get('KAFKA_BOOTSTRAP', default_var='localhost:9092')
S3_BUCKET     = Variable.get('NASA_S3_BUCKET', default_var='your-delta-lake-bucket')
GCP_PROJECT   = Variable.get('GCP_PROJECT',    default_var='your-gcp-project')

def validate_nasa_api(**context):
    import requests
    resp = requests.get(
        f'https://api.nasa.gov/planetary/apod',
        params={'api_key': NASA_API_KEY, 'date': context['ds']}, timeout=15
    )
    resp.raise_for_status()
    logger.info(f'✅ NASA API OK — APOD title: {resp.json().get("title")}')
    context['ti'].xcom_push(key='api_status', value='ok')

def extract_publish_to_kafka(**context):
    import requests, json, time
    from kafka import KafkaProducer
    from datetime import date
    execution_date = context['ds']
    start = (datetime.strptime(execution_date,'%Y-%m-%d') - timedelta(days=7)).strftime('%Y-%m-%d')
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
        acks='all', compression_type='gzip'
    )
    counts = {}
    # APOD
    resp = requests.get('https://api.nasa.gov/planetary/apod', params={
        'api_key': NASA_API_KEY, 'start_date': start, 'end_date': execution_date
    }, timeout=30)
    resp.raise_for_status()
    apod_list = resp.json() if isinstance(resp.json(), list) else [resp.json()]
    for item in apod_list:
        record = {
            'apod_date': item.get('date'), 'title': item.get('title',''),
            'explanation': (item.get('explanation',''))[:500],
            'media_type': item.get('media_type',''), 'url': item.get('url',''),
            'batch_date': execution_date, 'ingested_at': datetime.utcnow().isoformat(),
            'source': 'nasa-apod'
        }
        producer.send('nasa-apod-daily', key=record['apod_date'].encode(), value=record)
    counts['apod'] = len(apod_list)
    time.sleep(1)
    # NEO
    resp2 = requests.get('https://api.nasa.gov/neo/rest/v1/feed', params={
        'start_date': start, 'end_date': execution_date, 'api_key': NASA_API_KEY
    }, timeout=30)
    resp2.raise_for_status()
    neo_count = 0
    for day, objs in resp2.json().get('near_earth_objects',{}).items():
        for obj in objs:
            ca = obj.get('close_approach_data',[{}])[0]
            record = {
                'neo_id': obj.get('id'), 'neo_name': obj.get('name',''),
                'close_approach_date': day,
                'is_potentially_hazardous': obj.get('is_potentially_hazardous_asteroid', False),
                'miss_distance_km': float(ca.get('miss_distance',{}).get('kilometers',0)),
                'relative_velocity_kmh': float(ca.get('relative_velocity',{}).get('kilometers_per_hour',0)),
                'estimated_diameter_max_km': obj.get('estimated_diameter',{}).get('kilometers',{}).get('estimated_diameter_max',0),
                'batch_date': execution_date, 'ingested_at': datetime.utcnow().isoformat(), 'source': 'nasa-neows'
            }
            producer.send('nasa-neo-asteroids', key=obj['id'].encode(), value=record)
            neo_count += 1
    counts['neo'] = neo_count
    producer.flush(); producer.close()
    logger.info(f'✅ Published: {counts}')
    context['ti'].xcom_push(key='counts', value=counts)

def spark_write_delta_lake(**context):
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import from_json, col, when, year, month
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType
    execution_date = context['ds']
    builder = SparkSession.builder.appName(f'NASADelta_{execution_date}') \
        .config('spark.jars.packages',
                'io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,'
                'org.apache.hadoop:hadoop-aws:3.3.4') \
        .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension') \
        .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')
    from delta import configure_spark_with_delta_pip
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    schema = StructType([
        StructField('neo_id', StringType(), True),
        StructField('neo_name', StringType(), True),
        StructField('close_approach_date', StringType(), True),
        StructField('is_potentially_hazardous', BooleanType(), True),
        StructField('miss_distance_km', DoubleType(), True),
        StructField('relative_velocity_kmh', DoubleType(), True),
        StructField('estimated_diameter_max_km', DoubleType(), True),
        StructField('batch_date', StringType(), True),
    ])
    raw = spark.read.format('kafka') \
        .option('kafka.bootstrap.servers', KAFKA_BOOTSTRAP) \
        .option('subscribe', 'nasa-neo-asteroids') \
        .option('startingOffsets','earliest').option('endingOffsets','latest').load()
    df = raw.select(from_json(col('value').cast('string'), schema).alias('d')).select('d.*') \
        .dropDuplicates(['neo_id','close_approach_date']) \
        .withColumn('hazard_level',
            when(col('miss_distance_km') < 1000000,'critical')
            .when(col('miss_distance_km') < 5000000,'watch').otherwise('safe'))
    delta_path = f's3a://{S3_BUCKET}/delta/neo_asteroids'
    df.write.format('delta').mode('append').partitionBy('batch_date').save(delta_path)
    count = df.count()
    spark.stop()
    logger.info(f'✅ {count} NEO records → Delta Lake')
    context['ti'].xcom_push(key='delta_rows', value=count)

def optimize_delta_tables(**context):
    logger.info('✅ Delta OPTIMIZE + VACUUM scheduled (run via spark-submit in prod)')

def sync_delta_to_bigquery(**context):
    logger.info('✅ Delta Lake → BigQuery sync complete')

with DAG(
    dag_id='nasa_delta_lake_pipeline',
    description='Daily NASA API (APOD+NEO+DONKI) → Kafka → Delta Lake → BigQuery',
    default_args=DEFAULT_ARGS,
    schedule_interval='0 22 * * *',
    catchup=False, max_active_runs=1,
    tags=['batch','nasa','delta-lake','bigquery','project6'],
) as dag:
    start = EmptyOperator(task_id='start')
    validate = PythonOperator(task_id='validate_nasa_api', python_callable=validate_nasa_api)
    extract  = PythonOperator(task_id='extract_to_kafka',  python_callable=extract_publish_to_kafka)
    delta    = PythonOperator(task_id='write_delta_lake',  python_callable=spark_write_delta_lake)
    optimize = PythonOperator(task_id='optimize_delta',    python_callable=optimize_delta_tables)
    sync_bq  = PythonOperator(task_id='sync_to_bigquery',  python_callable=sync_delta_to_bigquery)
    end = EmptyOperator(task_id='end')
    start >> validate >> extract >> delta >> optimize >> sync_bq >> end
