"""
Airflow DAG — Project 7: NYC Taxi Real-Time Analytics (Flink + ClickHouse)
Schedule  : Hourly
Author    : Ahmad Zulham Hamdan
"""
from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    'owner': 'zulham-hamdan', 'depends_on_past': False,
    'start_date': datetime(2024, 1, 1), 'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=25),
}

KAFKA_BOOTSTRAP = Variable.get('KAFKA_BOOTSTRAP', default_var='localhost:9092')
FLINK_JOBMANAGER = Variable.get('FLINK_JOBMANAGER', default_var='localhost:8081')

def check_nyc_api(**context):
    import requests
    resp = requests.get(
        'https://data.cityofnewyork.us/resource/uacg-pexx.json',
        params={'$limit': 1}, timeout=15
    )
    resp.raise_for_status()
    logger.info(f'✅ NYC TLC API OK — sample: {resp.json()[0].get("tpep_pickup_datetime","N/A")}')

def fetch_and_publish_trips(**context):
    import requests, json, time
    from kafka import KafkaProducer
    from datetime import timezone
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
        acks='all', compression_type='gzip'
    )
    total = 0
    PAYMENT_TYPES = {'1':'Credit card','2':'Cash','3':'No charge','4':'Dispute','5':'Unknown'}
    for offset in range(0, 2000, 500):
        try:
            resp = requests.get(
                'https://data.cityofnewyork.us/resource/uacg-pexx.json',
                params={'$limit': 500, '$offset': offset, '$order': 'tpep_pickup_datetime DESC'},
                timeout=30
            )
            resp.raise_for_status()
            for raw in resp.json():
                def sf(v, d=0.0):
                    try: return float(v)
                    except: return d
                def si(v, d=0):
                    try: return int(v)
                    except: return d
                pickup  = raw.get('tpep_pickup_datetime','')
                dropoff = raw.get('tpep_dropoff_datetime','')
                fare    = sf(raw.get('fare_amount'))
                tip     = sf(raw.get('tip_amount'))
                dist    = sf(raw.get('trip_distance'))
                pay_id  = str(raw.get('payment_type','5'))
                try:
                    pdt = datetime.fromisoformat(pickup.replace('T',' ').split('.')[0])
                    ddt = datetime.fromisoformat(dropoff.replace('T',' ').split('.')[0])
                    dur = max(0, (ddt - pdt).seconds // 60)
                except: dur = 0
                record = {
                    'vendor_id': str(raw.get('vendorid','')),
                    'pickup_datetime': pickup, 'dropoff_datetime': dropoff,
                    'passenger_count': si(raw.get('passenger_count'),1),
                    'trip_distance_miles': dist,
                    'pu_location_id': si(raw.get('pulocationid'),0),
                    'do_location_id': si(raw.get('dolocationid'),0),
                    'payment_type_name': PAYMENT_TYPES.get(pay_id,'Unknown'),
                    'fare_amount': fare, 'tip_amount': tip,
                    'total_amount': sf(raw.get('total_amount')),
                    'tip_rate_pct': round(tip/fare*100,2) if fare > 0 else 0.0,
                    'duration_minutes': dur,
                    'speed_mph': round(dist/(dur/60),2) if dur > 0 else 0.0,
                    'congestion_surcharge': sf(raw.get('congestion_surcharge')),
                    'ingested_at': datetime.now(timezone.utc).isoformat(),
                    'source': 'nyc-tlc-open-data'
                }
                producer.send('nyc-taxi-trips', key=f"{pickup}_{record['pu_location_id']}".encode(), value=record)
                total += 1
            producer.flush()
            time.sleep(0.5)
        except Exception as e:
            logger.warning(f'Batch error at offset {offset}: {e}')
    producer.close()
    logger.info(f'✅ Published {total} taxi trips')
    context['ti'].xcom_push(key='trip_count', value=total)

def verify_clickhouse(**context):
    import clickhouse_driver
    client = clickhouse_driver.Client(host='localhost', port=9000, database='nyc_taxi_analytics')
    result = client.execute('SELECT count() FROM taxi_trips WHERE pickup_datetime >= today()')
    logger.info(f'✅ ClickHouse today trips: {result[0][0]}')

with DAG(
    dag_id='nyc_taxi_flink_clickhouse',
    description='Hourly: NYC TLC API → Kafka → Flink → ClickHouse OLAP',
    default_args=DEFAULT_ARGS,
    schedule_interval='0 * * * *',
    catchup=False, max_active_runs=1,
    tags=['streaming','nyc-taxi','flink','clickhouse','project7'],
) as dag:
    start    = EmptyOperator(task_id='start')
    chk_api  = PythonOperator(task_id='check_nyc_api',     python_callable=check_nyc_api)
    publish  = PythonOperator(task_id='publish_to_kafka',  python_callable=fetch_and_publish_trips)
    flink    = BashOperator(task_id='trigger_flink_job',
                bash_command=f'curl -X POST http://{FLINK_JOBMANAGER}/jars/taxi_job.jar/run || true')
    verify   = PythonOperator(task_id='verify_clickhouse', python_callable=verify_clickhouse)
    end      = EmptyOperator(task_id='end')
    start >> chk_api >> publish >> flink >> verify >> end
