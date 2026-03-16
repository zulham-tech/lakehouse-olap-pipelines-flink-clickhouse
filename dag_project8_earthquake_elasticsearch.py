"""
Airflow DAG — Project 8: USGS Earthquake Monitor (Elasticsearch)
Schedule  : Every 30 minutes
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
    'start_date': datetime(2024, 1, 1), 'retries': 3,
    'retry_delay': timedelta(minutes=3),
    'execution_timeout': timedelta(minutes=15),
}

KAFKA_BOOTSTRAP = Variable.get('KAFKA_BOOTSTRAP', default_var='localhost:9092')
ES_HOST         = Variable.get('ES_HOST', default_var='http://localhost:9200')
MIN_MAGNITUDE   = float(Variable.get('EQ_MIN_MAGNITUDE', default_var='1.0'))

def fetch_and_publish_earthquakes(**context):
    import requests, json
    from kafka import KafkaProducer
    from datetime import timezone
    end   = datetime.now(timezone.utc)
    start = end - timedelta(hours=1)
    params = {
        'format': 'geojson', 'minmagnitude': MIN_MAGNITUDE,
        'starttime': start.strftime('%Y-%m-%dT%H:%M:%S'),
        'endtime':   end.strftime('%Y-%m-%dT%H:%M:%S'),
        'orderby': 'time', 'limit': 1000
    }
    resp = requests.get('https://earthquake.usgs.gov/fdsnws/event/1/query', params=params, timeout=30)
    resp.raise_for_status()
    features = resp.json().get('features', [])
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
        acks='all', compression_type='gzip'
    )
    now = datetime.now(timezone.utc).isoformat()
    published = 0
    for f in features:
        props = f.get('properties', {})
        geom  = f.get('geometry', {})
        coords = geom.get('coordinates', [None,None,None])
        lon, lat, depth = coords[0], coords[1], coords[2]
        mag = float(props.get('mag') or 0.0)
        time_ms = props.get('time', 0)
        eq_time = datetime.fromtimestamp(time_ms/1000, tz=timezone.utc).isoformat()
        def mag_class(m):
            if m>=8: return 'Great'
            if m>=7: return 'Major'
            if m>=6: return 'Strong'
            if m>=5: return 'Moderate'
            if m>=4: return 'Light'
            if m>=3: return 'Minor'
            return 'Micro'
        record = {
            'event_id': f.get('id',''),
            'event_time': eq_time,
            'magnitude': mag,
            'magnitude_class': mag_class(mag),
            'depth_km': float(depth or 0),
            'latitude': float(lat or 0),
            'longitude': float(lon or 0),
            'location': {'lat': float(lat or 0), 'lon': float(lon or 0)},
            'place': props.get('place',''),
            'tsunami_flag': int(props.get('tsunami', 0)),
            'alert': props.get('alert',''),
            'felt_reports': int(props.get('felt', 0) or 0),
            'sig': int(props.get('sig', 0) or 0),
            'event_type': props.get('type', 'earthquake'),
            'ingested_at': now, 'source': 'usgs-earthquake-api'
        }
        producer.send('usgs-earthquakes', key=record['event_id'].encode(), value=record)
        published += 1
    producer.flush(); producer.close()
    logger.info(f'✅ Published {published} earthquake events')
    context['ti'].xcom_push(key='eq_count', value=published)

def index_to_elasticsearch(**context):
    from elasticsearch import Elasticsearch, helpers
    import json
    from kafka import KafkaConsumer
    es = Elasticsearch(ES_HOST)
    consumer = KafkaConsumer(
        'usgs-earthquakes',
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset='earliest',
        group_id='airflow-es-indexer',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        consumer_timeout_ms=5000
    )
    actions = []
    for msg in consumer:
        doc = msg.value
        actions.append({'_index': 'earthquakes-enriched', '_id': doc['event_id'], '_source': doc})
    if actions:
        success, _ = helpers.bulk(es, actions, raise_on_error=False)
        logger.info(f'✅ Indexed {success} events in Elasticsearch')
    consumer.close()

def check_major_events(**context):
    from elasticsearch import Elasticsearch
    es = Elasticsearch(ES_HOST)
    result = es.search(index='earthquakes-enriched', body={
        'query': {'bool': {'filter': [
            {'range': {'magnitude': {'gte': 6.0}}},
            {'range': {'event_time': {'gte': 'now-24h'}}}
        ]}},
        'size': 5
    })
    major = result['hits']['total']['value']
    logger.info(f'✅ Major earthquakes (M≥6) last 24h: {major}')
    if major > 0:
        logger.warning(f'⚠️ ALERT: {major} major earthquakes detected!')

with DAG(
    dag_id='usgs_earthquake_elasticsearch',
    description='Every 30min: USGS Earthquakes → Kafka → PySpark → Elasticsearch',
    default_args=DEFAULT_ARGS,
    schedule_interval='*/30 * * * *',
    catchup=False, max_active_runs=1,
    tags=['streaming','earthquake','elasticsearch','usgs','project8'],
) as dag:
    start   = EmptyOperator(task_id='start')
    fetch   = PythonOperator(task_id='fetch_earthquakes', python_callable=fetch_and_publish_earthquakes)
    index   = PythonOperator(task_id='index_elasticsearch', python_callable=index_to_elasticsearch)
    alerts  = PythonOperator(task_id='check_major_events',  python_callable=check_major_events)
    end     = EmptyOperator(task_id='end')
    start >> fetch >> index >> alerts >> end
