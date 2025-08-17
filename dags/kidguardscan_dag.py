from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
import os

def check_system_health():
    import logging
    from kafka import KafkaProducer
    from pymongo import MongoClient
    logging.info("Checking system health...")
    health_status = {'kafka': False, 'mongodb': False, 'overall': False}
    # Check Kafka
    try:
        producer = KafkaProducer(bootstrap_servers=['kafka:9092'])
        producer.close()
        health_status['kafka'] = True
        logging.info("Kafka connection: OK")
    except Exception as e:
        logging.error(f"Kafka connection failed: {str(e)}")
    # Check MongoDB Atlas
    atlas_uri_path = '/opt/airflow/pipeline_code/uri_mongodb_atlas.txt'
    atlas_uri = None
    if os.path.exists(atlas_uri_path):
        try:
            with open(atlas_uri_path, 'r', encoding='utf-8') as f:
                atlas_uri = f.read().strip()
        except Exception as e:
            logging.error(f"Cannot read MongoDB Atlas URI file: {e}")
    else:
        logging.warning(f"MongoDB Atlas URI file not found at {atlas_uri_path}")
    if atlas_uri:
        try:
            client = MongoClient(atlas_uri, serverSelectionTimeoutMS=5000)
            client.admin.command('ping')
            client.close()
            health_status['mongodb'] = True
            logging.info("MongoDB Atlas connection: OK")
        except Exception as e:
            logging.error(f"MongoDB Atlas connection failed: {str(e)}")
    else:
        logging.warning("MongoDB Atlas URI not set, skipping Atlas health check.")
    health_status['overall'] = health_status['kafka'] and health_status['mongodb']
    if health_status['overall']:
        logging.info("System health check: PASSED")
        return "HEALTHY"
    else:
        logging.warning("System health check: FAILED")
        return "UNHEALTHY"

def generate_report():
    import logging
    from datetime import datetime
    logging.info("Generating analysis report...")
    try:
        report_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logging.info(f"Report generated at: {report_time}")
        return "REPORT_GENERATED"
    except Exception as e:
        logging.error(f"Error generating report: {str(e)}")
        raise

default_args = {
    'owner': 'kidguardscan',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'kidguardscan_pipeline',
    default_args=default_args,
    description='KidGuardScan TikTok Content Analysis Pipeline',
    schedule_interval=timedelta(hours=6),
    catchup=False,
    tags=['kidguardscan', 'tiktok', 'content-analysis'],
)

health_check_task = DockerOperator(
    task_id='check_system_health',
    image='tiktok_kidguard_pipeline',
    command='python check_health.py',
    mount_tmp_dir=False,
    auto_remove=True,
    network_mode='tiktok_kidguard_kidguardnet',
    dag=dag,
)

crawl_task = DockerOperator(
    task_id='crawl_tiktok_data',
    image='tiktok_kidguard_pipeline',
    command='python producer.py --hashtags "funny,viral,trending" --num_videos 3 --topic Test_with_MongoDB11 --browser chrome',
    auto_remove=True,
    network_mode='tiktok_kidguard_kidguardnet',
    dag=dag,
)

process_task = DockerOperator(
    task_id='process_data',
    image='tiktok_kidguard_pipeline',
    command='python consumer.py --max_messages 10 --max_seconds 300',
    auto_remove=True,
    network_mode='tiktok_kidguard_kidguardnet',
    dag=dag,
)

report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    dag=dag,
)

health_check_task >> [crawl_task, process_task] >> report_task 