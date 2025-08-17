import logging
import sys
import os
from kafka import KafkaProducer
from pymongo import MongoClient

def main():
    logging.basicConfig(level=logging.INFO)
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
    atlas_uri_path = 'uri_mongodb_atlas.txt'
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
        print("HEALTHY")
        sys.exit(0)
    else:
        logging.warning("System health check: FAILED")
        print("UNHEALTHY")
        sys.exit(1)

if __name__ == "__main__":
    main() 