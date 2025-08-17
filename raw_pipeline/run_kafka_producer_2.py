import sys
import time
import asyncio
import logging
import traceback
from datetime import datetime
from kafka_producer_2 import send_video_to_kafka, create_producer, KAFKA_CONFIG, FALLBACK_HASHTAGS

import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../TikTok-Content-Scraper')))
try:
    from TT_Scraper import TT_Scraper
except ImportError:
    TT_Scraper = None
try:
    from kafka_producer_2 import send_metadata_by_ids_to_kafka
except ImportError:
    send_metadata_by_ids_to_kafka = None

def setup_logging():
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
        handlers=[
            logging.FileHandler('run_producer_debug.log', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

def validate_inputs(hashtag, topic_name):
    """Validate input parameters với suggestions"""
    logger.info("🔍 Validating input parameters...")
    
    errors = []
    warnings = []
    
    if not hashtag or hashtag.strip() == "":
        errors.append("Hashtag is empty or None")
    elif hashtag == "fyp":
        warnings.append("Hashtag 'fyp' may have restrictions, consider using alternatives")
        logger.info(f"💡 Suggested alternative hashtags: {FALLBACK_HASHTAGS}")
    
    if not topic_name or topic_name.strip() == "":
        errors.append("Topic name is empty or None")
    
    if hashtag and len(hashtag) > 100:
        errors.append(f"Hashtag too long: {len(hashtag)} characters")
    
    if topic_name and len(topic_name) > 249:
        errors.append(f"Topic name too long: {len(topic_name)} characters")
    
    if errors:
        logger.error("❌ Input validation failed:")
        for error in errors:
            logger.error(f"   - {error}")
        return False
    
    if warnings:
        logger.warning("⚠️ Input validation warnings:")
        for warning in warnings:
            logger.warning(f"   - {warning}")
    
    logger.info("✅ Input validation passed")
    logger.info(f"   Hashtag: '{hashtag}'")
    logger.info(f"   Topic: '{topic_name}'")
    return True

def main(hashtag, topic_name, ids_file=None):
    """Main function với enhanced error handling"""
    logger.info("=" * 70)
    logger.info("🚀 RUN_KAFKA_PRODUCER STARTING")
    logger.info("=" * 70)
    logger.info(f"⏰ Start time: {datetime.now()}")
    
    try:
        # Validate inputs
        if ids_file is None and not validate_inputs(hashtag, topic_name):
            logger.error("💡 Try using one of these hashtags: funny, viral, trending, dance")
            return False
        
        # Configuration
        kafka_broker = KAFKA_CONFIG['broker']
        num_videos = KAFKA_CONFIG['num_videos']
        
        logger.info(f"⚙️ Configuration:")
        logger.info(f"   Kafka Broker: {kafka_broker}")
        logger.info(f"   Topic: {topic_name}")
        if ids_file:
            logger.info(f"   IDs file: {ids_file}")
        else:
            logger.info(f"   Hashtag: {hashtag}")
        logger.info(f"   Videos: {num_videos}")
        
        # Create producer
        logger.info("🔌 Creating Kafka producer...")
        producer_start = time.time()
        
        producer = create_producer(kafka_broker)
        
        producer_time = time.time() - producer_start
        
        if producer is None:
            logger.error("❌ Failed to create Kafka producer!")
            return False
        
        logger.info(f"✅ Producer created in {producer_time:.2f}s")
        
        # Send videos or metadata
        logger.info("📤 Starting sending process...")
        send_start = time.time()
        
        if ids_file and send_metadata_by_ids_to_kafka is not None:
            logger.info(f"📤 Sending metadata from ids file: {ids_file}")
            success = send_metadata_by_ids_to_kafka(ids_file, producer, topic_name)
        else:
            success = send_video_to_kafka(
                hashtag_name=hashtag, 
                producer=producer, 
                topic_name=topic_name, 
                kafka_broker=kafka_broker, 
                num_videos=num_videos
            )
        
        send_time = time.time() - send_start
        logger.info(f"✅ Sending completed in {send_time:.2f}s")
        
        # Cleanup
        logger.info("🧹 Cleaning up producer...")
        cleanup_start = time.time()
        
        producer.flush()
        producer.close()
        
        cleanup_time = time.time() - cleanup_start
        logger.info(f"✅ Cleanup completed in {cleanup_time:.2f}s")
        
        if success:
            logger.info("🎉 SUCCESS - All operations completed successfully!")
            return True
        else:
            logger.error("❌ FAILED - No videos/metadata were successfully sent!")
            logger.error("💡 Troubleshooting suggestions:")
            logger.error("   1. Try a different hashtag (funny, viral, trending)")
            logger.error("   2. Check your internet connection")
            logger.error("   3. Try running at a different time")
            return False
        
    except Exception as e:
        logger.error(f"❌ Critical error in main: {e}")
        logger.error(f"❌ Traceback: {traceback.format_exc()}")
        return False

if __name__ == "__main__":
    import time
    
    logger.info("🔍 Starting run_kafka_producer.py...")
    logger.info(f"📋 Command line arguments: {sys.argv}")
    
    # Cho phép truyền thêm ids_file
    import argparse
    parser = argparse.ArgumentParser(description="Run Kafka Producer for TikTok")
    parser.add_argument('--hashtag', type=str, help='Hashtag để scrape (không có #)')
    parser.add_argument('--topic', type=str, help='Kafka topic name')
    parser.add_argument('--ids_file', type=str, help='File chứa danh sách TikTok video IDs để lấy metadata')
    args = parser.parse_args()
    
    hashtag = args.hashtag
    topic_name = args.topic
    ids_file = args.ids_file
    
    try:
        main_start = time.time()
        success = main(hashtag, topic_name, ids_file)
        main_time = time.time() - main_start
        
        logger.info(f"⏱️ Total execution time: {main_time:.2f}s")
        
        if success:
            logger.info("🎉 Program completed successfully!")
            sys.exit(0)
        else:
            logger.error("❌ Program failed!")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("⚠️ Program interrupted by user (Ctrl+C)")
        sys.exit(0)
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        logger.error(f"❌ Traceback: {traceback.format_exc()}")
        sys.exit(1)