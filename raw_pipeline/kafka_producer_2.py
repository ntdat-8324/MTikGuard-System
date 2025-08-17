import os
import sys
import time
import json
import logging
import traceback
import re
import random
from datetime import datetime
from kafka import KafkaProducer
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.edge.options import Options as EdgeOptions
from urllib.parse import quote_plus
import glob
import tempfile
import shutil

# **DEBUG 1: Setup comprehensive logging**
def setup_logging():
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
        handlers=[
            logging.FileHandler('producer_debug.log', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

KAFKA_CONFIG = {
    'broker': 'localhost:9092',
    'topic': 'Test_with_MongoDB11',
    'num_videos': 3,
    'retry_attempts': 3,
    'timeout_seconds': 30
}

FALLBACK_HASHTAGS = ["funny", "viral", "trending", "dance", "comedy", "meme"]

# ===================== SELENIUM SCRAPER (FROM scape_tiktok2.py) =====================
def setup_driver(browser_choice="edge"):
    """Setup WebDriver với cấu hình từ scape_tiktok2.py"""
    if browser_choice.lower() == "edge":
        opts = EdgeOptions()
    else:
        opts = ChromeOptions()
    
    # REMOVED --headless để tránh bị phát hiện bot
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-web-security")
    opts.add_argument("--disable-features=VizDisplayCompositor")
    opts.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
    )
    # Tạo thư mục user-data-dir tạm thời duy nhất ở /dev/shm nếu có, hoặc cwd
    tmp_dir_base = "/dev/shm" if os.path.exists("/dev/shm") else os.getcwd()
    user_data_dir = tempfile.mkdtemp(dir=tmp_dir_base)
    logger.info(f"Using user-data-dir: {user_data_dir}")
    opts.add_argument(f"--user-data-dir={user_data_dir}")
    if browser_choice.lower() == "edge":
        driver = webdriver.Edge(opts)
        logger.info("🌐 Đang sử dụng Microsoft Edge")
    else:
        driver = webdriver.Chrome(opts)
        logger.info("🌐 Đang sử dụng Google Chrome")
    
    driver.execute_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
    driver._user_data_dir = user_data_dir  # Gắn vào driver để dọn dẹp sau này
    return driver

def load_source(driver, url: str, max_refresh: int = 3) -> bool:
    """Load trang với retry mechanism"""
    for attempt in range(max_refresh):
        try:
            logger.info(f"   🔄 Lần thử {attempt + 1}: Đang truy cập {url}")
            driver.get(url)
            time.sleep(8)
            
            links = driver.find_elements(By.TAG_NAME, "a")
            if any("/video/" in (l.get_attribute("href") or "") for l in links):
                logger.info(f"   ✅ Tìm thấy video links!")
                return True
            
            logger.info(f"   ⚠️ Chưa tìm thấy video, refresh lại...")
            driver.refresh()
            time.sleep(6)
        except Exception as e:
            logger.error(f"   ❌ Lỗi lần thử {attempt + 1}: {e}")
            time.sleep(5)
    
    logger.error(f"   ❌ Không thể load trang sau {max_refresh} lần thử")
    return False

# Regex patterns từ scape_tiktok2.py
URL_RE = re.compile(r"https://www\.tiktok\.com/@([\w\.-]+)/video/(\d+)")
VIEW_RE = re.compile(r"([\d,\.]+)\s*[Vv]iews?")

def quick_views(element):
    """Extract view count từ element"""
    try:
        parent_text = element.find_element(By.XPATH, ".//..").text
        m = VIEW_RE.search(parent_text)
        if m:
            view_str = m.group(1).replace(",", "").replace(".", "")
            return int(view_str)
    except:
        pass
    return None

def scrape_videos_by_hashtag_selenium(hashtag, num_videos=10, browser_choice="edge", min_views=1000):
    """Scrape videos sử dụng logic từ scape_tiktok2.py"""
    driver = setup_driver(browser_choice)
    videos_data = []
    
    try:
        # Construct URL
        if hashtag.startswith('#'):
            hashtag = hashtag[1:]
        
        url = f"https://www.tiktok.com/tag/{hashtag}"
        logger.info(f"🎯 Bắt đầu scrape hashtag: #{hashtag}")
        logger.info(f"   URL: {url}")
        logger.info(f"   Target: {num_videos} videos")
        logger.info(f"   Min views: {min_views}")
        
        if not load_source(driver, url):
            logger.error("❌ Không thể load trang!")
            return videos_data
        
        collected_ids = set()
        scroll_count = 0
        max_scrolls = 25
        
        while len(videos_data) < num_videos and scroll_count < max_scrolls:
            new_found = 0
            
            for link in driver.find_elements(By.TAG_NAME, "a"):
                if len(videos_data) >= num_videos:
                    break
                
                href = link.get_attribute("href") or ""
                m = URL_RE.match(href)
                
                if not m:
                    continue
                
                username, video_id = m.groups()
                
                if video_id in collected_ids:
                    continue
                
                # Check view count
                view_count = quick_views(link)
                if view_count is not None and view_count < min_views:
                    continue
                
                collected_ids.add(video_id)
                
                # Extract description
                description = ""
                try:
                    link_text = link.text.strip()
                    title_attr = link.get_attribute('title')
                    
                    if title_attr:
                        description = title_attr
                    elif link_text and len(link_text) > 5:
                        description = link_text
                except:
                    pass
                
                video_info = {
                    'url': href,
                    'username': username,
                    'video_id': video_id,
                    'description': description,
                    'views': view_count or '',
                    'likes': '',
                    'comments': ''
                }
                
                videos_data.append(video_info)
                new_found += 1
                
                logger.info(f"   ✅ Video {len(videos_data)}: @{username} (ID: {video_id})")
                if view_count:
                    logger.info(f"      Views: {view_count:,}")
                if description:
                    desc_preview = description[:50] + "..." if len(description) > 50 else description
                    logger.info(f"      Description: {desc_preview}")
            
            if new_found == 0:
                logger.info(f"   📜 Scroll để tìm thêm video... ({scroll_count + 1}/{max_scrolls})")
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(4)
                driver.execute_script("window.scrollBy(0, 1000);")
                time.sleep(1.5)
            else:
                logger.info(f"   📊 Tìm thấy {new_found} video mới, tổng: {len(videos_data)}")
                time.sleep(2)
            
            scroll_count += 1
        
        logger.info(f"🎉 Hoàn thành! Đã scrape được {len(videos_data)} video từ #{hashtag}")
        
    except Exception as e:
        logger.error(f"❌ Lỗi chung: {e}")
        logger.error(f"❌ Traceback: {traceback.format_exc()}")
    
    finally:
        logger.info("🔚 Đóng trình duyệt...")
        try:
            driver.quit()
        except:
            pass
        # Dọn dẹp user-data-dir
        try:
            if hasattr(driver, '_user_data_dir') and os.path.exists(driver._user_data_dir):
                shutil.rmtree(driver._user_data_dir)
        except Exception as e:
            logger.warning(f"Không xóa được user-data-dir: {e}")
    
    return videos_data

# ===================== KAFKA PRODUCER =====================
def test_kafka_connection(broker):
    logger.info(f"🔌 Testing Kafka connection to: {broker}")
    try:
        import socket
        host, port = broker.split(':')
        port = int(port)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((host, port))
        sock.close()
        if result == 0:
            logger.info(f"✅ Kafka connection test successful: {broker}")
            return True
        else:
            logger.error(f"❌ Kafka connection test failed: {broker}")
            return False
    except Exception as e:
        logger.error(f"❌ Kafka connection test error: {e}")
        return False

def create_producer(kafka_broker):
    logger.info(f"🔌 Creating Kafka producer for: {kafka_broker}")
    if not test_kafka_connection(kafka_broker):
        logger.error(f"❌ Cannot connect to Kafka broker: {kafka_broker}")
        return None
    try:
        logger.debug("⚙️ Configuring Kafka producer...")
        producer_config = {
            'bootstrap_servers': kafka_broker,
            'value_serializer': lambda x: json.dumps(x, ensure_ascii=False).encode('utf-8'),
            'key_serializer': lambda x: json.dumps(x, ensure_ascii=False).encode('utf-8'),
            'request_timeout_ms': 30000,
            'retry_backoff_ms': 1000,
            'reconnect_backoff_ms': 1000,
            'max_in_flight_requests_per_connection': 1,
            'acks': 'all',
            'retries': 3,
            'batch_size': 16384,
            'linger_ms': 10,
            'buffer_memory': 33554432
        }
        producer = KafkaProducer(**producer_config)
        logger.info("✅ Kafka producer created successfully")
        return producer
    except Exception as e:
        logger.error(f"❌ Failed to create Kafka producer: {e}")
        logger.error(f"❌ Traceback: {traceback.format_exc()}")
        return None

def send_video_to_kafka(hashtag_name, producer, topic_name, kafka_broker, num_videos, browser_choice="edge"):
    logger.info("=" * 60)
    logger.info(f"🚀 STARTING VIDEO KAFKA PIPELINE")
    logger.info("=" * 60)
    logger.info(f"   Hashtag: {hashtag_name}")
    logger.info(f"   Topic: {topic_name}")
    logger.info(f"   Videos: {num_videos}")
    logger.info(f"   Broker: {kafka_broker}")
    total_start_time = time.time()
    try:
        # STEP 1: Scrape videos bằng Selenium
        logger.info("🔄 STEP 1: Scraping videos using Selenium...")
        step1_start = time.time()
        videos_data = scrape_videos_by_hashtag_selenium(hashtag_name, num_videos, browser_choice)
        step1_time = time.time() - step1_start
        logger.info(f"✅ STEP 1 completed in {step1_time:.2f}s")
        logger.info(f"   Videos collected: {len(videos_data)}")
        if not videos_data:
            logger.error("❌ No videos collected - ABORTING")
            logger.error("💡 Troubleshooting suggestions:")
            logger.error("   1. Check if TikTok is accessible from your network")
            logger.error("   2. Try different hashtags (funny, viral, trending)")
            logger.error("   3. Try running at a different time")
            return False
        # STEP 2: Send videos to Kafka
        logger.info("🔄 STEP 2: Sending videos to Kafka...")
        step2_start = time.time()
        successful_sends = 0
        failed_sends = 0
        for i, video_info in enumerate(videos_data[:num_videos]):
            video_start_time = time.time()
            logger.info(f"🔄 Processing video {i+1}/{len(videos_data)}")
            try:
                data_size = len(json.dumps(video_info, ensure_ascii=False))
                logger.debug(f"   📊 Data size: {data_size} bytes")
                send_start = time.time()
                future = producer.send(topic_name, value=video_info, key=hashtag_name)
                try:
                    record_metadata = future.get(timeout=30)
                    send_time = time.time() - send_start
                    logger.info(f"   ✅ Video {i+1} sent successfully in {send_time:.2f}s")
                    logger.info(f"      Topic: {record_metadata.topic}")
                    logger.info(f"      Partition: {record_metadata.partition}")
                    logger.info(f"      Offset: {record_metadata.offset}")
                    successful_sends += 1
                except Exception as send_error:
                    logger.error(f"   ❌ Video {i+1} send failed: {send_error}")
                    failed_sends += 1
                    continue
                video_total_time = time.time() - video_start_time
                logger.info(f"   ⏱️ Video {i+1} total time: {video_total_time:.2f}s")
                if i < len(videos_data) - 1:
                    logger.debug(f"   😴 Sleeping 2s before next video...")
                    time.sleep(2)
            except Exception as video_error:
                logger.error(f"   ❌ Video {i+1} processing failed: {video_error}")
                logger.error(f"   ❌ Traceback: {traceback.format_exc()}")
                failed_sends += 1
                continue
        step2_time = time.time() - step2_start
        logger.info(f"✅ STEP 2 completed in {step2_time:.2f}s")
        # STEP 3: Final flush
        logger.info("🔄 STEP 3: Flushing producer...")
        step3_start = time.time()
        producer.flush()
        step3_time = time.time() - step3_start
        logger.info(f"✅ STEP 3 completed in {step3_time:.2f}s")
        # FINAL SUMMARY
        total_time = time.time() - total_start_time
        logger.info("=" * 60)
        logger.info(f"🎉 PIPELINE COMPLETED!")
        logger.info("=" * 60)
        logger.info(f"   Total time: {total_time:.2f}s")
        logger.info(f"   Successful sends: {successful_sends}")
        logger.info(f"   Failed sends: {failed_sends}")
        if successful_sends + failed_sends > 0:
            success_rate = (successful_sends/(successful_sends+failed_sends)*100)
            logger.info(f"   Success rate: {success_rate:.1f}%")
        return successful_sends > 0
    except Exception as e:
        logger.error(f"❌ Critical error in send_video_to_kafka: {e}")
        logger.error(f"❌ Traceback: {traceback.format_exc()}")
        return False

def crawl_ids_only(hashtag, num_ids=30, browser_choice="edge", min_views=1000, out_file="tiktok_ids.txt"):
    """Chỉ crawl ID TikTok theo hashtag, lưu vào file, không lấy metadata."""
    driver = setup_driver(browser_choice)
    ids = set()
    try:
        if hashtag.startswith('#'):
            hashtag = hashtag[1:]
        url = f"https://www.tiktok.com/tag/{hashtag}"
        logger.info(f"[ID-ONLY] Bắt đầu crawl ID cho hashtag: #{hashtag} ({url})")
        if not load_source(driver, url):
            logger.error("[ID-ONLY] Không thể load trang!")
            return 0
        scroll = 0
        max_scrolls = 25
        while len(ids) < num_ids and scroll < max_scrolls:
            new = 0
            for a in driver.find_elements(By.TAG_NAME, "a"):
                href = a.get_attribute("href") or ""
                m = URL_RE.match(href)
                vid = m.group(2) if m else None
                if not vid or vid in ids:
                    continue
                v = quick_views(a)
                if v is not None and v < min_views:
                    continue
                ids.add(vid)
                new += 1
                if len(ids) >= num_ids:
                    break
            if new:
                time.sleep(2)
            else:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(4)
                driver.execute_script("window.scrollBy(0,1000);")
                time.sleep(1.5)
                scroll += 1
        logger.info(f"[ID-ONLY] Đã crawl được {len(ids)} ID cho #{hashtag}")
        with open(out_file, "w", encoding="utf-8") as f:
            f.write("\n".join(ids))
        logger.info(f"[ID-ONLY] Đã lưu vào {out_file}")
    except Exception as e:
        logger.error(f"[ID-ONLY] Lỗi: {e}")
        logger.error(f"[ID-ONLY] Traceback: {traceback.format_exc()}")
    finally:
        try:
            driver.quit()
        except:
            pass
        # Dọn dẹp user-data-dir
        try:
            if hasattr(driver, '_user_data_dir') and os.path.exists(driver._user_data_dir):
                shutil.rmtree(driver._user_data_dir)
        except Exception as e:
            logger.warning(f"Không xóa được user-data-dir: {e}")
    return len(ids)

# ==== THÊM: Import TT_Scraper ====
import sys
import os
# Ensure TT_Scraper can be imported regardless of working directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), r'C:\Users\Admin\PycharmProjects\UIT-Projects\TikTok-Content-Scraper')))
try:
    from TT_Scraper import TT_Scraper
except ImportError as e:
    raise ImportError("Cannot import TT_Scraper. Please check that TikTok-Content-Scraper is present and sys.path is set correctly.") from e

# ==== Hàm gửi metadata từ file ids bằng TT_Scraper ====
def send_metadata_by_ids_to_kafka(ids_file, producer, topic_name, output_folder="data/"):
    import shutil
    tt = TT_Scraper(wait_time=0.3, output_files_fp=output_folder)
    # Clean up old metadata files
    if os.path.exists(output_folder):
        for file in glob.glob(os.path.join(output_folder, "tiktok_*_metadata.json")):
            try:
                os.remove(file)
            except Exception as e:
                logger.warning(f"Không thể xóa file cũ {file}: {e}")
    else:
        os.makedirs(output_folder, exist_ok=True)
    # Read ids
    with open(ids_file, "r", encoding="utf-8") as f:
        ids = [line.strip() for line in f if line.strip()]
    # Scrape metadata (files will be saved in output_folder)
    tt.scrape_list(ids=ids, scrape_content=False)
    # Read all metadata files
    metadata_files = glob.glob(os.path.join(output_folder, "tiktok_*_metadata.json"))
    sent_count = 0
    for meta_file in metadata_files:
        try:
            with open(meta_file, "r", encoding="utf-8") as f:
                meta = json.load(f)
            video = meta.get('video_metadata', {})
            author = meta.get('author_metadata', {})
            music = meta.get('music_metadata', {})
            filemeta = meta.get('file_metadata', {})
            hashtags = video.get('hashtags', [])
            filtered = {
                'video_id': str(video.get('id', '')),
                'url': f"https://www.tiktok.com/@{author.get('username', '')}/video/{video.get('id', '')}",
                'username': author.get('username', ''),
                'author_name': author.get('name', ''),
                'description': video.get('description', ''),
                'hashtags': hashtags,
                'playcount': video.get('playcount', 0),
                'diggcount': video.get('diggcount', 0),
                'commentcount': video.get('commentcount', 0),
                'sharecount': video.get('sharecount', 0),
                'music_title': music.get('title', ''),
                'music_author': music.get('author_name', ''),
                'duration': filemeta.get('duration', 0),
                'width': filemeta.get('width', 0),
                'height': filemeta.get('height', 0),
                'created_time': video.get('time_created', '')
            }
            producer.send(topic_name, value=filtered)
            logger.info(f"✅ Đã gửi metadata cho video {filtered['video_id']}")
            sent_count += 1
        except Exception as e:
            logger.error(f"❌ Lỗi khi xử lý file {meta_file}: {e}")
    producer.flush()
    logger.info(f"🎉 Đã gửi xong metadata cho {sent_count} video IDs trong {ids_file}!")
    return True

if __name__ == "__main__":
    import argparse
    logger.info("=" * 70)
    logger.info("🚀 TIKTOK KAFKA PRODUCER STARTING")
    logger.info("=" * 70)
    logger.info(f"⏰ Start time: {datetime.now()}")
    
    parser = argparse.ArgumentParser(description="Scrape TikTok videos by hashtag and optionally send to Kafka.")
    parser.add_argument('--hashtag', type=str, default='funny', help='Hashtag để scrape (không có #)')
    parser.add_argument('--num_videos', type=int, default=3, help='Số lượng video muốn lấy')
    parser.add_argument('--browser', type=str, default='edge', choices=['edge', 'chrome'], help='Chọn trình duyệt (edge hoặc chrome)')
    parser.add_argument('--min_views', type=int, default=1000, help='Số view tối thiểu để lọc video')
    parser.add_argument('--kafka', action='store_true', help='Nếu có flag này sẽ gửi vào Kafka, nếu không chỉ scrape và in ra')
    parser.add_argument('--crawl_ids_only', action='store_true', help='Chỉ crawl ID TikTok và lưu vào file, không lấy metadata')
    parser.add_argument('--ids_file', type=str, help='File chứa danh sách TikTok video IDs để lấy metadata')
    args = parser.parse_args()

    hashtag_name = args.hashtag
    num_videos = args.num_videos
    browser_choice = args.browser
    min_views = args.min_views
    use_kafka = args.kafka
    crawl_ids_flag = args.crawl_ids_only
    ids_file = args.ids_file
    
    logger.info(f"⚙️ Configuration:")
    logger.info(f"   Hashtag: {hashtag_name}")
    logger.info(f"   Videos: {num_videos}")
    logger.info(f"   Browser: {browser_choice}")
    logger.info(f"   Min views: {min_views}")
    logger.info(f"   Send to Kafka: {use_kafka}")
    logger.info(f"   Crawl IDs only: {crawl_ids_flag}")
    logger.info(f"   IDs file: {ids_file}")

    if crawl_ids_flag:
        logger.info("🔍 Chạy chế độ chỉ crawl ID TikTok...")
        n = crawl_ids_only(hashtag_name, num_videos, browser_choice, min_views)
        logger.info(f"[ID-ONLY] Đã crawl và lưu {n} ID vào tiktok_ids.txt")
        sys.exit(0)

    if ids_file:
        kafka_broker = KAFKA_CONFIG['broker']
        topic_name = KAFKA_CONFIG['topic']
        logger.info(f"Kafka Broker: {kafka_broker}")
        logger.info(f"Topic: {topic_name}")
        try:
            logger.info("🔌 Creating Kafka producer...")
            producer = create_producer(kafka_broker)
            if producer is None:
                logger.error("❌ Failed to create producer - EXITING")
                sys.exit(1)
            send_metadata_by_ids_to_kafka(ids_file, producer, topic_name)
            producer.close()
            logger.info("✅ Producer closed")
            sys.exit(0)
        except KeyboardInterrupt:
            logger.info("⚠️ Interrupted by user")
            sys.exit(0)
        except Exception as e:
            logger.error(f"❌ Critical error: {e}")
            logger.error(f"❌ Traceback: {traceback.format_exc()}")
            sys.exit(1)
    else:
        if not use_kafka:
            # Chạy thực nghiệm: chỉ scrape và in ra kết quả
            logger.info("🔍 Chạy thực nghiệm: chỉ scrape video, không gửi Kafka")
            videos_data = scrape_videos_by_hashtag_selenium(hashtag_name, num_videos, browser_choice, min_views)
            logger.info(f"🎉 Đã lấy được {len(videos_data)} video:")
            for i, video in enumerate(videos_data, 1):
                logger.info(f"\n{i}. @{video['username']}")
                logger.info(f"   🔗 {video['url']}")
                if video['views']:
                    logger.info(f"   👁️ Views: {video['views']:,}")
                if video['description']:
                    description = video['description'][:100] + "..." if len(video['description']) > 100 else video['description']
                    logger.info(f"   📝 {description}")
            logger.info("\n✅ Kết thúc thực nghiệm scrape!")
            sys.exit(0)
        else:
            # Gửi vào Kafka như pipeline chuẩn
            kafka_broker = KAFKA_CONFIG['broker']
            topic_name = KAFKA_CONFIG['topic']
            logger.info(f"Kafka Broker: {kafka_broker}")
            logger.info(f"Topic: {topic_name}")
            try:
                logger.info("🔌 Creating Kafka producer...")
                producer = create_producer(kafka_broker)
                if producer is None:
                    logger.error("❌ Failed to create producer - EXITING")
                    sys.exit(1)
                success = send_video_to_kafka(hashtag_name, producer, topic_name, kafka_broker, num_videos, browser_choice)
                logger.info("🧹 Cleaning up...")
                producer.close()
                logger.info("✅ Producer closed")
                if success:
                    logger.info("🎉 Producer completed successfully!")
                    sys.exit(0)
                else:
                    logger.error("❌ Producer failed!")
                    sys.exit(1)
            except KeyboardInterrupt:
                logger.info("⚠️ Interrupted by user")
                sys.exit(0)
            except Exception as e:
                logger.error(f"❌ Critical error: {e}")
                logger.error(f"❌ Traceback: {traceback.format_exc()}")
                sys.exit(1)
    
    logger.info("=" * 70)
    logger.info("🏁 PRODUCER FINISHED")
    logger.info(f"⏰ End time: {datetime.now()}")
    logger.info("=" * 70) 