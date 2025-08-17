import re, time, random, pathlib
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.edge.options import Options as EdgeOptions
import argparse
import tempfile
import uuid
import os
import shutil

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def setup_driver(browser: str = "chrome"):
    """Setup WebDriver với cấu hình tối ưu để tránh bị phát hiện bot"""
    if browser.lower() == "chrome":
        opts = ChromeOptions()
    else:
        opts = EdgeOptions()
    
    # REMOVED --headless để tránh bị phát hiện bot
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-web-security")
    opts.add_argument("--disable-features=VizDisplayCompositor")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-plugins")
    opts.add_argument("--disable-images")  # Tăng tốc độ load
    opts.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
    )
    # Tạo thư mục user-data-dir tạm thời duy nhất ở /dev/shm nếu có, hoặc cwd
    tmp_dir_base = "/dev/shm" if os.path.exists("/dev/shm") else os.getcwd()
    user_data_dir = tempfile.mkdtemp(dir=tmp_dir_base)
    logger.info(f"Using user-data-dir: {user_data_dir}")
    opts.add_argument(f"--user-data-dir={user_data_dir}")
    if browser.lower() == "chrome":
        driver = webdriver.Chrome(opts)
    else:
        driver = webdriver.Edge(opts)
    
    driver.execute_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
    driver._user_data_dir = user_data_dir  # Gắn vào driver để dọn dẹp sau này
    return driver

# Cập nhật regex patterns để khớp với nhiều format URL TikTok
URL_RE = re.compile(r"https://www\.tiktok\.com/@[\w\.-]+/video/(\d+)")
VIEW_RE = re.compile(r"([\d,\.]+)\s*[Vv]iews?")

def quick_views(a):
    """Extract view count từ element"""
    try:
        m = VIEW_RE.search(a.find_element(By.XPATH, ".//..").text)
        return int(m.group(1).replace(",", "").replace(".", "")) if m else None
    except: 
        return None

def crawl_ids_only(hashtag, num_videos=5, output_file='tiktok_ids.txt', min_views=0, browser="chrome"):
    """Crawl TikTok video IDs với debug và error handling tốt hơn"""
    driver = setup_driver(browser)
    url = f"https://www.tiktok.com/tag/{hashtag.lstrip('#')}"
    ids = set()
    
    logger.info(f"🔍 Bắt đầu crawl #{hashtag}")
    logger.info(f"   URL: {url}")
    logger.info(f"   Target: {num_videos} videos")
    logger.info(f"   Browser: {browser}")
    
    try:
        # Step 1: Load trang
        logger.info("   🌐 Đang mở trang...")
        driver.get(url)
        time.sleep(10)  # Tăng thời gian chờ
        
        # Step 2: Refresh để đảm bảo load đầy đủ
        logger.info("   🔄 Đang refresh trang...")
        driver.refresh()
        time.sleep(10)  # Tăng thời gian chờ
        
        # Step 3: Kiểm tra trang có load thành công không
        page_title = driver.title
        logger.info(f"   📄 Page title: {page_title}")
        
        # Step 4: Debug - Kiểm tra có bao nhiêu link trên trang
        all_links = driver.find_elements(By.TAG_NAME, "a")
        logger.info(f"   🔗 Tổng số links trên trang: {len(all_links)}")
        
        # Step 5: Debug - In ra một số link đầu tiên để kiểm tra
        tiktok_links = []
        for i, link in enumerate(all_links[:15]):
            href = link.get_attribute("href") or ""
            if "tiktok.com" in href:
                tiktok_links.append(href)
                logger.info(f"      Link {i+1}: {href}")
        
        logger.info(f"   🎯 Số TikTok links tìm thấy: {len(tiktok_links)}")
        
        # Step 6: Nếu không tìm thấy TikTok links, thử scroll
        if len(tiktok_links) == 0:
            logger.info("   ⚠️ Không tìm thấy TikTok links, thử scroll...")
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(5)
            driver.execute_script("window.scrollBy(0, 1000);")
            time.sleep(3)
            
            # Kiểm tra lại sau khi scroll
            all_links = driver.find_elements(By.TAG_NAME, "a")
            logger.info(f"   🔗 Sau scroll: {len(all_links)} links")

        # Step 7: Crawl video IDs
        scroll = 0
        while len(ids) < num_videos and scroll < 25:
            new = 0
            current_links = driver.find_elements(By.TAG_NAME, "a")
            logger.info(f"   📜 Scroll {scroll+1}: Tìm thấy {len(current_links)} links")
            
            for a in current_links:
                href = a.get_attribute("href") or ""
                m = URL_RE.match(href)
                vid = m.group(1) if m else None
                
                if not vid or vid in ids: 
                    continue
                    
                if min_views > 0:
                    v = quick_views(a)
                    if v is not None and v < min_views: 
                        continue
                        
                ids.add(vid)
                new += 1
                logger.info(f"      ✅ Tìm thấy video ID: {vid}")
                
                if len(ids) >= num_videos: 
                    break
                    
            logger.info(f"   📊 Đã crawl được {len(ids)} video IDs cho #{hashtag}")
            
            if new:
                time.sleep(3)
            else:
                logger.info("   📜 Không tìm thấy video mới, scroll xuống...")
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(6)  # Tăng thời gian chờ
                driver.execute_script("window.scrollBy(0,1000);")
                time.sleep(2)
                scroll += 1
                
    except Exception as e:
        logger.error(f"   ❌ Lỗi trong quá trình crawl: {e}")
        import traceback
        logger.error(f"   ❌ Traceback: {traceback.format_exc()}")
    finally:
        logger.info("   🔚 Đóng trình duyệt...")
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
        
    ids = list(ids)[:num_videos]
    if not ids:
        logger.warning(f"⚠️ Không crawl được video ID nào cho #{hashtag}")
        logger.info("💡 Gợi ý khắc phục:")
        logger.info("   1. Thử hashtag khác (viral, trending, dance)")
        logger.info("   2. Kiểm tra kết nối internet")
        logger.info("   3. Thử chạy vào thời điểm khác")
        logger.info("   4. TikTok có thể đã thay đổi cấu trúc trang")
        logger.info("   5. Thử browser khác (edge thay vì chrome)")
    else:
        logger.info(f"🎉 Thành công! Đã crawl được {len(ids)} video IDs")
        
    pathlib.Path(output_file).write_text("\n".join(ids), encoding="utf-8")
    return ids

import time
import subprocess
import os

def improved_streaming_producer(hashtags, num_videos=5, topic_name='Test_with_MongoDB11', browser='chrome'):
    logger.info("🚀 Starting improved streaming producer (batch mode)...")
    for hashtag in hashtags:
        logger.info(f'--- Crawl & send for #{hashtag} ---')
        try:
            ids = crawl_ids_only(hashtag, num_videos, output_file='tiktok_ids.txt', browser=browser)
            logger.info(f"IDs: {ids}")
            if not ids:
                logger.warning(f"⏩ Bỏ qua #{hashtag} vì không crawl được ID nào.")
                continue
            if not os.path.exists('tiktok_ids.txt') or os.path.getsize('tiktok_ids.txt') == 0:
                logger.warning("⏩ File tiktok_ids.txt rỗng, bỏ qua.")
                continue
            # Send to Kafka
            try:
                result = subprocess.run(
                    [
                        "python",
                        "run_kafka_producer.py",
                        "--ids_file", "tiktok_ids.txt",
                        "--topic", topic_name
                    ],
                    check=True,
                    capture_output=True,
                    text=True
                )
                logger.info("✅ Kafka producer completed successfully")
                logger.info(result.stdout)
            except subprocess.CalledProcessError as e:
                logger.error("❌ Error running run_kafka_producer.py")
                logger.error(f"STDOUT: {e.stdout}")
                logger.error(f"STDERR: {e.stderr}")
        except Exception as e:
            logger.error(f"❌ Error processing hashtag #{hashtag}: {e}")
            continue

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--hashtags", type=str, default="funny,viral,trending,dance,comedy", help="Comma-separated hashtags")
    parser.add_argument("--num_videos", type=int, default=5)
    parser.add_argument("--topic", type=str, default="Test_with_MongoDB11")
    parser.add_argument("--browser", type=str, default="chrome", help="Chọn trình duyệt (chrome hoặc edge)")
    args = parser.parse_args()
    hashtags = [h.strip() for h in args.hashtags.split(",") if h.strip()]
    improved_streaming_producer(hashtags, num_videos=args.num_videos, topic_name=args.topic, browser=args.browser)