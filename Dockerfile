FROM nvidia/cuda:12.1.1-cudnn8-runtime-ubuntu22.04

RUN apt-get update && apt-get install -y \
    python3-pip python3-dev git wget unzip ffmpeg libsm6 libxext6 \
    fonts-liberation libappindicator3-1 libasound2 libatk-bridge2.0-0 \
    libatk1.0-0 libcups2 libdbus-1-3 libgdk-pixbuf2.0-0 libnspr4 \
    libnss3 libx11-xcb1 libxcomposite1 libxdamage1 libxrandr2 \
    xdg-utils libu2f-udev libvulkan1 libdrm2 libgbm1 \
    wget gnupg \
    && rm -rf /var/lib/apt/lists/*

# Cài đặt Google Chrome
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - && \
    echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list && \
    apt-get update && \
    apt-get install -y google-chrome-stable && \
    rm -rf /var/lib/apt/lists/*

# Cài đặt chromedriver đúng version với Google Chrome 138.0.7204.157
RUN wget -O /tmp/chromedriver.zip https://edgedl.me.gvt1.com/edgedl/chrome/chrome-for-testing/138.0.7204.157/linux64/chromedriver-linux64.zip && \
    unzip /tmp/chromedriver.zip -d /tmp/ && \
    mv /tmp/chromedriver-linux64/chromedriver /usr/local/bin/chromedriver && \
    chmod +x /usr/local/bin/chromedriver && \
    rm -rf /tmp/chromedriver.zip /tmp/chromedriver-linux64

RUN ln -s /usr/bin/python3 /usr/bin/python
RUN pip install --upgrade pip

RUN pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu121

# Copy toàn bộ code pipeline và TikTok-Content-Scraper vào image
COPY pipeline /app/pipeline
COPY TikTok-Content-Scraper /app/TikTok-Content-Scraper
WORKDIR /app/pipeline

RUN pip install -v --no-cache-dir -r requirements.txt
RUN pip install -v --no-cache-dir -r requirements-airflow.txt --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.3/constraints-3.8.txt"

ENV CUDA_VISIBLE_DEVICES=0
ENV PYTORCH_CUDA_ALLOC_CONF=max_split_size_mb:128
ENV PATH="/usr/local/bin/chromedriver:${PATH}"

# Sau khi build, để chạy đúng file, chỉ cần:
# docker run --rm -it tiktok_kidguard_pipeline python producer.py --hashtags "funny,viral,trending" --num_videos 3 --topic Test_with_MongoDB11 --browser chrome
# KHÔNG cần prefix pipeline/ nữa vì WORKDIR đã là /app/pipeline