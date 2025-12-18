FROM apache/airflow:3.1.2-python3.11

# =====================
# root 권한
# =====================
USER root

# 1) 시스템 패키지 + Chrome
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    curl wget gnupg \
    libgl1-mesa-glx libglib2.0-0 libsm6 libxrender1 libxext6 \
    libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 \
    libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 libxrandr2 \
    libgbm1 libasound2 libpango-1.0-0 libcairo2 \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

RUN wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | apt-key add - \
 && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" \
    > /etc/apt/sources.list.d/google-chrome.list \
 && apt-get update \
 && apt-get install -y google-chrome-stable \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

# =====================
# airflow 사용자
# =====================
USER airflow

# 2) Airflow용 Python 패키지
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# 3) dbt 전용 venv (🔥 핵심 🔥)
USER root

RUN python -m venv /opt/dbt_venv \
 && /opt/dbt_venv/bin/pip install --upgrade pip setuptools wheel \
 && /opt/dbt_venv/bin/pip install --no-cache-dir \
    dbt-core==1.6.8 \
    dbt-postgres==1.6.8 \
    dbt-redshift==1.6.4 \
 && chown -R airflow:0 /opt/dbt_venv

# 다시 airflow 사용자로
USER airflow
