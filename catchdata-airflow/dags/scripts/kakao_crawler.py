import time
import json
import requests
import pandas as pd
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC    

import cv2
import numpy as np
from datetime import datetime


# ============================
# Selenium Driver 생성 (Chrome 설치 없이 실행 가능)
# ============================
def create_driver():
    chrome_options = Options()
    chrome_options.binary_location = "/opt/chrome/chrome" 
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-infobars")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-software-rasterizer")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--headless=new")

    service = Service("/usr/local/bin/chromedriver")
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver


# ============================
# Kakao API
# ============================
REST_API_KEY = "dc4d2dc6723659ce3f612ba8eafdd9eb"
headers = {"Authorization": f"KakaoAK {REST_API_KEY}"}


# ============================
# Canvas → Hourly Visit
# ============================
def extract_hourly_visit_from_canvas_image(img_path):
    img = cv2.imread(img_path)
    if img is None:
        return None

    h, w, _ = img.shape
    hsv = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)

    lower_color = np.array([90, 40, 40])
    upper_color = np.array([250, 180, 255])
    mask = cv2.inRange(hsv, lower_color, upper_color)

    x_positions = [int((i + 0.5) * w / 24) for i in range(24)]
    values = []

    for x in x_positions:
        ys = np.where(mask[:, x] > 0)[0]
        if len(ys) == 0:
            values.append(np.nan)
        else:
            y = ys[0]
            values.append(round((h - y) / h * 100, 1))

    clean = np.array(values)
    idx = np.arange(24)
    nan_mask = np.isnan(clean)

    if np.any(~nan_mask):
        clean[nan_mask] = np.interp(idx[nan_mask], idx[~nan_mask], clean[~nan_mask])

    return clean.tolist()


# ============================
# Kakao API 검색
# ============================
def collect_places_single_keyword(query, address_keyword):
    url = "https://dapi.kakao.com/v2/local/search/keyword.json"
    all_results = []

    for page in range(1, 46):
        params = {"query": query, "size": 15, "page": page}
        res = requests.get(url, params=params, headers=headers).json()
        docs = res.get("documents", [])

        if not docs:
            break

        all_results.extend(docs)
        time.sleep(0.2)

    df = pd.DataFrame(all_results)

    addr_col = "address_name" if "address_name" in df.columns else "address"
    df = df[df[addr_col].str.startswith(address_keyword)]
    df = df[df["category_group_code"] == "FD6"]

    return df.to_dict(orient="records")


# ============================
# 상세 페이지 크롤링
# ============================
def crawl_detail(driver, place_url):
    time.sleep(0.6)
    driver.get(place_url)
    time.sleep(1.2)

    soup = BeautifulSoup(driver.page_source, "html.parser")

    data = {}
    data["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    rating = soup.select_one("span.num_star")
    blog = soup.select_one("a.link_blog span.info_num")
    review = soup.select_one("a.link_reviewall strong.tit_total")

    data["rating"] = rating.text.strip() if rating else None
    data["blog_count"] = blog.text.strip() if blog else None
    data["review_count"] = review.text.replace("후기", "").strip() if review else None

    business_hours = []
    for line in soup.select("div.fold_detail div.line_fold"):
        day_tag = line.select_one("span.tit_fold")
        if not day_tag:
            continue

        day = day_tag.text.strip()
        times = [t.text.strip() for t in line.select("span.txt_detail")]
        business_hours.append({"day": day, "times": times})

    data["business_hours"] = business_hours

    try:
        wait = WebDriverWait(driver, 5)
        canvas = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.view_chart canvas")))

        driver.save_screenshot("page_full.png")
        canvas.screenshot("temp_canvas.png")
        data["hourly_visit"] = extract_hourly_visit_from_canvas_image("temp_canvas.png")
    except:
        data["hourly_visit"] = None

    return data


# ============================
# 지역 크롤링 (limit 지원)
# ============================
def run_region_crawler(records):
    driver = create_driver()
    final_results = []
    
    for info in records:
        detail = crawl_detail(driver, info["place_url"])
        merged = {
            "place_id": info["id"],
            "name": info["place_name"],
            "address": info.get("address_name", info.get("address", "")),
            "road_address": info.get("road_address_name", ""),
            "category": info["category_name"],
            "category_group_name": info["category_group_name"],
            "category_group_code": info["category_group_code"],
            "phone": info.get("phone"),
            "x": info.get("x"),
            "y": info.get("y"),
            "place_url": info["place_url"],
            "detail": detail
        }
        final_results.append(merged)

    driver.quit()
    return final_results
