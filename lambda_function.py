# -*- coding: utf-8 -*-
"""
UIT, Alwar Monitor – Auctions & Newsletters

Features:
- Scrapes live e-auctions and newsletters
- Compares with last saved state in S3
- Sends Telegram messages (with emoji icons) for:
    - New plots found
    - New newsletters found
    - No new plots/news found (separate messages)
- Saves updated state to S3
"""

import datetime
import hashlib
import json
import logging
import os
import time
from typing import List, Dict
import boto3
import botocore.exceptions
import requests
from bs4 import BeautifulSoup

# -----------------------
# Logging
# -----------------------
logger = logging.getLogger("uit_alwar_monitor")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
    logger.addHandler(handler)

# -----------------------
# Config
# -----------------------
BUCKET_NAME = os.environ.get("BUCKET_NAME")
OBJECT_KEY = os.environ.get("OBJECT_KEY", "uit_alwar_plots.json")
OBJECT_KEY_NEWS = os.environ.get("OBJECT_KEY_NEWS", "uit_alwar_news.json")

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
TELEGRAM_MESSAGE_DELAY_MS = int(os.environ.get("TELEGRAM_MESSAGE_DELAY_MS", "400"))
TELEGRAM_MAX_MESSAGES = int(os.environ.get("TELEGRAM_MAX_MESSAGES", "50"))

BASE_URL = "https://udhonline.rajasthan.gov.in"
SUMMARY_URL = f"{BASE_URL}/Portal/AuctionListNew"
NEWS_URL = "http://uitalwar.rajasthan.gov.in/Auction.aspx"

# -----------------------
# HTTP helpers
# -----------------------
def _get(session, url, params=None) -> BeautifulSoup:
    headers = {"User-Agent": "Mozilla/5.0"}
    logger.info(f"GET {url}")
    resp = session.get(url, params=params, headers=headers, timeout=30)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")

# -----------------------
# Scraper functions
# -----------------------
def fetch_unit_wise_summary(session):
    return _get(session, SUMMARY_URL, params={"_": "nocache"})

def extract_uit_alwar_link(soup):
    table = soup.find("table")
    if not table:
        raise ValueError("Unit summary table not found")
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) >= 2 and "uit, alwar" in tds[1].get_text(strip=True).lower():
            a = tr.find("a", href=True)
            if a:
                return requests.compat.urljoin(SUMMARY_URL, a["href"])
    raise ValueError("UIT, Alwar link not found")

def fetch_scheme_list(session, detail_url):
    soup = _get(session, detail_url)
    table = soup.find("table")
    schemes = []
    for row in table.find_all("tr")[1:]:
        cols = row.find_all("td")
        if len(cols) >= 3:
            scheme_name = cols[1].get_text(strip=True)
            link = cols[2].find("a", href=True)
            href = requests.compat.urljoin(detail_url, link["href"]) if link else None
            schemes.append({"scheme_name": scheme_name, "href": href})
    return schemes

def fetch_plot_details(session, scheme_url):
    soup = _get(session, scheme_url)
    result = []
    lis = soup.find_all("li")
    plot = {}

    def flush():
        nonlocal plot
        if plot:
            result.append(plot)
            plot = {}

    for li in lis:
        text = li.get_text(" ", strip=True)
        if not text:
            continue
        if text.startswith("Id :"):
            flush()
            plot["id"] = text.split(":", 1)[1].strip()
            continue
        mapping = {
            "Title :": "title",
            "Scheme Name :": "scheme_name",
            "Property Number :": "property_number",
            "Property Area :": "area",
            "Usage Type :": "usage_type",
            "EMD Deposit Start Date :": "emd_start",
            "EMD Deposit End Date :": "emd_end",
            "EMD Amount": "emd_amount",
            "Bid Start Date :": "bid_start",
            "Bid End Date :": "bid_end",
        }
        for prefix, key in mapping.items():
            if text.startswith(prefix):
                plot[key] = text.split(":", 1)[1].strip()
                break
        if "Assessed Property Value" in text:
            plot["assessed_value"] = text.split(":", 1)[1].strip()
    flush()
    return result

def fetch_newsletters(session):
    soup = _get(session, NEWS_URL)
    table = soup.find("table", id="ContentPlaceHolder1_gridview1")
    items = []
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) >= 5:
            date_txt = tds[1].get_text(strip=True)
            detail_txt = tds[2].get_text(strip=True)
            venue_txt = tds[3].get_text(strip=True)
            a = tds[4].find("a", href=True)
            url = requests.compat.urljoin(NEWS_URL, a["href"]) if a else ""
            key_src = url or f"{date_txt}|{detail_txt}|{venue_txt}"
            digest = hashlib.sha256(key_src.encode()).hexdigest()[:16]
            items.append({
                "id": digest,
                "date": date_txt,
                "detail": detail_txt,
                "venue_time": venue_txt,
                "url": url
            })
    return items

# -----------------------
# S3 helpers
# -----------------------
def load_json(s3_client, key):
    try:
        resp = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
        return json.loads(resp["Body"].read().decode("utf-8"))
    except botocore.exceptions.ClientError:
        return []

def save_json(s3_client, key, payload):
    s3_client.put_object(Bucket=BUCKET_NAME, Key=key, Body=json.dumps(payload, ensure_ascii=False))

# -----------------------
# Telegram helpers
# -----------------------
def send_telegram_message(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram not configured")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    requests.post(url, data={
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    })

def build_plot_message(p):
    return (
        f"🏗️ <b>New Plot Found!</b>\n"
        f"🆔 <b>ID:</b> {p.get('id')}\n"
        f"🏷️ <b>Title:</b> {p.get('title')}\n"
        f"📍 <b>Scheme:</b> {p.get('scheme_name')}\n"
        f"📐 <b>Area:</b> {p.get('area')}\n"
        f"🏢 <b>Usage:</b> {p.get('usage_type')}\n"
    )

def build_news_message(n):
    return (
        f"📰 <b>New Auction Newsletter!</b>\n"
        f"📅 <b>Date:</b> {n.get('date')}\n"
        f"📄 <b>Detail:</b> {n.get('detail')}\n"
        f"📍 <b>Venue:</b> {n.get('venue_time')}\n"
    )

# -----------------------
# Main
# -----------------------
def lambda_handler(event, context):
    session = requests.Session()
    s3 = boto3.client("s3")

    # Plots
    detail_link = extract_uit_alwar_link(fetch_unit_wise_summary(session))
    all_plots = []
    for scheme in fetch_scheme_list(session, detail_link):
        if scheme.get("href"):
            all_plots.extend(fetch_plot_details(session, scheme["href"]))
    prev_plots = load_json(s3, OBJECT_KEY)
    prev_ids = {x.get("id") for x in prev_plots}
    new_plots = [p for p in all_plots if p.get("id") not in prev_ids]
    save_json(s3, OBJECT_KEY, all_plots)
    if new_plots:
        for plot in new_plots:
            send_telegram_message(build_plot_message(plot))
    else:
        today = datetime.date.today().strftime("%d-%m-%Y")
        send_telegram_message(f"ℹ️ No new plots found today ({today}).")

    # Newsletters
    news_now = fetch_newsletters(session)
    prev_news = load_json(s3, OBJECT_KEY_NEWS)
    prev_news_ids = {x.get("id") for x in prev_news}
    new_news = [n for n in news_now if n.get("id") not in prev_news_ids]
    save_json(s3, OBJECT_KEY_NEWS, news_now)
    if new_news:
        for news in new_news:
            send_telegram_message(build_news_message(news))
    else:
        today = datetime.date.today().strftime("%d-%m-%Y")
        send_telegram_message(f"ℹ️ No new newsletters found today ({today}).")

    return {"statusCode": 200}

if __name__ == "__main__":
    lambda_handler({}, {})