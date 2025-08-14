# -*- coding: utf-8 -*-
"""
Monitors UIT, Alwar:
  1) UDH Live E-Auctions (UIT, Alwar plots)
  2) UIT Alwar site Auction page newsletters (docs)

- Compares with last-saved state in S3
- Sends ONE Telegram message per new plot/news item (HTML formatted, includes link)
- Saves current state back to S3 (separate keys for plots and news)

ENV VARS (required):
  BUCKET_NAME                         -> S3 bucket (e.g., jda-auction-list)
  OBJECT_KEY                          -> S3 key for plots state json (default: uit_alwar_plots.json)
  OBJECT_KEY_NEWS                     -> S3 key for news state json  (default: uit_alwar_news.json)

Notifications (optional; if not set, script skips notify step):
  TELEGRAM_BOT_TOKEN                  -> Telegram bot token from @BotFather
  TELEGRAM_CHAT_ID                    -> Target chat/channel/group id

Optional tuning / resiliency:
  TELEGRAM_MESSAGE_DELAY_MS           -> ms delay between sends (default 400)
  TELEGRAM_MAX_MESSAGES               -> safety cap per run (default 50)

AWS creds:
  Use IAM creds with s3:ListBucket on the bucket, and s3:GetObject/s3:PutObject on OBJECT_KEY & OBJECT_KEY_NEWS.
"""

from __future__ import annotations

import datetime
import hashlib
import json
import logging
import os
import time
from typing import Dict, List

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
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
    logger.addHandler(_h)

# -----------------------
# Config / Env
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

NEWS_BASE = "http://uitalwar.rajasthan.gov.in"
NEWS_URL = f"{NEWS_BASE}/Auction.aspx"

# -----------------------
# HTTP helpers
# -----------------------
def _get(session: requests.Session, url: str, params: dict | None = None) -> BeautifulSoup:
    """
    GET with a browser-ish User-Agent and optional params (3.11 syntax).
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    }
    logger.info(f"HTTP GET {url} params={params or {}}")
    resp = session.get(url, params=params, headers=headers, timeout=30)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")

def fetch_unit_wise_summary(session: requests.Session) -> BeautifulSoup:
    """Fetch the 'Live E-Auctions' summary page with a cache buster."""
    return _get(session, SUMMARY_URL, params={"_": "nocache"})

# -----------------------
# Summary -> UIT, Alwar link
# -----------------------
def extract_uit_alwar_link(soup: BeautifulSoup) -> str:
    """
    Find the UIT, Alwar row in the Unit Wise Summary table and return the first link href.
    NOTE: Unit Name is in the 2nd column (index 1). First column is S.No.
    Raises ValueError if UIT, Alwar is not found.
    """
    hdr = soup.find(lambda tag: tag.name in ("h2", "h3", "h4") and "Unit Wise Summary" in tag.get_text(strip=True))
    table = hdr.find_next("table") if hdr else soup.find("table")
    if not table:
        tables = soup.find_all("table")
        if not tables:
            raise ValueError("Could not find unit summary table")
        table = tables[0]

    # Log all available UIT entries for debugging
    available_units = []
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) >= 2:
            unit_name = " ".join(tds[1].get_text(strip=True).split())
            if unit_name.lower().startswith("uit"):
                available_units.append(unit_name)
    
    if available_units:
        logger.info(f"Available UIT units found: {available_units}")
    else:
        logger.warning("No UIT units found in the summary table")

    # Primary search: look for UIT, Alwar specifically
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) >= 2:
            unit_name = " ".join(tds[1].get_text(strip=True).split())
            if unit_name.lower().startswith("uit, alwar"):
                a = tr.find("a", href=True)
                if a:
                    href = requests.compat.urljoin(SUMMARY_URL, a["href"])
                    logger.info(f"Found UIT, Alwar link: {href}")
                    return href

    # Fallback: scan row text for both "uit" and "alwar"
    for tr in table.find_all("tr"):
        row_text = " ".join(tr.get_text(" ", strip=True).split()).lower()
        if "uit" in row_text and "alwar" in row_text:
            a = tr.find("a", href=True)
            if a:
                href = requests.compat.urljoin(SUMMARY_URL, a["href"])
                logger.info(f"Found UIT, Alwar link via fallback scan: {href}")
                return href

    # Provide a more informative error message
    error_msg = f"UIT, Alwar row not found in summary table. Available UIT units: {available_units}"
    logger.error(error_msg)
    raise ValueError(error_msg)

# -----------------------
# UIT, Alwar detail -> schemes list
# -----------------------
def fetch_scheme_list(session: requests.Session, detail_url: str) -> list[dict[str, str]]:
    """
    Parse the detail page showing schemes (name + count link) -> return list[{scheme_name, href, count}]
    """
    soup = _get(session, detail_url)
    table = soup.find("table")
    if not table:
        logger.warning("No schemes table found on UIT, Alwar detail page")
        return []

    out: list[dict[str, str]] = []
    rows = table.find_all("tr")
    # assume first row is header
    for row in rows[1:]:
        cols = row.find_all("td")
        if len(cols) < 3:
            continue
        scheme_name = cols[1].get_text(strip=True)
        link = cols[2].find("a", href=True)
        count_text = cols[2].get_text(strip=True)
        href = requests.compat.urljoin(detail_url, link["href"]) if link else None
        out.append({"scheme_name": scheme_name, "href": href, "count": count_text})
    logger.info(f"Schemes found: {len(out)}")
    return out

# -----------------------
# Scheme page -> plot details (with optional detail_url)
# -----------------------
def fetch_plot_details(session: requests.Session, scheme_url: str) -> list[dict[str, str]]:
    """
    Parse scheme page with "Auction Details" list. Return plots[]
    Each plot dict includes:
      id, title, scheme_name, property_number, area, usage_type, emd_start, emd_end, emd_amount, bid_start, bid_end, assessed_value, detail_url?
    """
    soup = _get(session, scheme_url)
    result: list[dict[str, str]] = []

    # The page tends to have an UL/LI list with lines like "Id :", "Title :", etc.
    # We'll treat a new "Id :" as start of a new plot block.
    lis = soup.find_all("li")
    plot: dict[str, str] = {}

    def flush():
        nonlocal plot
        if plot:
            result.append(plot)
            plot = {}

    def capture_link_from_li(li) -> str | None:
        a = li.find("a", href=True)
        if a and a["href"]:
            return requests.compat.urljoin(scheme_url, a["href"])
        return None

    for li in lis:
        text = li.get_text(" ", strip=True)
        if not text:
            continue

        # If this LI contains a link, keep the first one as a potential detail link
        href = capture_link_from_li(li)
        if href and "detail_url" not in plot:
            plot["detail_url"] = href

        if text.startswith("Id :"):
            # new plot starts
            flush()
            plot["id"] = text.split(":", 1)[1].strip()
            continue

        # field mappings
        pairs = [
            ("Title :", "title"),
            ("Scheme Name :", "scheme_name"),
            ("Property Number :", "property_number"),
            ("Property Area :", "area"),
            ("Usage Type :", "usage_type"),
            ("EMD Deposit Start Date :", "emd_start"),
            ("EMD Deposit End Date :", "emd_end"),
            ("EMD Amount", "emd_amount"),
            ("Bid Start Date :", "bid_start"),
            ("Bid End Date :", "bid_end"),
        ]
        matched = False
        for prefix, key in pairs:
            if text.startswith(prefix):
                plot[key] = text.split(":", 1)[1].strip()
                matched = True
                break
        if matched:
            continue

        if "Assessed Property Value" in text:
            parts = text.split(":", 1)
            if len(parts) > 1:
                plot["assessed_value"] = parts[1].strip()

    flush()
    logger.info(f"Plots found on scheme page: {len(result)}")
    return result

# -----------------------
# UIT Alwar Newsletter scrape (by exact table id)
# -----------------------
def fetch_newsletters(session: requests.Session) -> list[dict[str, str]]:
    """
    Scrape http://uitalwar.rajasthan.gov.in/Auction.aspx
    Table: id='ContentPlaceHolder1_gridview1'
    Columns (by index):
      0: Sr.No.
      1: Auction Date
      2: Auction Detail
      3: Venue and Time for Auction
      4: Uploaded File (anchor)
    Returns items with keys: id, date, detail, venue_time, url, title
    """
    soup = _get(session, NEWS_URL, params={"_": "nocache"})
    table = soup.find("table", id="ContentPlaceHolder1_gridview1")
    if not table:
        logger.warning("News table not found: ContentPlaceHolder1_gridview1")
        return []

    items: list[dict[str, str]] = []
    rows = table.find_all("tr")
    for tr in rows:
        ths = tr.find_all("th")
        if ths:
            # header row -> skip
            continue
        tds = tr.find_all("td")
        if len(tds) < 5:
            continue

        date_txt = tds[1].get_text(" ", strip=True)
        detail_txt = tds[2].get_text(" ", strip=True)
        venue_txt = tds[3].get_text(" ", strip=True)

        # Uploaded file link
        a = tds[4].find("a", href=True)
        url = requests.compat.urljoin(NEWS_URL, a["href"]) if a else ""
        title = a.get_text(" ", strip=True) if a else "View Document"

        # Make a stable ID (prefer the file URL if available)
        key_src = url or f"{date_txt}|{detail_txt}|{venue_txt}"
        digest = hashlib.sha256(key_src.encode("utf-8")).hexdigest()[:16]

        items.append({
            "id": digest,
            "date": date_txt,
            "detail": detail_txt,
            "venue_time": venue_txt,
            "url": url,
            "title": title,
        })

    logger.info("Newsletters discovered (table rows): %d", len(items))
    return items

# -----------------------
# State: S3 read/write
# -----------------------
def load_json(s3_client: boto3.client, key: str) -> list[dict[str, str]]:
    try:
        resp = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
        body = resp["Body"].read().decode("utf-8")
        return json.loads(body) if body else []
    except botocore.exceptions.ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
            return []
        raise

def save_json(s3_client: boto3.client, key: str, payload: list[dict[str, str]]) -> None:
    s3_client.put_object(Bucket=BUCKET_NAME, Key=key, Body=json.dumps(payload, ensure_ascii=False))

# -----------------------
# Telegram notifications (per-item)
# -----------------------
def _fmt(val: str | None) -> str:
    return (val or "").strip()

def _build_plot_message_html(p: dict[str, str]) -> str:
    link_html = ""
    if p.get("detail_url"):
        link_html = f'\n<a href="{_fmt(p["detail_url"])}">🔗 View Plot Details</a>'

    parts = [
        f"🏗️ <b>UIT, Alwar – New Plot</b>",
        f"🆔 <b>ID:</b> {_fmt(p.get('id'))}",
        f"🏷️ <b>Title:</b> {_fmt(p.get('title'))}",
        f"📍 <b>Scheme:</b> {_fmt(p.get('scheme_name'))}",
        f"🏢 <b>Property #:</b> {_fmt(p.get('property_number'))}",
        f"📐 <b>Area:</b> {_fmt(p.get('area'))}",
        f"🏢 <b>Usage:</b> {_fmt(p.get('usage_type'))}",
        f"📅 <b>EMD:</b> {_fmt(p.get('emd_start'))} → {_fmt(p.get('emd_end'))}  (Amt: {_fmt(p.get('emd_amount'))})",
        f"📅 <b>Bid:</b> {_fmt(p.get('bid_start'))} → {_fmt(p.get('bid_end'))}",
        f"💰 <b>Assessed Value:</b> {_fmt(p.get('assessed_value'))}",
    ]
    return "\n".join(parts) + link_html

def _build_news_message_html(n: dict[str, str]) -> str:
    parts = [
        "📰 <b>UIT, Alwar – New Auction Newsletter</b>",
        f"📅 <b>Auction Date:</b> { _fmt(n.get('date')) }",
        f"📄 <b>Detail:</b> { _fmt(n.get('detail')) }",
        f"📍 <b>Venue & Time:</b> { _fmt(n.get('venue_time')) }",
    ]
    url = _fmt(n.get("url"))
    title = _fmt(n.get("title")) or "View Document"
    if url:
        parts.append(f'<a href="{url}">📄 {title}</a>')
    return "\n".join(parts)

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

def send_telegram_messages(items: list[dict[str, str]], builder) -> None:
    """
    Send one message per item using `builder(item) -> HTML text`.
    """
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        logger.warning("Telegram creds not set; skipping notification step.")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    sent = 0
    for it in items:
        if sent >= TELEGRAM_MAX_MESSAGES:
            logger.warning("Hit TELEGRAM_MAX_MESSAGES cap (%s). Not sending more.", TELEGRAM_MAX_MESSAGES)
            break

        msg = builder(it)
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": msg,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        try:
            r = requests.post(url, data=payload, timeout=20)
            r.raise_for_status()
            sent += 1
            logger.info("Sent Telegram message for item id=%s", it.get("id"))
        except Exception as e:
            logger.warning("Failed to send Telegram message for item id=%s: %s", it.get("id"), e)

        time.sleep(TELEGRAM_MESSAGE_DELAY_MS / 1000.0)

# -----------------------
# Main handler
# -----------------------
def lambda_handler(event, context):
    if not BUCKET_NAME:
        logger.error("Missing BUCKET_NAME")
        return {"statusCode": 500, "body": "Missing BUCKET_NAME"}

    session = requests.Session()
    s3 = boto3.client("s3")

    # Initialize default values
    all_plots = []
    new_plots = []
    news_now = []
    new_news = []
    
    # ====== PLOTS ======
    try:
        logger.info("Starting plot parsing...")
        summary = fetch_unit_wise_summary(session)
        try:
            detail_link = extract_uit_alwar_link(summary)
            schemes = fetch_scheme_list(session, detail_link)

            for s in schemes:
                if not s.get("href"):
                    continue
                plots = fetch_plot_details(session, s["href"])
                for p in plots:
                    p.setdefault("scheme_name", s.get("scheme_name"))
                    # If no detail_url captured from LI, fallback to scheme page (at least something clickable)
                    p.setdefault("detail_url", s.get("href"))
                all_plots.extend(plots)

            prev_plots = load_json(s3, OBJECT_KEY)
            prev_ids = {x.get("id") for x in prev_plots if x.get("id")}
            new_plots = [p for p in all_plots if p.get("id") and p["id"] not in prev_ids]
            save_json(s3, OBJECT_KEY, all_plots)
            
            if new_plots:
                send_telegram_messages(new_plots, _build_plot_message_html)
                logger.info(f"Sent notifications for {len(new_plots)} new plots")
            else:
                today = datetime.date.today().strftime("%d-%m-%Y")
                send_telegram_message(f"ℹ️ No new plots found today ({today}).")
                
        except ValueError as e:
            # Handle case where UIT, Alwar is not found
            logger.warning(f"UIT, Alwar not found in current auctions: {e}")
            today = datetime.date.today().strftime("%d-%m-%Y")
            send_telegram_message(f"⚠️ UIT, Alwar not found in current auctions ({today}). {str(e)}")
            # Keep all_plots and new_plots as empty lists
            
    except Exception as e:
        logger.exception("Plot parsing failed")
        today = datetime.date.today().strftime("%d-%m-%Y")
        send_telegram_message(f"❌ Plot parsing failed ({today}): {str(e)}")

    # ====== NEWSLETTERS ======
    try:
        logger.info("Starting newsletter parsing...")
        news_now = fetch_newsletters(session)
        prev_news = load_json(s3, OBJECT_KEY_NEWS)
        prev_news_ids = {x.get("id") for x in prev_news if x.get("id")}
        new_news = [n for n in news_now if n.get("id") and n["id"] not in prev_news_ids]
        save_json(s3, OBJECT_KEY_NEWS, news_now)
        
        if new_news:
            send_telegram_messages(new_news, _build_news_message_html)
            logger.info(f"Sent notifications for {len(new_news)} new newsletters")
        else:
            today = datetime.date.today().strftime("%d-%m-%Y")
            send_telegram_message(f"ℹ️ No new newsletters found today ({today}).")
            
    except Exception as e:
        logger.exception("Newsletter parsing failed")
        today = datetime.date.today().strftime("%d-%m-%Y")
        send_telegram_message(f"❌ Newsletter parsing failed ({today}): {str(e)}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "total_plots": len(all_plots),
            "new_plots": len(new_plots),
            "total_news": len(news_now),
            "new_news": len(new_news),
        })
    }

# -----------------------
# Allow running via `python lambda_function.py`
# -----------------------
if __name__ == "__main__":
    import sys
    try:
        res = lambda_handler({}, {})
        print("[Runner] lambda_handler() returned:", res)
        code = 0 if isinstance(res, dict) and res.get("statusCode") == 200 else 1
        sys.exit(code)
    except Exception as e:
        import traceback
        print("[Runner] Unhandled exception:", e)
        traceback.print_exc()
        sys.exit(1)