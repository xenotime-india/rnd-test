# -*- coding: utf-8 -*-
"""
Monitors UIT, Alwar auctions on the Rajasthan UDH portal.
- Scrapes UIT, Alwar schemes and plots
- Compares with last-saved state in S3
- Sends ONE Telegram message per new plot (HTML formatted, includes detail link when available)
- Saves current state back to S3

ENV VARS (required):
  BUCKET_NAME                         -> S3 bucket (e.g., jda-auction-list)
  OBJECT_KEY                          -> S3 key for state json (default: uit_alwar_plots.json)

Notifications (optional; if not set, script skips notify step):
  TELEGRAM_BOT_TOKEN                  -> Telegram bot token from @BotFather
  TELEGRAM_CHAT_ID                    -> Target chat/channel/group id

Optional tuning / resiliency:
  TELEGRAM_MESSAGE_DELAY_MS           -> ms delay between sends (default 400)
  TELEGRAM_MAX_MESSAGES               -> safety cap per run (default 50)
  FALLBACK_DETAIL_URL                 -> If summary parsing fails, use this direct URL for UIT, Alwar schemes

AWS creds:
  Use IAM creds with s3:ListBucket on the bucket, and s3:GetObject/s3:PutObject on OBJECT_KEY.
"""

import json
import logging
import os
import time
from typing import Dict, List, Optional

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

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
TELEGRAM_MESSAGE_DELAY_MS = int(os.environ.get("TELEGRAM_MESSAGE_DELAY_MS", "400"))
TELEGRAM_MAX_MESSAGES = int(os.environ.get("TELEGRAM_MAX_MESSAGES", "50"))

FALLBACK_DETAIL_URL = os.environ.get("FALLBACK_DETAIL_URL")  # optional manual override

BASE_URL = "https://udhonline.rajasthan.gov.in"
SUMMARY_URL = f"{BASE_URL}/Portal/AuctionListNew"

# -----------------------
# HTTP helpers
# -----------------------
def _get(session: requests.Session, url: str, params: dict | None = None) -> BeautifulSoup:
    """
    GET with a browser-ish User-Agent and optional params.
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
    """
    Fetch the 'Live E-Auctions' summary page with a cache buster.
    """
    return _get(session, SUMMARY_URL, params={"_": "nocache"})

# -----------------------
# Summary -> UIT, Alwar link
# -----------------------
def extract_uit_alwar_link(soup: BeautifulSoup) -> str:
    """
    Find the UIT, Alwar row in the Unit Wise Summary table and return the first link href.
    NOTE: Unit Name is in the 2nd column (index 1). First column is S.No.
    """
    # Try to locate the summary table, but be robust if headings differ
    hdr = soup.find(lambda tag: tag.name in ("h2", "h3", "h4") and "Unit Wise Summary" in tag.get_text(strip=True))
    table = hdr.find_next("table") if hdr else soup.find("table")
    if not table:
        # fallback: scan any tables
        tables = soup.find_all("table")
        if not tables:
            # ultimate fallback: manual override
            if FALLBACK_DETAIL_URL:
                logger.warning("No table found; using FALLBACK_DETAIL_URL.")
                return FALLBACK_DETAIL_URL
            raise ValueError("Could not find unit summary table")
        table = tables[0]

    # Primary: read unit from 2nd column
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

    # Fallback: scan row text
    for tr in table.find_all("tr"):
        row_text = " ".join(tr.get_text(" ", strip=True).split()).lower()
        if "uit" in row_text and "alwar" in row_text:
            a = tr.find("a", href=True)
            if a:
                href = requests.compat.urljoin(SUMMARY_URL, a["href"])
                logger.info(f"Found UIT, Alwar link via fallback scan: {href}")
                return href

    if FALLBACK_DETAIL_URL:
        logger.warning("Using FALLBACK_DETAIL_URL due to summary parse failure.")
        return FALLBACK_DETAIL_URL

    raise ValueError("UIT, Alwar row not found in summary table")

# -----------------------
# UIT, Alwar detail -> schemes list
# -----------------------
def fetch_scheme_list(session: requests.Session, detail_url: str) -> List[Dict[str, str]]:
    """
    Parse the detail page showing schemes (name + count link) -> return list[{scheme_name, href, count}]
    """
    soup = _get(session, detail_url)
    table = soup.find("table")
    if not table:
        logger.warning("No schemes table found on UIT, Alwar detail page")
        return []

    out: List[Dict[str, str]] = []
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
def fetch_plot_details(session: requests.Session, scheme_url: str) -> List[Dict[str, str]]:
    """
    Parse scheme page with "Auction Details" list. Return plots[]
    Each plot dict includes:
      id, title, scheme_name, property_number, area, usage_type, emd_start, emd_end, emd_amount, bid_start, bid_end, assessed_value, detail_url?
    """
    soup = _get(session, scheme_url)
    result: List[Dict[str, str]] = []

    # The page tends to have an UL/LI list with lines like "Id :", "Title :", etc.
    # We'll treat a new "Id :" as start of a new plot block.
    lis = soup.find_all("li")
    plot: Dict[str, str] = {}

    def flush():
        nonlocal plot
        if plot:
            result.append(plot)
            plot = {}

    def capture_link_from_li(li) -> Optional[str]:
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
            # if the current LI had a link, detail_url was already set above
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
# State: S3 read/write
# -----------------------
def load_previous_plots(s3_client: boto3.client) -> List[Dict[str, str]]:
    try:
        resp = s3_client.get_object(Bucket=BUCKET_NAME, Key=OBJECT_KEY)
        body = resp["Body"].read().decode("utf-8")
        return json.loads(body) if body else []
    except botocore.exceptions.ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
            return []
        raise

def save_current_plots(s3_client: boto3.client, plots: List[Dict[str, str]]) -> None:
    s3_client.put_object(Bucket=BUCKET_NAME, Key=OBJECT_KEY, Body=json.dumps(plots, ensure_ascii=False))

# -----------------------
# Telegram notifications (per-plot)
# -----------------------
def _fmt(val: Optional[str]) -> str:
    return (val or "").strip()

def _build_plot_message_html(p: Dict[str, str]) -> str:
    link_html = ""
    if p.get("detail_url"):
        link_html = f'\n<a href="{_fmt(p["detail_url"])}">🔗 View Plot Details</a>'

    parts = [
        f"<b>UIT, Alwar – New Plot</b>",
        f"<b>ID:</b> {_fmt(p.get('id'))}",
        f"<b>Title:</b> {_fmt(p.get('title'))}",
        f"<b>Scheme:</b> {_fmt(p.get('scheme_name'))}",
        f"<b>Property #:</b> {_fmt(p.get('property_number'))}",
        f"<b>Area:</b> {_fmt(p.get('area'))}",
        f"<b>Usage:</b> {_fmt(p.get('usage_type'))}",
        f"<b>EMD:</b> {_fmt(p.get('emd_start'))} → {_fmt(p.get('emd_end'))}  (Amt: {_fmt(p.get('emd_amount'))})",
        f"<b>Bid:</b> {_fmt(p.get('bid_start'))} → {_fmt(p.get('bid_end'))}",
        f"<b>Assessed Value:</b> {_fmt(p.get('assessed_value'))}",
    ]
    return "\n".join(parts) + link_html

def send_telegram_messages_per_plot(new_plots: List[Dict[str, str]]) -> None:
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        logger.warning("Telegram creds not set; skipping notification step.")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    sent = 0
    for p in new_plots:
        if sent >= TELEGRAM_MAX_MESSAGES:
            logger.warning("Hit TELEGRAM_MAX_MESSAGES cap (%s). Not sending more.", TELEGRAM_MAX_MESSAGES)
            break

        msg = _build_plot_message_html(p)
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
            logger.info("Sent Telegram message for plot id=%s", p.get("id"))
        except Exception as e:
            logger.warning("Failed to send Telegram message for plot id=%s: %s", p.get("id"), e)

        time.sleep(TELEGRAM_MESSAGE_DELAY_MS / 1000.0)

# -----------------------
# Main handler
# -----------------------
def lambda_handler(event, context):
    if not BUCKET_NAME:
        logger.error("Missing BUCKET_NAME")
        return {"statusCode": 500, "body": "Missing BUCKET_NAME"}

    session = requests.Session()
    try:
        # 1) Summary -> UIT, Alwar link
        summary = fetch_unit_wise_summary(session)
        detail_link = extract_uit_alwar_link(summary)

        # 2) UIT, Alwar schemes
        schemes = fetch_scheme_list(session, detail_link)

        # 3) Scrape plots per scheme
        all_plots: List[Dict[str, str]] = []
        for s in schemes:
            if not s.get("href"):
                continue
            plots = fetch_plot_details(session, s["href"])
            for p in plots:
                p.setdefault("scheme_name", s.get("scheme_name"))
                # If no detail_url captured from LI, fallback to scheme page (at least something clickable)
                p.setdefault("detail_url", s.get("href"))
            all_plots.extend(plots)

        # 4) Load previous, detect new by plot id
        s3 = boto3.client("s3")
        prev = load_previous_plots(s3)
        prev_ids = {x.get("id") for x in prev if x.get("id")}
        new_plots = [p for p in all_plots if p.get("id") and p["id"] not in prev_ids]

        logger.info(f"Total plots now: {len(all_plots)} | New: {len(new_plots)}")

        # 5) Save current for next run
        save_current_plots(s3, all_plots)

        # 6) Notify
        if new_plots:
            send_telegram_messages_per_plot(new_plots)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "total_plots": len(all_plots),
                "new_plots": len(new_plots),
            })
        }
    except Exception as exc:
        logger.exception("Execution failed")
        return {"statusCode": 500, "body": str(exc)}

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