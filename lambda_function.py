# -*- coding: utf-8 -*-
"""
Monitors UIT, Alwar auctions on the Rajasthan UDH portal.
- Scrapes UIT, Alwar schemes and plots
- Compares with last-saved state in S3
- Sends Telegram message if new plots appear
- Saves current state back to S3

ENV VARS (required):
  BUCKET_NAME               -> S3 bucket (e.g., jda-auction-list)
  OBJECT_KEY                -> S3 key for state json (default: uit_alwar_plots.json)

Notifications (optional; if not set, script skips notify step):
  TELEGRAM_BOT_TOKEN        -> Telegram bot token from @BotFather
  TELEGRAM_CHAT_ID          -> Target chat/channel/group id

AWS creds:
  Use IAM creds with s3:GetObject/s3:PutObject on the OBJECT_KEY and s3:ListBucket on the bucket.
"""

import json
import logging
import os
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

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

BASE_URL = "https://udhonline.rajasthan.gov.in"

SUMMARY_URL = f"{BASE_URL}/Portal/AuctionListNew"
# Example detail urls look like: /Portal/LiveAuctionDetailReport?q=...
# Scheme drill-down pages are HTML lists we parse for plots.


# -----------------------
# HTTP helpers
# -----------------------
def _get(session: requests.Session, url: str) -> BeautifulSoup:
    logger.info(f"HTTP GET {url}")
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")


# -----------------------
# Scrape: summary -> UIT, Alwar detail link
# -----------------------
def fetch_unit_wise_summary(session: requests.Session) -> BeautifulSoup:
    return _get(session, SUMMARY_URL)


def extract_uit_alwar_link(soup: BeautifulSoup) -> str:
    """
    Find the UIT, Alwar row, return first numeric link (total/corner/etc.) href (absolute).
    """
    # Find the unit summary table by looking for "Unit Wise Summary" heading then the next <table>
    hdr = soup.find(lambda tag: tag.name in ("h2", "h3", "h4") and "Unit Wise Summary" in tag.get_text(strip=True))
    if not hdr:
        # Fallback: just take the first big table
        tables = soup.find_all("table")
        if not tables:
            raise ValueError("Could not find any table on summary page")
        table = tables[0]
    else:
        table = hdr.find_next("table")
        if not table:
            raise ValueError("Could not find unit summary table")

    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue
        unit_text = " ".join(tds[0].get_text(strip=True).split())
        if unit_text.lower().startswith("uit, alwar"):
            link = tr.find("a")
            if link and link.has_attr("href"):
                href = requests.compat.urljoin(SUMMARY_URL, link["href"])
                logger.info(f"Found UIT, Alwar link: {href}")
                return href
    raise ValueError("UIT, Alwar row not found in summary table")


# -----------------------
# Scrape: UIT, Alwar -> schemes list
# -----------------------
def fetch_scheme_list(session: requests.Session, detail_url: str) -> List[Dict[str, str]]:
    """
    Parse the detail page showing schemes (name + count link) -> return list[{scheme_name, href, count}]
    """
    soup = _get(session, detail_url)
    table = soup.find("table")
    if not table:
        logger.warning("No schemes table found on detail page (UIT, Alwar)")
        return []

    out: List[Dict[str, str]] = []
    rows = table.find_all("tr")
    for row in rows[1:]:
        cols = row.find_all("td")
        if len(cols) < 3:
            continue
        scheme_name = cols[1].get_text(strip=True)
        link = cols[2].find("a")
        count_text = cols[2].get_text(strip=True)
        href = requests.compat.urljoin(detail_url, link["href"]) if link and link.has_attr("href") else None
        out.append({"scheme_name": scheme_name, "href": href, "count": count_text})
    logger.info(f"Schemes found: {len(out)}")
    return out


# -----------------------
# Scrape: scheme page -> plots (Auction Details list)
# -----------------------
def fetch_plot_details(session: requests.Session, scheme_url: str) -> List[Dict[str, str]]:
    """
    Parse scheme page with "Auction Details" list. Return plots[]
    Each plot dict includes:
      id, title, scheme_name, property_number, area, usage_type, emd_start, emd_end, emd_amount, bid_start, bid_end, assessed_value
    """
    soup = _get(session, scheme_url)
    result: List[Dict[str, str]] = []

    # Pages structure: UL/LI list with "Id :", "Title :", etc.
    # We'll scan all <li> and create plot records.
    lis = soup.find_all("li")
    plot: Dict[str, str] = {}
    def flush():
        nonlocal plot
        if plot:
            result.append(plot)
            plot = {}

    for li in lis:
        text = li.get_text(" ", strip=True)
        # Normalize key:value splits
        if not text:
            continue
        # Start of a new plot indicated by "Id :"
        if text.startswith("Id :"):
            flush()
            plot["id"] = text.split(":", 1)[1].strip()
            continue

        # map by prefix
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
# Notify: Telegram
# -----------------------
def send_telegram_message(new_plots: List[Dict[str, str]]) -> None:
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        logger.warning("Telegram creds not set; skipping notification step.")
        return

    lines = [f"🆕 UIT, Alwar: {len(new_plots)} new plot(s)"]
    for p in new_plots[:20]:  # cap preview to avoid huge messages
        lines.append(
            "\n".join(filter(None, [
                f"• ID: {p.get('id')}",
                f"  Title: {p.get('title')}",
                f"  Scheme: {p.get('scheme_name')}",
                f"  Prop#: {p.get('property_number')}",
                f"  Area: {p.get('area')} | Usage: {p.get('usage_type')}",
                f"  EMD: {p.get('emd_start')} → {p.get('emd_end')} (Amt: {p.get('emd_amount')})",
                f"  Bid: {p.get('bid_start')} → {p.get('bid_end')}",
                f"  Value: {p.get('assessed_value')}",
            ]))
        )
    if len(new_plots) > 20:
        lines.append(f"\n…and {len(new_plots) - 20} more")

    msg = "\n".join(lines)

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": msg}
    try:
        r = requests.post(url, data=payload, timeout=20)
        r.raise_for_status()
        logger.info("Telegram message sent.")
    except Exception as e:
        logger.warning(f"Failed to send Telegram message: {e}")


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
            # Fill missing scheme_name if not present
            for p in plots:
                p.setdefault("scheme_name", s.get("scheme_name"))
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
            send_telegram_message(new_plots)

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
# (e.g., in GitHub Actions or locally)
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