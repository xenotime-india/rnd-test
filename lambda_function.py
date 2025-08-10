"""
AWS Lambda function to monitor UIT, Alwar plot auctions and send email alerts.

This function fetches the Urban Housing and Development Department (UDH) “Live E‑Auctions”
pages for UIT, Alwar, extracts the currently available plot auctions and compares them
against a previously stored list. When new plots are detected (plots that were not
present the last time the function ran), the function sends an email with details
of the new plots using Amazon Simple Email Service (SES). The current list of plots
is then saved to an S3 bucket so that the next run can perform the comparison.

Environment variables expected:

    BUCKET_NAME   – Name of the S3 bucket used to store the JSON file.
    OBJECT_KEY    – Key (path) within the S3 bucket for storing the JSON data.
    FROM_EMAIL    – Verified SES email address used as the sender.
    TO_EMAIL      – Comma‑separated list of recipient email addresses.

This code uses the `requests` and `bs4` (BeautifulSoup) libraries for scraping.
The websites used by UDH sometimes maintain state with session cookies.  The
implementation below first visits the Unit‑wise summary page, then follows
links to the UIT, Alwar details page and finally to each individual scheme’s
auction listing.  If the portal changes its structure, the selectors used here
may need to be updated.

Note: network restrictions in this environment prevent direct requests from
executing.  This code is provided as a template; it should be tested and
adjusted within an environment that allows outbound HTTP requests.
"""

import json
import os
import logging
from datetime import datetime
from typing import List, Dict

import boto3
import botocore.exceptions
import requests
from bs4 import BeautifulSoup

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment configuration
BUCKET_NAME = os.environ.get("BUCKET_NAME")
OBJECT_KEY = os.environ.get("OBJECT_KEY", "uit_alwar_plots.json")

# Notification settings:
# Instead of sending an email, this script can post updates to a Telegram
# channel or group.  To use Telegram notifications, create a bot via
# BotFather (https://t.me/BotFather), obtain its API token and add the bot
# to your target group/channel.  Then fetch the chat ID by sending a
# message and using the getUpdates API.
#
# Expected environment variables:
#   TELEGRAM_BOT_TOKEN – API token of your Telegram bot.
#   TELEGRAM_CHAT_ID   – Numeric ID of the group or channel where the
#                        notification should be sent.
#
# If these variables are set, messages will be sent via Telegram.  If
# they are not set, the send_message function will be a no‑op.
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

def fetch_unit_wise_summary(session: requests.Session) -> BeautifulSoup:
    """Fetch the unit wise summary page and return a BeautifulSoup object."""
    url = "https://udhonline.rajasthan.gov.in/Portal/AuctionListNew"
    response = session.get(url)
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")


def extract_uit_alwar_link(soup: BeautifulSoup) -> str:
    """
    Parse the summary page for UIT, Alwar and return the URL to its detailed
    listing.  The summary table contains links on the numeric counts.  This
    function picks the first numeric link for UIT, Alwar (total auctions) and
    returns its href.
    """
    # Locate the unit wise summary table by looking for the header "Unit Wise Summary"
    header = soup.find(text=lambda t: t and t.strip() == "Unit Wise Summary")
    if not header:
        raise ValueError("Could not locate Unit Wise Summary header in the page")
    # The table is immediately after the header
    table = header.find_next("table")
    if not table:
        raise ValueError("Could not locate summary table")
    rows = table.find_all("tr")
    for row in rows:
        cols = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
        if not cols:
            continue
        # We expect the first data column to be the unit name
        unit_name = cols[1] if cols[0].isdigit() else cols[0]
        if unit_name.lower().startswith("uit, alwar"):
            # Find the first anchor in this row – typically the total count
            link = row.find("a")
            if link and link.has_attr("href"):
                return requests.compat.urljoin("https://udhonline.rajasthan.gov.in", link["href"])
    raise ValueError("UIT, Alwar row not found in summary table")


def fetch_scheme_list(session: requests.Session, detail_url: str) -> List[Dict[str, str]]:
    """
    Given the detail URL for UIT, Alwar, fetch the page and return a list of
    schemes with their names and links (to scheme auction lists).
    Each entry in the returned list has the shape:
        {
            "scheme_name": <name>,
            "href": <absolute url to scheme auctions>,
            "count": <number of plots>
        }
    """
    resp = session.get(detail_url)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    scheme_table = soup.find("table")
    schemes = []
    if not scheme_table:
        logger.warning("No schemes table found in detail page")
        return schemes
    rows = scheme_table.find_all("tr")
    for row in rows[1:]:  # skip header row
        cols = row.find_all("td")
        if len(cols) >= 3:
            # Column 1 is S.No., Column 2 is scheme name, Column 3 contains the anchor with the count
            scheme_name = cols[1].get_text(strip=True)
            link = cols[2].find("a")
            count = cols[2].get_text(strip=True)
            href = None
            if link and link.has_attr("href"):
                href = requests.compat.urljoin(detail_url, link["href"])
            schemes.append({"scheme_name": scheme_name, "href": href, "count": count})
    return schemes


def fetch_plot_details(session: requests.Session, scheme_url: str) -> List[Dict[str, str]]:
    """
    Fetch the auction list for a given scheme and return a list of plot details.
    Each entry contains keys like id, title, property_number, area, usage_type,
    emd_start, emd_end, bid_start, bid_end and assessed_value.
    """
    resp = session.get(scheme_url)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    result = []
    # Locate the auction details section by searching for the phrase 'Auction Details'
    header = soup.find(text=lambda t: t and t.strip() == "Auction Details")
    if not header:
        logger.warning("Could not find auction details header on scheme page")
        return result
    # The details are presented in list format; parse each list entry.
    # Each plot starts with '* Id :' so we can search for that pattern.
    items = soup.find_all("li")
    current_plot = {}
    for item in items:
        text = item.get_text(separator=" ", strip=True)
        if text.startswith("Id :"):
            if current_plot:
                result.append(current_plot)
                current_plot = {}
            current_plot["id"] = text.split(":", 1)[1].strip()
        elif text.startswith("Title :"):
            current_plot["title"] = text.split(":", 1)[1].strip()
        elif text.startswith("Scheme Name :"):
            current_plot["scheme_name"] = text.split(":", 1)[1].strip()
        elif text.startswith("Property Number :"):
            current_plot["property_number"] = text.split(":", 1)[1].strip()
        elif text.startswith("Property Area :"):
            current_plot["area"] = text.split(":", 1)[1].strip()
        elif text.startswith("Usage Type :"):
            current_plot["usage_type"] = text.split(":", 1)[1].strip()
        elif text.startswith("EMD Deposit Start Date :"):
            current_plot["emd_start"] = text.split(":", 1)[1].strip()
        elif text.startswith("EMD Deposit End Date :"):
            current_plot["emd_end"] = text.split(":", 1)[1].strip()
        elif text.startswith("EMD Amount"):
            current_plot["emd_amount"] = text.split(":", 1)[1].strip()
        elif text.startswith("Bid Start Date :"):
            current_plot["bid_start"] = text.split(":", 1)[1].strip()
        elif text.startswith("Bid End Date :"):
            current_plot["bid_end"] = text.split(":", 1)[1].strip()
        elif "Assessed Property Value" in text:
            # Extract the numeric part from the assessed value line
            parts = text.split(":", 1)
            if len(parts) > 1:
                current_plot["assessed_value"] = parts[1].strip()
    if current_plot:
        result.append(current_plot)
    return result


def load_previous_plots(s3_client: boto3.client) -> List[Dict[str, str]]:
    """Load the previously stored plots list from S3."""
    try:
        obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=OBJECT_KEY)
        body = obj["Body"].read().decode("utf-8")
        return json.loads(body)
    except botocore.exceptions.ClientError as e:
        if e.response.get("Error", {}).get("Code") == "NoSuchKey":
            return []
        raise


def save_current_plots(s3_client: boto3.client, plots: List[Dict[str, str]]) -> None:
    """Save the current plots list to S3 as a JSON string."""
    s3_client.put_object(Bucket=BUCKET_NAME, Key=OBJECT_KEY, Body=json.dumps(plots))


def format_plots_message(new_plots: List[Dict[str, str]]) -> str:
    """
    Construct a message summarising the new plots.  The output is a plain
    text string suitable for sending via Telegram or other chat services.
    Each plot will be listed with its key details on separate lines.
    """
    header = f"🆕 *UIT, Alwar* – {len(new_plots)} new plot auction(s) found\n\n"
    lines = [header]
    for plot in new_plots:
        lines.append(
            f"*ID:* {plot.get('id')}\n"
            f"*Title:* {plot.get('title')}\n"
            f"*Property Number:* {plot.get('property_number')}\n"
            f"*Area:* {plot.get('area')}\n"
            f"*Usage:* {plot.get('usage_type')}\n"
            f"*EMD Start:* {plot.get('emd_start')}\n"
            f"*EMD End:* {plot.get('emd_end')}\n"
            f"*Bid Start:* {plot.get('bid_start')}\n"
            f"*Bid End:* {plot.get('bid_end')}\n"
            f"*Assessed Value:* {plot.get('assessed_value')}\n\n"
        )
    return "".join(lines)


def send_telegram_message(session: requests.Session, message: str) -> None:
    """
    Send a message via the Telegram Bot API.  Requires the TELEGRAM_BOT_TOKEN
    and TELEGRAM_CHAT_ID environment variables to be set.  If they are not
    provided, this function logs a warning and returns without sending.
    """
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        logger.warning("Telegram configuration missing; skipping notification")
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "Markdown"
        }
        resp = session.post(url, data=payload, timeout=10)
        resp.raise_for_status()
        logger.info("Telegram message sent successfully")
    except Exception as exc:
        logger.warning(f"Failed to send Telegram message: {exc}")


def lambda_handler(event, context):
    """Main lambda entry point."""
    # Ensure the S3 bucket is configured; telegram credentials are optional
    if not BUCKET_NAME:
        logger.error("Required environment variable BUCKET_NAME is missing")
        return {"statusCode": 500, "body": "Missing BUCKET_NAME"}
    session = requests.Session()
    try:
        # 1. Fetch summary and locate UIT, Alwar link
        summary_soup = fetch_unit_wise_summary(session)
        detail_link = extract_uit_alwar_link(summary_soup)
        logger.info(f"Detail link for UIT, Alwar: {detail_link}")

        # 2. Fetch scheme list for UIT, Alwar
        schemes = fetch_scheme_list(session, detail_link)
        logger.info(f"Found {len(schemes)} schemes for UIT, Alwar")
        all_plots: List[Dict[str, str]] = []
        for scheme in schemes:
            if not scheme.get("href"):
                continue
            plots = fetch_plot_details(session, scheme["href"])
            logger.info(f"Scheme {scheme['scheme_name']} has {len(plots)} plots")
            all_plots.extend(plots)

        # 3. Load previous plots from S3
        s3_client = boto3.client("s3")
        previous_plots = load_previous_plots(s3_client)
        previous_ids = {p["id"] for p in previous_plots}

        # 4. Identify new plots
        new_plots = [plot for plot in all_plots if plot.get("id") not in previous_ids]
        logger.info(f"Detected {len(new_plots)} new plots")

        # 5. Save current plots for next run
        save_current_plots(s3_client, all_plots)

        # 6. Send email if new plots exist
        if new_plots:
            # Compose and send a Telegram message summarising the new plots
            message = format_plots_message(new_plots)
            send_telegram_message(session, message)
        else:
            logger.info("No new plots to report")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Execution completed",
                "total_plots": len(all_plots),
                "new_plots": len(new_plots)
            })
        }
    except Exception as exc:
        logger.exception("Error during execution")
        return {"statusCode": 500, "body": str(exc)}