import os
import requests
import xml.etree.ElementTree as ET
import uuid
import time
import random
from datetime import datetime, timezone
from urllib.parse import urlparse
from bs4 import BeautifulSoup 
from dotenv import load_dotenv
import psycopg
import boto3
from botocore.client import Config
import webpage_summarizer
import re
import generate_summary_for_news
#import random
import html
load_dotenv()
PG_CONN = psycopg.connect(
    host=os.environ['PG_HOST'],
    port=os.environ['PG_PORT'],
    dbname=os.environ['PG_DATABASE'],
    user=os.environ['PG_USER'],
    password=os.environ['PG_PASSWORD'],
    autocommit=True
    #connect_timeout=600
)

s3 = boto3.client(
    's3',
    endpoint_url=os.environ['S3_ENDPOINT'],
    aws_access_key_id=os.environ['S3_ACCESS_KEY'],
    aws_secret_access_key=os.environ['S3_SECRET_KEY'],
    region_name=os.environ['S3_REGION'],
    config=Config(signature_version='s3v4')
)
BUCKET = os.environ['S3_BUCKET']
USER_AGENT = "13F Downloader (guddu.kumar@aitoxr.com)"

# Correct namespaces from the XML
namespaces = {
    'dc': 'http://purl.org/dc/elements/1.1/',
    'nasdaq': 'http://nasdaq.com/reference/feeds/1.0'
}

def load_existing_links_from_db():
    """Fetch existing GUIDs and links from news.raw table to avoid duplicates."""
    existing_guids = set()
    existing_links = set()
    with PG_CONN.cursor() as cur:
        cur.execute("SELECT guid, link FROM news.raw")
        for guid, link in cur.fetchall():
            if guid:
                existing_guids.add(guid.strip())
            if link:
                existing_links.add(link.strip())
    return existing_guids, existing_links

def parse_rss_items(rss_content, existing_guids, existing_links, source):
    soup = BeautifulSoup(rss_content, 'xml')  # Use lenient XML parser
    items = []
    for item in soup.find_all('item'):
        guid = item.find('guid').get_text(strip=True) if item.find('guid') else ''
        link = item.find('link').get_text(strip=True) if item.find('link') else ''
        
        if guid and guid in existing_guids:
            continue
        if link and link in existing_links:
            continue

        title = html.unescape(item.title.get_text(strip=True)) if item.title else ''
        description = html.unescape(item.description.get_text(strip=True)) if item.description else ''

        data = {
            'title': title,
            'description': description,
            'link': link,
            'guid': guid,
            'pubDate': item.pubDate.get_text(strip=True) if item.pubDate else '',
            'source': source,
            'category': item.category.get_text(strip=True) if item.category else '',
            'tags': item.find('nasdaq:tickers').get_text(strip=True) if item.find('nasdaq:tickers') else '',
            'creator': item.find('dc:creator').get_text(strip=True) if item.find('dc:creator') else ''
        }
        items.append(data)
    return items

def save_normalized_data(items):
    now = datetime.now(timezone.utc).isoformat()
    with PG_CONN.cursor() as cur:
        category_id_map = load_existing_category_names(cur)

        for item in items:
            raw_id = str(uuid.uuid4())

            # Step 1: Insert into news.raw
            cur.execute("""
                INSERT INTO news.raw (raw_id, guid, source, title, link, description, pub_date, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (guid) DO NOTHING
                RETURNING raw_id
            """, (
                raw_id,
                item['guid'],
                item['source'],
                item['title'],
                item['link'],
                item['description'],
                parse_timestamp(item['pubDate']),
                now
            ))
            inserted = cur.fetchone()
            if inserted:
                # Step 2: Insert into news.raw_category
                if item['category']:
                    categories = re.split(r'[;,|]', item['category'])
                    for cat in categories:
                        cat = cat.strip()
                        if not cat:
                            continue
                        if cat not in category_id_map:
                            # Insert missing category
                            new_cat_id = str(uuid.uuid4())
                            try:
                                cur.execute("""
                                    INSERT INTO news.category (category_id, name)
                                    VALUES (%s, %s)
                                    ON CONFLICT (name) DO NOTHING
                                """, (new_cat_id, cat))
                                category_id_map[cat] = new_cat_id
                                print(f"[Info] Inserted new category: '{cat}'")
                            except Exception as e:
                                print(f"[Error] Failed to insert category '{cat}': {e}")
                                continue
                        category_id = category_id_map[cat]
                        cur.execute("""
                            INSERT INTO news.raw_category (raw_id, category_id)
                            VALUES (%s, %s)
                            ON CONFLICT (raw_id, category_id) DO NOTHING
                        """, (raw_id, category_id))

                # Step 3: Insert into news.raw_ticker
                if item['tags']:
                    tickers = [ticker.strip().upper() for ticker in item['tags'].split(',')]
                    for ticker in tickers:
                        if ticker:
                            cur.execute("""
                                INSERT INTO news.raw_ticker (raw_id, ticker_symbol)
                                VALUES (%s, %s)
                                ON CONFLICT (raw_id, ticker_symbol) DO NOTHING
                            """, (raw_id, ticker))
        PG_CONN.commit()

def load_existing_category_names(cur):
    cur.execute("SELECT category_id, name FROM news.category")
    return {row[1]: row[0] for row in cur.fetchall()}
def parse_timestamp(pub_date_str):
    try:
        return datetime.strptime(pub_date_str, '%a, %d %b %Y %H:%M:%S %z').isoformat()
    except ValueError:
        print(f"[Warning] Failed to parse pubDate: {pub_date_str}")
        return datetime.now(timezone.utc).isoformat()

def fetch_rss_feed(mode, value, retries=3):
    if mode == 'category':
        url = f'https://www.nasdaq.com/feed/rssoutbound?category={value}'
    elif mode == 'symbol':
        url = f'https://www.nasdaq.com/feed/rssoutbound?symbol={value}'
    else:
        raise ValueError("Invalid mode. Use 'category' or 'symbol'.")

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
    }
    parsed_url = urlparse(url)
    source = parsed_url.hostname.replace('www.', '') if parsed_url.hostname else ''
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            return response.text, source
        except (requests.exceptions.RequestException, requests.exceptions.ConnectionError) as e:
            wait_time = 2 ** attempt + random.uniform(0, 1)
            print(f"[Attempt {attempt+1}] Error: {e} - Retrying in {wait_time:.2f}s")
            time.sleep(wait_time)
    raise Exception(f"Failed to fetch RSS feed after {retries} attempts.")
def main():
    categories = [
        "Investing", "Retirement", "Saving", "Money", "Artificial Intelligence", "Blockchain",
        "Corporate Governance", "Financial Advisors", "Innovation", "Nasdaq Inc. News", "Technology", "Commodities", "Cryptocurrencies", "Dividends", "Earnings",
        "ETFs", "IPOs", "Markets", "Options", "Stocks"
    ]

    symbols = ["AAPL", "AMZN", "FB", "TSLA", "AMD", "NVDA", "MSFT", "NFLX", "BABA", "F"]

    all_tasks = [("category", cat) for cat in categories] + [("symbol", sym) for sym in symbols]
    #random.shuffle(all_tasks)
    
    for mode, value in all_tasks:
        existing_guids, existing_links = load_existing_links_from_db()
        print(f"\n[{datetime.now()}] Fetching RSS feed for {mode}: {value}")
        try:
            rss_content, source = fetch_rss_feed(mode, value)
            new_items = parse_rss_items(rss_content, existing_guids, existing_links, source)
            if new_items:
                save_normalized_data(new_items)
                print(f"[{datetime.now()}] ✅ Appended {len(new_items)} items from {value}")
            else:
                print(f"[{datetime.now()}] ℹ️ No new items for {mode} '{value}'")
        except Exception as e:
            print(f"[{datetime.now()}] ❌ Error fetching data for {value}: {e}")
        print('waiting for 3 sec to inserting data for other category or symbol: ')
        time.sleep(random.uniform(1.0, 3.0))
if __name__ == '__main__':
    try:
        main()
        time.sleep(5)  # Wait for a few seconds before generating summaries
        generate_summary_for_news.generate_missing_summaries()
    finally:
        PG_CONN.close()
