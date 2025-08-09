"""import argparse
import os
import time
import uuid
import random
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
import psycopg
import boto3
from botocore.client import Config
from dotenv import load_dotenv
"""
"""def load_config():
    #Load environment variables and initialize clients.
    load_dotenv()

    pg_conn = psycopg.connect(
        host=os.environ['PG_HOST'],
        port=os.environ['PG_PORT'],
        dbname=os.environ['PG_DATABASE'],
        user=os.environ['PG_USER'],
        password=os.environ['PG_PASSWORD'],
        autocommit=True
    )

    s3 = boto3.client(
        's3',
        endpoint_url=os.environ['S3_ENDPOINT'],
        aws_access_key_id=os.environ['S3_ACCESS_KEY'],
        aws_secret_access_key=os.environ['S3_SECRET_KEY'],
        region_name=os.environ['S3_REGION'],
        config=Config(signature_version='s3v4')
    )

    return pg_conn, s3, os.environ['S3_BUCKET']


def parse_args():
    parser = argparse.ArgumentParser(description='Scrape Nasdaq RSS feed by category or symbol.')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--category', help='RSS feed category (e.g., Mergers)')
    group.add_argument('--symbol', help='Stock symbol (e.g., AAPL)')
    return parser.parse_args()


def fetch_existing_links(conn):
    existing = set()
    with conn.cursor() as cur:
        cur.execute("SELECT guid, link FROM news.raw")
        for guid, link in cur.fetchall():
            if guid:
                existing.add(guid.strip())
            elif link:
                existing.add(link.strip())
    return existing
"""

"""def fetch_rss_feed(mode, value, retries=3):
    url = f'https://www.nasdaq.com/feed/rssoutbound?{mode}={value}'
    headers = {'User-Agent': 'Mozilla/5.0'}
    source = urlparse(url).hostname.replace('www.', '')

    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            return response.text, source
        except Exception as e:
            wait_time = 2 ** attempt + random.random()
            print(f"[Attempt {attempt+1}] {e} - Retrying in {wait_time:.2f}s")
            time.sleep(wait_time)
    raise RuntimeError(f"Failed to fetch RSS feed after {retries} attempts.")


def parse_rss(rss_content, existing_links, source):
    soup = BeautifulSoup(rss_content, 'xml')
    items = []

    for item in soup.find_all('item'):
        guid = item.guid.get_text(strip=True) if item.guid else ''
        link = item.link.get_text(strip=True) if item.link else ''
        dedup_key = guid or link
        if dedup_key in existing_links:
            continue

        items.append({
            'guid': guid,
            'link': link,
            'title': item.title.get_text(strip=True) if item.title else '',
            'description': item.description.get_text(strip=True) if item.description else '',
            'pubDate': item.pubDate.get_text(strip=True) if item.pubDate else '',
            'category': item.category.get_text(strip=True) if item.category else '',
            'tags': item.find('nasdaq:tickers').get_text(strip=True) if item.find('nasdaq:tickers') else '',
            'creator': item.find('dc:creator').get_text(strip=True) if item.find('dc:creator') else '',
            'source': source
        })
    return items
"""

"""def parse_timestamp(pub_date_str):
    try:
        return datetime.strptime(pub_date_str, '%a, %d %b %Y %H:%M:%S %z').isoformat()
    except Exception:
        print(f"[Warning] Failed to parse date: {pub_date_str}")
        return datetime.now(timezone.utc).isoformat()


def load_category_map(cur):
    cur.execute("SELECT category_id, name FROM news.category")
    return {name: cid for cid, name in cur.fetchall()}

"""
"""def save_to_db(conn, items):
    with conn.cursor() as cur:
        for item in items:
            raw_id = item['raw_id']
            guid = item['guid']
            source = item['source']
            title = item['title']
            description = item['description']
            link = item['link']
            published = item['published']
            category_name = item['category_name']  # Category name (e.g. "Stocks")
            tickers = item.get('tickers', [])

            # Step 1: Ensure category exists and fetch its ID
            #cur.execute("""
            #    INSERT INTO news.category (name)
            #    VALUES (%s)
            #    ON CONFLICT (name) DO NOTHING
            #    RETURNING category_id
            #""", (category_name,))
            #cat_row = cur.fetchone()

            #if not cat_row:
                # If category already existed, fetch its ID manually
            #    cur.execute("SELECT category_id FROM news.category WHERE name = %s", (category_name,))
            #    cat_row = cur.fetchone()

            #category_id = cat_row[0]

            # Step 2: Insert raw news item
            #cur.execute("""
            #    INSERT INTO news.raw (raw_id, guid, source, title, description, link, pub_date)
            #    VALUES (%s, %s, %s, %s, %s, %s, %s)
            #    ON CONFLICT (guid) DO NOTHING
            #""", (raw_id, guid, source, title, description, link, published))

            # Step 3: Insert raw_category relation
            #cur.execute("""
            #    INSERT INTO news.raw_category (raw_id, category_id)
            #    VALUES (%s, %s)
            #    ON CONFLICT DO NOTHING
            #""", (raw_id, category_id))

            # Step 4: Insert raw_ticker relations
            #for ticker in tickers:
            #    cur.execute("""
            #        INSERT INTO news.raw_ticker (raw_id, ticker_symbol)
            #        VALUES (%s, %s)
            #        ON CONFLICT DO NOTHING
            #    """, (raw_id, ticker))

    #conn.commit()
"""
"""
"""def main():
    args = parse_args()
    conn, _, _ = load_config()

    mode = 'category' if args.category else 'symbol'
    value = args.category if args.category else args.symbol

    print(f"[{datetime.now()}] Fetching feed for {mode}: {value}")
    existing_links = fetch_existing_links(conn)
    rss_content, source = fetch_rss_feed(mode, value)
    new_items = parse_rss(rss_content, existing_links, source)

    if new_items:
        save_to_db(conn, new_items)
        print(f"[{datetime.now()}] Saved {len(new_items)} items.")
    else:
        print(f"[{datetime.now()}] No new items found.")


if __name__ == '__main__':
    main()"""
