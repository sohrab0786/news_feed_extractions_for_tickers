
import os

from urllib.parse import urlparse

from dotenv import load_dotenv
import psycopg

import webpage_summarizer

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

def generate_missing_summaries():
    with PG_CONN.cursor() as cur:
        # Step 1: Get raw_id and link where summary is missing
        cur.execute("""
            SELECT r.raw_id, r.link
            FROM news.raw r
            LEFT JOIN news.news_summary s ON r.raw_id = s.raw_id
            WHERE s.raw_id IS NULL
        """)
        rows = cur.fetchall()

        print(f"[Info] Found {len(rows)} unsummarized articles.")

        for raw_id, link in rows:
            try:
                summary = webpage_summarizer.summarize_webpage(link)
                if summary:
                    cur.execute("""
                        INSERT INTO news.news_summary (raw_id, summary)
                        VALUES (%s, %s) 
                        ON CONFLICT (raw_id) DO NOTHING
                    """, (raw_id, summary.strip()))
                    print(f"[✓] Summary added for raw_id: {raw_id}")
            except Exception as e:
                print(f"[Warning] Failed to summarize {link}: {e}")
            #time.sleep(random.uniform(1.5, 3.0))

        PG_CONN.commit()


if __name__ == "__main__":
    try:
        print("[Info] Starting summary generation for news articles...")
        generate_missing_summaries()
        print("[Info] Summary generation completed.")
    except Exception as e:
        print(f"[Error] An error occurred: {e}")
    finally:
        PG_CONN.close()
        print("[Info] Database connection closed.")
