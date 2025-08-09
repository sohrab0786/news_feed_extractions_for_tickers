import os
import logging
from dotenv import load_dotenv
import psycopg
import webpage_summarizer
import pandas as pd

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Connect to the PostgreSQL database
try:
    PG_CONN = psycopg.connect(
        host=os.environ['PG_HOST'],
        port=os.environ['PG_PORT'],
        dbname=os.environ['PG_DATABASE'],
        user=os.environ['PG_USER'],
        password=os.environ['PG_PASSWORD'],
    )
    PG_CONN.autocommit = False
    logging.info("Database connection established.")
except Exception as e:
    logging.error(f"Failed to connect to the database: {e}")
    raise

def fetch_links_and_raw_ids() -> list[tuple[str, str]]:
    """
    Fetches link and raw_id from the news.raw table.

    Returns:
        list of tuples: Each tuple contains (raw_id, link).
    """
    try:
        with PG_CONN.cursor() as cursor:
            cursor.execute("SELECT raw_id, link FROM news.raw")
            results = cursor.fetchall()
        logging.info(f"Fetched {len(results)} records from news.raw.")
        return results
    except Exception as e:
        logging.error(f"Failed to fetch records from news.raw: {e}")
        return []

if __name__ == "__main__":
    print("⚠️  WARNING: proceed to save in csv")
    confirmation = input("Are you absolutely sure you want to proceed? Type 'yes' to confirm: ").strip().lower()

    if confirmation == 'yes':
        results = fetch_links_and_raw_ids()
        data_result = pd.DataFrame(columns=["raw_id", "link", "summary"])

        for i in range(min(10, len(results))):  # Avoid index error if <10 records
            raw_id, link = results[i]
            summary = webpage_summarizer.summarize_webpage(link)
            if summary:
                data_result.loc[len(data_result)] = {
                    "raw_id": raw_id,
                    "link": link,
                    "summary": summary
                }

        # Save to CSV
        csv_path = "summarized_news5.csv"
        data_result.to_csv(csv_path, index=False)
        logging.info(f"Summaries saved to {csv_path}")
    else:
        logging.info("Operation cancelled by user.")

    PG_CONN.close()
    logging.info("Database connection closed.")

