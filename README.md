This is used to extract the tickers from supabase then fetch its latest news via url then it uses ollama model for summarizing the news then it append this summarized news in supabase. 

1. git clone 
2. cd 
3. python -m venv venv
4. .\venv\Scripts\activate
5. pip install -r requirements.txt 
6. then setup the .env file copy .env_example into .env then give necessary details.
7. run the python nasdaq_news_feed_data_save_to_db.py it will fetch current news of tickers then summarize it and save to db. 
8. If there are pending news already present in db which needs to be summarized run the python generate_summary_for_news.py it will generate all pending news summary and save in db.

