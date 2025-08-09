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