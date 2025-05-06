import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

conn = psycopg2.connect(
    dbname=os.getenv('DBNAME'),
    user=os.getenv('USER'),
    password=os.getenv('PASSWORD'),
    host=os.getenv('HOST'),
    port=os.getenv('PORT')
)

cur = conn.cursor()
cur.execute("SELECT COUNT(*) FROM sales;")
print("Number of rows in sales:", cur.fetchone()[0])
cur.close()
conn.close()