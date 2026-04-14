# load.py
import json
import os
import mysql.connector
from datetime import date, timedelta

conn = mysql.connector.connect(
    host="localhost",
    user="apod_user",
    password="apodpass123",
    database="apod_warehouse"
)

cursor = conn.cursor()

INSERT_SQL = """
INSERT INTO apod_raw
    (apod_date, title, explanation, url, hdurl, media_type, copyright)
VALUES
    (%(apod_date)s, %(title)s, %(explanation)s, %(url)s, %(hdurl)s, %(media_type)s, %(copyright)s)
ON DUPLICATE KEY UPDATE
    title       = VALUES(title),
    explanation = VALUES(explanation),
    url         = VALUES(url),
    hdurl       = VALUES(hdurl),
    media_type  = VALUES(media_type),
    copyright   = VALUES(copyright);
"""

def load_file(target_date):
    filepath = f"storage/raw/{target_date}/apod.json"
    if not os.path.exists(filepath):
        print(f"⚠️  File not found: {filepath}")
        return

    with open(filepath) as f:
        data = json.load(f)

    record = {
        "apod_date": target_date,
        "title": data.get("title", "").strip(),
        "explanation": data.get("explanation", "").strip(),
        "url": data.get("url"),
        "hdurl": data.get("hdurl"),
        "media_type": data.get("media_type", "").lower(),
        "copyright": data.get("copyright", "").strip() if data.get("copyright") else None,
    }

    cursor.execute(INSERT_SQL, record)
    conn.commit()
    print(f"✅ Loaded {target_date}")

def run(days=7):
    for i in range(days):
        target_date = str(date.today() - timedelta(days=i))
        load_file(target_date)

if __name__ == "__main__":
    run()

cursor.close()
conn.close()