# setup_db.py
import mysql.connector

conn = mysql.connector.connect(
    host="localhost",
    user="apod_user",
    password="apodpass123",
    database="apod_warehouse"
)

cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS apod_raw (
    id            INT AUTO_INCREMENT PRIMARY KEY,
    apod_date     DATE NOT NULL,
    title         VARCHAR(500),
    explanation   LONGTEXT,
    url           TEXT,
    hdurl         TEXT,
    media_type    VARCHAR(50),
    copyright     VARCHAR(500),
    extracted_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_apod_date (apod_date)
);
""")

conn.commit()
cursor.close()
conn.close()
print("✅ Table apod_raw created successfully!")