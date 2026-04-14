# quality_checks.py
import mysql.connector

DB_CONFIG = {
    "host": "localhost",
    "user": "apod_user",
    "password": "apodpass123",
    "database": "apod_warehouse",
}

def run_checks(target_date):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)

    print(f"\n🔍 Running quality checks for {target_date}...")

    # Check 1: Row exists
    cursor.execute(
        "SELECT * FROM apod_raw WHERE apod_date = %s", (target_date,)
    )
    row = cursor.fetchone()
    if not row:
        raise ValueError(f" No row found for {target_date}")
    print("Check 1 passed: Row exists")

    # Check 2: Title is not empty
    if not row["title"] or len(row["title"].strip()) == 0:
        raise ValueError(f"Title is empty for {target_date}")
    print("Check 2 passed: Title is not empty")

    # Check 3: URL is not empty
    if not row["url"] or len(row["url"].strip()) == 0:
        raise ValueError(f"URL is empty for {target_date}")
    print("Check 3 passed: URL is not empty")

    # Check 4: media_type is valid
    if row["media_type"] not in ("image", "video"):
        raise ValueError(f"Invalid media_type: {row['media_type']}")
    print("Check 4 passed: media_type is valid")

    # Check 5: Explanation is long enough
    if not row["explanation"] or len(row["explanation"]) < 50:
        raise ValueError(f"Explanation too short for {target_date}")
    print("Check 5 passed: Explanation length is valid")

    cursor.close()
    conn.close()
    print(f"\n🎉 All quality checks passed for {target_date}!\n")

if __name__ == "__main__":
    from datetime import date
    run_checks(str(date.today()))