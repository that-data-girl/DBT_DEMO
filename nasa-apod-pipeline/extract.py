# extract.py
import requests
import json
import os
from datetime import date, timedelta

API_KEY = "oo648hDtfqONNhXzeOvJxD7DcG0hcPNLNLi8Esk5"
RAW_PATH = "storage/raw"

def fetch_apod(target_date):
    url = "https://api.nasa.gov/planetary/apod"
    params = {"api_key": API_KEY, "date": target_date}
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()

def save_raw(data, target_date):
    folder = f"{RAW_PATH}/{target_date}"
    os.makedirs(folder, exist_ok=True)
    filepath = f"{folder}/apod.json"
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)
    print(f"✅ Saved {target_date} → {filepath}")

def run(days=7):
    for i in range(days):
        target_date = date.today() - timedelta(days=i)
        data = fetch_apod(str(target_date))
        save_raw(data, str(target_date))

if __name__ == "__main__":
    run()