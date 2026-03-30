from flask import Flask, jsonify, request
import requests
import gzip
import io
import csv
import os
from redis import Redis
from redisbloom.client import Client as RedisBloom
from dotenv import load_dotenv
import traceback
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import timezone

load_dotenv()

app = Flask(__name__)

# ------------------------
# Config
# ------------------------
PHISHTANK_URL = "http://data.phishtank.com/data/online-valid.csv.gz"
USER_AGENT = "phishfinder/1.0 (malik.jgomez@gmail.com)"

REDIS_HOST = os.environ.get("REDIS_HOST")
REDIS_PORT = int(os.environ.get("REDIS_PORT"))
REDIS_USERNAME = os.environ.get("REDIS_USERNAME")
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD")


BLOOM_KEY = "phishtank_bloom"
ERROR_RATE = 0.001
CAPACITY = 2_000_000  # adjust based on dataset size

# ------------------------
# RedisBloom setup
# ------------------------
rb = RedisBloom(
    host=REDIS_HOST,
    port=REDIS_PORT,
    username=REDIS_USERNAME,
    password=REDIS_PASSWORD,
    decode_responses=True
)

# ------------------------
# Download PhishTank
# ------------------------
def download_phishtank():
    headers = {"User-Agent": USER_AGENT}
    response = requests.get(PHISHTANK_URL, headers=headers, timeout=30)
    response.raise_for_status()

    with gzip.open(io.BytesIO(response.content), mode="rt", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        urls = [row["url"] for row in reader if row["online"].lower() == "yes"]

    return urls

# ------------------------
# Build Redis Bloom filter
# ------------------------
def rebuild_bloom():
    print("[INFO] Downloading PhishTank...")
    urls = download_phishtank()
    print(f"[INFO] Got {len(urls)} URLs")

    print("[INFO] Rebuilding Bloom filter...")

    new_key = BLOOM_KEY + "_new"

    # Delete temp key if exists
    try:
        rb.delete(new_key)
    except:
        pass

    # Create new filter
    rb.bfCreate(new_key, ERROR_RATE, max(len(urls), 100000))

    # Batch insert
    pipe = rb.pipeline()
    for url in urls:
        pipe.bfAdd(new_key, url)
    pipe.execute()

    # Atomic swap
    rb.rename(new_key, BLOOM_KEY)

    print("[INFO] Bloom filter updated (zero downtime)")

    return len(urls)

# ------------------------
# Routes
# ------------------------

@app.route("/update_phishtank", methods=["GET"])
def update():
    if request.args.get("key") != os.environ.get("CRON_SECRET"):
        return jsonify({"error": "unauthorized"}), 403
    try:
        count = rebuild_bloom()
        return jsonify({"success": True, "count": count})
    except Exception as e:
        # Print full stack trace to console
        print("[ERROR] Exception in /update_phishtank:")
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/")
def home():
    return jsonify({"message": "PhishTank RedisBloom API running"})

scheduler = BackgroundScheduler(timezone=timezone.utc)

def scheduled_job():
    print("[SCHEDULER] Running PhishTank update...")
    try:
        rebuild_bloom()
    except Exception as e:
        print("[SCHEDULER ERROR]", e)

# Run at 12 AM and 12 PM UTC
scheduler.add_job(scheduled_job, 'cron', hour='0,12', minute=0)
scheduler.start()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)