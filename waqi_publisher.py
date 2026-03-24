import json
import os
import requests
from datetime import datetime, timezone
from google.cloud import pubsub_v1

PROJECT_ID = os.environ.get("PROJECT_ID","project-5dd8f491-cc9c-4f1e-951")
TOPIC_ID = os.environ.get("TOPIC_ID", "data_engineering_topic")
WAQI_TOKEN = os.environ.get("WAQI_TOKEN", "2546085f564bc2e24a499c3d0bfbb57ffd42ed34")
CITY = os.environ.get("CITY", "hyderabad")

publisher = pubsub_v1.PublisherClient()
topic_path = f"projects/{PROJECT_ID}/topics/{TOPIC_ID}"

def fetch_waqi_data(request):
    url = f"https://api.waqi.info/feed/{CITY}/?token={WAQI_TOKEN}"
    response = requests.get(url, timeout=20)
    response.raise_for_status()
    payload = response.json()

    message = {
        "source": "waqi",
        "city": CITY,
        "ingestion_time": datetime.now(timezone.utc).isoformat(),
        "event_time": payload["data"]["time"]["iso"],
        "raw_payload": payload
    }

    print("Publishing message:", message)

    # Publish to Pub/Sub with fully qualified topic path
    future = publisher.publish(
        topic_path,
        json.dumps(message).encode("utf-8")
    )
    print("Message ID:", future.result())

    return "Published", 200

if __name__ == "__main__":
    # Fake request object for local testing
    class DummyRequest:
        pass

    fetch_waqi_data(DummyRequest())
