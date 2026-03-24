import json
import os
import requests
from datetime import datetime, timezone
from google.cloud import pubsub_v1
from google.auth import default

creds, project = default()

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
    print(creds.service_account_email)

    # Publish to Pub/Sub with fully qualified topic path
    future = publisher.publish(
        topic_path,
        json.dumps(message).encode("utf-8")
    )
    print("Message ID:", future.result())

    return "Published", 200

# Run the below command in cloud shell to check if the cloud function is working fine.
"""
curl -X GET https://asia-south1-project-5dd8f491-cc9c-4f1e-951.cloudfunctions.net/fetch_waqi_data
"""

"""
gcloud projects add-iam-policy-binding project-5dd8f491-cc9c-4f1e-951 \
  --member="serviceAccount:306164924329-compute@developer.gserviceaccount.com" \
  --role="roles/cloudfunctions.developer"

  
gcloud projects add-iam-policy-binding project-5dd8f491-cc9c-4f1e-951 \
  --member="serviceAccount:306164924329-compute@developer.gserviceaccount.com" \
  --role="roles/iam.serviceAccountUser"

  
gcloud projects add-iam-policy-binding project-5dd8f491-cc9c-4f1e-951 \
  --member="serviceAccount:306164924329-compute@developer.gserviceaccount.com" \
  --role="roles/artifactregistry.writer"


# Compute service account
gcloud projects add-iam-policy-binding project-5dd8f491-cc9c-4f1e-951 \
  --member="serviceAccount:306164924329-compute@developer.gserviceaccount.com" \
  --role="roles/artifactregistry.writer"

gcloud projects add-iam-policy-binding project-5dd8f491-cc9c-4f1e-951 \
  --member="serviceAccount:306164924329-compute@developer.gserviceaccount.com" \
  --role="roles/artifactregistry.reader"

# Cloud Build service account
gcloud projects add-iam-policy-binding project-5dd8f491-cc9c-4f1e-951 \
  --member="serviceAccount:306164924329@cloudbuild.gserviceaccount.com" \
  --role="roles/artifactregistry.writer"

gcloud projects add-iam-policy-binding project-5dd8f491-cc9c-4f1e-951 \
  --member="serviceAccount:306164924329@cloudbuild.gserviceaccount.com" \
  --role="roles/artifactregistry.reader"

gcloud projects add-iam-policy-binding project-5dd8f491-cc9c-4f1e-951 \
  --member="serviceAccount:306164924329@cloudbuild.gserviceaccount.com" \
  --role="roles/pubsub.publisher"
"""

# Cloud Scheduler
"""
gcloud scheduler jobs create http waqi-hourly-job \
  --location asia-south1 \
  --schedule "10 * * * *" \
  --uri "https://asia-south1-project-5dd8f491-cc9c-4f1e-951.cloudfunctions.net/fetch_waqi_data" \
  --http-method GET \
  --time-zone "Asia/Kolkata"
"""

# Check scheduler jobs
"""
gcloud scheduler jobs list --location asia-south1
"""

# Update schedule
"""
gcloud scheduler jobs update http waqi-hourly-job --location asia-south1 --schedule "10 12 * * *"
"""

"""
import json
import os
import requests
from datetime import datetime, timezone
from google.cloud import pubsub_v1

PROJECT_ID = "project-5dd8f491-cc9c-4f1e-951"
TOPIC_ID = "data_engineering_topic"
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
"""
