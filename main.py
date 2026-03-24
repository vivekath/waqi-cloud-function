import json
import os
import requests
from datetime import datetime, timezone
from google.cloud import pubsub_v1

PROJECT_ID = os.environ.get("PROJECT_ID")
TOPIC_ID = os.environ.get("TOPIC_ID")
WAQI_TOKEN = os.environ.get("WAQI_TOKEN")
CITY = os.environ.get("CITY")

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

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

    publisher.publish(
        topic_path,
        json.dumps(message).encode("utf-8")
    )

    return "Published", 200


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

"""