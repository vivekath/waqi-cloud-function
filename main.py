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
PROJECT_ID="project-5dd8f491-cc9c-4f1e-951"
PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format="value(projectNumber)")
CLOUDBUILD_SA="$PROJECT_NUMBER@cloudbuild.gserviceaccount.com"

# Cloud Functions Developer
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$CLOUDBUILD_SA" \
  --role="roles/cloudfunctions.developer"

# Service Account User
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$CLOUDBUILD_SA" \
  --role="roles/iam.serviceAccountUser"

# For Gen 2 functions, also needed:
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$CLOUDBUILD_SA" \
  --role="roles/run.admin"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$CLOUDBUILD_SA" \
  --role="roles/eventarc.admin"

# If publishing to Pub/Sub:
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$CLOUDBUILD_SA" \
  --role="roles/pubsub.publisher"


"""