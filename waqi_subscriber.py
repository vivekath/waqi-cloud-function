import os

# 🔴 CRITICAL: disable opentelemetry BEFORE importing pubsub - needed for python3.13
os.environ["OTEL_SDK_DISABLED"] = "true"
os.environ["OTEL_TRACES_EXPORTER"] = "none"
os.environ["OTEL_METRICS_EXPORTER"] = "none"
os.environ["OTEL_LOGS_EXPORTER"] = "none"

import json
from google.cloud import pubsub_v1

PROJECT_ID = "project-5dd8f491-cc9c-4f1e-951"
SUBSCRIPTION_ID = "data_engineering_subscription"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(
    PROJECT_ID, SUBSCRIPTION_ID
)

# def callback(message):
#     try:
#         payload = json.loads(message.data.decode("utf-8"))
#         print("\n===== MESSAGE =====")
#         print(json.dumps(payload, indent=2))
#         message.ack()
#     except Exception as e:
#         print("❌ Error:", e)

def callback(message):
    raw = message.data.decode("utf-8")
    print("\n===== RAW MESSAGE =====")
    print(raw)
    try:
        payload = json.loads(raw)
        print("\n===== PARSED JSON =====")
        print(json.dumps(payload, indent=2))
    except Exception as e:
        print("❌ Error parsing:", e)
    finally:
        message.ack()



subscriber.subscribe(subscription_path, callback=callback)
print("Listening... Ctrl+C to stop")

try:
    while True:
        pass
except KeyboardInterrupt:
    print("\nStopped")