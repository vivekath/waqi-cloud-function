import json
from datetime import datetime, timezone
import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    StandardOptions,
    SetupOptions,
    GoogleCloudOptions
)
from apache_beam.io.gcp.internal.clients import bigquery
from datetime import datetime, timezone

PROJECT_ID = "project-5dd8f491-cc9c-4f1e-951"
REGION = "asia-south2"
SUBSCRIPTION = "projects/project-5dd8f491-cc9c-4f1e-951/subscriptions/data_engineering_subscription"
BQ_TABLE = "project-5dd8f491-cc9c-4f1e-951:assignment_2.waqi_hyd_bronze"


class ParseWAQI(beam.DoFn):
    def process(self, message):
        msg = json.loads(message.decode("utf-8"))
        data = msg["raw_payload"]["data"]
        iaqi = data.get("iaqi", {})
        city_info = data.get("city", {})

        # Parse event_time and normalize to UTC
        event_time = datetime.fromisoformat(msg["event_time"])
        event_time_utc = event_time.astimezone(timezone.utc)

        def safe_float(val):
            try:
                return float(val)
            except (TypeError, ValueError):
                return None

        def safe_int(val):
            try:
                return int(val)
            except (TypeError, ValueError):
                return None

        yield {
            "event_date": event_time_utc.date().isoformat(),
            "city": str(msg.get("city", "")),
            "station_name": str(city_info.get("name", "")),
            "lat": safe_float(city_info.get("geo", [None, None])[0]),
            "lon": safe_float(city_info.get("geo", [None, None])[1]),
            "aqi": safe_int(data.get("aqi")),
            "dominant_pollutant": str(data.get("dominentpol", "")),
            "pm25": safe_float(iaqi.get("pm25", {}).get("v")),
            "pm10": safe_float(iaqi.get("pm10", {}).get("v")),
            "co": safe_float(iaqi.get("co", {}).get("v")),
            "no2": safe_float(iaqi.get("no2", {}).get("v")),
            "so2": safe_float(iaqi.get("so2", {}).get("v")),
            "temperature": safe_float(iaqi.get("t", {}).get("v")),
            "humidity": safe_float(iaqi.get("h", {}).get("v")),
            "wind": safe_float(iaqi.get("w", {}).get("v")),
            "event_time": event_time_utc.strftime("%Y-%m-%d %H:%M:%S"),
            "bq_load_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        }



# BigQuery schema
table_schema = bigquery.TableSchema()

def add_field(name, type_, mode="NULLABLE"):
    field = bigquery.TableFieldSchema()
    field.name = name
    field.type = type_
    field.mode = mode
    table_schema.fields.append(field)

add_field("event_date", "DATE")
add_field("city", "STRING")
add_field("station_name", "STRING")
add_field("lat", "FLOAT")
add_field("lon", "FLOAT")
add_field("aqi", "INTEGER")
add_field("dominant_pollutant", "STRING")
add_field("pm25", "FLOAT")
add_field("pm10", "FLOAT")
add_field("co", "FLOAT")
add_field("no2", "FLOAT")
add_field("so2", "FLOAT")
add_field("temperature", "FLOAT")
add_field("humidity", "FLOAT")
add_field("wind", "FLOAT")
add_field("event_time", "TIMESTAMP")
add_field("bq_load_time", "TIMESTAMP")


def run():
    options = PipelineOptions()
    gcloud_options = options.view_as(GoogleCloudOptions)
    gcloud_options.project = PROJECT_ID
    gcloud_options.region = REGION
    gcloud_options.job_name = "waqi-dataflow-job"
    gcloud_options.staging_location = "gs://temp_test_bucket_11022026/staging"
    gcloud_options.temp_location = "gs://temp_test_bucket_11022026/temp"

    options.view_as(StandardOptions).streaming = True
    options.view_as(SetupOptions).save_main_session = True
    options.view_as(StandardOptions).runner = "DirectRunner"  # switch to DataflowRunner for production

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Read from PubSub" >> beam.io.ReadFromPubSub(subscription=SUBSCRIPTION)
            | "Parse WAQI Payload" >> beam.ParDo(ParseWAQI())
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                table=BQ_TABLE,
                schema=table_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )


if __name__ == "__main__":
    run()
