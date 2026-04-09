import os
import time
import json
import requests
from dotenv import load_dotenv
from azure.eventhub import EventHubProducerClient, EventData
from databricks import sql
from databricks.sdk import WorkspaceClient


# Load secrets from .env
load_dotenv()
CONNECTION_STR = os.getenv("EVENTHUB_CONNECTION_STR")
EVENTHUB_NAME = os.getenv("EVENTHUB_NAME")


DATABRICKS_SERVER_HOSTNAME = os.getenv("DATABRICKS_SERVER_HOSTNAME")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DATABRICKS_ACCESS_TOKEN = os.getenv("DATABRICKS_ACCESS_TOKEN")

db_client = sql.connect(
    server_hostname=DATABRICKS_SERVER_HOSTNAME,
    http_path=DATABRICKS_HTTP_PATH,
    access_token=DATABRICKS_ACCESS_TOKEN
)

eh_producer = EventHubProducerClient.from_connection_string(CONNECTION_STR, eventhub_name=EVENTHUB_NAME)


def get_active_cities():
    print("Fetching active cities from Databricks...")
    query = "SELECT city_name, latitude, longitude FROM weather.bronze.dim_city_configs WHERE is_active = 'Y'"
    
    with sql.connect(
        server_hostname=DATABRICKS_SERVER_HOSTNAME,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_ACCESS_TOKEN
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            return [{"name": r[0], "lat": r[1], "lon": r[2]} for r in result]

def fetch_weather(lat, long):
    """Fetch real-time weather from Open-Meteo"""
    url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={long}&current_weather=true"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()['current_weather']
    return None


def run_producer():
    with eh_producer:
        while True:
            active_cities = get_active_cities()

            if not active_cities:
                print("No active cities found. Retrying in 60 seconds...")
                time.sleep(60)
                continue
            else:
                timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

                for city in active_cities:
                    weather = fetch_weather(city['lat'], city['lon'])

                    if weather:
                        payload = {
                            "city_name": city['name'],
                            "latitude": city['lat'],
                            "longitude": city['lon'],
                            "temperature": weather['temperature'],
                            "windspeed": weather['windspeed'],
                            "winddirection": weather['winddirection'],
                            "weathercode": weather['weathercode'],
                            "timestamp": timestamp
                        }
                        event_data_batch = eh_producer.create_batch()
                        event_data_batch.add(EventData(json.dumps(payload)))
                        eh_producer.send_batch(event_data_batch)
                        print(f"Sent weather data for {city['name']} at {timestamp}")
                
            print("Loop completed. Sleeping for 2 minutes...")
            time.sleep(120)

if __name__ == "__main__":
    run_producer()