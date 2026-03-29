import os
import time
import json
import requests
from dotenv import load_dotenv
from azure.eventhub import EventHubProducerClient, EventData

# Load secrets from .env
load_dotenv()
CONNECTION_STR = os.getenv("EVENTHUB_CONNECTION_STR")
EVENTHUB_NAME = os.getenv("EVENTHUB_NAME")

# Weather API Config (Delhi coordinates)
LAT, LON = 28.61, 77.20 

def fetch_weather():
    """Fetch real-time weather from Open-Meteo"""
    url = f"https://api.open-meteo.com/v1/forecast?latitude={LAT}&longitude={LON}&current_weather=true"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()['current_weather']
    return None

def run_producer():
    client = EventHubProducerClient.from_connection_string(CONNECTION_STR, eventhub_name=EVENTHUB_NAME)
    
    print(f"--- Starting Stream for Delhi ({LAT}, {LON}) ---")
    
    try:
        with client:
            while True:
                try:
                    data = fetch_weather()
                    if data:
                        data['ingested_at_utc'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
                        
                        event_data_batch = client.create_batch()
                        event_data_batch.add(EventData(json.dumps(data)))
                        
                        client.send_batch(event_data_batch)
                        print(f"PRODUCER: Sent Temp={data['temperature']}°C")
                
                except Exception as e:
                    # If the API fails or SSL breaks, don't crash! 
                    # Just log it and wait for the next cycle.
                    print(f"ERROR: Connection issue: {e}. Retrying in 60s...")

                time.sleep(60)
    except KeyboardInterrupt:
        print("Stopping Producer...")

if __name__ == "__main__":
    run_producer()