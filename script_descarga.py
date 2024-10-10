import pandas as pd
import requests
import json
from datetime import datetime, timedelta

# Function to fetch weather data
def fetch_weather_data(start_date, end_date, station_id):
    url = "https://inamhi.gob.ec/api_rest/station_data_hourly/data"
    payload = json.dumps({
        "id_estacion": str(station_id),
        "table_names": ["017140801h"],  # Correct table name for PRECIPITACION
        "start_date": start_date,  # Ensure this is in YYYY-MM-DD format
        "end_date": end_date       # Ensure this is in YYYY-MM-DD format
    })
    headers = {'Content-Type': 'application/json'}
    response = requests.post(url, headers=headers, data=payload)

    if response.status_code == 400:
        raise Exception(f"HTTP 400 - Bad Request for station {station_id}")

    if response.status_code != 200:
        raise Exception(f"Failed to retrieve data for station {station_id}: {response.status_code}")

    data = response.json()

    if not data:
        print(f"No data returned for station {station_id}.")
        return pd.DataFrame()  # Return an empty DataFrame if no data

    flattened_data = []

    for measurement in data:
        variable_name = measurement['name']
        for entry in measurement['data']:
            flattened_data.append({
                "fecha": entry.get('fecha'),
                variable_name: entry.get('valor')
            })

    df = pd.DataFrame(flattened_data)
    return df

# Load station IDs from CSV files
epmaps_stations = pd.read_csv('EPMAPS_stations.csv')  # Update with your actual path
fonag_stations = pd.read_csv('FONAG_stations.csv')    # Update with your actual path

# Extract unique station IDs and their proprietary information
epmaps_station_ids = epmaps_stations['id_estacion'].unique()
fonag_station_ids = fonag_stations['id_estacion'].unique()

# Combine station IDs from both sources
all_station_ids = list(set(epmaps_station_ids) | set(fonag_station_ids))

# Loop through each station and fetch the raw data
for station_id in all_station_ids:
    try:
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')  # Last 30 days for example
        end_date = datetime.now().strftime('%Y-%m-%d')
        raw_data = fetch_weather_data(start_date, end_date, station_id)
        
        if not raw_data.empty:
            raw_data['station_id'] = station_id
            # Save the raw data to a CSV file for each station
            raw_data.to_csv(f'station_{station_id}_raw_data.csv', index=False)
        else:
            print(f"No data for station {station_id}")

    except Exception as e:
        print(f"Failed to fetch data for station {station_id}: {e}")
