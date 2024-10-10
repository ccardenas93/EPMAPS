import pandas as pd
import requests
from datetime import datetime, timedelta

# Function to fetch weather data (ensure using correct table name for PRECIPITACION)
def fetch_weather_data(start_date, end_date, station_id):
    url = "https://inamhi.gob.ec/api_rest/station_data_hourly/data"
    payload = {
        "id_estacion": str(station_id),
        "table_names": ["017140801h"],  # Correct table name for PRECIPITACION
        "start_date": start_date,
        "end_date": end_date
    }
    response = requests.post(url, json=payload)
    
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

    # Explore the columns if there's an issue with 'fecha'
    if 'fecha' not in df.columns:
        print(f"'fecha' column not found for station {station_id}. Available columns: {df.columns}")
        return pd.DataFrame()  # Return an empty DataFrame if 'fecha' is missing

    df = df.groupby('fecha').first().reset_index()
    
    return df

# Function to calculate daily and monthly rainfall accumulation
def calculate_rainfall_accumulations(station_id):
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    
    # Define start of month for monthly accumulation
    start_of_month = today.replace(day=1).strftime('%Y-%m-%dT00:00:00')

    # Fetch data from start of month until today (inclusive)
    start_date = start_of_month
    end_date = today.strftime('%Y-%m-%dT00:00:00')

    df_weather = fetch_weather_data(start_date, end_date, station_id)

    if df_weather.empty:
        print(f"No valid data for station {station_id}. Skipping.")
        return pd.DataFrame(), pd.DataFrame()

    # Convert 'fecha' to datetime format and extract only date part for grouping
    df_weather['fecha'] = pd.to_datetime(df_weather['fecha'])
    df_weather['date'] = df_weather['fecha'].dt.date
    
    # Daily Rainfall for Yesterday
    daily_rainfall = df_weather[df_weather['date'] == yesterday.date()]
    daily_rainfall['station_id'] = station_id  # Add station ID

    # Monthly Accumulation from 1st to Yesterday
    if 'PRECIPITACION SUM' in df_weather.columns:
        monthly_accumulation = df_weather[df_weather['date'] <= yesterday.date()]
        monthly_accumulation = monthly_accumulation.groupby('date')['PRECIPITACION SUM'].sum().reset_index()
        monthly_accumulation['station_id'] = station_id  # Add station ID
    else:
        print(f"PRECIPITACION SUM column not found for station {station_id}.")
        monthly_accumulation = pd.DataFrame()

    return daily_rainfall, monthly_accumulation

# Load station IDs from CSV files
epmaps_stations = pd.read_csv('EPMAPS_stations.csv')  # Update with your actual path
fonag_stations = pd.read_csv('FONAG_stations.csv')    # Update with your actual path

# Extract unique station IDs and their proprietary information
epmaps_station_ids = epmaps_stations['id_estacion'].unique()
fonag_station_ids = fonag_stations['id_estacion'].unique()

# Combine station IDs from both sources
all_station_ids = list(set(epmaps_station_ids) | set(fonag_station_ids))

# Initialize empty DataFrames to store daily and monthly data for all stations
all_daily_rainfall = pd.DataFrame()
all_monthly_accumulation = pd.DataFrame()

# Initialize report list to track the status of each station
station_report = []

# Track the number of successfully processed stations and those with errors
stations_processed = 0
stations_with_errors = 0

# Determine proprietary of each station
def get_proprietary(station_id):
    if station_id in epmaps_station_ids:
        return "EPMAPS"
    elif station_id in fonag_station_ids:
        return "FONAG"
    else:
        return "UNKNOWN"

# Loop through each station and process the data
for station_id in all_station_ids:
    try:
        # Calculate daily and monthly rainfall accumulations for each station
        daily_rainfall_df, monthly_accumulation_df = calculate_rainfall_accumulations(station_id)
        
        # Determine the proprietary of the station
        proprietary = get_proprietary(station_id)
        
        # Determine the status of the station
        if daily_rainfall_df.empty and monthly_accumulation_df.empty:
            station_report.append({"station_id": station_id, "status": "NO DATA", "proprietary": proprietary})
        elif daily_rainfall_df.empty or monthly_accumulation_df.empty:
            station_report.append({"station_id": station_id, "status": "SOME DATA MISSING", "proprietary": proprietary})
        else:
            station_report.append({"station_id": station_id, "status": "OK", "proprietary": proprietary})
            stations_processed += 1  # Count successful processing
        
        # Append the daily and monthly data to the consolidated DataFrames
        all_daily_rainfall = pd.concat([all_daily_rainfall, daily_rainfall_df], ignore_index=True)
        all_monthly_accumulation = pd.concat([all_monthly_accumulation, monthly_accumulation_df], ignore_index=True)

    except Exception as e:
        print(f"Failed to process data for station {station_id}: {e}")
        proprietary = get_proprietary(station_id)
        station_report.append({"station_id": station_id, "status": "ERROR", "proprietary": proprietary})
        stations_with_errors += 1  # Increment error count

# Save the consolidated data to single CSV files
all_daily_rainfall.to_csv('all_stations_daily_rainfall.csv', index=False)
all_monthly_accumulation.to_csv('all_stations_monthly_accumulation.csv', index=False)

# Save the station report as a CSV file
report_df = pd.DataFrame(station_report)
report_df.to_csv('station_processing_report.csv', index=False)

# Print summary report
print(f"Total stations processed successfully: {stations_processed}")
print(f"Total stations with errors: {stations_with_errors}")
print("Station report saved successfully.")
