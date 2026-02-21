import json
import boto3
import requests
from datetime import datetime
import os

# Configuration
OPENWEATHER_API_KEY = os.environ['OPENWEATHER_API_KEY']
S3_BUCKET = os.environ['S3_BRONZE_BUCKET']
BASE_URL = "https://api.openweathermap.org/data/2.5"

CITIES = [
    {"name": "New York", "lat": 40.7128, "lon": -74.0060, "country": "US"},
    {"name": "London", "lat": 51.5074, "lon": -0.1278, "country": "GB"},
    {"name": "Tokyo", "lat": 35.6762, "lon": 139.6503, "country": "JP"},
    {"name": "Sydney", "lat": -33.8688, "lon": 151.2093, "country": "AU"},
    {"name": "Mumbai", "lat": 19.0760, "lon": 72.8777, "country": "IN"},
    {"name": "Dubai", "lat": 25.2048, "lon": 55.2708, "country": "AE"},
    {"name": "São Paulo", "lat": -23.5505, "lon": -46.6333, "country": "BR"},
    {"name": "Toronto", "lat": 43.6532, "lon": -79.3832, "country": "CA"},
]

s3_client = boto3.client('s3')
glue_client = boto3.client('glue')

def get_current_weather(lat, lon):
    """Fetch current weather"""
    url = f"{BASE_URL}/weather"
    params = {
        'lat': lat,
        'lon': lon,
        'appid': OPENWEATHER_API_KEY,
        'units': 'metric'
    }
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching current weather: {e}")
        return None

def get_forecast(lat, lon):
    """Fetch 5-day forecast"""
    url = f"{BASE_URL}/forecast"
    params = {
        'lat': lat,
        'lon': lon,
        'appid': OPENWEATHER_API_KEY,
        'units': 'metric'
    }
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching forecast: {e}")
        return None

def save_to_s3(data, data_type, timestamp):
    """Save JSON data to S3 Bronze layer"""
    year = timestamp.strftime('%Y')
    month = timestamp.strftime('%m')
    day = timestamp.strftime('%d')
    hour = timestamp.strftime('%H')
    
    s3_key = f"raw/{data_type}/year={year}/month={month}/day={day}/hour={hour}/data_{timestamp.strftime('%Y%m%d_%H%M%S')}.json"
    
    try:
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json.dumps(data, indent=2),
            ContentType='application/json'
        )
        print(f"Saved to s3://{S3_BUCKET}/{s3_key}")
        return s3_key
    except Exception as e:
        print(f"Error saving to S3: {e}")
        raise

def trigger_glue_job(job_name):
    """Trigger AWS Glue ETL job"""
    try:
        response = glue_client.start_job_run(JobName=job_name)
        print(f"Triggered Glue job: {job_name}, RunId: {response['JobRunId']}")
        return response['JobRunId']
    except Exception as e:
        print(f"Error triggering Glue job: {e}")
        return None

def lambda_handler(event, context):
    """
    Main Lambda handler
    Triggered daily by EventBridge
    """
    print("=== Weather Data Extraction Started ===")
    timestamp = datetime.utcnow()
    batch_id = timestamp.strftime('%Y%m%d_%H%M%S')
    
    current_weather_data = []
    forecast_data = []
    
    # Extract data for all cities
    for city in CITIES:
        print(f"Processing {city['name']}...")
        
        # Get current weather
        current = get_current_weather(city['lat'], city['lon'])
        if current:
            current['extraction_timestamp'] = timestamp.isoformat()
            current['batch_id'] = batch_id
            current['data_type'] = 'current'
            current['city_name'] = city['name']
            current['country'] = city['country']
            current_weather_data.append(current)
        
        # Get forecast
        forecast = get_forecast(city['lat'], city['lon'])
        if forecast:
            forecast['extraction_timestamp'] = timestamp.isoformat()
            forecast['batch_id'] = batch_id
            forecast['data_type'] = 'forecast'
            forecast['city_name'] = city['name']
            forecast['country'] = city['country']
            forecast_data.append(forecast)
    
    # Save to S3 Bronze
    current_key = save_to_s3(current_weather_data, 'current', timestamp)
    forecast_key = save_to_s3(forecast_data, 'forecast', timestamp)
    
    # Trigger Glue ETL jobs
    bronze_to_silver_run = trigger_glue_job('weather-bronze-to-silver')
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Weather data extraction completed',
            'batch_id': batch_id,
            'current_records': len(current_weather_data),
            'forecast_records': len(forecast_data),
            'current_s3_key': current_key,
            'forecast_s3_key': forecast_key,
            'glue_job_run_id': bronze_to_silver_run
        })
    }