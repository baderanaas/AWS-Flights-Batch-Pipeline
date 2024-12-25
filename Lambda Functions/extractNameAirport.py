import json
import boto3
import requests
import os
from datetime import datetime, timedelta

s3 = boto3.client('s3')

flight_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

def lambda_handler(event, context):
    api_key = os.getenv('FLIGHTS_API')
    airport_icao = os.getenv('airport_icao')
    
    if not api_key or not airport_icao:
        return {
            'statusCode': 400,
            'body': json.dumps('Missing required environment variables: FLIGHTS_API or airport_icao.')
        }

    api_url = f"https://api.aviationstack.com/v1/flights?access_key={api_key}&dep_icao={airport_icao}&offset="
    
    all_flight_data = []
    
    offset = 0
    stop = False
    while not stop:
        try:
            response = requests.get(f"{api_url}{offset}")
            response.raise_for_status()
            page_data = response.json()
        except requests.exceptions.RequestException as e:
            return {
                'statusCode': 500,
                'body': json.dumps(f'Error fetching data from API: {str(e)}')
            }

        if 'data' not in page_data or not page_data['data']:
            break

        for flight in page_data['data']:
            if flight.get('flight_date') == flight_date:
                all_flight_data.append(flight)
            elif flight.get('flight_date') < flight_date:
                stop = True
        
        offset += 100 
        
        if offset >= page_data['pagination']['total']:
            break

    if not all_flight_data:
        return {
            'statusCode': 404,
            'body': json.dumps(f'No flights found for {flight_date}.')
        }

    bucket_name = 'flights-data-storage'
    s3_subfolder = f"raw/{airport_icao}/"
    s3_key = f"{s3_subfolder}{flight_date}-flights_data.json"
    
    try:
        s3.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json.dumps(all_flight_data),
            ContentType='application/json'
        )
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error storing data in S3: {str(e)}')
        }

    return {
        'statusCode': 200,
        'body': json.dumps(f'Data for {flight_date} successfully stored in S3.')
    }