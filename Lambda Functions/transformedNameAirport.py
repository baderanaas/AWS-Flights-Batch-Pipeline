import os
import json
import time
import boto3
import pandas as pd
from io import BytesIO

s3_client = boto3.client('s3')
glue = boto3.client('glue')

def lambda_handler(event, context):
    try:
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        object_key = event['Records'][0]['s3']['object']['key']
        
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        file_content = response['Body'].read().decode('utf-8')
        sample_data = json.loads(file_content)
        
        data = pd.json_normalize(sample_data)

        selected_columns = {
            "flight_date": "flight_date",
            "flight_status": "flight_status",
            "departure.airport": "departure_airport_name",
            "departure.icao": "departure_airport_icao",
            "departure.timezone": "airport_timezone",
            "departure.terminal": "departure_terminal",
            "departure.gate": "departure_gate",
            "departure.delay": "departure_delay",
            "departure.scheduled": "scheduled_departure_time",
            "departure.actual": "actual_departure_time",
            "arrival.airport": "arrival_airport_name",
            "arrival.icao": "arrival_airport_icao",
            "arrival.timezone": "arrival_timezone",
            "airline.name": "airline_name",
            "airline.icao": "airline_icao",
            "flight.icao": "flight_icao",
            "aircraft.icao24": "aircraft_icao24"
        }

        filtered_data = data[list(selected_columns.keys())]
        filtered_data.rename(columns=selected_columns, inplace=True)

        parquet_buffer = BytesIO()
        filtered_data.to_parquet(parquet_buffer, engine='pyarrow', index=False)

        new_object_key = f"transformed/{object_key.split('/')[1]}/{object_key.split('/')[-1].replace('.json', '.parquet')}"

        parquet_buffer.seek(0)
        s3_client.put_object(
            Bucket=bucket_name,
            Key=new_object_key,
            Body=parquet_buffer.getvalue(),
            ContentType='application/octet-stream'
        )

        time.sleep(10)
        crawler = os.getenv("CRAWLER_NAME")
        response = glue.start_crawler(Name=crawler)

        return {
            'statusCode': 200,
            'body': f"Processed file saved to {bucket_name}/{new_object_key}"
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': f"Error processing file: {str(e)}"
        }
