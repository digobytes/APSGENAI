import json
import boto3
import csv
import io
import os

def lambda_handler(event, context):
 # Define S3 client
    s3 = boto3.client('s3')
    
    # Retrieve bucket and key from event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    # Define output bucket
    bucket_arn = 'aps-document-extratedate-medinput'  # Output bucket ARN
    filename_without_extension, _ = os.path.splitext(os.path.basename(key))
    text_file_key = filename_without_extension + ".txt"  # Use the same object key with .txt extension
    # Read CSV file from S3
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        csv_content = response['Body'].read().decode('utf-8')
    except Exception as e:
        print(f"Error reading CSV file:event:{event} error {e}")
        raise e
    # Convert CSV to text format, excluding the "name" and "value" columns
    try:
        text_data = ''
        csv_reader = csv.DictReader(io.StringIO(csv_content))
        for row in csv_reader:
            # Exclude the "name" and "value" columns
            row_without_name_value = {key: value for key, value in row.items() if key not in ["'Confidence Score % (Key)", "'Confidence Score % (Value)"]}
            text_data += ', '.join(row_without_name_value.values()) + '\n'  # Join row elements with comma and add newline
    except Exception as e:
        print(f"Error converting CSV to text: {e}")
        raise e
    # Write text data to output bucket
    try:
        text_file_content = text_data.encode('utf-8')
        # Upload the text file to the output bucket
        s3.put_object(Bucket=bucket_arn, Key=text_file_key, Body=text_file_content)
        print(f"Text file saved to {bucket_arn}/{text_file_key}")
    except Exception as e:
        print(f"Error writing text data to S3:output_bucket{bucket_arn} key {text_file_key} error {e}")
        raise e

    return {
        'statusCode': 200,
        'body': 'CSV to text conversion successful!'
    }