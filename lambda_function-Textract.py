import boto3
import json
import urllib.parse
import time
import csv
from io import StringIO

def start_job(client, s3_bucket_name, object_name):
    response = client.start_document_analysis(
        DocumentLocation={
            'S3Object': {
                'Bucket': s3_bucket_name,
                'Name': object_name
            }},
        FeatureTypes=['FORMS'])

    return response["JobId"]

def is_job_complete(client, job_id):
    time.sleep(1)
    response = client.get_document_analysis(JobId=job_id)
    status = response["JobStatus"]
    print("Job status: {}".format(status))

    while status == "IN_PROGRESS":
        time.sleep(1)
        response = client.get_document_analysis(JobId=job_id)
        status = response["JobStatus"]
        print("Job status: {}".format(status))

    return status

def get_kv_pairs(client, job_id):
    response = client.get_document_analysis(JobId=job_id)
    blocks = response['Blocks']
    key_map = {}
    value_map = {}
    block_map = {}
    extracted_text = ''
    for block in blocks:
        if 'Text' in block:
            extracted_text += block['Text'] + '\n'
        block_id = block['Id']
        block_map[block_id] = block
        if block['BlockType'] == "KEY_VALUE_SET":
            if 'KEY' in block['EntityTypes']:
                key_map[block_id] = block
            else:
                value_map[block_id] = block

    kvs = []
    for block_id, key_block in key_map.items():
        value_block = find_value_block(key_block, value_map)
        key = get_text(key_block, block_map)
        val = get_text(value_block, block_map)
        kvs.append({'Page number': response['Blocks'][0]['Page'],
                    'Key': key,
                    'Value': val})

    return extracted_text,kvs

def find_value_block(key_block, value_map):
    for relationship in key_block.get('Relationships', []):
        if relationship.get('Type') == 'VALUE':
            for value_id in relationship.get('Ids', []):
                value_block = value_map.get(value_id)
                if value_block:
                    return value_block
    return None

def get_text(result, blocks_map):
    text = ''
    if result and 'Relationships' in result:
        for relationship in result['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    word = blocks_map.get(child_id)
                    if word and word['BlockType'] == 'WORD':
                        text += word['Text'] + ' '
                    if word and word['BlockType'] == 'SELECTION_ELEMENT':
                        if word['SelectionStatus'] == 'SELECTED':
                            text += 'X '
    return text

def lambda_handler(event, context):
    textract_client = boto3.client('textract')
    s3_client = boto3.client('s3')

    input_bucket = event['Records'][0]['s3']['bucket']['name']
    output_bucket = 'aps-document-ocr-textract'

    input_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    job_id = start_job(textract_client, input_bucket, input_key)

    if is_job_complete(textract_client, job_id) != 'SUCCEEDED':
        print(f"Textract job failed for {input_key}")
        return {
            'statusCode': 400,
            'body': json.dumps('Textract job failed')
        }

    extracted_text,kv_pairs = get_kv_pairs(textract_client, job_id)

    output_key = input_key.split('.')[0] + '_extracted.csv'

    csv_buffer = StringIO()
    csv_writer = csv.DictWriter(csv_buffer, fieldnames=['Page number', 'Key', 'Value'])
    csv_writer.writeheader()
    csv_writer.writerows(kv_pairs)
    csv_writer.writerow({'Key': 'Extracted Text', 'Value': extracted_text})

    s3_client.put_object(
        Body=csv_buffer.getvalue(),
        Bucket=output_bucket,
        Key=output_key
    )

    return {
        'statusCode': 200,
        'body': json.dumps(f'CSV file saved to s3://{output_bucket}/{output_key}')
    }
