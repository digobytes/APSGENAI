import boto3
import json
from io import BytesIO
import csv

def convert_csv_to_json(csv_data):
    # Initialize an empty list to store the JSON data
    json_data = []

    # Parse the CSV data
    csv_reader = csv.DictReader(csv_data.splitlines())
    
    # Iterate over each row in the CSV data
    for row in csv_reader:
        # Convert each row to a dictionary and append it to the list
        json_data.append(row)
    
    # Return the JSON data
    return json_data


def lambda_handler(event, context):
    # Get the S3 bucket and key from the event
    s3_bucket = event['Records'][0]['s3']['bucket']['name']
    input_key = event['Records'][0]['s3']['object']['key']
    
    s3_bucket_output = 'aps-document-medicalcomprehend-output'
    s3_bucket_sentence = 'aps-sagemaker-inputtext'  # Corrected bucket name
    
    # Initialize the S3 and Comprehend Medical clients
    s3_client = boto3.client('s3')
    comprehend_medical_client = boto3.client('comprehendmedical')
    
    # Read text data from the input file in S3
    response = s3_client.get_object(Bucket=s3_bucket, Key=input_key)
    text_data = response['Body'].read().decode('utf-8')
    
    # Split text data into chunks of 5000 characters each
    chunk_size = 5000
    text_chunks = [text_data[i:i+chunk_size] for i in range(0, len(text_data), chunk_size)]
    
    # Initialize lists to store entities from all chunks
    all_entities = []
    for chunk in text_chunks:
        # Perform medical entity detection using Amazon Comprehend Medical for each chunk
        medical_response = comprehend_medical_client.detect_entities_v2(Text=chunk)
        
        # Extract entities with score > 0.75
        entities = [entity for entity in medical_response['Entities'] if entity['Score'] > 0.75]
        
        # Extract RxNorm concepts if available
        rxnorm_concepts = medical_response.get('RxNormConcepts', [])
        rxnorm_concepts = [concept for concept in rxnorm_concepts]
        
        # Extract ICD-10-CM concepts with score > 0.75
        icd10_concepts = [concept for concept in medical_response.get('ICD10CMConcepts', [])]
        
        # Extract SNOMED CT concepts with score > 0.75
        snomed_concepts = [concept for concept in medical_response.get('SNOMEDCTConcepts', [])]
        
        # Combine all entities
        all_entities.extend(entities + rxnorm_concepts + icd10_concepts + snomed_concepts)
    
    # Create a CSV buffer
    csv_buffer = BytesIO()
    
    # Write entity detection results to the CSV buffer
    csv_buffer.write('Type,Text,Category,Score\n'.encode('utf-8'))
    for entity in all_entities:
        csv_buffer.write(f"{entity['Type']},{entity['Text']},{entity.get('Category', '')},{entity['Score']}\n".encode('utf-8'))
    
    # Upload the CSV file to S3
    output_key = input_key.replace('.txt', '_medical_entities.csv')
    s3_client.put_object(Body=csv_buffer.getvalue(), Bucket=s3_bucket_output, Key=output_key)
    
    # Convert CSV to JSON
    csv_data = csv_buffer.getvalue().decode('utf-8')
    json_data = convert_csv_to_json(csv_data)
    
    # Initialize variables to store sentences for each category
    medical_condition_sentence = "The patient has the following medical conditions: "
    anatomy_sentence = "Anatomical details: "
    test_treatment_procedure_sentence = "Tests, treatments, and procedures involved: "
    behavioral_environmental_social_sentence = "Behavioral, environmental, and social factors: "
    medication_sentence = "Medications prescribed: "
    first_condition_met = False
    
    # Sets to track added texts
    medical_conditions = set()
    anatomy_details = set()
    test_treatment_procedures = set()
    behavioral_environmental_social_factors = set()
    medications = set()
    
    # Iterate over the JSON data
    for row in json_data:
        if row['Category'] == 'PROTECTED_HEALTH_INFORMATION':
            continue
        
        category = row['Category']
        text = row['Text']
        type_value = row['Type']
        
        if category == 'MEDICAL_CONDITION' and float(row['Score']) > 0.5 and text not in medical_conditions:
            if not first_condition_met:
                medical_condition_sentence += text
                first_condition_met = True
            else:
                medical_condition_sentence += ", " + text
            medical_conditions.add(text)
        elif category == 'ANATOMY' and float(row['Score']) > 0.5 and text not in anatomy_details:
            anatomy_sentence += text + ", "
            anatomy_details.add(text)
        elif category == 'TEST_TREATMENT_PROCEDURE' and float(row['Score']) > 0.5 and text not in test_treatment_procedures:
            test_treatment_procedure_sentence += text + ", "
            test_treatment_procedures.add(text)
        elif category == 'BEHAVIORAL_ENVIRONMENTAL_SOCIAL' and float(row['Score']) > 0.5 and text not in behavioral_environmental_social_factors:
            behavioral_environmental_social_sentence += f"{type_value} is {text}, "
            behavioral_environmental_social_factors.add(text)
        elif category == 'MEDICATION' and float(row['Score']) > 0.5 and text not in medications:
            medication_sentence += text + ", "
            medications.add(text)
    
    # Remove trailing commas and spaces
    medical_condition_sentence = medical_condition_sentence.rstrip(", ")
    anatomy_sentence = anatomy_sentence.rstrip(", ")
    test_treatment_procedure_sentence = test_treatment_procedure_sentence.rstrip(", ")
    behavioral_environmental_social_sentence = behavioral_environmental_social_sentence.rstrip(", ")
    medication_sentence = medication_sentence.rstrip(", ")
    
    # Combine all sentences into one final sentence
    final_sentence = f"{medical_condition_sentence}. {anatomy_sentence}. {test_treatment_procedure_sentence}. {behavioral_environmental_social_sentence}. {medication_sentence}."
    
    # Print the final sentence
        
    
    # Upload the sentence to a different S3 bucket
    sentence_key = input_key.replace('.txt', '_medical_sentence.txt')
    s3_client.put_object(Body=final_sentence.encode('utf-8'), Bucket=s3_bucket_sentence, Key=sentence_key)
    
    # Return the response
    return {
        'statusCode': 200,
        'body': json.dumps(final_sentence)
    }
