# APSGENAI

Concept - Use APS statement to decide user claim to be be approved or rejetec
The POC uses S3 Bucket,AMazon Textract,Amazon Medical comprehend Lamda functions and Sagemaker

STEPS

Step 1: Input -> An Image or Text DOC - Uploaded to a bucket name - aps-document-input

Step 2: A lamda functions ambda_function-Textract.py will trigger on uploading the doc and use Amazon textract to convert the doc to Text and save the categorized excel in a bucket aps-document-ocr-textract.(sample file name :OCRData.csv)

Step 3: The csv file in the location is picked up by another lambda function lambda_function-formMedInputFromTextractedData.py and save a proper text input in another bucket aps-document-extratedate-medinput.(sample file name :OCRText.txt)

Step 4: The lambda_function-sageInput(Medcomprehend and classification).py function will pick the doument from the S3 and do medical comprehend , extract the medical entities and the function filter out protected medical information and convert medical code to proper medical term , the formed sentence is save to another bucket aps-sagemaker-inputtext
(sample file name :FIlteredTextforPrediction.txt)

The Medical comprehend results are save in a bucket aps-document-medicalcomprehend-output( sample file name :MedicalComprehendDataOutput.csv)

We are saving the indermedite results in a bucket to analyse and understand the service behaviours

Step 4: Train a model using AWS sagemake studio/canvas. Once the model is created predict the result using a sample text.








