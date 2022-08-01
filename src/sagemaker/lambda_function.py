import os
import io
import boto3
import json


def lambda_handler(event, context):
    
    runtime= boto3.client('runtime.sagemaker')
    
    response = {
        "statusCode": 200,
        "statusDescription": "200 OK",
        "isBase64Encoded": False,
        "headers": {
            "Content-Type": "application/json;"
        }
    }
   
    try:
        json_body = json.loads(event['body'])
        endpoint_to_call = json_body['input'][0]['model_endpoint']
        
        string_body = json.dumps(json_body)
        response_sagemaker = runtime.invoke_endpoint(
            EndpointName=endpoint_to_call,
            ContentType='application/json',
            Body=string_body
        )
        
        result = json.loads(response_sagemaker['Body'].read().decode())
        response['body'] = json.dumps(result)

    except Exception as e:
        response['statusCode'] = 400
        response['statusDescription'] = "400 Bad Request"
        sagemaker_error = {
            'error_message' : 'Bad Request:',
            'details' : ''+str(e)
        }
        response['body'] = json.dumps(sagemaker_error)

    return response
