import boto3
import json

sf_client = boto3.client('stepfunctions')

def lambda_handler(event, context):
    response = sf_client.start_execution(
        stateMachineArn='arn:aws:states:us-east-2:<enter_your_own_user_id>:stateMachine:airline-project-stateMachine',
        input=json.dumps({})  # empty input
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps('Step Function started successfully!')
    }