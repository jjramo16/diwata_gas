import json
import re
import boto3
from botocore.exceptions import ClientError


# Define some functions to perform the operations
def list_gold():
    """List gold files.
    
    Returns
    -------
    dict_gold : dict
        Dict containing list of gold files
    """
    dict_gold = {}
    dict_gold['Items'] = []
    for obj in bucket.objects.all():
        if re.match(r"gold/.+", obj.key):
            dict_gold['Items'].append(obj.key)
    return dict_gold

def list_work():
    """List work files.
    
    Returns
    -------
    dict_work : dict
        Dict containing list of work files
    """
    dict_work = {}
    dict_work['Items'] = []
    for obj in bucket.objects.all():
        if re.match(r"work/.+", obj.key):
            dict_work['Items'].append(obj.key)
    return dict_work
    
def download_gold(payload):
    """Provide presigned URL for S3 gold object
    
    Parameters
    ----------
    payload : dict
        API Payload
    
    Returns
    -------
    dict
        Dict containing presigned URL for S3 gold object or error message
    """
    if payload['fact_table'] == 'logistics':
        try:
            response = s3_2.generate_presigned_url('get_object',
                        Params={'Bucket': 'diwata-gas-bucket',
                        'Key': f'gold/LogisticsFactTable_{payload['month'].title()}_{payload['year']}.csv'},
                        ExpiresIn=3600)
            return {
                'statusCode': 200,
                'body': response
            }
        except ClientError as e:
            return {
                'statusCode': 400,
                'body': json.dumps('Downloading failed...')
            }

    elif payload['fact_table'] == 'sales':
        try:
            response = s3_2.generate_presigned_url('get_object',
                        Params={'Bucket': 'diwata-gas-bucket',
                        'Key': f'gold/SalesFactTable_{payload['month'].title()}_{payload['year']}.csv'},
                        ExpiresIn=3600)
            return {
                'statusCode': 200,
                'body': response
            }
        except ClientError as e:
            return {
                'statusCode': 400,
                'body': json.dumps('Downloading failed...')
            }
    elif payload['fact_table'] == 'inventory':
        try:
            response = s3_2.generate_presigned_url('get_object',
                        Params={'Bucket': 'diwata-gas-bucket',
                        'Key': f'gold/InventoryFactTable_{payload['month'].title()}_{payload['year']}.csv'},
                        ExpiresIn=3600)
            return {
                'statusCode': 200,
                'body': response
            }
        except ClientError as e:
            return {
                'statusCode': 400,
                'body': json.dumps('Downloading failed...')
            }
    else:
        return {
                'statusCode': 400,
                'body': json.dumps('No such file...')
            }

def download_work(payload):
    """Provide presigned URL for S3 work object.
    
    Parameters
    ----------
    payload : dict
        API Payload
    
    Returns
    -------
    response : dict
        Dict containing presigned URL for S3 work object or error message
    """
    if payload['table'] == 'customer_order':
        try:
            response = s3_2.generate_presigned_url('get_object',
                        Params={'Bucket': 'diwata-gas-bucket',
                        'Key': f'work/CustomerOrder_{payload['month'].title()}_{payload['year']}.parquet'},
                        ExpiresIn=3600)
            return {
                'statusCode': 200,
                'body': response
            }
        except ClientError as e:
            return {
                'statusCode': 400,
                'body': json.dumps('Downloading failed...')
            }

    elif payload['table'] == 'logistics':
        try:
            response = s3_2.generate_presigned_url('get_object',
                        Params={'Bucket': 'diwata-gas-bucket',
                        'Key': f'work/Logistics_{payload['month'].title()}_{payload['year']}.parquet'},
                        ExpiresIn=3600)
            return {
                'statusCode': 200,
                'body': response
            }
        except ClientError as e:
            return {
                'statusCode': 400,
                'body': json.dumps('Downloading failed...')
            }
            
def echo(payload):
    """Echo payload.
    
    Parameters
    ----------
    payload : dict
        API Payload
    
    Returns
    -------
    payload : dict
        API Payload
    """
    return payload
            
operations = {
    'list_gold': list_gold,
    'list_work': list_work,
    'download_gold': download_gold,
    'download_work': download_work,
    'echo': echo,
}

def lambda_handler(event, context):
    """Handles Lambda calls.
    
    Parameters
    ----------
    event : dict
        Overall API Payload
    
    Returns
    -------
    dict
        API Response
    """
    
    print('Loading function')
    
    with open('api_key.txt') as f:
        api_key = f.read().strip()
    
    try:
        if event['api_key'] == api_key:
            pass
        else:
            return {
                'statusCode': 400,
                'body': 'Wrong API Key input'
            }
    except Exception:
        return {
                'statusCode': 400,
                'body': 'No API Key input'
            }
        
    
    global s3
    s3 = boto3.resource('s3')
    global s3_2
    s3_2 = boto3.client('s3')

    global bucket
    bucket = s3.Bucket('diwata-gas-bucket')
    
    operation = event['operation']
    try:
        payload = event['payload']
    except Exception:
        pass
    
    if operation in operations:
        try:
            return operations[operation](payload)
        except Exception:
            return operations[operation]()
        
    else:
        raise ValueError(f'Unrecognized operation "{operation}"')