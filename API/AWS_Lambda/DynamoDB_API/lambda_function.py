import boto3
import json
from decimal import Decimal

# Define the DynamoDB table that Lambda will connect to
table_name = "DiwataGasComplaints"

# Create the DynamoDB resource
dynamo = boto3.resource('dynamodb').Table(table_name)

# Set Encoder
class SetEncoder(json.JSONEncoder):
    """
    A class module based from json.JSONEncoder
    named SetEncoder that encodes Python objects into
    JSON-serializable objects.

    Methods
    -------
    default(obj):
        Return JSON-serializable counterpart of Python object.
    """
    def default(self, obj):
        """Return JSON-serializable counterpart of Python object.
    
        Parameters
        ----------
        obj
            Python object
    
        Returns
        -------
        JSON-serializable object
        """
        if isinstance(obj, set):
            return list(obj)
        elif isinstance(obj, Decimal):
            return int(obj)
        return json.JSONEncoder.default(self, obj)

# Define some functions to perform the operations
def scan():
    """Scan DynamoDB table.
    
    Returns
    -------
    dict
        Dict containing list of items
    """
    return json.loads(json.dumps(dynamo.scan()['Items'], cls=SetEncoder))

def find(payload):
    """Find specific item using key-pair.
    
    Parameters
    ----------
    payload : dict
        API Object
    
    Returns
    -------
    dict
        Dict containing details of specific item
    """
    return json.loads(json.dumps(dynamo.get_item(Key=payload['Key']),
        cls=SetEncoder))
    
def query_item(payload):
    """Query specific detail about a cylinder.
    
    Parameters
    ----------
    payload : dict
        API Object
    
    Returns
    -------
    response : dict
        Dict containing list of a specific detail about a cylinder
    """
    if payload['Item'] == 'Date':
        return json.loads(json.dumps(dynamo.query(
            KeyConditionExpression='SerialNo = :serialno',
            ExpressionAttributeNames={'#D': 'Date'},
            ExpressionAttributeValues={':serialno': payload['SerialNo']},
            ProjectionExpression='#D'), cls=SetEncoder))
    elif payload['Item'] == 'Time':
        return json.loads(json.dumps(dynamo.query(
            KeyConditionExpression='SerialNo = :serialno',
            ExpressionAttributeNames={'#T': 'Time'},
            ExpressionAttributeValues={':serialno': payload['SerialNo']},
            ProjectionExpression='#T'), cls=SetEncoder))
    else:
        return json.loads(json.dumps(dynamo.query(
            KeyConditionExpression='SerialNo = :serialno',
            ExpressionAttributeValues={':serialno': payload['SerialNo']},
            ProjectionExpression=payload['Item'])['Items'], cls=SetEncoder))

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
    'scan': scan,
    'find': find,
    'query_item': query_item,
    'echo': echo,
}

def lambda_handler(event, context):
    '''Provide an event that contains the following keys:
      - operation: one of the operations in the operations dict below
      - payload: a JSON object containing parameters to pass to the 
        operation being performed
    '''
    
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