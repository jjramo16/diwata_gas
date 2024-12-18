import json
import boto3
import psycopg2
from decimal import Decimal


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
        else:
            return str(obj)
        return json.JSONEncoder.default(self, obj)
    
def list_dimensions():
    """List dimensions.
    
    Returns
    -------
    response : dict
        Dict containing list of dimensions
    """
    response = client.list_tables(
        ClusterIdentifier='diwata-gas-olap',
        Database='diwatagas_olap_db',
        SecretArn="arn:aws:secretsmanager:us-east-1:590184004366:secret:diwata-secret-6zcPUM",
        MaxResults=123,
        TablePattern="%dimension"
    )
    return response
    
def list_facttables():
    """List fact tables.
    
    Returns
    -------
    response : dict
        Dict containing list of fact tables
    """
    response = client.list_tables(
        ClusterIdentifier='diwata-gas-olap',
        Database='diwatagas_olap_db',
        SecretArn="arn:aws:secretsmanager:us-east-1:590184004366:secret:diwata-secret-6zcPUM",
        MaxResults=123,
        TablePattern="%facttable"
    )
    return response

    
def describe_table(payload):
    """Describe specific table.
    
    Parameters
    ----------
    payload : dict
        API Payload
    
    Returns
    -------
    dict
        Dict containing description of table.
    """
    response = client.describe_table(
        ClusterIdentifier='diwata-gas-olap',
        Database='diwatagas_olap_db',
        SecretArn="arn:aws:secretsmanager:us-east-1:590184004366:secret:diwata-secret-6zcPUM",
        MaxResults=123,
        Table=payload['table']
    )
    return {'ColumnList': [i['name'] for i in response['ColumnList']],
            'TableName': response['TableName']}

def query(payload):
    """Query from OLAP.
    
    Parameters
    ----------
    payload : dict
        API Payload
    
    Returns
    -------
    response : dict
        Dict containing query result or error message.
    """
    try:
        if payload['query'].startswith("SELECT"):
            DB_NAME = "diwatagas_olap_db"
            DB_PORT = 5439
            DB_HOST = "diwata-gas-olap.cxcjt2ltmyli.us-east-1.redshift.amazonaws.com"
            DB_USER = cluster_creds['DbUser']
            DB_PWD = cluster_creds['DbPassword']
            conn_string = "dbname='{}' port='{}' host='{}' user='{}' password='{}'".format(DB_NAME, DB_PORT, DB_HOST, DB_USER, DB_PWD)
            con = psycopg2.connect(conn_string)
            cur = con.cursor()
            cur.execute(payload['query'])
            lst_items = cur.fetchall()
            lst_desc = [desc[0] for desc in cur.description]
            con.close()
            return json.loads(json.dumps({'ColumnList': lst_desc,
                                        'Items': lst_items
            }, cls=SetEncoder))
        else:
            return {
                'statusCode': 400,
                'body': json.dumps('Request is not a query. Start your query with SELECT.')
            }
    except Exception as err:
        return str(err)
    
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
    'list_facttables': list_facttables,
    'list_dimensions': list_dimensions,
    'describe_table': describe_table,
    'query': query,
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
    
    global client
    client = boto3.client('redshift-data', region_name="us-east-1")
    
    client_2 = boto3.client('redshift', region_name="us-east-1")
    global cluster_creds
    cluster_creds = client_2.get_cluster_credentials(DbUser="jj",
                    DbName="diwatagas_olap_db",
                    ClusterIdentifier="diwata-gas-olap",
                    AutoCreate=False)
    
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