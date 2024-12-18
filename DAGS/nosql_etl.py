from airflow import DAG
from datetime import datetime
from airflow.io.path import ObjectStoragePath
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.dynamodb import DynamoDBHook
from boto3.dynamodb.conditions import Key
import pendulum

with DAG(dag_id='nosql_etl',
         schedule='@daily',
         start_date=pendulum.datetime(2024, 9, 19, tz="Asia/Manila"),
        ) as dag:
    import json
    import pandas as pd
    import re
    import boto3
    
    base1 = ObjectStoragePath("s3://s3_diwata_gas@diwata-gas-bucket/landing/CustomerComplaint/")
    base2 = ObjectStoragePath("s3://s3_diwata_gas@diwata-gas-bucket/landing/CustomerResolution/")
    
    @task
    def nosql_etl():
        """Extract, transform, and load data for DynamoDB.
        """
        dynamodb = boto3.resource('dynamodb')
        diwata_gas = dynamodb.Table('DiwataGasComplaints')
        nosql_files = [f for f in base1.iterdir() if f.is_file()]
        s3 = boto3.resource('s3')
        for files in nosql_files:
            nosql_json = json.loads(files.read_text())
            try:
                if len(diwata_gas.query(KeyConditionExpression=Key('SerialNo').eq(nosql_json['ComplaintSerialNo']))['Item']) == 0:
                    complaint_id = 1
                else:
                    complaint_id = 2
            except Exception:
                complaint_id = len(diwata_gas.query(KeyConditionExpression=Key('SerialNo').eq(nosql_json['ComplaintSerialNo']))['Items']) + 1
            diwata_gas.put_item(
                Item={
                    'SerialNo': nosql_json['ComplaintSerialNo'],
                    'ComplaintID': complaint_id,
                    'CustomerName': nosql_json['CustomerComplaintName'],
                    'Date': datetime.strftime(datetime.strptime(nosql_json['ComplaintDateTime'], "%Y-%m-%dT%H:%M"), '%Y-%m-%d'),
                    'Time': datetime.strftime(datetime.strptime(nosql_json['ComplaintDateTime'], "%Y-%m-%dT%H:%M"), '%H:%M:00'),
                    'CustomerContactNumber': nosql_json['CustomerComplaintContactNumber'],
                    'ChiefComplaints': set(nosql_json['ChiefComplaint'].split(', ')),
                    'PictureS3Path': nosql_json['picture'],
                    'CustomerReviews': nosql_json['ComplaintDetails']
                })
            s3.Object("diwata-gas-bucket", f"landing/CustomerComplaint/Processed/{files.name}").copy_from(
            CopySource=f"diwata-gas-bucket/landing/CustomerComplaint/{files.name}")
            
            s3.Object("diwata-gas-bucket", f"landing/CustomerComplaint/{files.name}").delete()
        
    
    @task
    def nosql_update():
        """Update data for DynamoDB.
        """
        dynamodb = boto3.resource('dynamodb')
        diwata_gas = dynamodb.Table('DiwataGasComplaints')
        nosql_files = [f for f in base2.iterdir() if f.is_file()]
        s3 = boto3.resource('s3')
        for files in nosql_files:
            nosql_json = json.loads(files.read_text())
            if nosql_json['ForReplacement'] == 'Yes':
                replace = True
            else:
                replace = False
            try:
                diwata_gas.update_item(
                    Key={
                        'SerialNo': nosql_json['SerialNo'],
                        'ComplaintID': int(nosql_json['ComplaintID'])
                    },
                    UpdateExpression="set IsReplaced = :r, MaintenanceComments = :m",
                    ExpressionAttributeValues={
                        ':r': replace, ':m': nosql_json['ResComment']
                    }
                )
                s3.Object("diwata-gas-bucket", f"landing/CustomerResolution/Processed/{files.name}").copy_from(
                CopySource=f"diwata-gas-bucket/landing/CustomerResolution/{files.name}")
                
                s3.Object("diwata-gas-bucket", f"landing/CustomerResolution/{files.name}").delete()
            except Exception:
                s3.Object("diwata-gas-bucket", f"landing/CustomerResolution/Error/{files.name}").copy_from(
                CopySource=f"diwata-gas-bucket/landing/CustomerResolution/{files.name}")
                
                s3.Object("diwata-gas-bucket", f"landing/CustomerResolution/{files.name}").delete()

    
    nosql_etl() >> nosql_update()
    