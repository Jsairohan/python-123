import json
import io as io
import boto3
import pandas as pd
import openpyxl
import simplejson as sj
import urllib.parse
import os as os

s3 = boto3.client('s3')
dynamoDb = boto3.resource('dynamodb')
client = boto3.client('dynamodb')

def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    print("BUCKET : " + bucket)
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    print("KEY : " + key)
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        csv_data_df = pd.read_csv(io.BytesIO(response['Body'].read()), sep="|", index_col=False, dtype=str,
                    names=['STXPlanInfoID', 'ControlPlanCode', 'ControlStationCode', 'ProcessingPlanCode',
                           'ProcessingStationCode', 'Prefix', 'AuditRecInsertDate', 'AuditRecInsertBy', 'EffectiveDate',
                           'ExpirationDate'])

        print(len(csv_data_df))
        csv_data_df = csv_data_df.astype(str)
        csv_data_df['STXPlanInfoID'] = csv_data_df['STXPlanInfoID'].astype('int')
        json_data = csv_data_df.to_json(orient='records')
        deleteExistingRecords()
        pushDataToTable(json_data)
        return "Data has been inserted!!"
    except Exception as e:
        print(e)
        raise e
        
def deleteExistingRecords():
    table_name = os.environ.get('TABLE_NAME')
    table = dynamoDb.Table(table_name)
    table_name_suffix = table_name.split('-')[1]
    print(table_name_suffix)
    #,'ControlPlanCode':item['ControlPlanCode']
    if table_name_suffix == 'STXPlanInfo':
        scan = table.scan()
        print('Existing Records')
        print(len(scan['Items']))
        with table.batch_writer() as batch:
            for item in scan['Items']:
                batch.delete_item({'STXPlanInfoID':item['STXPlanInfoID']})
    print('Existing Data Deleted')

def pushDataToTable(json_array):
    print('Inside pushDataToTable')
    try:
        role_data = json.loads(json_array)
        table_name = os.environ.get('TABLE_NAME')
        print("Table Name :- "+table_name)
        table = dynamoDb.Table(table_name)
        with table.batch_writer() as batch:
            for item in role_data:
              table.put_item(Item=item)
        print('Data has been inserted ')
        #scan = table.scan()
        #print('Inserted Records')
        #print(len(scan['Items']))
    except Exception as e:
        print(e)
        raise e
