import json
import ast
import time
import boto3
from botocore.exceptions import ClientError


def lambda_handler(event, context):
    with open('data.txt', 'r') as file:
        data_list = []
        for data in file:
            data_list.append(ast.literal_eval(data))
        insert_data(data_list)
    return


def insert_data(data_list, db=None, table='yelp-restaurants'):
    if not db:
        db = boto3.resource('dynamodb')
    table = db.Table(table)
    with table.batch_writer() as batch:
        # overwrite if the same index is provided
        for data in data_list:
            response = table.put_item(Item=data)
    return response


def lookup_data(key, db=None, table='yelp-restaurants'):
    if not db:
        db = boto3.resource('dynamodb')
    table = db.Table(table)
    try:
        response = table.get_item(Key=key)
    except ClientError as e:
        print('Error', e.response['Error']['Message'])
    else:
        print(response['Item'])
        return response['Item']


def update_item(key, feature, db=None, table='yelp-restaurants'):
    if not db:
        db = boto3.resource('dynamodb')
    table = db.Table(table)
    # change student location
    response = table.update_item(
        Key=key,
        UpdateExpression="set #feature=:f",
        ExpressionAttributeValues={
            ':f': feature
        },
        ExpressionAttributeNames={
            "#feature": "from"
        },
        ReturnValues="UPDATED_NEW"
    )
    print(response)
    return response


def delete_item(key, db=None, table='yelp-restaurants'):
    if not db:
        db = boto3.resource('dynamodb')
    table = db.Table(table)
    try:
        response = table.delete_item(Key=key)
    except ClientError as e:
        print('Error', e.response['Error']['Message'])
    else:
        print(response)
        return response
