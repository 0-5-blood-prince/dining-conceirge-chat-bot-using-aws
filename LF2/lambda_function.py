import json
import os
import random

import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from botocore.exceptions import ClientError

from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart

REGION = 'us-east-1'
HOST = 'search-search-restaurants-gbow43n6gwqdrtl3xm5zpkhovm.us-east-1.es.amazonaws.com'
INDEX = 'restaurants'


def send_email(recipient_email, body):
    CHARSET = "utf-8"
    client = boto3.client('ses')
    Sender = "ag4797@columbia.edu"
    msg = MIMEMultipart('mixed')
    msg['Subject'] = "A Message from your Conceirge"
    msg['From'] = Sender
    msg['To'] = recipient_email

    msg_body = MIMEMultipart('alternative')

    textpart = MIMEText(body.encode(CHARSET), 'plain', CHARSET)

    msg_body.attach(textpart)
   
    msg.attach(msg_body)
    try:
        #Provide the contents of the email.
        response = client.send_raw_email(
            Source=Sender,
            Destinations=[
                recipient_email
            ],
            RawMessage={
                'Data':msg.as_string(),
            })
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])

def lambda_handler(event, context):

    sqs = boto3.client('sqs')
    queue_url = 'https://sqs.us-east-1.amazonaws.com/803282570448/Q1'
    response = sqs.receive_message(
            QueueUrl=queue_url,
            AttributeNames=['All'],
            WaitTimeSeconds=0,
        )

    if 'Messages' in response:
        for message in response['Messages']:
            print(message)
            message_data = json.loads(message['Body'].replace("\'", "\""))
            print(message_data)
            cuisine = message_data['Cuisine']
            email = message_data['Email']
            Reservation_Time = message_data['Dining Time']
            Reservation_Date = message_data['Reservation Date']
            Num_People = message_data['Number of people']
            sessionId = message_data['sessionId']
            results = query(cuisine)
            
                    
            data_list = []
            for result in results:
                data = lookup_data({'Business ID': result["restaurant"]})
                print(data)
                data_list.append(data)

            email_message = 'Hello! Here are my {} restaurant suggestions for {} people, for today at {}:\n'.format(cuisine, Num_People, Reservation_Time)
            for i in range(len(data_list)):
                email_message = email_message + "{}, {}, located at {},\n".format(i+1, data_list[i]["Name"], data_list[i]["Address"])
            past_recommendations = lookup_data({'sessionId': sessionId},table='recommendations')
            
            
            if past_recommendations:
                print(past_recommendations)
                for history in past_recommendations['history']:
                    if history['location'] == message_data['Location'] and history['cuisine'] == cuisine:
                        recommendations = history['recommendation']
                        email_message += "You might want to take a look at your previous restaurant suggestions\n"
                        j = 0
                        for recommendation in recommendations:
                            data = lookup_data({'Business ID': recommendation})
                            email_message += "{}, {}, located at {},\n".format(j+1,data["Name"], data["Address"] )
                            j+=1
                        break
                    
                
                
            send_email(email, email_message)
            
            sqs.delete_message(QueueUrl=queue_url , ReceiptHandle=message['ReceiptHandle'])
            if sessionId:
                if past_recommendations:
                    past_history_list = past_recommendations['history']
                    a = []
                    for result in results:
                        a.append(result["restaurant"])
                    history = []
                    history.append({
                            "location": message_data['Location'],
                            "cuisine": cuisine,
                            "recommendation": a
                        })
                    history.extend(past_history_list)
                    recommendation = {
                        "sessionId": sessionId,
                        "history": history
                    }
                    delete_item({'sessionId': sessionId},table='recommendations')
                    insert_data([recommendation])
                else:
                    a = []
                    for result in results:
                        a.append(result["restaurant"])
                    history =[{
                        "location": message_data['Location'],
                            "cuisine": cuisine,
                            "recommendation": a
                            }]
                    recommendation = {
                        "sessionId": sessionId,
                        "history": history
                    }
                    insert_data([recommendation])


def query(term):

    client = OpenSearch(hosts=[{
        'host': HOST,
        'port': 443
    }],
                        http_auth=get_awsauth(REGION, 'es'),
                        use_ssl=True,
                        verify_certs=True,
                        connection_class=RequestsHttpConnection)


    q = {'query': {'multi_match': {'query': term}}}
    res = client.count(index=INDEX, body=q)
    total = res['count']

    start = random.randint(0,total-1)
    size = random.randint(1,20)
    if start+size > total:
        start = total-size-1

    q = {'from': start,'size': size, 'query': {'multi_match': {'query': term}}}
    res = client.search(index=INDEX, body=q)

    hits = res['hits']['hits']
    results = []
    print(len(hits))
    for hit in hits:
        results.append(hit['_source'])

    return results

def insert_data(data_list, db=None, table='recommendations'):
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
        return None
    else:
        if 'Item' in response:
            return response['Item']
        else:
            return None
            
        
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

def get_awsauth(region, service):
    cred = boto3.Session().get_credentials()
    return AWS4Auth(cred.access_key,
                    cred.secret_key,
                    region,
                    service,
                    session_token=cred.token)
