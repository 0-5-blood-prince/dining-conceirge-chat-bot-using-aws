import boto3
import random
# Define the client to interact with Lex
client = boto3.client('lexv2-runtime')

def lambda_handler(event, context):
    # msg_from_user = event['messages'][0]
    # change this to the message that user submits on
    # your website using the 'event' variable
        msg_from_user = event['messages'][0]
        user_msg = ""
        sessionId = "testUser"
        if (msg_from_user['type'] == 'unstructured'):
            user_msg = msg_from_user['unstructured']['text']
            print(msg_from_user)
            sessionId = msg_from_user['unstructured']['sessionId']
        print(f"Message from frontend: {user_msg}")
        # Initiate conversation with Lex
         
        response = client.recognize_text(
                                        botId='EPY7H8RUEC', # MODIFY HERE
                                        botAliasId='XGDNW2ZYRK', # MODIFY HERE
                                        localeId='en_US',
                                        sessionId=str(sessionId),
                                        text=user_msg)
        print(response)
        msg_from_lex = response.get('messages', [])
        resp = ""
        if msg_from_lex:
            resp = {
                'statusCode': 200,
                'messages': ([({'type': 'unstructured','unstructured': ({'text': msg_from_lex[0]['content']})})])
            }
        else:
            resp = {
                'statusCode': 400,
                'body': ""
            }
        return resp
        
    
