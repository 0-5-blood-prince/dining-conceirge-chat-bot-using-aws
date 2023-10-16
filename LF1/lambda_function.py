import json
import boto3
import re
from dateutil import parser
import datetime
from word2number import w2n

emailRegex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'


def formAResponse(intentName, msgResponse):
    response = {
        "sessionState": {
            "dialogAction": {
                "type": "Close"
            },
            "intent": {
                "name": intentName,
                "state": "Fulfilled"
            }
        },
        "messages": [ {
            "contentType": "PlainText",
            "content": msgResponse
        }]
       }
    return response
def createGreetingIntentResponse(intentName, event):
    return formAResponse(intentName,"Hi There! How can I help you?")
def createThankYouIntentResponse(intentName, event):
    return formAResponse(intentName,"You're Welcome")
def formASlotResponse(intentName, sessionState):
    response = {
    "sessionState": sessionState
    }
    return response
def sendUserRequestToQueue(SQSAddress, msg):
    sqs_client = boto3.client('sqs')
    return sqs_client.send_message(
        QueueUrl = SQSAddress,
        MessageBody = str(msg)
        )
def createDiningSuggestionResponse(intentName, event):
    cuisines = ["chinese", "indian", "japanese", "mexican", "american", "french", "italian","greek","korean","mediterranean"]
    state = event['sessionState']['intent']['state']
    if state == "ReadyForFulfillment":
        cityArea = event['sessionState']['intent']['slots']['cityArea']['value']['originalValue']
        cuisine = event['sessionState']['intent']['slots']['cuisine']['value']['originalValue']
        numPeople = event['sessionState']['intent']['slots']['numPeople']['value']['originalValue']
        reservationDate = event['sessionState']['intent']['slots']['reservationDate']['value']['originalValue']
        reservationTime = event['sessionState']['intent']['slots']['reservationTime']['value']['originalValue']
        email = event['sessionState']['intent']['slots']['email']['value']['originalValue']
        sessionId = event['sessionId']
        requestJSON = {
            "Location" : cityArea,
            "Cuisine" : cuisine,
            "Number of people": numPeople,
            "Reservation Date": reservationDate,
            "Dining Time": reservationTime,
            "Email": email,
            "sessionId": sessionId
        }
        if sendUserRequestToQueue("https://sqs.us-east-1.amazonaws.com/803282570448/Q1", str(requestJSON)):
            print("Sent to Queue Successfully")
            dialogAction = { "type": "Delegate"}
            event['sessionState']['intent']['state'] = "Fulfilled"
            event['sessionState']['dialogAction'] = dialogAction
        else:
            dialogAction = { "type": "Delegate"}
            event['sessionState']['intent']['state'] = "Failed"
            event['sessionState']['dialogAction'] = dialogAction

        return formASlotResponse(intentName,event['sessionState'])
    elif "cityArea" in event['transcriptions'][0]['resolvedSlots']:
        userInput = event['sessionState']['intent']['slots']['cityArea']['value']['originalValue']
        if "manhattan" in userInput.lower() or "newyork" in userInput.lower() or "new york" in userInput.lower():
            value = {
                    "originalValue": userInput,
                    "interpretedValue": userInput,
                    "resolvedValues": [
                            userInput
                            ]
                    }
            dialogAction = {
                "slotToElicit": "cuisine",
                "type": "ElicitSlot"
            }
            event['sessionState']['intent']['slots']['cityArea']['value'] = value
            event['sessionState']['dialogAction'] = dialogAction
            return formASlotResponse(intentName,event['sessionState'] )
        else:
            dialogAction = {
                    "slotToElicit": "cityArea",
                    "type": "ElicitSlot"
                }
            event['sessionState']['dialogAction'] = dialogAction
            return formASlotResponse(intentName,event['sessionState'] )

    elif "cuisine" in event['transcriptions'][0]['resolvedSlots']:
        userInput = event['sessionState']['intent']['slots']['cuisine']['value']['originalValue']

        for cuisine in cuisines:
            if cuisine.lower() in userInput.lower():

                value = {
                    "originalValue": userInput,
                    "interpretedValue": userInput,
                    "resolvedValues": [
                            userInput
                            ]
                    }
                dialogAction = {
                    "slotToElicit": "numPeople",
                    "type": "ElicitSlot"
                }
                event['sessionState']['intent']['slots']['cuisine']['value'] = value
                event['sessionState']['dialogAction'] = dialogAction
                return formASlotResponse(intentName,event['sessionState'] )
        dialogAction = {
                    "slotToElicit": "cuisine",
                    "type": "ElicitSlot"
                }
        event['sessionState']['dialogAction'] = dialogAction
        return formASlotResponse(intentName,event['sessionState'] )
    elif "numPeople" in event['transcriptions'][0]['resolvedSlots']:
        userInput = event['sessionState']['intent']['slots']['numPeople']['value']['originalValue']
        try:
            matches = w2n.word_to_num(userInput)
            if matches > 0:
                value = {
                            "originalValue": userInput,
                            "interpretedValue": str(matches),
                            "resolvedValues": [
                                    str(matches)
                                    ]
                            }
                event['sessionState']['intent']['slots']['numPeople']['value'] = value
                dialogAction = {
                                    "slotToElicit": "reservationDate",
                                    "type": "ElicitSlot"
                                }
                event['sessionState']['dialogAction'] = dialogAction
                return formASlotResponse(intentName,event['sessionState'] )
            else:
                dialogAction = {
                                "slotToElicit": "numPeople",
                                "type": "ElicitSlot"
                            }
                event['sessionState']['dialogAction'] = dialogAction
                return formASlotResponse(intentName,event['sessionState'] )
        except ValueError as e:
            dialogAction = {
                                "slotToElicit": "numPeople",
                                "type": "ElicitSlot"
                            }
            event['sessionState']['dialogAction'] = dialogAction
            return formASlotResponse(intentName,event['sessionState'] )

    elif "reservationDate" in event['transcriptions'][0]['resolvedSlots']:
        userInput = event['sessionState']['intent']['slots']['reservationDate']['value']['originalValue']
        try:
            matches = [parser.parse(userInput, fuzzy=True)]
        except:
            dialogAction = {
                    "slotToElicit": "reservationDate",
                    "type": "ElicitSlot"
                }
            event['sessionState']['dialogAction'] = dialogAction
            return formASlotResponse(intentName,event['sessionState'] )
        if matches[0] > datetime.datetime.now():
            date = matches[0].date().strftime('%A %d %B %Y')
            value = {
                        "originalValue": userInput,
                        "interpretedValue": date,
                        "resolvedValues": [
                                date
                                ]
                        }
            dialogAction = {
                        "slotToElicit": "reservationTime",
                        "type": "ElicitSlot"
                    }
            event['sessionState']['intent']['slots']['reservationDate']['value'] = value
            event['sessionState']['dialogAction'] = dialogAction
            return formASlotResponse(intentName,event['sessionState'] )
        dialogAction = {
                    "slotToElicit": "reservationDate",
                    "type": "ElicitSlot"
                }
        event['sessionState']['dialogAction'] = dialogAction
        return formASlotResponse(intentName,event['sessionState'] )

    elif "reservationTime" in event['transcriptions'][0]['resolvedSlots']:
        userInput = event['sessionState']['intent']['slots']['reservationTime']['value']['originalValue']
        reservationDate = event['sessionState']['intent']['slots']['reservationDate']['value']['originalValue']
        if "noon" in userInput.lower():
            userInput += "12 pm"
        
        if "o'clock" in userInput.lower():
            userInput.replace("o'clock", "pm")
        try:
            matches = [parser.parse(reservationDate + " " + userInput, fuzzy=True)]
        except:
            dialogAction = {
                    "slotToElicit": "reservationTime",
                    "type": "ElicitSlot"
                }
            event['sessionState']['dialogAction'] = dialogAction
            return formASlotResponse(intentName,event['sessionState'] )
        if matches[0] > datetime.datetime.now():
            time = matches[0].strftime('%I:%M %p')
            value = {
                        "originalValue": userInput,
                        "interpretedValue": time,
                        "resolvedValues": [
                                time
                                ]
                        }
            dialogAction = {
                        "slotToElicit": "email",
                        "type": "ElicitSlot"
                    }
            event['sessionState']['intent']['slots']['reservationTime']['value'] = value
            event['sessionState']['dialogAction'] = dialogAction
            return formASlotResponse(intentName,event['sessionState'] )
        dialogAction = {
                    "slotToElicit": "reservationTime",
                    "type": "ElicitSlot"
                }
        event['sessionState']['dialogAction'] = dialogAction
        return formASlotResponse(intentName,event['sessionState'] )

    elif "email" in event['transcriptions'][0]['resolvedSlots']:
        userInput = event['sessionState']['intent']['slots']['email']['value']['originalValue']
        if (re.fullmatch(emailRegex, userInput)):
            value = {
                        "originalValue": userInput,
                        "interpretedValue": userInput,
                        "resolvedValues": [
                                userInput
                                ]
                        }
            dialogAction = { "type": "Delegate"}
            event['sessionState']['intent']['slots']['email']['value'] = value
            event['sessionState']['intent']['state'] = "ReadyForFulfillment"
            event['sessionState']['dialogAction'] = dialogAction
            return formASlotResponse(intentName,event['sessionState'] )
        else:
            dialogAction = {
                    "slotToElicit": "email",
                    "type": "ElicitSlot"
                }
            event['sessionState']['dialogAction'] = dialogAction
            return formASlotResponse(intentName,event['sessionState'] )
    else:
        return ""


def lambda_handler(event, context):
    print(event)
    intentName = event['sessionState']['intent']['name']
    response = ""
    if intentName == "DiningSuggestionsIntent":
        response = createDiningSuggestionResponse(intentName, event)
    elif intentName == "GreetingIntent":
        response = createGreetingIntentResponse(intentName, event)
    elif intentName == "ThankYouIntent":
        response = createThankYouIntentResponse(intentName, event)
    return response
