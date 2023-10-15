# A Dining Conceirge Bot which sends to restaurant suggestions via email

bot-uri: http://diningconciergeziom.com.s3-website-us-east-1.amazonaws.com/

Authors:
- Mooizz (ma4496)
- Abhilash (ag4797)

Code details:
- frontend - contains a simple UI interface to interact with the bot
- LF0 - lambda function called by frontend via API gateway
- A Lex bot(configured in aws) - Amazon lex service called by LFO
- LF1 - This lambda function is called by Lex bot to collect data from user to form restaurant suggestions
- Q1 - This is a SQS queue in aws which was created to enable ascynchronous restaurant suggestion for every user convo
- LF2 - a lambda function which retrieves user convo data from SQS and performs the suggestion search on DynamoDB, Opensearch. This function also sends email to the user
- DynamoDB and Opensearch contain data scraped form yelp API
- Add_data_to_Dynamo_DB is a lambda function that adds scraped data to aws dynamoDB server, data.txt in Add_data_to_Dynamo_DB contains the scraped restaurant data
