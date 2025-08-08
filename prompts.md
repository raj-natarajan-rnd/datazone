* We are creating a proof of concept of using AWS Event Bus in a publication and subscription model. Create an architecture (a diagram if possible). 
* DataLounge account will host all the iceberg tables in S3 bucket
* AeroInsight account will use Athena to read / write iceberg tables and data in DataLounge Account
* A event bus will be created in the main account called WingSafe. 
* All accounts involved in the poc will use this command event bus shared accross account to publish a event message
* Any account may consume these events in the bus and take action
* For example FlightRadar account will subscribe and receive an event to trigger a lambda
Take this is as a very high level idea. Correct if some features are wrongly used. Suggest the best practice and create an architecture. At this point no need to create any code

Great
* Create all new scripts in the EventBusPOC folder
* prefix all files with account name in which it should be run
* Create a simple POC as per this poc requirements and it will be expaned in further prompts
* Follow all best practices

Simple POC:
* Event Bus and all realted resources setup
* Write sample data set to one of the table from AeroInsight account
* After writing inform event bus 
* Create a simple step function in FlightRadar and to subscribe to the event
* A step function should run in the FlightRadar account to export the data to another bucket in the flightradar account to get the newly inserted records
* Send a notification after retreiving data in flightradar - this notification can be just an email

Correct python code to read / write data to iceburg is already created in AeroInsight/python folder. Check for reference in this folder and create something similar

Refer to demo-datalounge-multi-application.py how test code assumes role
AeroInsight has a lambda and it will use Athena in same account to write to bucket in DataLounge
Lambda will be triggered by DataScientist by assuming the role WingSafe-DataScientist-CrossAccount-dev in 184838390535
Ensure all appropriate permissions are given in Lamda and in cross account roles. 
If other infra code needs to be updated please update accordingly