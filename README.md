# Real time monitor internal repo

This repo contains code for streaming real time tweets, modify the dictionary with abuse, extracting insults and saving them to a postgres database to be ingested by the dashboard.

- **main.py** (runs with cronjob): call abuse_detection_tools.py, tweets_tools.py and sql_tools.py to read the json with tweets and process them accordingly.
- **stream_tweets_new.py**: stream and save the tweets that have mentions of the ids we selected and store them in json files.
- **sql_tools.py**: create the tables that we will upload to postgres and update postgres.
- **tweets_tools.py**: store functions necessary to clean text, extract abuse and obtain tweets' information.
- **abuse_detection_tools.py**: predict abuse based on transformer and extract insults.
- **create_postgres.py**: create postgres tables and update some tables that are static: metadata and the insults categories.

Also, we have three folders with the following content:
- **data**: hass seeds users' information and metadata and abuse detection dictionaries.

### How to run

To start the app, launch a new Linux screen with the withdata.cer key: *ssh -i ~/.ssh/withdata.cer ubuntu@IP*.

Inside the instance we should create a folder named monitor_uruguay and change our directory there: *cd monitor_uruguay*

In the folder monitor_uruguay we should clone all the scripts and folders from this repo. 

To set up the instance, we should install the requirements file first with the following line: *pip install -r requirements.txt*.

#### Running the streamer

To run the code that streams and stores tweets in json files we should run the following line of code for one time only: *nohup python -u stream_tweets_new.py ./ 0 > stream.log &*.

This way, the stream will run indefinetely.

#### Running the classifier

The classifier was set up to run once an hour. To do so we should set up a crontab instruction. To enter the crontab run: *crontab -e*.
Inside the crontab write the following command that will tell the EC2 instance to run the main.py once an hour, ten minutes past the hour.

*10 * * * * cd /home/ubuntu/monitor_uruguay && /home/ubuntu/anaconda3/bin/python main.py*

Save the crontab and write out.
