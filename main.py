import boto3
import pandas as pd
import re
import os
import datetime as dt
import re
from subprocess import check_call
import warnings
warnings.filterwarnings('ignore', '.*do not.*', )
warnings.filterwarnings("ignore")
from tweets_tools import *
from sql_tools import create_tables
from abuse_detection_tools import * 
import time

class detect_abuse(): 
    def __init__(self, file_name, keywords = False):
        self.seed_data = pd.read_csv('data/all_screen_names_ids.csv') 
        self.seed_data['user_id'] = 'id_' + self.seed_data['user_id'].astype(str)

        if keywords: ##If we are collecting keywords
            df_keywords = pd.read_csv('data/keywords.csv')
            print(">> tidy stream. Keeping tweets directed to female and keywords") 
            self.tweets = tidy_tweets(file_name = file_name, only_female=False, users = self.seed_data, keywords=df_keywords, track_keywords=True) 
            self.raw_tweets = self.tweets[['id','text','datetime','user_id', 'keywords', 'valid']]
        else:   ##If we don't have keywords
            print(">> tidy stream.Keeping tweets directed to female") 
            df_keywords = pd.DataFrame()
            self.tweets = tidy_tweets(file_name = file_name, only_female=False, users = self.seed_data, keywords=df_keywords, track_keywords=False)
            self.raw_tweets = self.tweets[['id','text','datetime','user_id', 'valid']]
            self.raw_tweets['keywords'] = np.nan
            self.raw_tweets = self.raw_tweets[['id','text','datetime','user_id', 'keywords', 'valid']]

    def run(self):
         
        self.tweets = self.tweets[self.tweets['valid'] ==True]
        self.f_tweets = self.tweets[self.tweets.directed_at_f == 1]
        self.m_tweets = self.tweets[self.tweets.directed_at_m == 1]
      
        print(">> Got {} tweets, from which {} are directed at female and {} at male".format(
            len(self.tweets), len(self.f_tweets), len(self.m_tweets)))

        self.tweets = self.tweets[['datetime', 'id', 'user_id','screen_name', 'text',
                                   'text_clean', 'f_seed', 'm_seed', 'hashtags']]
        
        ### Map special tokens
        self.seed_data.rename(columns = {'Tipo':'token'}, inplace = True)
        self.f_tweets["replaced_text"] = self.f_tweets['text'].apply(lambda x: clean_text_transformer(x))
        self.f_tweets["replaced_text"], df_matched_fields = special_token_map(self.f_tweets["replaced_text"], self.seed_data[['screen_name','token']])
        
        print(">> Predict abuse")
        pred_abuse = transformer_pred(self.f_tweets) 
        self.f_tweets["abuse"] = pred_abuse
        
        insults = pd.read_excel('data/Diccionario_dev.xlsx', sheet_name = '2022-05-23')
        insults_all, insults_fiu, insults_female, insults_male =  create_insult_lists(insults)
        gendered_insults_dict = create_dictionary(insults)

        print(">> Extract insults")
        self.insults_f = extract_insults(self.f_tweets,insults_all,insults,insults_female, insults_fiu, gendered_insults_dict)
             
    def export(self):
        '''Exports file to postgres. '''

        df_categorias = pd.read_excel('data/Diccionario_de_insultos.xlsx')        
        print(">> generating tables and saving to data/processed")
        try:
            this_create_tables = create_tables(all_tweets = self.f_tweets, insults_female=self.insults_f, f_info=self.f_tweets, raw_data = self.raw_tweets, ids = self.seed_data, categorias = df_categorias)
            this_create_tables.create_raw_tables()
            this_create_tables.create_hourly_tables()
            this_create_tables.update_postgres()

        except Exception as e:
            message = {
                "source": "process_tweets", "error_msg": f"SQL tables update for file {gzip_f} failed", "exception": str(e)}
            sns_client.publish(
                TargetArn=sns_arn,
                Message=json.dumps(
                    {'default': json.dumps(message)}),
                MessageStructure='json')
        print("\n\n")


s3client = boto3.client("s3")
sns_client = boto3.client('sns', region_name="us-east-2")
sns_arn = "arn:aws:s3:::monitor-uruguay"

if __name__ == "__main__":

    now = dt.datetime.now()
    path = 'raw/'
    print("Now it is", now)
    process_freq = 1
    files = os.listdir(path)
    files = [x for x in files if ".json" in x]
    pr = True

    for f in files:
        #print("Processing files:",files)
        file_day = re.search("([0-9]+)-([0-9]+)-([0-9]+)-([0-9]+)", f).group(1)
        file_month = re.search("([0-9]+)-([0-9]+)-([0-9]+)-([0-9]+)", f).group(2)
        file_year = re.search("([0-9]+)-([0-9]+)-([0-9]+)-([0-9]+)", f).group(3)
        file_hour = re.search("([0-9]+)-([0-9]+)-([0-9]+)-([0-9]+)", f).group(4)
        file_datetime = file_year + file_month + file_day + file_hour
        file_datetime = dt.datetime.strptime(file_datetime, '%Y%m%d%H')
        file_date = dt.datetime.strftime(file_datetime, '%Y%m%d')
        time_dif = (now - file_datetime).seconds/60 / \
            60 + (now - file_datetime).days*24

        if 'gz' not in f:# and time_dif > process_freq:
            if pr:
                print("processing file:", f, time_dif)
    
                this_detect_abuse = detect_abuse(file_name=path+f)
                this_detect_abuse.run()
                this_detect_abuse.export()
                time.sleep(2)
                gzip_files = [x for x in files if f[:22] in x]
                for gzip_f in gzip_files:
                    check_call(['gzip', path+gzip_f])
                    zipped_filename = path + gzip_f + ".gz"
                    with open(zipped_filename, "rb") as s3file:
                        try:
                            s3client.upload_fileobj(
                               s3file, "monitor-uruguay", f"raw/{gzip_f}.gz")
                            os.remove(f"/home/ubuntu/monitor_uruguay/raw/{gzip_f}.gz")
                        except Exception as e:
                            message = {
                                "source": "process_tweets", "error_msg": f"S3 upload file {gzip_f} failed", "exception": str(e)}
                            sns_client.publish(
                                TargetArn=sns_arn,
                                Message=json.dumps(
                                    {'default': json.dumps(message)}),
                                MessageStructure='json')
