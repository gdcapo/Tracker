''' Code with necessary functions to update postgres' tables. '''

import pandas as pd
import numpy as np
import psycopg2
from psycopg2 import extras
from accessPoint_db import Postgres
import warnings
warnings.filterwarnings('ignore', '.*do not.*', )
warnings.filterwarnings("ignore")

class create_tables():
    def __init__(self, all_tweets, insults_female, f_info, raw_data, ids, categorias): ### does this only work for female? Should we say insults, info? 
        
        self.all_tweets = all_tweets
        self.insults_f = insults_female
        self.f_tweets = f_info
        self.raw_tweets = raw_data
        self.seed_ids = ids
        self.df_categorias = categorias
        
    def create_raw_tables(self):
        ''' Create tables with abuse, mentions and hashtags by id to upload to postgres. '''
        
        # id to mentions
        self.df_mentions = pd.concat([pd.Series(row['id'], row['f_seed']) for _, row in self.f_tweets.iterrows()]).reset_index()
        self.df_mentions.columns = ['mentions', 'id']
        self.seed_ids['Twitter'] = self.seed_ids['Twitter'].apply(lambda x: x.strip())
        self.df_mentions = self.df_mentions.merge(self.seed_ids[['Twitter','user_id']], how = 'left', left_on = 'mentions', right_on = 'Twitter').drop(columns = {'Twitter', 'mentions'})
        self.df_rt = self.f_tweets[['id', 'rt_from_user_id']]
        self.df_mentions = self.df_mentions.merge(self.df_rt, how = 'left', on = 'id')

        # id to hashtags
        self.df_hashtags = pd.concat([pd.Series(row['id'], row['hashtags']) for _, row in self.f_tweets.iterrows()]).reset_index()
        self.df_hashtags.columns = ['hashtags', 'id']

        # abuse
        if len(self.all_tweets) != 0:
            self.df_abuse = self.all_tweets[['id', 'abuse']]
        if len(self.all_tweets) == 0:
            self.df_abuse = pd.DataFrame(columns = ['abuse', 'id'])

        # insults
        if len(self.insults_f) != 0:
            self.df_f_insult = pd.concat([pd.Series(row['id'], row['insults_name'])
                                for _, row in self.insults_f.iterrows()]).reset_index()
            self.df_f_insult.columns = ['gendered_insults', 'id']
        if len(self.insults_f) == 0:
            self.df_f_insult = pd.DataFrame(columns = ['gendered_insults', 'id'])
        
        self.df_abuse_insults = self.df_abuse.merge(self.df_f_insult, how = 'left', on = 'id')
    
        # id to users
        self.df_users = self.f_tweets[['user_id', 'id', 'rt_from_user_id']]
        self.df_users_abuse = self.df_abuse_insults.merge(self.f_tweets[['id', 'user_id', 'rt_from_user_id']], how = 'left', on = 'id')


    def create_hourly_tables(self): 
        ''' Create summary tables by hour to upload to postgres. '''

        # Merge tables to have datetime, insults, mentions and hashtags in the summary.
        id_to_datetime = self.f_tweets[['id', 'datetime']]
        id_to_datetime['Hour'] =  pd.to_datetime(pd.to_datetime(id_to_datetime['datetime']).dt.strftime('%Y-%m-%d %H'))
        self.df_mentions['rt_from_user_id'].fillna(value=np.nan, inplace=True)
        conditions = [(self.df_mentions['rt_from_user_id'].isna()), (self.df_mentions['rt_from_user_id'].notna())]
        values = ['original','rt']
        self.df_mentions['tweet_type'] = np.select(conditions, values)
        id_to_datetime_mentions = id_to_datetime.merge(self.df_mentions, how = 'left', on = 'id').rename(columns = {'user_id':'influencer_id'})

        # Hashtags
        id_to_datetime_mentions_hash = id_to_datetime_mentions.merge(self.df_hashtags, how = 'left', on = 'id')
        self.hashtags_summary = id_to_datetime_mentions_hash[id_to_datetime_mentions_hash['hashtags'].notna()].groupby(['Hour', 'influencer_id', 'hashtags'])['id'].size().to_frame().reset_index().rename(columns = {'id': 'N_hashtag'})

        # Create summary tables
        id_to_datetime_mentions = id_to_datetime_mentions.merge(self.df_abuse_insults, how = 'left', on = 'id')
        mentions_summary = id_to_datetime_mentions.groupby(['Hour','influencer_id'])['id'].size().to_frame().reset_index().rename(columns = {'id': 'N_mentions'})
        abuse_summary = id_to_datetime_mentions[id_to_datetime_mentions['abuse']==1].groupby(['Hour', 'influencer_id'])['id'].size().to_frame().reset_index().rename(columns = {'id': 'N_abuse'})

        self.mentions_and_abuse_summary = mentions_summary.merge(abuse_summary[['Hour', 'influencer_id', 'N_abuse']], how = 'left', on = ['Hour', 'influencer_id'])
        self.mentions_and_abuse_summary['N_abuse'] = self.mentions_and_abuse_summary['N_abuse'].fillna(0)
        
         # Rt 
        mentions_summary_original = id_to_datetime_mentions[id_to_datetime_mentions['tweet_type']=='original'].groupby(['Hour','influencer_id'])['id'].size().to_frame().reset_index().rename(columns = {'id': 'N_mentions_original'})
        mentions_summary_rt = id_to_datetime_mentions[id_to_datetime_mentions['tweet_type']=='rt'].groupby(['Hour','influencer_id'])['id'].size().to_frame().reset_index().rename(columns = {'id': 'N_mentions_rt'})
        abuse_summary_mentions_original = id_to_datetime_mentions[(id_to_datetime_mentions['abuse']==1) & (id_to_datetime_mentions['tweet_type']=='original')].groupby(['Hour', 'influencer_id'])['id'].size().to_frame().reset_index().rename(columns = {'id': 'N_mentions_abuse_original'})
        abuse_summary_mentions_rt = id_to_datetime_mentions[(id_to_datetime_mentions['abuse']==1) & (id_to_datetime_mentions['tweet_type']=='rt')].groupby(['Hour', 'influencer_id'])['id'].size().to_frame().reset_index().rename(columns = {'id': 'N_mentions_abuse_rt'})
        self.mentions_and_abuse_summary = self.mentions_and_abuse_summary.merge(mentions_summary_original, how = 'left', on = ['Hour','influencer_id'])
        self.mentions_and_abuse_summary = self.mentions_and_abuse_summary.merge(mentions_summary_rt, how = 'left', on = ['Hour','influencer_id'])
        self.mentions_and_abuse_summary = self.mentions_and_abuse_summary.merge(abuse_summary_mentions_original, how = 'left', on = ['Hour','influencer_id'])
        self.mentions_and_abuse_summary = self.mentions_and_abuse_summary.merge(abuse_summary_mentions_rt, how = 'left', on = ['Hour','influencer_id'])
        self.mentions_and_abuse_summary = self.mentions_and_abuse_summary.fillna(0)
        
        self.insults_summary = id_to_datetime_mentions[id_to_datetime_mentions['gendered_insults'].notna()].groupby(['Hour', 'influencer_id', 'gendered_insults'])['id'].size().to_frame().reset_index().rename(columns = {'id': 'N_abuse'})
        
        self.subc_summary = id_to_datetime_mentions[id_to_datetime_mentions['gendered_insults'].notna()].merge(self.df_categorias[['Insulto', 'Subcategoría']], how = 'left', left_on = 'gendered_insults', right_on = 'Insulto').drop(columns = {'Insulto'})
        self.subc_summary = self.subc_summary.groupby(['Hour', 'influencer_id', 'Subcategoría'])['id'].size().to_frame().reset_index().rename(columns = {'id': 'N_abuse'})
        
        # Create summary by user
        self.df_users['rt_from_user_id'].fillna(value=np.nan, inplace=True)
        conditions = [(self.df_users['rt_from_user_id'].isna()), (self.df_users['rt_from_user_id'].notna())]
        values = ['original','rt']
        self.df_users['tweet_type'] = np.select(conditions, values)

        self.df_users_abuse['rt_from_user_id'].fillna(value=np.nan, inplace=True)
        conditions = [(self.df_users_abuse['rt_from_user_id'].isna()), (self.df_users_abuse['rt_from_user_id'].notna())]
        values = ['original','rt']
        self.df_users_abuse['tweet_type'] = np.select(conditions, values)

        id_to_datetime_users = id_to_datetime.merge(self.df_users, how = 'left', on = 'id')
        id_to_datetime_users_abuse = id_to_datetime.merge(self.df_users_abuse, how = 'left', on = 'id')
        users_summary = id_to_datetime_users.groupby(['Hour','user_id'])['id'].size().to_frame().reset_index().rename(columns = {'id': 'N_user'})
        abuse_summary_users = id_to_datetime_users_abuse[id_to_datetime_users_abuse['abuse']==1].groupby(['Hour', 'user_id'])['id'].size().to_frame().reset_index().rename(columns = {'id': 'N_user_abuse'})
        self.users_abuse_summary = users_summary.merge(abuse_summary_users[['Hour', 'user_id','N_user_abuse']], how = 'left', on = ['Hour', 'user_id'])

        # Rt 
        users_summary_original = id_to_datetime_users[id_to_datetime_users['tweet_type']=='original'].groupby(['Hour','user_id'])['id'].size().to_frame().reset_index().rename(columns = {'id': 'N_user_original'})
        users_summary_rt = id_to_datetime_users[id_to_datetime_users['tweet_type']=='rt'].groupby(['Hour','user_id'])['id'].size().to_frame().reset_index().rename(columns = {'id': 'N_user_rt'})
        abuse_summary_users_original = id_to_datetime_users_abuse[(id_to_datetime_users_abuse['abuse']==1) & (id_to_datetime_users_abuse['tweet_type']=='original')].groupby(['Hour', 'user_id'])['id'].size().to_frame().reset_index().rename(columns = {'id': 'N_user_abuse_original'})
        abuse_summary_users_rt = id_to_datetime_users_abuse[(id_to_datetime_users_abuse['abuse']==1) & (id_to_datetime_users_abuse['tweet_type']=='rt')].groupby(['Hour', 'user_id'])['id'].size().to_frame().reset_index().rename(columns = {'id': 'N_user_abuse_rt'})
        self.users_abuse_summary = self.users_abuse_summary.merge(users_summary_original, how = 'left', on = ['Hour','user_id'])
        self.users_abuse_summary = self.users_abuse_summary.merge(users_summary_rt, how = 'left', on = ['Hour','user_id'])
        self.users_abuse_summary = self.users_abuse_summary.merge(abuse_summary_users_original, how = 'left', on = ['Hour','user_id'])
        self.users_abuse_summary = self.users_abuse_summary.merge(abuse_summary_users_rt, how = 'left', on = ['Hour','user_id'])
        self.users_abuse_summary = self.users_abuse_summary.fillna(0)

    def update_postgres(self): 
        ''' Insert new lines in postgres tables. '''

        connection = psycopg2.connect(host=Postgres.host,
                                  database=Postgres.database,user=Postgres.user,password=Postgres.password)
        connection.autocommit = True
        cursor = connection.cursor()

        tpls_raw = [tuple(x) for x in self.raw_tweets[['id','text','datetime','user_id', 'keywords', 'valid']].to_numpy()]
        # tpls_hashtag = [tuple(x) for x in self.df_hashtags.to_numpy()]
        # tpls_datetime = [tuple(x) for x in self.f_tweets[['id', 'datetime']].to_numpy()]
        # tpls_text = [tuple(x) for x in self.f_tweets[['id', 'text']].to_numpy()]
        # tpls_mention = [tuple(x) for x in self.df_mentions.to_numpy()]
        # tpls_insults = [tuple(x) for x in self.df_f_insult.to_numpy()]
        tpls_subcategoria = [tuple(x) for x in self.subc_summary.to_numpy()]
        tpls_mentions_and_abuse = [tuple(x) for x in self.mentions_and_abuse_summary.to_numpy()]
        tpls_hashtags_summary = [tuple(x) for x in self.hashtags_summary.to_numpy()]
        tpls_insults_summary = [tuple(x) for x in self.insults_summary.to_numpy()]
        tpls_users_summary = [tuple(x) for x in self.users_abuse_summary.to_numpy()]
        
        # sql_mention = "INSERT INTO id_to_mentions(mention,id) VALUES(%s,%s)"
        sql_raw = "INSERT INTO raw_data(id,text,datetime,user_id, keywords, valid) VALUES(%s,%s,%s,%s,%s,%s)"
        # sql_text = "INSERT INTO id_to_text(id,text) VALUES(%s,%s)"
        # sql_datetime = "INSERT INTO id_to_datetime(id,datetime) VALUES(%s,%s)"
        # sql_insults = "INSERT INTO id_to_insults(insult,id) VALUES(%s,%s)"
        # sql_hashtag = "INSERT INTO id_to_hashtags(hashtag,id) VALUES(%s,%s)"
        sql_mentions_and_abuse = 'INSERT INTO mentions_and_abuse_summary(Hour, influencer_id, N_mentions, N_abuse, N_mentions_orig, N_mentions_rt, N_abuse_orig, N_abuse_rt) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)'
        sql_insults_summary = 'INSERT INTO insults_summary(Hour, influencer_id, Insults, N_abuse) VALUES(%s,%s,%s,%s)'
        sql_hashtags_summary = 'INSERT INTO hashtags_summary(Hour, influencer_id, Hashtag, N_hashtags) VALUES(%s,%s,%s,%s)'
        sql_users_summary ='''INSERT INTO users_abuse_summary(Hour, user_id, N_user, N_user_abuse, N_user_original, N_user_rt, N_user_abuse_original, N_user_abuse_rt) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)'''
        sql_subcategoria_summary = 'INSERT INTO subc_summary(Hour, influencer_id, Subcategoría, N_abuse) VALUES(%s,%s,%s,%s)'

        print('insert raw')
        extras.execute_batch(cursor, sql_raw, tpls_raw) 
        # print('insert text')
        # extras.execute_batch(cursor, sql_text, tpls_text)
        # print('insert datetime')
        # extras.execute_batch(cursor, sql_datetime, tpls_datetime) 
        # print('insert mentions')
        # extras.execute_batch(cursor, sql_mention, tpls_mention)
        # print('insert insults')
        # extras.execute_batch(cursor, sql_insults, tpls_insults)
        # print('insert hashtag')
        # extras.execute_batch(cursor, sql_hashtag, tpls_hashtag)
        print('insert mentions and abuse')
        extras.execute_batch(cursor, sql_mentions_and_abuse, tpls_mentions_and_abuse)
        print('insert insults summary')
        extras.execute_batch(cursor, sql_insults_summary, tpls_insults_summary)
        print('insert hashtags summary')
        extras.execute_batch(cursor, sql_hashtags_summary, tpls_hashtags_summary)
        print('insert users summary')
        extras.execute_batch(cursor, sql_users_summary, tpls_users_summary)
        print('insert subcategoria summary')
        extras.execute_batch(cursor, sql_subcategoria_summary, tpls_subcategoria)
      
        print("Records inserted........")
        connection.close()
