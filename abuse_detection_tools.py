''' get_abuse '''
''' lists and dictionary are read in detect_insults.py in the if __name__ == "__main__": '''

import pandas as pd
from tweets_tools import *
import os
from transformers import BertForSequenceClassification, BertTokenizerFast
from nlp import load_dataset
import torch
from accessPoint_db import huggingface_user
import time

def create_insult_lists(insults):
    ''' Create lists with insults by female, male, undefined and all groups. '''
    insults_all = "|".join(insults[insults.target != "Exclude"].regex3.values)
    insults_fiu = "|".join(insults[insults.target.isin(['FS', 'FP', 'US', 'IS', 'IP', 'MP'])].regex3.values)

    insults_female = insults[insults.gender == 'F']
    insults_female = "|".join(insults_female.regex3.values)

    insults_male = insults[insults.gender == 'M']
    insults_male = "|".join(insults_male.regex3.values)

    return insults_all, insults_fiu, insults_female, insults_male

def create_dictionary(insults):
    ''' Create dictionary with the name of the insult and the regex. '''

    gendered_insults_dict = {}
    for idx, row in insults[insults['target'].isin(['FS', 'FP', 'US', 'IS', 'IP', 'MP'])].iterrows():
        gendered_insults_dict[row['regex3']] = {'name': row['orig_name'],
                                                'target': row['target']}
    return gendered_insults_dict


def extract_insults(f_tweets,insults_all,insults, insults_female, insults_fiu, gendered_insults_dict):
        
        
        print(">> Processing female tweets")
        print(">>> Filtering tweets with at least one insult")
        # Check if there is at least one insult
        f_tweets['insults_search'] = f_tweets.text_clean.apply(
            lambda x: re.search(insults_all, x.lower()))
        #f_tweets['insults_search'] = f_tweets.text_clean.apply(lambda x: re.search(insults_female, x.lower()))
        f_tweets['insults_search'] = f_tweets.insults_search.apply(
            lambda x: x.group() if x is not None else "")
        df_abusive = f_tweets[(f_tweets.insults_search != "")&(f_tweets.abuse == 1)]

        print(">>> Removing expressions")
        #Find and filter out all expressions
        if len(df_abusive) == 0:
            print("No insults detected")
            df_f = pd.DataFrame(columns = ['id', 'screen_name', 'user_id', 'text', 'text_clean', 'rt_from_screen_name', 'rt_from_user_id', 'rt_from_id', 
                                            'qt_from_screen_name', 'qt_status', 'in_reply_to_screen_name', 'in_reply_to_user_id', 'in_reply_to_status_id',
                                            'mentions', 'f_seed', 'm_seed', 'directed_at_f', 'directed_at_m', 'hashtags', 'datetime', 'location', 
                                            'tweet_type', 'seed_target', 'replaced_text', 'abuse','insults_search', 'not_f', 'text_filtered', 
                                            'female_insults', 'fiu_insults', 'insults_name'])
            
            return df_f
            
        expressions = "|".join(insults[insults.target == 'Exclude'].regex3.values)
        df_abusive['not_f'] = df_abusive.text_clean.apply(
            lambda x: custom_findall(x, expressions, fuzzy=True))
        df_abusive['text_filtered'] = df_abusive.apply(lambda row: re.sub(
            "|".join(row['not_f']), "", row['text_clean'].lower()), axis=1)

        print(">>> Removing male insults")
        exclusions = "|".join(insults[insults.target.isin(["MS", "MP"])].regex3.values)
        # exclusions = "|".join(insults[insults.gender != 'F'].regex3.values) ### need to add nonbinary
        df_abusive['not_f'] = df_abusive.text_clean.apply(
            lambda x: custom_findall(x, exclusions))
        df_abusive['text_filtered'] = df_abusive.apply(lambda row: re.sub(
            "|".join(row['not_f']), "", row['text_filtered'].lower()), axis=1)

        print(">>> Extracting female insults")
        # Find female insults once all others have been excluded
        df_abusive['female_insults'] = df_abusive.text_filtered.apply(
            lambda x: custom_findall(x, insults_female))
        df_abusive['female_insults'] = df_abusive.female_insults.apply(
            lambda x: "" if len(x) == 0 else x)
        # All insults that weren't excluded. This doesn't make sense right now
        # df_abusive['all_insults'] = df_abusive.text_filtered.apply(lambda x: custom_findall(x, insults_all))
        # df_abusive['all_insults'] = df_abusive.all_insults.apply(lambda x: "" if len(x) == 0 else x)
        df_abusive['fiu_insults'] = df_abusive.text_filtered.apply(
            lambda x: custom_findall(x, insults_fiu))
        df_abusive['fiu_insults'] = df_abusive.fiu_insults.apply(
            lambda x: "" if len(x) == 0 else x)

        hdr = False if os.path.isfile('data/processed/df_female.csv') else True

        # Filtering female abuse if: a) there's at least one female insults, or, all female politicians
        df_f = df_abusive[(df_abusive.female_insults != "") | (
            (df_abusive.fiu_insults != "") & (df_abusive.m_seed.astype(str) == "[]"))]

        print(">>> Matching insult to name")
        df_f['insults_name'] = df_f['fiu_insults'].apply(
            lambda x: extract_gendered_slurs(x, gendered_insults_dict))

        #self.df_f[['id','datetime','text','female_insults','fiu_insults','insults_name','f_politicians','m_politicians']].to_csv('data/processed/df_female.csv',header=hdr, mode='a',index=False)
        print("Number of female abusive tweets: ", len(df_f))

        # print('>> Processing male tweets')
        # print('>>> Filtering tweets with at list one insult')
        # self.m_tweets['insults_search'] = self.m_tweets.text_clean.apply(lambda x: re.search(insults_male, x.lower()))
        # self.m_tweets['insults_search'] = self.m_tweets.insults_search.apply(lambda x: x.group() if x is not None else "")
        # df_abusive = self.m_tweets[self.m_tweets.insults_search != ""]

        # print('>>> Filtering only male insults')
        # exclusions = "|".join(insults[insults['target'] == 'Exclude'].regex3.values)
        # df_abusive['not_m'] = df_abusive.text_clean.apply(lambda x: custom_findall(x, exclusions))
        # df_abusive['text_filtered'] = df_abusive.apply(lambda row: re.sub("|".join(row['not_m']), "", row['text_clean'].lower()),axis=1)

        # df_abusive['male_insults'] = df_abusive.text_filtered.apply(lambda x: custom_findall(x, insults_male))
        # df_abusive['male_insults'] = df_abusive.male_insults.apply(lambda x: "" if len(x) == 0 else x)

        # self.df_m = df_abusive[df_abusive.male_insults != ""]
        # print(">>> Pct of female tweets with at least one insult:", len(self.df_f)/len(self.f_tweets), "out of",len(self.f_tweets))
        # print(">>> Pct of male tweets with at least one insult:", len(self.df_m)/len(self.m_tweets), "out of",len(self.m_tweets))
        # hdr = False  if os.path.isfile('data/processed/df_m.csv') else True
        #self.df_m[['id','datetime','text','male_insults','m_politicians']].to_csv('data/processed/df_m_.csv',mode='a', header=hdr)
        #self.df_f[['id','datetime','text','female_insults','fiu_insults','all_insults','f_politicians','m_politicians']].to_csv('data/processed/df_fiu.csv',mode='a', header=hdr)
        
        return df_f

def special_token_map(tweet_series: pd.Series, usernames: pd.DataFrame):
    """Takes series of tweet text, replaces usernames of interest with special tokens. """
    special_tokens = list(usernames['token'].unique())
    
    replaced_series = tweet_series.copy()
    matched_fields =  {}
    for token in special_tokens:
        subset_usernames = usernames[usernames['token']==token]
        # Get regex for each username to match only correct/exact mentions of usernames
        replacements = [
            '(?<![^\s.,;:!?()\-\=\[\]])@' + name +'(?![^\s.,;:!?()\-\=\[\]])' 
            for name in subset_usernames['screen_name']
        ]
        # OLD: replacements = ['@' + name  for name in subset_usernames['screen_name']]
        regx = r'({})'.format('|'.join(replacements))
        matches = replaced_series.map(lambda x: re.findall(regx, x, flags=re.IGNORECASE))
        matched_fields[token] = matches
        replaced_series = replaced_series.str.replace(regx, token, regex=True, case = False)
    # Replace other usernmaes
    matches = replaced_series.map(lambda x: re.findall('@[^\s]+', x, flags=re.IGNORECASE))
    replaced_series = replaced_series.str.replace('@[^\s]+', "USER", regex=True, case=False)
    matched_fields['USER'] = matches
    # Add back in @ sign
    for token in special_tokens + ['USER']:
        replaced_series = replaced_series.str.replace(token, f'@{token}', regex=False, case=True)
    df_matched_fields = pd.DataFrame(matched_fields) #This is a modification to the original code
    return replaced_series, df_matched_fields

def transformer_pred(tweets):
    texts = list(tweets.replaced_text.values)
    print('Download model')
    model_tipo = BertForSequenceClassification.from_pretrained("witpact/vg_uruguay", use_auth_token=huggingface_user.token)
    print('Download tokenizer')
    tokenizer = BertTokenizerFast.from_pretrained("witpact/vg_uruguay", use_auth_token=huggingface_user.token)
    
    pred = []
    for i in np.arange(0,len(tweets),10):
        print(i)
        try:
            inputs = tokenizer(texts[i:i+10], padding=True, truncation=True, max_length=512, return_tensors="pt")
            tipo = model_tipo(**inputs)[0].softmax(1).argmax(axis=1)
            pred += tipo.tolist()
        except:
            pass

    return pred
