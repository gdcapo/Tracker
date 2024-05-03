''' Code that stores necessary functions to process the tweets. '''
import gzip
import shutil
import unicodedata
import numpy as np
import pandas as pd
import re
import json
import time
import string
import regex
from html import unescape

def elimina_tildes(cadena):
    ''' Return the normal form of the unicode string removing accent marks. '''

    s = ''.join((c for c in unicodedata.normalize('NFD', cadena) if unicodedata.category(c) != 'Mn'))
    return s

def clean_text(text):
    ''' Return plain text: lower text, without spaces and punctuation, remove accent marks, digits and duplicated letters. '''
    text = text.lower()
    text = re.sub("@\S+", "", text)
    # remove punctuation
    # replace punctuation with space
    remove_punct = str.maketrans(string.punctuation, ' '*len(string.punctuation))
    text = text.translate(remove_punct)
    # remove whitespaces
    text = " ".join(text.split())
    # remove digits and convert to lower case
    remove_digits = str.maketrans('', '', string.digits)
    text = text.translate(remove_digits)
    # remove 'tildes'
    text = elimina_tildes(text)
    # remove duplicated letters
    text = re.sub(r'([a-z])\1+', r'\1', text)

    return text

def clean_text_transformer(text):
    ''' Return text processed for transformer: convert HTML, remove URLs, \n and \t and whitespaces.'''
    # convert HTML codes
    text = unescape(text)
    text = re.sub(r"http\S+",'[URL]',text)
    # remove newline and tab characters
    text = text.replace('\n',' ')
    text = text.replace('\t',' ')
    #Split by @ to avoid mistakes while matching
    d = "@"
    if text[0]== '@':
        text = ' '.join([d+e.strip() for e in text.split(d) if e])
    if text[0]!='@':
        text = ' '.join([d+e.strip() for e in text.split(d) if e])[1:]
    # strip whitespace
    text = text.strip()
    
    return text

def tidy_tweets(users , file_name, keywords, only_female=False, track_keywords = False):
    ''' Read tweets' information stored in json and store them in a Data Frame.
        We will obtain the id, screen_name and user_id information, the cleaned text, rt, qt and reply information, mentions,
        the female and male users mentioned, hashtags, datetime and location.'''

    if only_female:
        print("Only keeping tweets with at least a female user mention")
    id_list, user_id_list, screen_name_list = [], [], []
    language_list = []
    text_list, text_clean_list = [], []
    in_reply_to_user_id, in_reply_to_status_id, in_reply_to_screen_name = [], [], []
    rt_screen_name_list, rt_user_id_list, rt_id_list = [], [], []
    qt_screen_name_list, qt_id_list, qt_status_list = [], [], []
    tweet_type_list, seed_target_list, keywords_matched_list = [], [], []

    mentions_list = []
    mentions_f_list, directed_at_f = [], []
    mentions_m_list, directed_at_m = [], []
    hashtags_list = []
    datetime = []
    location = []

    f_seed = users[users.Genero == "F"].screen_name.str.lower().values
    m_seed = users[users.Genero == "M"].screen_name.str.lower().values    

    users_with_id = []
    [users_with_id.append(user) for user in users.user_id.values]

    with open(file_name) as json_data:
        for idx, tweet in enumerate(json_data):
            try:
                tweet = json.loads(tweet)
            except Exception as e:
                print(e, tweet)
                continue
            
            screen_name = tweet["user"]['screen_name']
            screen_name_list.append(screen_name)
            user_id_list.append("id_" + str(tweet["user"]['id']))
            tweet_type, seed_target_mention, seed_target_keyword = None, None, None

            
            #print(tweet['entities']['user_mentions']['screen_name'])
            if 'extended_tweet' in tweet:
                text = tweet['extended_tweet']['full_text']
                mentions = set([x['screen_name'] for x in tweet['extended_tweet']
                            ['entities']['user_mentions'] if x['screen_name'] != screen_name])
            else:
                try:
                    text = tweet['text']
                except:
                    text = tweet['full_text']
                mentions = set([x['screen_name'] for x in tweet['entities']
                            ['user_mentions'] if x['screen_name'] != screen_name])

            text = text.replace("\n", "")

            id_list.append("id_" + str(tweet['id']))
            language_list.append(str(tweet['lang']))

            rp_user_id = "id_" + str(tweet["in_reply_to_user_id"]) if tweet["in_reply_to_user_id"] is not None else None
            rp_screen_name = tweet["in_reply_to_screen_name"]
            rp_status_id = "id_" + str(tweet["in_reply_to_status_id"]) if tweet["in_reply_to_status_id"] is not None else None
            
            if rp_user_id is not None:
                tweet_type = 'reply'
                seed_target_mention = rp_user_id in users_with_id

            t = time.strftime(
                '%Y-%m-%d %H:%M:%S', time.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y'))
            datetime.append(t)

            location.append(tweet['place'])

            rt_screen_name, rt_user_id,  rt_id, rt_type, qt_id, qt_screen_name, qt_status = None, None, None, None, None, None, None

            if 'retweeted_status' in tweet:  # It is a retweet
                if 'extended_tweet' in tweet['retweeted_status']:
                    text = tweet['retweeted_status']['extended_tweet']['full_text']
                    mentions = set([x['screen_name'] for x in tweet['retweeted_status']['extended_tweet']
                                ['entities']['user_mentions'] if x['screen_name'] != screen_name])
                else:
                    try:
                        text = tweet['retweeted_status']['text']
                    except:
                        text = tweet['retweeted_status']['full_text']
                    mentions = set([x['screen_name'] for x in tweet['retweeted_status']
                                ['entities']['user_mentions'] if x['screen_name'] != screen_name])
                rt_screen_name = tweet['retweeted_status']['user']['screen_name']
                rt_user_id = "id_" + str(tweet['retweeted_status']['user']['id'])
                rt_id = "id_" + str(tweet['retweeted_status']['id'])
                
                ### Look for replies in retweet
                if rp_user_id is None:
                    rp_user_id = "id_" + str(tweet['retweeted_status']["in_reply_to_user_id"]) if tweet['retweeted_status']["in_reply_to_user_id"] is not None else None
                    rp_screen_name = tweet['retweeted_status']["in_reply_to_screen_name"]
                    rp_status_id = "id_" + str(tweet['retweeted_status']["in_reply_to_status_id"]) if tweet['retweeted_status']["in_reply_to_status_id"] is not None else None
                if rp_user_id is not None:
                    tweet_type = 'reply'
                    seed_target_mention = rp_user_id in users_with_id
                
                mentions = list(mentions)

                mentions_f = [x for x in mentions if x.lower() in f_seed]
                mentions_m = [x for x in mentions if x.lower() in m_seed]

                directed_at_female = 1 if len(mentions_f) > 0 else 0
                directed_at_male = 1 if len(mentions_m) > 0 else 0

                ### Look for quotes in retweet
                if 'quoted_status' in tweet['retweeted_status']:  # In reply to tweet data
                    qt_id = "id_" + str(tweet['retweeted_status']["quoted_status"]["user"]["id"])
                    qt_screen_name = tweet['retweeted_status']["quoted_status"]["user"]["screen_name"]
                    if 'extended_tweet' in tweet['retweeted_status']['quoted_status']:
                        qt_status = tweet['retweeted_status']["quoted_status"]["extended_tweet"]['full_text']
                    else:
                        try:
                            qt_status = tweet['retweeted_status']["quoted_status"]["text"]
                        except:
                            qt_status = tweet['retweeted_status']["quoted_status"]["full_text"]

                    if tweet_type is None: 
                        tweet_type = 'quote'
                        ### Need to check if the user is explicitely mentioning the seed user in the quote
                        ### This is different than checking the quoted text.
                        seed_target_mention = directed_at_female == 1 or directed_at_male == 1
                    elif tweet_type == 'reply': tweet_type = 'reply_with_quote'

            mentions_f = [x for x in mentions if x.lower() in f_seed]
            mentions_m = [x for x in mentions if x.lower() in m_seed]

            directed_at_female = 1 if len(mentions_f) > 0 else 0
            directed_at_male = 1 if len(mentions_m) > 0 else 0
            
            if 'quoted_status' in tweet:  # In reply to tweet data
                qt_id = "id_" + str(tweet["quoted_status"]["user"]["id"])
                qt_screen_name = tweet["quoted_status"]["user"]["screen_name"]
                if 'extended_tweet' in tweet['quoted_status']:
                    qt_status = tweet["quoted_status"]["extended_tweet"]['full_text']
                else:
                    try:
                        qt_status = tweet["quoted_status"]["text"]
                    except:
                        qt_status = tweet["quoted_status"]["full_text"]
                        
                if tweet_type is None: 
                    tweet_type = 'quote'
                    ### Need to check if the user is explicitely mentioning the seed user in the quote
                    ### This is different than checking the quoted text.
                    seed_target_mention = directed_at_female == 1 or directed_at_male == 1
                elif tweet_type == 'reply': tweet_type = 'reply_with_quote'                   
            
            if tweet_type is None: 
                tweet_type = 'standalone'
                seed_target_mention = directed_at_female == 1 or directed_at_male == 1
            
            if only_female:
                print("Only keeping tweets with at least a female politician mention")
                if len(mentions_f) == 0:
                    continue
            
            finding = []
            if track_keywords:
                keywords_list = list(keywords.word)
                for word in keywords_list:
                    if re.findall(word, clean_text(text)):
                        finding.append(re.findall(word, clean_text(text))[0])
                seed_target_keyword = finding!=[]

            seed_target = seed_target_mention or seed_target_keyword
            
            text_list.append(text)
            text_clean_list.append(clean_text(text))
            hashtags_list.append(re.findall("#\S+", text))
            in_reply_to_status_id.append(rp_status_id)
            in_reply_to_user_id.append(rp_user_id)
            in_reply_to_screen_name.append(rp_screen_name)
            rt_screen_name_list.append(rt_screen_name)
            rt_user_id_list.append(rt_user_id)
            rt_id_list.append(rt_id)
            qt_id_list.append(qt_id)
            qt_screen_name_list.append(qt_screen_name)
            qt_status_list.append(qt_status)
            mentions_list.append(mentions)
            mentions_f_list.append(mentions_f)
            mentions_m_list.append(mentions_m)
            directed_at_f.append(directed_at_female)
            directed_at_m.append(directed_at_male)
            tweet_type_list.append(tweet_type)
            keywords_matched_list.append(finding)
            seed_target_list.append(seed_target)

    data = pd.DataFrame({
        "id": id_list,
        "screen_name": screen_name_list,
        "user_id": user_id_list,
        "text": text_list,
        "text_clean": text_clean_list,
        "rt_from_screen_name": rt_screen_name_list,
        "rt_from_user_id": rt_user_id_list,
        "rt_from_id": rt_id_list,
        "qt_from_screen_name": qt_screen_name_list,
        'qt_status': qt_status_list,
        "in_reply_to_screen_name": in_reply_to_screen_name,
        "in_reply_to_user_id":in_reply_to_user_id,
        "in_reply_to_status_id": in_reply_to_status_id,
        "mentions": mentions_list,
        "f_seed": mentions_f_list,
        "m_seed": mentions_m_list,
        "directed_at_f": directed_at_f,
        "directed_at_m": directed_at_m,
        "hashtags": hashtags_list,
        "datetime": datetime,
        "location": location,
        "tweet_type":tweet_type_list,
        'keywords':keywords_matched_list,
        "valid":seed_target_list})
 
    return data


def custom_findall(x, regex_search, fuzzy=False):
    '''Finds multiple patterns in string. 
    For some reason regular re.findall does not work well'''
    x = x.lower()
    insults = []
    if fuzzy:
        while len(x) > 0:
            s = regex.search(regex_search, x)
            if s is None:
                break
            elif s.fuzzy_counts[0] <= 1 and s.fuzzy_counts[1] <= 1 and s.fuzzy_counts[2] <= 1:
                insults.append(s.group(0))
                x = x[s.span()[1]:]
            else:
                break
    else:
        while len(x) > 0:
            s = re.search(regex_search, x)
            if s is None:
                break
            else:
                insults.append(s.group(0))
                x = x[s.span()[1]:]
    return insults

def extract_gendered_slurs(text, dictionary):
    ''' Finds the name of the insult from the dictionary. '''

    name, target = [], []
    for word in text:
        for insult, cat in dictionary.items():
            if re.search(insult, word):
                name.append(cat['name'])
                target.append(cat['target'])
                break
    
    return name


#Code taken from tools_twitter.py
def special_token_map(tweet_series: pd.Series, usernames: pd.DataFrame):
    """
    Takes series of tweet text, replaces usernames of interest with special tokens.
    """
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