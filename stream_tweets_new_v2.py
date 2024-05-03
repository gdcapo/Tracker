import sys
import pandas as pd
import json
import time
import datetime as dt
import tweepy
import os

# Configuración de autenticación
bearer_token = os.environ.get("TWITTER_BEARER_TOKEN")
client = tweepy.Client(bearer_token=bearer_token)

class StreamListener(tweepy.StreamingClient):
    def __init__(self, bearer_token):
        super().__init__(bearer_token)

    def on_response(self, response):
        tweet = response.data
        if tweet is not None:
            # Procesar el tweet
            tweet_data = {
                'id': tweet.id,
                'text': tweet.text,
                'author_id': tweet.author_id,
                'created_at': tweet.created_at.isoformat(),
                'date': tweet.created_at.date().isoformat(),
                'datetime': tweet.created_at.isoformat()
            }

            # Guardar el tweet en un archivo JSON lines
            current_time = dt.datetime.now()
            date = current_time.strftime("%d-%m-%Y")
            hour_window = current_time.strftime("%H")
            output_name = f"raw/output-{date}-{hour_window}.jsonl"

            with open(output_name, "a") as output:
                json.dump(tweet_data, output)
                output.write('\n')

            print(f"Tweet guardado: {tweet_data['text']}")

if __name__ == "__main__":
    # Cargar los datos de usuarios semilla y palabras clave
    project_path = sys.argv[1]
    n_group = int(sys.argv[2])

    seed_users = pd.read_csv(f"{project_path}/data/all_screen_names_ids.csv", dtype={'user_id': 'str'})
    keywords = pd.read_csv(f"{project_path}/data/keywords.csv")

    # Obtener los IDs de usuarios semilla y las palabras clave para el grupo actual
    user_ids = list(seed_users[seed_users.user_id.notna()].user_id.values)[n_group * 400:(n_group + 1) * 400]
    tracking_words = list(keywords.word)

    # Iniciar el flujo de tweets
    print(f"Rastreando usuarios: {user_ids[:3]} y palabras clave: {tracking_words[:3]}")
    print(dt.datetime.now())
    print("*** STARTING LOOP ***")

    stream = StreamListener(bearer_token)
    rules = tweepy.StreamRule(f"from:{','.join(user_ids)} OR {' OR '.join(tracking_words)}")
    stream.add_rules(rules)
    stream.filter()