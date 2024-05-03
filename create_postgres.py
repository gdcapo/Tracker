import psycopg2
from psycopg2 import extras
import pandas as pd
import datetime
from accessPoint_db import Postgres

connection = psycopg2.connect(host=Postgres.host,
                                  database=Postgres.database,user=Postgres.user,password=Postgres.password)
connection.autocommit = True
cursor = connection.cursor()

# metadata = pd.read_csv("data/all_screen_names_ids.csv")
# metadata = metadata[['screen_name','Nombre', 'Tipo',  'followers_count', 'friends_count', 'user_id']]
# metadata['user_id'] = 'id_' + metadata['user_id'].astype(str)

# cursor.execute("DROP TABLE IF EXISTS metadata")
# sql_metadata ='''CREATE TABLE metadata(screen_name CHAR(100), Nombre CHAR(500), Tipo CHAR(100),followers_count INT, friends_count INT,user_id CHAR(100))'''
# cursor.execute(sql_metadata)
# print("Table sql_metadata created successfully........")

# tpls_metadata = [tuple(x) for x in metadata.to_numpy()]
# sql_metadata = 'INSERT INTO metadata(screen_name, Nombre, Tipo,followers_count, friends_count,user_id) VALUES(%s,%s,%s,%s,%s,%s)'
# print('insert metadata')
# extras.execute_batch(cursor, sql_metadata, tpls_metadata)

diccionario = pd.read_excel("data/Diccionario_de_insultos.xlsx")
diccionario = diccionario[['Insulto', 'Insulto_gendered','Categoría', 'Subcategoría']]
diccionario = diccionario[~diccionario['Categoría'].isin(['Criticas al ejercicio de su profesión'])]

# cursor.execute("DROP TABLE IF EXISTS insult_to_group")
# sql_diccionario ='''CREATE TABLE insult_to_group(Insulto CHAR(200), Insulto_gendered CHAR(200),Categoria CHAR(200), Subcategoria CHAR(300))'''
# cursor.execute(sql_diccionario)
# print("Table sql_diccionario created successfully........")

tpls_diccionario = [tuple(x) for x in diccionario.to_numpy()]
sql_diccionario = 'INSERT INTO insult_to_group(Insulto,Insulto_gendered,Categoria, Subcategoria) VALUES(%s,%s,%s,%s)'
print('insert diccionario')
extras.execute_batch(cursor, sql_diccionario, tpls_diccionario)

# cursor.execute("DROP TABLE IF EXISTS mentions_and_abuse_summary")
# sql_mentions_abuse ='''CREATE TABLE mentions_and_abuse_summary(Hour TIMESTAMP, influencer_id CHAR(500), N_mentions INT, N_abuse INT, N_mentions_orig INT, N_mentions_rt INT, N_abuse_orig INT, N_abuse_rt INT)'''
# cursor.execute(sql_mentions_abuse)
# print("Table sql_mentions_abuse created successfully........")

# cursor.execute("DROP TABLE IF EXISTS insults_summary")
# sql_insults_summary ='''CREATE TABLE insults_summary(Hour TIMESTAMP, influencer_id CHAR(500), Insults CHAR(500), N_abuse INT)'''
# cursor.execute(sql_insults_summary)
# print("Table sql_insults_summary created successfully........")

# cursor.execute("DROP TABLE IF EXISTS subc_summary")
# sql_insults_summary ='''CREATE TABLE subc_summary(Hour TIMESTAMP, influencer_id CHAR(500), Subcategoría CHAR(500), N_abuse INT)'''
# cursor.execute(sql_insults_summary)
# print("Table subc_summary created successfully........")

# cursor.execute("DROP TABLE IF EXISTS hashtags_summary")
# sql_hashtags_summary ='''CREATE TABLE hashtags_summary(Hour TIMESTAMP, influencer_id CHAR(500), Hashtag CHAR(500), N_hashtags INT)'''
# cursor.execute(sql_hashtags_summary)
# print("Table sql_hashtags_summary created successfully........")

# cursor.execute("DROP TABLE IF EXISTS raw_data")
# sql_raw_data ='''CREATE TABLE raw_data(id CHAR(500) NOT NULL,text CHAR(2500),datetime TIMESTAMP,user_id CHAR(200), keywords CHAR(500), valid CHAR(10))'''
# cursor.execute(sql_raw_data)
# print("Table sql_raw_data created successfully........")

connection.autocommit = True
cursor = connection.cursor()

cursor.close()
connection.close()