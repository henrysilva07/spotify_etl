import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime, timedelta
from LoadData import LoadData


DATABASE_LOCATION = "sqlite:///minhas_musicas.db"
USER_ID = "Henry" # your Spotify username
TOKEN = 'BQBwiqIbWGG_9HqmLYRHPnxuDevzMV3TE_8EBqVg1kVWDVFH3hwk5SYGagYtg5kgS_QolKWTbNJqE-EOO83C3sNK2tlGBCdCznG-mzEiFpFq2OPVZV7FGJgSK0WBEOrWGRo4DK_KuQXKQXS4r9fpF94FrqJ3uBVRQJO_-6Xe'

def check_data(df: pd.DataFrame) -> bool:
    if df.empty:
        print("No songs downloaded. Finishing process")

        return False

    if df.isnull().values.any():
        raise Exception("Null value found")
    else:
        pass

    yesterday = datetime.now() - timedelta(days = 1)
    yesterday.replace(hour=0, minute=0, second=0 , microsecond=0)
    timestamps = df['timestamp'].tolist()
    for timestamp in timestamps:
        if datetime.strptime(timestamp ,  '%Y-%m-%d') != yesterday:
            pass
        else:
            raise Exception("Pelo menos uma das música não apresenta a data de ontem")
    return True

if __name__ == "__main__":

    # Extract part of the ETL process
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token=TOKEN)
    }

    today = datetime.now()
    yesterday = today - timedelta( days= 1)
    yesterday_unix_timestamp = int(yesterday.timestamp())*1000

    r = requests.get(
        "https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp),
        headers=headers)

    data = r.json()
    print(r)

    # Creating Song Data Structure
    musica_lista = []
    for musica in data['items']:
        id_musica = musica['track']['id']
        nome_musica = musica['track']['name']
        duracao_musica = musica['track']['duration_ms']
        url_musica = musica['track']['external_urls']['spotify']
        data_played = musica['played_at']
        timestamp = musica["played_at"][0:10]
        id_album = musica['track']['album']['id']
        id_artista = musica['track']['artists'][0]['id']
        musicas_atributos = { 'id_musica': id_musica , 'nome_musica': nome_musica , 'duracao_musica': duracao_musica,
                            'url_musica': url_musica , 'data': data_played, 'timestamp':timestamp,
                           'id_album': id_album , 'id_artista': id_artista}
        musica_lista.append(musicas_atributos)



    artista_list = []
    for musica in data['items']:
        id_artista = musica['track']['artists'][0]['id']
        nome_artista = musica['track']['artists'][0]['name']
        url_artista = musica['track']['artists'][0]['external_urls']['spotify']
        timestamp = musica["played_at"][0:10]
        artista_atributos = {'id_artista': id_artista, 'nome_artista': nome_artista, 'url_artista': url_artista,
                             'timestamp': timestamp}
        artista_list.append(artista_atributos)

    album_list = []
    for musica in data['items']:
        id_album = musica['track']['album']['id']
        nome_album = musica['track']['album']['name']
        lancamento = musica['track']['album']['release_date']
        total_musicas = musica['track']['album']['total_tracks']
        timestamp = musica["played_at"][0:10]
        url_album = musica['track']['album']['external_urls']['spotify']
        album_atributos = {'id_album': id_album, 'nome-album': nome_album, 'lancamento': lancamento,
                         'total_musicas': total_musicas, 'url_album': url_album, 'timestamp': timestamp}
        album_list.append(album_atributos)


    album_df = pd.DataFrame.from_dict(album_list)
    album_df = album_df.drop_duplicates(  subset=['id_album'])

    artist_df = pd.DataFrame.from_dict(artista_list)
    artist_df = artist_df.drop_duplicates( subset=['id_artista'])


    musica_df = pd.DataFrame.from_dict(musica_lista)

    musica_df['data'] = pd.to_datetime(musica_df['data'])
    musica_df['unix_timestamp'] = musica_df['data'].apply( lambda x: int (datetime.timestamp(x)))

    musica_df['id_musica'] = musica_df['id_musica'] + '-' + musica_df['unix_timestamp'].astype("str")
    musica_df = musica_df[['id_musica', 'nome_musica', 'duracao_musica' , 'url_musica', 'data', 'id_album', 'id_artista', 'timestamp']]

    dados = []

    dados = [album_df, artist_df , musica_df]

    for df in dados:
        if check_data(df):
            print("Data valid, proceed to Load stage")
            pass


    #Criação das tabelas
    sql_query_artista = """
     CREATE TABLE IF NOT EXISTS artistas(
         id_artista VARCHAR(200),
         nome_artista VARCHAR(200),
         url_artista VARCHAR(200),
         timestamp VARCHAR(200),
         CONSTRAINT primary_key_constraint PRIMARY KEY (id_artista)
     )
     """

    sql_query_album = """
        CREATE TABLE IF NOT EXISTS albuns(
            id_album VARCHAR(200),
            nome_album VARCHAR(200),
            lançamento VARCHAR(200),
            url_album VARCHAR(200),
            numero_musicas VARCHAR(200),
            timestamp VARCHAR(200),
            CONSTRAINT primary_key_constraint PRIMARY KEY (id_album)
        )
        """

    sql_query_musica = """
        CREATE TABLE IF NOT EXISTS musicas(
            id_musica VARCHAR(200),
            nome_musica VARCHAR(200),
            duracao_musica VARCHAR(200),
            url_musica VARCHAR(200),
            data_played VARCHAR(200),
            id_album   VARCHAR(200),
            id_artista VARCHAR(200),
            timestamp VARCHAR(200), 
            CONSTRAINT primary_key_constraint PRIMARY KEY (id_musica)
        )
        """
   # artist_df = artist_df[['id_artista', 'nome_artista', 'url_artista']]

    dados = {"albuns": [album_df, "id_album",sql_query_album ], "artistas": [artist_df, "id_artista", sql_query_artista],  "musicas" :  [musica_df, "id_musica", sql_query_musica]}

    for key , value in dados.items():

        conexao = LoadData( DATABASE_LOCATION, "minhas_musicas.db", value[0])

        conexao.criando_table(value[2])

        conexao.inserindo_dados(key, value[1])









