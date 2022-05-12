import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime, timedelta
import sqlite3



DATABASE_LOCATION = "sqlite:///minhas_musicas.sqlite"
USER_ID = "Henry" # your Spotify username
TOKEN = 'BQAE83oM7nsxq6t0FPl1xLfY5fXqVZLGjG_hemgKr9Lzv9s3_v7kPqULfTQxdBT4wCFE657NNgXLTd6_xhEmIb8U9y7YtwqGPjEwsGRRqEkU3JLIuOw0249PeXvqJRvLfR-2ueW0wDwxi5hmSgogN_LDCNwtmj0ALaZzLn36'


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

    # Creating Song Data Structure
    song_list = []
    for song in data['items']:
        song_id = song['track']['id']
        song_name = song['track']['name']
        song_duration = song['track']['duration_ms']
        song_url = song['track']['external_urls']['spotify']
        song_time_played = song['played_at']
        timestamp = song["played_at"][0:10]
        album_id = song['track']['album']['id']
        artist_id = song['track']['artists'][0]['id']
        song_attributes = { 'song_id': song_id , 'song_name': song_name , 'song_duration': song_duration,
                            'song_url': song_url , 'song_time_played': song_time_played, 'timestamp':timestamp,
                           'album_id': album_id , 'artist_id': artist_id}
        song_list.append(song_attributes)


    #Create Artist Data Structure
    artist_list = []
    for song in data['items']:
        artist_id = song['track']['artists'][0]['id']
        artist_name = song['track']['artists'][0]['name']
        artist_url = song['track']['artists'][0]['external_urls']['spotify']
        timestamp = song["played_at"][0:10]
        artist_attributes = {'artist_id': artist_id , 'artist_name': artist_name, 'artist_url': artist_url,
                             'timestamp': timestamp}
        artist_list.append(artist_attributes)

    album_list = []
    for song in data['items']:
        album_id = song['track']['album']['id']
        album_name = song['track']['album']['name']
        album_release_date = song['track']['album']['release_date']
        album_total_tracks = song['track']['album']['total_tracks']
        timestamp = song["played_at"][0:10]
        album_url = song['track']['album']['external_urls']['spotify']
        album_attributes = {'album_id': album_id, 'name': album_name, 'release_date': album_release_date,
                         'total_tracks': album_total_tracks, 'url': album_url, 'timestamp': timestamp }
        album_list.append(album_attributes)



    # We will need to do some cleaning and add our Unique ID for the Track
    # Then load into SQLite3 from the dataframe

    album_df = pd.DataFrame.from_dict(album_list)
    album_df = album_df.drop_duplicates(  subset=['album_id'] )

    artist_df = pd.DataFrame.from_dict(artist_list)
    artist_df = artist_df.drop_duplicates( subset=['artist_id'] )

    song_df = pd.DataFrame.from_dict(song_list)

    song_df['song_time_played'] = pd.to_datetime(song_df['song_time_played'])
    song_df['unix_timestamp'] =  song_df['song_time_played'].apply( lambda x: int (datetime.timestamp(x)))

    song_df['identificador_unico'] = song_df['song_id'] + '-' + song_df['unix_timestamp'].astype("str")
    song_df = song_df[['identificador_unico', 'song_name', 'song_duration' , 'song_url', 'song_time_played', 'album_id', 'artist_id', 'timestamp']]

    dados = []

    dados = [album_df, artist_df , song_df]

    for df in dados:
        if check_data(df):
            print("Data valid, proceed to Load stage")
            pass

    song_df.to_csv("song.csv", index = False)
    album_df.to_csv("album.csv", index = False)
    artist_df.to_csv("artist.csv", index = False)

    #Criação das tabelas
    sql_query_artista = """
     CREATE TABLE IF NOT EXISTS artistas(
         artista_id VARCHAR(200),
         nome_artista VARCHAR(200),
         url_artista VARCHAR(200),
         CONSTRAINT primary_key_constraint PRIMARY KEY (artista_id)
     )
     """

    sql_query_album = """
        CREATE TABLE IF NOT EXISTS albuns(
            album_id VARCHAR(200),
            nome_album VARCHAR(200),
            lançamento VARCHAR(200),
            url_album VARCHAR(200),
            numero_musicas VARCHAR(200),
            CONSTRAINT primary_key_constraint PRIMARY KEY (album_id)
        )
        """




    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('minhas_musicas.sqlite')

    cursor = conn.cursor()
    cursor.execute(sql_query_artista)
    cursor.execute(sql_query_album)
    print("Opened database successfully")
    cursor.close()








