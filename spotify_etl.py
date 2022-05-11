import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import sqlite3
from datetime import timedelta


DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"
USER_ID = "Henry" # your Spotify username
TOKEN = 'BQDF2ZkuXpgWHmLloP-t2_UP5DKrdc2Sd3MfEwhcarkXXhaLCo0NVlSfCL7pYtOgHJ3J7Z5FG0Z6siVYfsnphQEVQqheF0s4WVuQjkFVhyCXz6WdPIRsewOYF80NqpPqxnh5vOeyXRpG4ONo-pBAZHllNZyUbpwYxcpFNJiQ'

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

    song_names = []
    artist_names = []
    albums_music = []
    played_at_list = []
    year_releases =[]
    timestamps = []

    for song in data['items']:

       song_names.append(song["track"]["name"])
       artist_names.append(song["track"]["album"]["artists"][0]["name"])
       albums_music.append(song["track"]["album"]['name'])
       year_releases.append(song["track"]["album"]['release_date'])
       played_at_list.append(song["played_at"])
       timestamps.append(song["played_at"][0:10])

    song_dict = {
        "song_name": song_names,
        "artist_name": artist_names,
        "album_music": albums_music,
        "year_release" : year_releases,
        "played_at": played_at_list,
        "timestamp": timestamps
    }

    df_song = pd.DataFrame( data = song_dict , columns = ['song_name', 'artist_name', 'album_music', 'year_release',
                                                            'played_at', 'timestamp'])

    df_song.to_csv("songs.csv", index = False)








