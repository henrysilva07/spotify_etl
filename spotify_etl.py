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
TOKEN = 'BQCFyiNg8IgjHN4ThdBrzSXJOLFynipvaj-NrQKK0alNGWULC8cnsGXbHG5gSghpMkWXYRlwcLOXZSVDHUQCv4X-zL-LsTBlgkduig6BG-oApQuqbbYPIFCgIFBiMO4SaR-YqanB-hhbBAFNmBwWfhvFCjOEgmG_FEi4i3Ev'

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

    # Creating the Album Data Structure:
    album_list = []
    for song in data['items']:
        album_id = song['track']['album']['id']
        album_name = song['track']['album']['name']
        album_release_date = song['track']['album']['release_date']
        album_total_track = song['track']['album']['total_tracks']
        album_url = song['track']['album']['external_urls']['spotify']
        album_information = {'album_id': album_id, 'name': album_name, 'release_date': album_release_date,
                         'total_tracks': album_total_track, 'url': album_url}
        album_list.append(album_information)



    df_song = pd.DataFrame( data = song_dict , columns = ['song_name', 'artist_name', 'album_music', 'year_release',
                                                            'played_at', 'timestamp'])

    df_song.to_csv("songs.csv", index = False)











