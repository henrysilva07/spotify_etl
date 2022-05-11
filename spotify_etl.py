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
TOKEN = 'BQCHgDT-RCggex3sSM-3tYBlelssAhkIbWkxaiGQXJtR7CIWwTLXYRjXDAIypH30wr4Z1iVOCWXMOqGZNtnMLaYLl9JtY7FzpFi-5BkK8s_2k0VUuM7sd-Uv3F8gT0TyRSyfYNT8sXW1r6f0UEWXHBIATDljX7B_7rhqLv6t'


def check_valid_data(df: pd.DataFrame) -> bool:
    if df.empty:
        print("No songs downloaded. Finishing process")

        return False

    #Primary key check
    if pd.series(df[0]).is_unique:
        pass
    else:
        raise Exception("Primary key check is violated")

    if df.isnull().values.any():
        raise Exception("Null value found")
    else:
        pass
    pass


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
        album_id = song['track']['album']['id']
        artist_id =song['track']['artists'][0]['id']
        song_attributes = { 'song_id': song_id , 'song_name': song_name , 'song_duration': song_duration,
                            'song_utl': song_url , 'song_time_played': song_time_played,
                           'album_id': album_id , 'artist_id': artist_id}
        song_list.append(song_attributes)


    #Create Artist Data Struture
    artist_list = []
    for song in data['items']:
        artist_id = song['track']['artists'][0]['id']
        artist_name = song['track']['artists'][0]['name']
        artist_url = song['track']['external_urls']['spotify']
        song_time_played = song['played_at']
        artist_attributes = {'artist_id': artist_id , 'artist_name': artist_name, 'artist_url': artist_url,
                             'song_time_played': song_time_played}
        artist_list.append(artist_attributes)

    album_list = []
    for song in data['items']:
        album_id = song['track']['album']['id']
        album_name = song['track']['album']['name']
        album_release_date = song['track']['album']['release_date']
        album_total_tracks = song['track']['album']['total_tracks']
        song_time_played = song['played_at']
        album_url = song['track']['album']['external_urls']['spotify']
        album_attributes = {'album_id': album_id, 'name': album_name, 'release_date': album_release_date,
                         'total_tracks': album_total_tracks, 'url': album_url, 'song_time_played': song_time_played}
        album_list.append(album_attributes)



    # We will need to do some cleaning and add our Unique ID for the Track
    # Then load into SQLite3 from the dataframe

    album_df = pd.DataFrame.from_dict(album_list)
    album_df = album_df.drop_duplicates(  subset=['album_id'] )

    artist_df = pd.DataFrame.from_dict(artist_list)
    artist_df = artist_df.drop_duplicates( subset=['artist_id'] )

    song_df = pd.DataFrame.from_dict(song_list)


    song_df.to_csv("songs.csv", index = False)










