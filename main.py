from email import header
from os import times
import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import date, datetime
import datetime
import sqlite3


DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"
USER_ID = "davidcarrevedo"
TOKEN = "BQD1-cyEVrXKO5UnZzj_ausPV0tyEHYacBhqAk9pVD1JbfSG83tdkSy_FMd9M3hNzuh9pfb2KIQ9Gd2Qsnn4tFukmyd0GINmnR9o76-wsgete-rhGOvEZhwjQqYFy_nPfw4pgrNG6PoJYH6CSQCRuwcamufjfwujHac1Get8L2h7ZRE8XEgVjISRpvo"


# Generar token aqui: https://developer.spotify.com/console/get-recently-played/
# Nota: se necesita tener una cuenta de Spotify

def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Chequear si la informacion esta vacia
    if df.empty:
        print("No se han reproducido canciones. Se finaliza la ejecucion")
        return False

    # Verificar Primary Key
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Violacion de Primary Key")

    # Verificar nulos
    if df.isnull().values.any():
        raise Exception("Null valued found")

    # Verificar fecha de timestamp
    yesterday = datetime.datetime.now() - datetime.timedelta(days = 1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = df["timestamp"].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, "%Y-%m-%d") < yesterday:
            raise Exception("Al menos una de las canciones retornadas no fue reproducida en las ultimas 24 hs.")

    return True 


if __name__ == "__main__":

    headers = {
        "Accept" : "application/json",
        "Content-Type": "application/json",
        "Authorization" : "Bearer {token}".format(token=TOKEN)
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestap = int(yesterday.timestamp()) * 1000

    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestap), headers = headers)
    
    data = r.json()

    song_name = []
    artist_names = []
    played_at_list = []
    timestamps = []

    for song in data["items"]:
        song_name.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])

    song_dict = {
        "song_name" : song_name,
        "artist_name" : artist_names,
        "played_at" : played_at_list,
        "timestamp" : timestamps
    }

    song_df = pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "played_at", "timestamp"])

    #print (song_df)
    
    # Validacion
    if check_if_valid_data(song_df):
        print("Data valida, proceder a cargarla")


    #Load
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('my_played_tracks.sqlite')
    cursor = conn.cursor()

    sql_query = """
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR (200),
        artist_name VARCHAR (200),
        played_at VARCHAR (200),
        timestamp VARCHAR (200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """

    cursor.execute(sql_query)
    print("Base de datos abierta con exito")

    try:
        song_df.to_sql("my_played_tracks", engine, index=False, if_exists='append')
    except:
        print("data already exists in the database")
    