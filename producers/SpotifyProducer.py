from kafka import KafkaProducer
import json
import requests
import base64
import os
import time
import random2 as random

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# producer.send('test_sales', value={"test": "test"})

client_id = 'XXXXXXXXXXXXXXXX'
client_secret = 'XXXXXXXXXXXXXXXX'
requests_limit = 50

def get_token():
    auth_string = client_id + ':' + client_secret
    auth_bytes = auth_string.encode("utf-8")
    auth_base64 = str(base64.b64encode(auth_bytes), "utf-8")

    url = 'https://accounts.spotify.com/api/token'
    headers = {
        "Authorization": f"Basic {auth_base64}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {"grant_type": "client_credentials"}
    result = requests.post(url, headers=headers, data=data)
    json_resut = json.loads(result.content)
    token = json_resut['access_token']
    return token

def get_auth_headers():
    return {"Authorization": f"Bearer {get_token()}"}

def get_spotify_recommendations():
    url = 'https://api.spotify.com/v1/recommendations'
    headers = get_auth_headers()
    params = {
        "limit": requests_limit,
        # "market": "US",
        # "seed_artists": "3q7HBObVc0L8jNeTe5Gofh",
        # "seed_genres": f"{genres}"
        "seed_genres": "pop",
        # "seed_tracks": "0c6xIDDpzE81m2q797ordA"
    }
    result = requests.get(url, headers=headers, params=params)
    return json.loads(result.content)

def get_comma_separated_genre_string():
    url = 'https://api.spotify.com/v1/recommendations/available-genre-seeds'
    headers = get_auth_headers()
    result = requests.get(url, headers=headers)
    genres = json.loads(result.content)['genres']

    # select 5 random genres
    genres = random.sample(genres, 5)
    return ', '.join(genres)


for i in range(requests_limit):
# while True:
    # genres = get_comma_separated_genre_string()
    # time.sleep(5)

    rec = get_spotify_recommendations()
    #write variable to store an array of artist names, string for [external_urls][spotify], and track names
    track_name = rec['tracks'][0]['name']
    available_markets = list(rec['tracks'][0]['available_markets'])
    artist_names = [artist['name'] for artist in rec['tracks'][0]['artists']]
    album_name = rec['tracks'][0]['album']['name']
    release_date = rec['tracks'][0]['album']['release_date']
    total_tracks = rec['tracks'][0]['album']['total_tracks']
    track_duration = rec['tracks'][0]['duration_ms']
    popularity = rec['tracks'][0]['popularity']

    print("\n**********************************\n")
    print("track_name: ", track_name)
    print("artist_names: ", artist_names)
    print("available_markets: ", available_markets)
    print("album_name: ", album_name)
    print("release_date: ", release_date)
    print("total_tracks: ", total_tracks)
    print("track_duration: ", track_duration)
    print("popularity: ", popularity)
    print("\n**********************************\n")

    data = {
        "track_name": track_name,
        "artist_names": artist_names,
        "available_markets": available_markets,
        "album_name": album_name,
        "release_date": release_date,
        "total_tracks": total_tracks,
        "track_duration": track_duration,
        "popularity": popularity
    }
    producer.send('spotify_recommendations', value=data)
    print(f'Sent data to Kafka: {data}')
    time.sleep(30)
