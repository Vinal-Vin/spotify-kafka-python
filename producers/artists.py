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
artists_list = [
    "Taylor Swift", "Ariana Grande", "Ed Sheeran", "Billie Eilish", "Justin Bieber",
    "Dua Lipa", "The Weeknd", "Harry Styles", "Olivia Rodrigo", "Selena Gomez",
    "Drake", "Kendrick Lamar", "Travis Scott", "Cardi B", "Post Malone",
    "J. Cole", "Nicki Minaj", "Lil Nas X", "Megan Thee Stallion", "Doja Cat",
    "Beyoncé", "Rihanna", "Chris Brown", "SZA", "H.E.R.",
    "Usher", "Frank Ocean", "Alicia Keys", "Khalid", "Giveon",
    "Imagine Dragons", "Coldplay",
    "Bad Bunny", "J Balvin", "Shakira", "Karol G", "Maluma",
    "Anitta", "Becky G", "Ozuna", "Rosalia", "Daddy Yankee","Hanumankind",
    "BTS", "BLACKPINK", "EXO", "TWICE", "SEVENTEEN",
    "IU"
]
artists_Ids = ""

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

# def search_artist(artist_name):
#     url = f"https://api.spotify.com/v1/search?q={artist_name}&type=artist&limit=1"
#     headers = get_auth_headers()
#     response = requests.get(url, headers=headers)
#     if response.status_code == 200:
#         artists = response.json()['artists']['items']
#         time.sleep(30)  # To avoid hitting rate limits
#         if artists:
#             return artists[0]['id']
#     return None

def get_several_artists(artists_Ids):
    url = f"https://api.spotify.com/v1/artists?ids={artists_Ids}"
    headers = get_auth_headers()
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        artists = response.json()
        return json.loads(artists)

# artist_ids_list = []
# for artist in artists_list:
#     artist_id = search_artist(artist)
#     if artist_id:
#         artist_ids_list.append(artist_id)

# artists_Ids = ",".join(artist_ids_list)
# print(artists_Ids)

# Commented code used to get artists_Ids and used it in the get_several_artists function
artists_Ids = "06HL4z0CvFAxyc27GXpf02,66CXWjxzNUsdJxJ2JdwvnR,6eUKZXaKkcviH0Ku9w2n3V,6qqNVTkY8uBg9cP3Jd7DAH,1uNFoZAHBGtllmzznpCI3s,6M2wZ9GZgrQXHCFfjv46we,1Xyo4u8uXC1ZmMpatF05PJ,6KImCVD70vtIoJWnq6nGn3,1McMsnEElThX1knmY4oliG,0C8ZW7ezQVs4URX5aX7Kqx,3TVXtAsR1Inumwj472S9r4,2YZyLoL8N0Wb9xBt1NhZWg,0Y5tJX1MQlPlqiwlOH1tJY,4kYSro6naA4h99UJvo89HB,246dkjvS1zLTtiykXe5h60,6l3HvQ5sa6mXTsMTB19rO5,0hCNtLu0JehylgoiP8L4Gh,7jVv8c5Fj3E9VhNjxT4snq,181bsRPaVXVlUKXrxwZfHK,5cj0lLjcoR7YOSnhnX0Po5,6vWDO969PvNqNYHIOW5v0m,5pKCCKE2ajJHZ9KAiaK11H,7bXgB6jMjp9ATFy66eO08Z,7tYKF4w9nC0nq9CsPZTHyP,3Y7RZ31TRPVadSFVy1o8os,23zg3TcAtWQy7J6upgbUnj,2h93pZq0e7k5yf4dywlkpM,3DiDSECUqqY1AuBP8qtaIa,6LuN9FCkKOj5PcnpouEgny,4fxd5Ee7UefO4CUXgwJ7IP,53XhwfbYqKCa1cC15pYq2q,4gzpq5DPGxSnKTe4SA8HAU,4q3ewBCX7sLwd24euuV69X,1vyhD5VmyZ7KMfW5gqLgo5,0EmeFodog0BfCgMzAIvKQp,790FomKkXshlbRYZFtlgla,1r4hJ1h58CWwUQe3MxPuau,1Xyo4u8uXC1ZmMpatF05PJ,4obzFoKoKRHIphyHzJ35G3,1i8SpTcr7yvPOmcqrbnVXY,7ltDVBr6mKbRvohxheJ9h1,4VMYDCV2IEDYJArk749S6m,4nVa6XlBFlIkF6msW57PHp,3Nrfpe0tUJi4K4DXYWgMUX,41MozSoPIsD1dJM0CLPjZF,3cjEqqelV9zb4BYE3qDQ4O,7n2Ycct7Beij7Dj7meI4X0,7nqOGRxlXj7N2JYbgNEjYH,3HqSLMAZ3g3d5poNaI7GOU"
all_artists = get_several_artists(artists_Ids)
print(all_artists)

producer.send("artists_data", all_artists)
print(f'Sent data to Kafka: {all_artists}')
time.sleep(30)
