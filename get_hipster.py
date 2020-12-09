import spotipy
import datetime
import os
from spotipy.oauth2 import SpotifyClientCredentials

client_id = "0e78c09001fc4e989cbea4fd826e34e2"
client_secret = "05caaea96d2d40ba8ffdfa967eb26fca"

client_credentials_manager = SpotifyClientCredentials(
    client_id=client_id, client_secret=client_secret)

sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

NewHipster = open("NewHipster.csv", 'a', encoding='utf-8')

if os.stat("NewHipster.csv").st_size == 0:
        NewHipster.write("album_type;album_id;album_name;artist_id;artist_names;release_date;popularity\n")

hipster_ids = []

for x in range(0, 2000, 20):
    hipster = sp.search(q='tag:hipster', type='album', limit=20, offset=x)

    hipster = hipster['albums']
    hipster_ids = []

    for i, item in enumerate(hipster['items']):
        hipster_ids.append(item['id'])
    
    if not hipster_ids:
        break

    albums = sp.albums(hipster_ids)

    for i, item in enumerate(albums['albums']):
        NewHipster.write(item["album_type"] + ';' + item['id'] + ';' + item["name"] + ';' + item["artists"][0]["id"] + ';' + item["artists"][0]["name"] + ';' + item["release_date"] + ';' + str(item["popularity"]) + '\n')
    
