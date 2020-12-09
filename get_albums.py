import spotipy
import datetime
import os
from spotipy.oauth2 import SpotifyClientCredentials

client_id = "0e78c09001fc4e989cbea4fd826e34e2"
client_secret = "05caaea96d2d40ba8ffdfa967eb26fca"

client_credentials_manager = SpotifyClientCredentials(
    client_id=client_id, client_secret=client_secret)

sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

NewAlbums = open("NewRelease.csv", 'a', encoding='utf-8')

if os.stat("NewRelease.csv").st_size == 0:
        NewAlbums.write("album_type;album_id;album_name;artist_id;artist_names;release_date;popularity\n")

new_albums_ids = []

for x in range(0, 2000, 20):
    new = sp.search(q='tag:new', type='album', limit=20, offset=x)  # search query for new realese

    new_albums = new['albums']
    new_albums_ids = []

    for i, item in enumerate(new_albums['items']):
        new_albums_ids.append(item['id'])

    if not new_albums_ids:
        break

    albums = sp.albums(new_albums_ids)

    for i, item in enumerate(albums['albums']):
        NewAlbums.write(item["album_type"] + ';' + item["id"] + ';' + item["name"] + ';' + item["artists"][0]["id"] + ';' + item["artists"][0]["name"] + ';' + item["release_date"] + ';' + str(item["popularity"]) + '\n')
    
