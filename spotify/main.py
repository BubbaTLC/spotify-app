
# %%
from py2neo import Graph
import apache_beam as beam
import requests as req

import sys
sys.path.append('../')

import json

from beam_functions import *

from loader.main import Neo4jLoader, destroy
# %%

def get_data(url) -> dict:
    access_token = SpotifyOAuth(scope="user-library-read", client_id=SPOTIPY_CLIENT_ID, client_secret=SPOTIPY_CLIENT_SECRET, redirect_uri=SPOTIPY_REDIRECT_URI).get_cached_token()
    try:
        res = req.get(url, headers={'Authorization': f"Bearer {access_token['access_token']}"})
        if res.status_code == 200:
            return res.json()
        else:
            return res.reason
    except Exception as e:
        print(e)

    return res.text
# %%
print(get_data("https://api.spotify.com/v1/me/top/artists"))

# %%


def remove_none(dic) -> dict:
    for key, value in dict(dic).items():
        if value is None:
            del dic[key]
        if isinstance(value, list):
            for x in value:
                remove_none(x)
        if isinstance(value, dict):
            remove_none(value)
    return dict(dic)

def load_user():
    with beam.Pipeline() as p:
        pipe = (
            p 
            | beam.Create(["https://api.spotify.com/v1/me"])
            | 'Call API' >> beam.ParDo(SpotifyPipe())
            | 'Flatten JSON' >> beam.ParDo(flatten_json())
            | 'Load GDB' >> beam.ParDo(
                Neo4jLoader(
                    host=NEO4J_HOST,
                    user=NEO4J_USER,
                    password=NEO4J_PASSWORD,
                    query="CREATE (n:User $props);"
                )
            )
        )

def load_playlists():
    with beam.Pipeline() as p:
        pipe = (
            p 
            | beam.Create(["https://api.spotify.com/v1/me/playlists"])
            | 'Call API' >> beam.ParDo(SpotifyPipe())
            | 'Get Items' >> beam.ParDo(extract_items())
            | 'Load GDB' >> beam.ParDo(
                Neo4jLoader(
                    host=NEO4J_HOST,
                    user=NEO4J_USER,
                    password=NEO4J_PASSWORD,
                    query="CREATE (n:Playlist $props);"
                )
            )
        )

def load_tracks():
    with beam.Pipeline() as p:
        pipe = (
            p 
            | beam.Create(["https://api.spotify.com/v1/me/playlists"])
            | 'Call API' >> beam.ParDo(SpotifyPipe())
            | 'Get Items' >> beam.ParDo(extract_items())
            | 'Get Href' >> beam.ParDo(extract_track_href())
            | 'GET Songs' >> beam.ParDo(GETSongs())
            | 'Flatten JSON' >> beam.ParDo(flatten_json())
            | 'Load GDB' >> beam.ParDo(
                Neo4jLoader(
                    host=NEO4J_HOST,
                    user=NEO4J_USER,
                    password=NEO4J_PASSWORD,
                    query="MERGE (t:Track {track_id: $props['track_id']}) ON CREATE set t = $props;"
                )
            )
        )


def load_track():
    with beam.Pipeline() as p:
        pipe = (
            p 
            | beam.Create(["https://api.spotify.com/v1/playlists/7qvRAJPFslXNl2ppeeYXmh"])
            | 'Call API' >> beam.ParDo(SpotifyPipe())
            | 'Flatten JSON1' >> beam.ParDo(flatten_json())
            | 'Get Href' >> beam.ParDo(extract_track_href())
            | 'GET Songs' >> beam.ParDo(GETSongs())
            | 'Flatten JSON' >> beam.ParDo(flatten_json())
            | 'Load GDB' >> beam.ParDo(
                Neo4jLoader(
                    host=NEO4J_HOST,
                    user=NEO4J_USER,
                    password=NEO4J_PASSWORD,
                    query="CREATE (n:Track $props);"
                )
            )
        )

def load_liked_songs():
    graph = Graph(NEO4J_HOST, name=NEO4J_DATABASE, auth=(NEO4J_USER, NEO4J_PASSWORD))
    print(graph.run("Create (p:Playlist {name:'liked_songs'})").stats())
    with beam.Pipeline() as p:
        pipe = (
            p 
            | beam.Create(["https://api.spotify.com/v1/me/tracks?limit=50"])
            | 'Call API' >> beam.ParDo(SpotifyPipe())
            | 'Get Href' >> beam.ParDo(extract_href())
            | 'GET Songs' >> beam.ParDo(GETTracks())
            | 'Flatten JSON' >> beam.ParDo(flatten_json())
            | 'Load GDB' >> beam.ParDo(
                Neo4jLoader(
                    host=NEO4J_HOST,
                    user=NEO4J_USER,
                    password=NEO4J_PASSWORD,
                    query="MATCH (u:User {id: 'bubbatlc'}) MERGE (t:Track {track_id: $props['track_id']}) ON CREATE set t = $props  CREATE (u)-[:LIKED]->(t);"
                )
            )
        )



def get_artists():
    # Create apache beam class to read from neo4j
    # Pull data and get artists names/ids
    # Pull data from spotify
    # load into gdb
    pass

def create_relationsips():
    graph = Graph(NEO4J_HOST, name=NEO4J_DATABASE, auth=(NEO4J_USER, NEO4J_PASSWORD))
    print(graph.run("MATCH (p:Playlist), (u:User) CREATE (p)<-[:FOLLOWS]-(u)").stats())
    print(graph.run("MATCH (p:Playlist), (t:Track) WHERE p.href = t.playlist_href CREATE (p)<-[:ADDED_TO]-(t)").stats())
    # print(graph.run("MATCH (p:Playlist)<-[]-(t:Track) WHERE p.href = t.playlist_href CREATE (p)<-[:FOUND_IN]-(t)").stats())

    """
MATCH (t:Track)-[:FOUND_IN]->(p:Playlist)<-[:FOUND_IN]-(t2:Track)
WHERE t.track_id = t2.track_id
MERGE (t)-[r:ALSO_IN]-(t2)
SET r.playlist_name = p.name;
    """

def main() -> None:
    destroy()
    load_user()
    load_playlists()
    load_tracks()
    create_relationsips()
    load_liked_songs()
    # load_track()
    


    # load("https://api.spotify.com/v1/me/playlists", ,
    #         extra_pipes=[, beam.ParDo(extract_track_href()), beam.ParDo(GETSongs()), beam.ParDo(flatten_json())])

    # load("https://api.spotify.com/v1/playlists/7qvRAJPFslXNl2ppeeYXmh", "CREATE (n:Track $props) RETURN n",
    #         extra_pipes=[beam.ParDo(GETSongs()), beam.ParDo(flatten_json())])
    
    return None




if __name__ == '__main__':
    main()


