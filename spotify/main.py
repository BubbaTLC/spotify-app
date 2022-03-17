
from http import client
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from py2neo import Graph
import apache_beam as beam
import requests as req
import sys

import json

from config import (
    SPOTIPY_CLIENT_ID, 
    SPOTIPY_CLIENT_SECRET, 
    SPOTIPY_REDIRECT_URI,
    NEO4J_DATABASE,
    NEO4J_HOST,
    NEO4J_PASSWORD,
    NEO4J_USER
)

from beam_functions import flatten_json, extract_items, extract_track_href

from loader.main import Neo4jLoader, destroy



class SpotifyPipe(beam.DoFn):
    def __init__(self, *unused_args, **unused_kwargs):
        super().__init__(*unused_args, **unused_kwargs)
        self.scope = "user-library-read"
        self.headers = []
        self.access_token = SpotifyOAuth(scope="user-library-read", client_id=SPOTIPY_CLIENT_ID, client_secret=SPOTIPY_CLIENT_SECRET, redirect_uri=SPOTIPY_REDIRECT_URI).get_cached_token()

    def process(self, url):
        try:
            res = req.get(url, headers={'Authorization': f"Bearer {self.access_token['access_token']}"})
        except Exception as e:
            print(e)

        yield res.json()

class GETSongs(beam.DoFn):
    def __init__(self, *unused_args, **unused_kwargs):
        super().__init__(*unused_args, **unused_kwargs)
        self.batch = []
        self.response = {}
        self.access_token = SpotifyOAuth(scope="user-library-read", client_id=SPOTIPY_CLIENT_ID, client_secret=SPOTIPY_CLIENT_SECRET, redirect_uri=SPOTIPY_REDIRECT_URI).get_cached_token()

    def finish_bundle(self):
        self.response = {}
        self.batch = []

    def start_bundle(self):
        self.response = {}
        self.batch = []

    def process(self, element, *args, **kwargs):
        self.fetch(element)
        ret_val = self.batch.copy()
        self.batch = []
        self.response = []
        return ret_val

    def fetch(self, element):
        try:
            self.response = req.get(element, headers={'Authorization': f"Bearer {self.access_token['access_token']}"}).json()
            for x in self.response['items']:
                x['playlist_href'] = "/".join(element.split('/')[:-1])
                self.batch.append(x)
            if not self.response.get('next'):
                return
            else:
                self.fetch(self.response['next'])
        except Exception as e:
            print(e)


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
                    query="CREATE (n:Track $props);"
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

def create_relationsips():
    graph = Graph(NEO4J_HOST, name=NEO4J_DATABASE, auth=(NEO4J_USER, NEO4J_PASSWORD))
    print(graph.run("MATCH (p:Playlist), (u:User) CREATE (p)<-[:FOLLOWS]-(u)").stats())
    print(graph.run("MATCH (p:Playlist), (t:Track) WHERE p.href = t.playlist_href CREATE (p)<-[:FOUND_IN]-(t)").stats())

def main() -> None:
    destroy()
    load_user()
    load_playlists()
    load_tracks()
    create_relationsips()
    # load_track()
    


    # load("https://api.spotify.com/v1/me/playlists", ,
    #         extra_pipes=[, beam.ParDo(extract_track_href()), beam.ParDo(GETSongs()), beam.ParDo(flatten_json())])

    # load("https://api.spotify.com/v1/playlists/7qvRAJPFslXNl2ppeeYXmh", "CREATE (n:Track $props) RETURN n",
    #         extra_pipes=[beam.ParDo(GETSongs()), beam.ParDo(flatten_json())])
    
    return None




if __name__ == '__main__':
    main()


