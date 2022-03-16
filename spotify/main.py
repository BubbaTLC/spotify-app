
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

from beam_functions import flatten_json, extract_items

from loader.main import Neo4jLoader, destroy



class SpotifyPipe(beam.DoFn):
    def __init__(self, *unused_args, **unused_kwargs):
        super().__init__(*unused_args, **unused_kwargs)
        self.scope = "user-library-read"
        self.headers = []
        self.access_token = SpotifyOAuth(scope="user-library-read", client_id=SPOTIPY_CLIENT_ID, client_secret=SPOTIPY_CLIENT_SECRET, redirect_uri=SPOTIPY_REDIRECT_URI).get_access_token()


    def process(self, url):
        try:
            res = req.get(url, headers={'Authorization': f"Bearer {self.access_token['access_token']}"})
        except Exception as e:
            print(e)

        yield res.json()


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

def load(url, query, extra_pipes=[]):
    with beam.Pipeline() as p:
        pipe = (
            p 
            | beam.Create([url])
            | 'Call API' >> beam.ParDo(SpotifyPipe())
        )

        for x in extra_pipes:
            pipe = (pipe | x)

        pipe = (
            pipe
            | "Write Neo4j" >> beam.ParDo(
                Neo4jLoader(
                    host=NEO4J_HOST,
                    user=NEO4J_USER,
                    password=NEO4J_PASSWORD,
                    query=query
                )
            )
        )


def main() -> None:
    destroy()
    load("https://api.spotify.com/v1/me", "CREATE (n:User $props) RETURN n", 
            extra_pipes=[beam.ParDo(flatten_json())])
    
    load("https://api.spotify.com/v1/me/playlists", "CREATE (n:Playlist $props) RETURN n",
            extra_pipes=[beam.ParDo(extract_items())])
    return None




if __name__ == '__main__':
    main()


