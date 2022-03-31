import imp
import apache_beam as beam
import requests as req
from spotipy.oauth2 import SpotifyOAuth

from utils import flatten_dict

from config import (
    SPOTIPY_CLIENT_ID, 
    SPOTIPY_CLIENT_SECRET, 
    SPOTIPY_REDIRECT_URI,
    NEO4J_DATABASE,
    NEO4J_HOST,
    NEO4J_PASSWORD,
    NEO4J_USER
)

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


class GETTracks(beam.DoFn):
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
                self.batch.append(x)
            if not self.response.get('next'):
                return
            else:
                self.fetch(self.response['next'])
        except Exception as e:
            print(e)


class flatten_json(beam.DoFn):
    def process(self, element):
        element = flatten_dict(element)
        return [element]


class extract_items(beam.DoFn):
    def process(self, element, *args, **kwargs):
        ret_val = []
        for x in element['items']:
            ret_val.append(flatten_dict(x))
        
        return ret_val


class extract_track_href(beam.DoFn):
    def process(self, element):
        ret_val = []
        ret_val.append(element['tracks_href'])
        return ret_val


class extract_href(beam.DoFn):
    def process(self, element):
        ret_val = []
        ret_val.append(element['href'])
        return ret_val


