from dotenv import load_dotenv
import os
import sys
from os.path import dirname, join

load_dotenv(join(dirname(__file__), '.env'))

SPOTIPY_CLIENT_ID = os.getenv("SPOTIPY_CLIENT_ID")
SPOTIPY_CLIENT_SECRET = os.getenv("SPOTIPY_CLIENT_SECRET")
SPOTIPY_REDIRECT_URI = os.getenv("SPOTIPY_REDIRECT_URI")

NEO4J_HOST = os.getenv("NEO4J_HOST")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")