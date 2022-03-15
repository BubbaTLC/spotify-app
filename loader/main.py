# %%
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from py2neo import Graph

from config import SPOTIPY_CLIENT_ID, SPOTIPY_CLIENT_SECRET, SPOTIPY_REDIRECT_URI
scope = "user-library-read"

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


# %%
def flatten_dict(d: dict, parent_key: str = '', sep: str ='_') -> dict:
    items = []
    if isinstance(d, dict):
        for k, v in dict(d).items():
            new_key = parent_key + sep + k if parent_key else k
            if isinstance(v, dict):
                items.extend(flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                for x in v:
                    items.extend(flatten_dict(x, new_key, sep=sep).items())
            elif v is None:
                del d[k]
            else:
                items.append((new_key, v))
    return dict(items)
# %%

def load_user(graph:Graph, spotify:spotipy.Spotify) -> None:
    user = spotify.current_user()
    user = flatten_dict(user)
    res = graph.run("CREATE (n:User $props) RETURN n", parameters={'props': user})
    print(res.stats())

# %%
def main() -> None:
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope, client_id=SPOTIPY_CLIENT_ID, client_secret=SPOTIPY_CLIENT_SECRET, redirect_uri=SPOTIPY_REDIRECT_URI))
    g = Graph("http://localhost:7474", auth=('neo4j', 'password'))

    load_user(g, sp)

    return None
# %%

if __name__ == '__main__':
    main()

# %%
