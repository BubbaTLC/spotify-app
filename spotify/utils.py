




def flatten_dict( d: dict, parent_key: str = '', sep: str ='_') -> dict:
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
