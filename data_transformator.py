import json
import pandas as pd


with open("./data.json") as f:
    data = json.load(f)

tracks_dict = data['tracks_of_the_month']

# for key in tracks_dict.keys():
#     print(key)

"tracks_dict KEYS: ['items', 'total', 'limit', 'offset', 'href', 'next', 'previous']"

"""ITEMS[0] keys: dict_keys(['album', 'artists', 'available_markets', 'disc_number', 'duration_ms',
'explicit', 'external_ids', 'external_urls', 'href', 'id', 'is_local', 'name',
'popularity', 'preview_url', 'track_number', 'type', 'uri'])"""


for item in tracks_dict['items']:
    for key, value in item.items():
        print(key, value)
