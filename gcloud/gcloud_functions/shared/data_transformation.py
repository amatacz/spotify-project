import pandas as pd
from datetime import datetime
import json
import functions_framework
from shared.gcloud_integration import GCloudIntegration


class DataTransformator:
    def __init__(self) -> None:
        pass

    def artist_data_transform(self, msg):
        try:
            artists_entries = [
                {
                    "artist": artist["name"],
                    "artist_link": artist["external_urls"]["spotify"],
                    "genres": str(artist["genres"]),
                    "popularity": artist["popularity"],
                    "timestamp": datetime.today().date()
                }
                for artist in msg["items"]
                ]
            artists_dict_flattened = {index: entry for index, entry
                                      in enumerate(artists_entries)}

            artists_df = pd.DataFrame.from_dict(artists_dict_flattened,
                                                orient="index")
            artists_df.sort_values("popularity", ascending=False, inplace=True)
            artists_df.reset_index(drop=True, inplace=True)
        except Exception as e:
            print(f"Error with artist data transformation\n {e}.")
            return None
        return artists_df

    def tracks_data_transform(self, msg):
        try:
            tracks_entries = [
                {
                    "artist": track["artists"][0]["name"],
                    "artist_link": track["artists"][0]["external_urls"]["spotify"],
                    "track_title": track["name"],
                    "track_link": track["external_urls"]["spotify"],
                    "album": track["album"]["name"],
                    "album_type": track["album"]["album_type"],
                    "release_date": track["album"]["release_date"],
                    "popularity": track["popularity"],
                    "timestamp": datetime.today().date()
                    }
                for track in msg['items']
                ]
            tracks_dict_flattened = {index: entry for index, entry
                                     in enumerate(tracks_entries)}
            tracks_df = pd.DataFrame.from_dict(tracks_dict_flattened,
                                               orient="index")
            tracks_df.sort_values("popularity", ascending=False, inplace=True)
            tracks_df.reset_index(drop=True, inplace=True)
        except Exception as e:
            print(f"Error with tracks data transformation\n {e}")
            return None
        return tracks_df
