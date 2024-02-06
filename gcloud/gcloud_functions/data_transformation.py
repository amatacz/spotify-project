import pandas as pd
from datetime import datetime
import json
import functions_framework
from shared.gcloud_integration import GCloudIntegration


@functions_framework.http
def transform_spotify_data():
    with open("../../data.json") as f:
        data = json.loads(f)
    try:
        artists_dict = data['artists_of_the_month']
        artists_entries = [
            {
                "artist": artist["name"],
                "artist_link": artist["external_urls"]["spotify"],
                "genres": artist["genres"],
                "popularity": artist["popularity"],
                "timestamp": datetime.today()
            }
            for artist in artists_dict["items"]
            ]
        artists_dict_flattened = {index: entry for index, entry
                                in enumerate(artists_entries)}

        artists_df = pd.DataFrame.from_dict(artists_dict_flattened, orient="index")
        artists_df.sort_values("popularity", ascending=False, inplace=True)
        artists_df.reset_index(drop=True, inplace=True)
        print(artists_df)
    except Exception as e:
        print(f"Error while artist data transformation:\n {e}")

    # try:
    #     gcloud_integrator = GCloudIntegration()
    #     gcloud_integrator.get_secret("deft-melody-404117",
    #                                  "spotify-app-engine-key")
    #     gcloud_integrator._insert_data_from_df_to_bigquery_table(
    #         artists_df,
    #         "spotify_dataset",
    #         "spotify_monthly_artists")

    # except Exception as e:
    #     print(f"Error while uploading artist data to BQ:\n {e}")

    try:
        tracks_dict = data['tracks_of_the_month']
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
                "timestamp": datetime.today()
                }
            for track in tracks_dict['items']
            ]
        tracks_dict_flattened = {index: entry for index, entry in enumerate(tracks_entries)}
        tracks_df = pd.DataFrame.from_dict(tracks_dict_flattened, orient="index")
        tracks_df.sort_values("popularity", ascending=False, inplace=True)
        tracks_df.reset_index(drop=True, inplace=True)

    except Exception:
        return None
