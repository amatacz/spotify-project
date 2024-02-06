from datetime import datetime
import json
import pandas as pd
import functions_framework
from shared.gcloud_integration import GCloudIntegration
from shared.utils import DataConfigurator


@functions_framework.http
def get_spotify_monthly_data_from_bucket(request):
    """
    This function connects to bucket and downloads newest spotify raw data.
    Returns downloaded data in JSON format.
    """
    # Connect to bucket
    gcloud_integrator = GCloudIntegration()
    gcloud_integrator.get_secret("deft-melody-404117", "spotify-app-engine-key")

    # Download data from bucket
    try:
        downloaded_data = gcloud_integrator.download_data_from_cloud_to_dict(
            "spotify_monthly_data_bucket",
            f"{datetime.today().date()}_spotify_monthly_data")
        downloaded_data_json = json.loads(downloaded_data)

        if downloaded_data_json:
            return "New spotify data has been downlaoded."

    except Exception as e:
        print(f"Error when downloading data from bucket: {e}")
        return None


@functions_framework.http
def transform_spotify_data(data):
    """
    This function takes newest spotify data downloaded from bucket
    and transforms it to DataFrames.
    DataFrames are uploaded to tables in BigQuery.
    """
    try:
        artists_dict = data['artists_of_the_month']
        artists_entries = [
            {
                "artist": artist["name"],
                "artist_link": artist["external_urls"]["spotify"],
                "genres": str(artist["genres"]),
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
    except Exception as e:
        print(f"Error while artist data transformation:\n {e}")

    try:
        gcloud_integrator = GCloudIntegration()
        gcloud_integrator.get_secret("deft-melody-404117",
                                     "spotify-app-engine-key")
    except Exception as e:
        print(f"Error while creating BQ client:\n {e}")

    try:
        data_configurator = DataConfigurator()
        artists_table_schema = data_configurator.load_artists_schema_from_yaml()
        gcloud_integrator._insert_data_from_df_to_bigquery_table(
            artists_df,
            "spotify_dataset",
            "spotify_monthly_artists",
            schema=artists_table_schema)
    except Exception as e:
        print(f"Error while uploading artists data to table:\n {e}")

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

    except Exception as e:
        print(f"Error while tracks data transformation:\n {e}")
        return None

    try:
        gcloud_integrator = GCloudIntegration()
        gcloud_integrator.get_secret("deft-melody-404117", "spotify-app-engine-key")
        data_configurator = DataConfigurator()
        tracks_table_schema = data_configurator.load_tracks_schema_from_yaml()
        gcloud_integrator._insert_data_from_df_to_bigquery_table(
            tracks_df,
            "spotify_dataset",
            "spotify_monthly_tracks",
            schema=tracks_table_schema)
    except Exception as e:
        print(f"Error while uploading tracks data to table:\n {e}")
        return None

    return "DATA SUCCESFULLY TRANSFORMED AND UPLOADED"
