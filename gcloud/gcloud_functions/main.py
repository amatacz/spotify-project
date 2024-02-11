from datetime import datetime
import pandas as pd
import json
import functions_framework
import base64
import ast

from shared.gcloud_integration import GCloudIntegration
from shared.utils import DataConfigurator
from shared.data_transformation import DataTransformator


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
            print(f"{datetime.today().date()} - Spotify data has been downloaded.")
            return str(downloaded_data_json)

    except Exception as e:
        print(f"Error when downloading data from bucket: {e}")
        return None


@functions_framework.http
def transform_spotify_data(request, context=None):
    """
    This function takes newest spotify data downloaded from bucket
    and transforms it to DataFrames.
    DataFrames are uploaded to tables in BigQuery.
    """
    encoded_inner_json = request.get_json()["data"]["data"]
    decoded_inner_json = base64.b64decode(encoded_inner_json).decode("utf-8")

    dict_data = ast.literal_eval(decoded_inner_json)
    json_data = json.dumps(dict_data)
    spotify_dataframe = json.loads(json_data)

    artist_dict = spotify_dataframe["artists_of_the_month"]
    tracks_dict = spotify_dataframe["tracks_of_the_month"]

    DataTransformatorObject = DataTransformator()

    artist_data_frame = DataTransformatorObject.artist_data_transform(artist_dict)
    tracks_data_frame = DataTransformatorObject.tracks_data_transform(tracks_dict)

    print(artist_data_frame.head())
    print(tracks_data_frame.head())
