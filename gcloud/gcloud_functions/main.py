from datetime import datetime
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
        else:
            print("Todays data unavailable.")
            return None

    except Exception as e:
        print(f"Error when downloading data from bucket: {e}")
        return None


@functions_framework.http
def transform_spotify_data(request):
    """
    This function takes newest spotify data downloaded from bucket
    and transforms it to DataFrames.
    DataFrames are uploaded to tables in BigQuery.
    """

    # Extract the encoded inner JSON string
    encoded_inner_json = request.get_json()["data"]["data"]
    # Decode the inner JSON string
    decoded_inner_json = base64.b64decode(encoded_inner_json).decode("utf-8")
    # Parse the decoded JSON string
    dict_data = ast.literal_eval(decoded_inner_json)
    json_data = json.dumps(dict_data)
    spotify_dataframe = json.loads(json_data)

    # Create new dictionaries for artists and tracks
    artist_dict = spotify_dataframe["artists_of_the_month"]
    tracks_dict = spotify_dataframe["tracks_of_the_month"]

    # Create object to transform data
    DataTransformatorObject = DataTransformator()
    artist_data_frame = DataTransformatorObject.artist_data_transform(artist_dict)
    tracks_data_frame = DataTransformatorObject.tracks_data_transform(tracks_dict)

    # Connect to bucket
    gcloud_integrator = GCloudIntegration()
    gcloud_integrator.get_secret("deft-melody-404117",
                                 "spotify-app-engine-key")

    # Load table schemas
    DataConfiguratorObject = DataConfigurator()
    artists_table_schema = DataConfiguratorObject.load_artists_schema_from_yaml()
    tracks_table_schema = DataConfiguratorObject.load_tracks_schema_from_yaml()

    #  TU OPCJA Z ZAPYTANIEM DO TABELI, ZEBY SPRAWDZIĆ, CZY DANE Z DZISIAJ JUŻ ISTNIEJA
    # # Verify if todays data is already present in the table. If not send new data to BQ.
    # if gcloud_integrator.get_data_from_bigquery_table(dataset_name="spotify_dataset",
    #                                                   table_name="spotify_monthly_artists",
    #                                                   condition=f"WHERE ranking_date = {datetime.today().date()}"):
    #     print("Todays artist data already present in the table!")
    # else:
    #     gcloud_integrator._insert_data_from_df_to_bigquery_table(dataframe=artist_data_frame,
    #                                                              dataset_name="spotify_dataset",
    #                                                              table_name="spotify_monthly_artists2",
    #                                                              schema=artists_table_schema)

    # # Verify if todays data is already present in the table. If not send new data to BQ.
    # if gcloud_integrator.get_data_from_bigquery_table(dataset_name="spotify_dataaset",
    #                                                   table_name="spotify_monthly_tracks",
    #                                                   condition=f"WHERE ranking_date = {datetime.today().date()}"):
    #     print("Todays tracks data already present in the table!")
    # else:
    #     gcloud_integrator._insert_data_from_df_to_bigquery_table(dataframe=tracks_data_frame,
    #                                                              dataset_name="spotify_dataset",
    #                                                              table_name="spotify_monthly_tracks",
    #                                                              schema=tracks_table_schema)
    # return "DONE"

    gcloud_integrator._insert_data_from_df_to_bigquery_table(dataframe=artist_data_frame,
                                                             dataset_name="spotify_dataset",
                                                             table_name="spotify_monthly_artists2",
                                                             schema=artists_table_schema)
    gcloud_integrator._insert_data_from_df_to_bigquery_table(dataframe=tracks_data_frame,
                                                             dataset_name="spotify_dataset",
                                                             table_name="spotify_monthly_tracks",
                                                             schema=tracks_table_schema)

    return "done"
