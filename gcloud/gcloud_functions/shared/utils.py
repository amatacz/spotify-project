import yaml
import os
import datetime


class DataConfigurator:

    def __init__(self) -> None:
        self.artists_yaml = os.path.join(os.path.dirname(__file__), 'config/spotify_monthly_artists_schema.yaml') 
        self.tracks_yaml = os.path.join(os.path.dirname(__file__), 'config/spotify_monthly_tracks_schema.yaml') 

    def load_artists_schema_from_yaml(self):
        ''' Extract data about schema for artists table '''
        try:
            with open(self.artists_yaml, 'r') as file:
                data = yaml.safe_load(file)
            return data.get("fields", [])
        except FileNotFoundError:
            print(f"Error: File {self.artists_yaml} not found.")
        except PermissionError:
            print(f"Error: No permission to read the file {self.artists_yaml}.")
        except yaml.YAMLError as exc:
            print(f"Error parsing the YAML file: {exc}.")
        return []

    def load_tracks_schema_from_yaml(self):
        ''' Extract data about schema for tracks table'''
        try:
            with open(self.tracks_yaml, 'r') as file:
                data = yaml.safe_load(file)
            return data.get("fields", [])
        except FileNotFoundError:
            print(f"Error: File {self.tracks_yaml} not found.")
        except PermissionError:
            print(f"Error: No permission to read the file {self.tracks_yaml}.")
        except yaml.YAMLError as exc:
            print(f"Error parsing the YAML file: {exc}.")
        return []
