import spotify_authentication_and_data_extraction
import functions_framework


@functions_framework.http
def main():
    app = spotify_authentication_and_data_extraction.app

    if __name__ == "__main__":
        app.run(host="0.0.0.0", port=5000, debug=True, use_reloader=False)
