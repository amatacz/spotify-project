import spotify_authentication_and_data_extraction


app = spotify_authentication_and_data_extraction.app

if __name__ == "__main__":
    app.run(debug=False, use_reloader=False)
