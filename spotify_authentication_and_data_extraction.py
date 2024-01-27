import os
import requests
import urllib.parse
from dotenv import load_dotenv
from flask import Flask, request, url_for, session, redirect
import json
from datetime import datetime


# # Load environmental variables
# load_dotenv()

CLIENT_ID = os.environ.get("CLIENT_ID", "NIE")
CLIENT_SECRET = os.environ.get("CLIENT_SECRET", "NIE")
REDIRECT_URI = os.environ.get("REDIRECT_URI", "NIE")
SCOPE = os.environ.get("SCOPE", "NIE")

# Set URL variables
AUTH_URL = "https://accounts.spotify.com/authorize"
TOKEN_URL = "https://accounts.spotify.com/api/token"
API_BASE_URL = "https://api.spotify.com/v1/"

# Flask is used because of OAuth, it allows us store token in session and refresh it

# Create Flask app
app = Flask(__name__)

# set the name of the session cookie
app.config["SESSION_COOKIE_NAME"] = "Spotify Cookie"

# set a random secret key to sign the cookie
app.secret_key = "qwertyuiopasdfghjklzxcvbnm"


# This function will be called when home address is reached.
@app.route("/")
def login():
    # Set parameters for authentication url
    params = {
        "client_id": CLIENT_ID,
        "response_type": "code",
        "scope": SCOPE,
        "redirect_uri": REDIRECT_URI,
    }
    # Create auth_url for create authorization code
    auth_url = f"{AUTH_URL}?{urllib.parse.urlencode(params)}"
    print(f"Logged in.\n {auth_url}")
    return redirect(auth_url)


@app.route("/callback")
def callback():
    # Check if there are any errors
    if "error" in request.args:
        return {"error": request.args["error"]}

    # Check if autorization code is in request if yes, request for token
    if "code" in request.args:
        req_body = {
            "code": request.args["code"],
            "grant_type": "authorization_code",
            "redirect_uri": REDIRECT_URI,
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET
        }
    # Request for token using TOKEN_URL and requred parameters
    response = requests.post(TOKEN_URL, data=req_body)
    token_info = response.json()

    # Store access details in session
    session["access_token"] = token_info["access_token"]
    session["refresh_token"] = token_info["refresh_token"]
    session["expires_at"] = datetime.now().timestamp() + token_info["expires_in"]
    print("I GOT A TOKEN")

    return redirect("/get-data")



@app.route("/top-artists")
def get_artists_of_the_month():
    # Check if access token is in the session. If not ask user for login.
    if "access_token" not in session:
        return redirect("/")

    # Check if access token expired. If yes, refresh it.
    if datetime.now().timestamp() > session["expires_at"]:
        # Store current url in session. Allows to get back here after token is refreshed.
        session["url"] = url_for("get_artists_of_the_month")
        print("TOKEN EXPIRED. REFRESHING")
        return redirect("/refresh-token")

    # Set headers with valid token
    headers = {
        "Authorization": f"Bearer {session["access_token"]}"
    }
    # Request my top 20 artists of the month (short_term = 4 weeks).
    response = requests.get(API_BASE_URL + "me/top/artists?time_range=short_term",
                            headers=headers)

    # Store requested artists as json
    artists_of_the_month = response.json()
    return artists_of_the_month


@app.route("/top-tracks")
def get_tracks_of_the_month():
    # Check if access token is in session. If not, ask user for login.
    if "access_token" not in session:
        redirect("/")
    # Check is token expired. If yes, refresh it.
    if datetime.now().timestamp() > session["expires_at"]:
        print("TOKEN EXPIRED. REFRESHING")
        # Save current url in session. Allows to get back here after token is refreshed.
        session["url"] = url_for("get_tracks_of_the_month")
        return redirect("/refresh-token")

    # Set headers with valid token
    headers = {
        "Authorization": f"Bearer {session["access_token"]}"
    }
    # Request my top 50 tracks of the month (short_term=4 weeks).
    response = requests.get(API_BASE_URL + "me/top/tracks?time_range=short_term&limit=50", headers=headers)

    # Store requested tracks as json.
    tracks_of_the_month = response.json()
    return tracks_of_the_month


@app.route("/refresh-token")
def refresh_token():
    # Check if "refresh_token" attr is in session. If not, ask user to log in.
    if "refresh_token" not in session:
        return redirect("/")

    # Check if token expired. If yes, refresh it.
    if datetime.now().timestamp() > session["expires_at"]:
        req_body = {
            "grant_type": "refresh_token",
            "refresh_token": session["refresh_token"],
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET
        }

        response = requests.post(TOKEN_URL, data=req_body)
        new_token_info = response.json()

        session["access_token"] = new_token_info["access_token"]
        session["expires_at"] = datetime.now().timestamp() + new_token_info["expires_in"]

        # Check if "url" attr is in session. If yes, redirect to this url. Pop() removes it from the list, to clear this attribute.
        if "url" in session:
            return redirect(session.pop("url", None))

    return redirect("/playlists")


@app.route("/get-data")
def main():
    """
    This function goes through all get functions to retrieve data for the last month
    and saves it in dictionary.
    Returns dictionary with all data.
    """
    data = {}
    tracks_of_the_month = get_tracks_of_the_month()
    artists_of_the_month = get_artists_of_the_month()

    data['tracks_of_the_month'] = tracks_of_the_month
    data['artists_of_the_month'] = artists_of_the_month

    # with open('data.json', 'w') as f:
    #     json.dump(data, f)

    return data

# DATA NOT RELEVANT - WILL BE DELETED LATER???
# @app.route("/playlists")
# def get_playlists():
#     # Check if access token is in the session. If not ask user for login.
#     if "access_token" not in session:
#         return redirect("/")

#     # Check if access token expired if yes, refresh it
#     if datetime.now().timestamp() > session["expires_at"]:
#         # Store current url in session. Allows to get back here after token refresh.        
#         session["url"] = url_for("get_playlists")
#         print("TOKEN EXPIRED. REFRESHING")
#         return redirect("/refresh-token")

#     # Set headers with valid token
#     headers = {
#         "Authorization": f"Bearer {session["access_token"]}"
#     }
#     # Request my playlists.
#     response = requests.get(API_BASE_URL + "me/playlists", headers=headers)

#     # Store requested playlists as json
#     playlists = json.dumps(response.json())
#     return playlists




# @app.route("/redirect")
# def redirect_page():
#     session.clear()
#     code = request.args.get("code")
#     token_info = create_spotify_oauth().get_access_token(code)
#     session[TOKEN_INFO] = token_info
#     return redirect(url_for("save_discover_weekly", _external=True))


# @app.route("/saveDiscoverWeekly")
# def save_discover_weekly():
#     try:
#         token_info = get_token()
#     except Exception as e:
#         print("User not logged in")
#         return redirect("/")


# def get_token():
#     token_info = session.get(TOKEN_INFO, None)
#     if not token_info:
#         redirect(url_for("login", _external=False))

#     now = int(time.time())
#     is_expired = token_info["expires_at"] - now < 60

#     if (is_expired):
#         spotify_oauth = create_spotify_oauth()
#         token_info = spotify_oauth.refresh_access_token(token_info["refresh_token"])

#     return token_info


# def create_spotify_oauth():
#     return SpotifyOAuth(
#         client_id=CLIENT_ID,
#         client_secret=CLIENT_SECRET,
#         redirect_uri=REDIRECT_URI,
#         scope=SCOPE)


# app.run(debug=True)
