import json
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import csv
import matplotlib.pyplot as plt
import os
import sqlite3
from typing import List, Dict, Any, Optional
from datetime import datetime
import requests
from collections import Counter

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "media_data.db")

#--------------------SPOTIPY API (Claire Fuller)-------------------------
#----------Calling the API----------
CLIENT_ID = 'be1ea16df5c24ab195ff21e6c8a82cd1'
CLIENT_SECRET = 'fe32b6a7ae92441bb57db162f56e75a3'
# DB_PATH = "/Users/clairefuller/Desktop/umich/Si201/SI201_FinalProject_RCCG/projects/fall25-finalproject/media_data.db"
#create an authorized variable to use for future API calls
sp = spotipy.Spotify(
    auth_manager=SpotifyClientCredentials(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET
    )
)

#----------Getting Data----------
def get_spotify_data(query="track:a", limit=25, offset=0):
    print(f"call to get sptoify data: query = {query} limit = {limit} offset = {offset}")
    # Search for tracks matching query
    results = sp.search(q=query, type="track", limit=limit, offset=offset)
    print(f"spotifyapi returned {len(results.get('tracks', {}).get('items', []))}")
    tracks = []
    for item in results.get('tracks', {}).get('items', []):
        track = {
        "id": item.get("id"),  # Spotify track ID (string)
        "name": item.get("name"),
        "album": item.get("album", {}).get("name"),
        "artist": item.get("artists", [{}])[0].get("name") if item.get("artists") else None,
        "artist_id": item.get("artists", [{}])[0].get("id") if item.get("artists") else None,
        "popularity": item.get("popularity"),
        "release_date": item.get("album", {}).get("release_date")
        }
        tracks.append(track)
    return tracks

#----------Find Genres----------
def enrich_tracks_with_genres(tracks): #find genre for spotipy through artist id
    for t in tracks:
        artist_id = t.get("artist_id")
        if artist_id:
            artist_data = sp.artist(artist_id)
            genres = artist_data.get("genres", [])
            t["genres"] = genres
        else:
            t["genres"] = []
    return tracks

#----------Initialize ALL Table Sets----------
def init_database(db_name):
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    # Enable foreign key constraints (in case we use them)
    cur.execute("PRAGMA foreign_keys = ON;")
    # Create Movies table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Movies (
            id INTEGER PRIMARY KEY,
            title TEXT,
            release_date TEXT,
            popularity REAL,
            revenue INTEGER,
            avg_rating REAL,
            genre_ids TEXT
        )
    """)        
    # Create Songs table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Songs (
            id TEXT PRIMARY KEY,
            title TEXT,
            artist TEXT,
            album TEXT,
            popularity INTEGER,
            release_date TEXT

        )
    """)
    # Create Shows table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Shows (
            id INTEGER PRIMARY KEY,
            name TEXT,
            premiere_date TEXT,
            avg_rating REAL,
            genres TEXT,
            weight INTEGER
        );
    """)
    #genres for songs
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Genres (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            song_id TEXT,
            genre TEXT,
            FOREIGN KEY(song_id) REFERENCES Songs(id)
        );
    """)

    conn.commit()
    return conn

#----------Insert Songs Into Database----------
def insert_songs(conn, songs):
    cur = conn.cursor()
    for s in songs:
        cur.execute("""
            INSERT OR IGNORE INTO Songs (id, title, artist, album, popularity, release_date)
            VALUES (?, ?, ?, ?, ?, ?);
        """, (
            s.get('id'),
            s.get('name'),
            s.get('artist'),
            s.get('album'),
            s.get('popularity'),
            s.get('release_date')
        ))

        genres = s.get('genres', []) #insert genres seperately
        for g in genres:
            cur.execute("""
                INSERT OR IGNORE INTO Genres (song_id, genre)
                VALUES (?, ?)
            """, (s.get('id'), g))

    conn.commit()

#----------Spotipy Fetching Helpers----------
def fetch_and_store_spotify_tracks(conn):
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM Songs")
    current = cur.fetchone()[0]

    if current >= 100:
        print("Reached 100 songs. Stopping.")
        return

    offset = current
    print("Fetching offset =", offset)

    # Use a stable huge-query search
    tracks = get_spotify_data(query='e', limit=25, offset=offset)

    if len(tracks) == 0:
        print("NO TRACKS RETURNED — Query too restrictive.")
        return

    tracks = enrich_tracks_with_genres(tracks)
    insert_songs(conn, tracks)

def my_spotipy_query(): # for debug
    tracks = []
    for index in range(4):
        local_tracks = get_spotify_data(query="year:2023", limit=25, offset=(index * 25))
        tracks.extend(local_tracks)
    print(len(tracks))
    #print(tracks)
        
    for index in range(4):
        local_tracks = get_spotify_data(query="year:2024", limit=25, offset=(index * 25))
        tracks.extend(local_tracks)
    print(len(tracks))

#----------Calculations----------
def calculate_spotify_genre_popularity(conn, output_file="spotify_genre_popularity.txt"):
    cur = conn.cursor()
    query = """
        SELECT g.genre, AVG(s.popularity) AS avg_popularity
        FROM Genres g
        JOIN Songs s ON g.song_id = s.id
        GROUP BY g.genre
        ORDER BY avg_popularity DESC;
    """

    rows = cur.execute(query).fetchall()

    #results to a text file
    with open(output_file, "w") as f:
        for genre, avg_pop in rows:
            f.write(f"{genre}: {avg_pop:.2f}\n")

    print(f"Written genre popularity data to {output_file}")

    return [{"genre": genre, "avg_popularity": avg_pop} for genre, avg_pop in rows]

#-----------Visualization----------
def visualize_genre_popularity(data):
    genres = [item["genre"] for item in data]
    avg_popularity = [item["avg_popularity"] for item in data]

    plt.figure(figsize=(12, 7))
    colors = ["skyblue", "magenta", "lightgreen", "violet", "pink", "turquoise"]
    plt.barh(genres, avg_popularity, color=colors[:len(genres)])

    plt.xlabel("Average Popularity")
    plt.ylabel("Genre")
    plt.title("Average Spotify Popularity by Genre")


    plt.tight_layout()

    plt.show()





#--------------------TVMAZE API (Willow Traylor)-------------------------
#----------Calling the API and Fetching Data----------
def get_tvmaze_data(page=0):
    url = f"https://api.tvmaze.com/shows?page={page}"
    response = requests.get(url)
    # If the page is out of range, TVMaze returns 404. Handle that:
    if response.status_code != 200:
        return []  # no data (page might not exist)
    shows_data = response.json()
    shows = []
    for item in shows_data:
        show = {
        "id": item.get("id"),
        "name": item.get("name"),
        "premiere_date": item.get("premiered"),
        "rating": item.get("rating", {}).get("average"),
        "genres": item.get("genres", []),
        "weight": item.get("weight")
        }
        shows.append(show)
    return shows

#----------Connect to DB and Add Show Data----------
def insert_shows(conn, shows):
    cur = conn.cursor()
    for tv in shows:
        cur.execute("""
        INSERT OR IGNORE INTO Shows (id, name, premiere_date, avg_rating, genres, weight)
        VALUES (?, ?, ?, ?, ?, ?);
        """, (
        tv.get('id'),
        tv.get('name'),
        tv.get('premiere_date'),
        tv.get('rating'),
        ", ".join(tv.get('genres', [])) if tv.get('genres') else None,
        tv.get('weight')
        ))
    conn.commit()





#--------------------TMDB API (Anna Kerhoulas)-------------------------
#----------Initilizing API Key and Base Url----------

TMDB_API_KEY = "6b8a91e2db2dc97ffc2363b4dc8e6298"
TMDB_BASE_URL = "https://api.themoviedb.org/3/discover/movie"

#----------Change Genre IDs to Genre Names----------
genre_mapping = {
    10759: "Action & Adventure",
    16: "Animation",
    35: "Comedy",
    80: "Crime",
    99: "Documentary",
    18: "Drama",
    10751: "Family",
    10762: "Kids",
    9648: "Mystery",
    10763: "News",
    10764: "Reality",
    10765: "Sci-Fi & Fantasy",
    10766: "Soap",
    10767: "Talk",
    10768: "War & Politics",
    37: "Western",
    -1: "Brainsuck"
}
def get_genre_names(genre_ids):
    genres = []
    for g in genre_ids:
        if g in genre_mapping:
            genres.append(genre_mapping[g])
        else:
            genres.append("Unknown")
    return genres

#----------Get Data----------
def get_tmdb_data(api_key: str, page_number: int = 1):
    """
    Fetch one page (20 results) of popular movies.
    """
    params = {
        "api_key": api_key,
        "sort_by": "popularity.desc",
        "page": page_number,
        "language": "en-US"
    }
    response = requests.get(TMDB_BASE_URL, params=params)
    response.raise_for_status()
    data = response.json()

    movies = []
    for item in data.get("results", []):
        movie = {
            "id": item.get("id"),
            "title": item.get("title"),
            "release_date": item.get("release_date"),
            "popularity": item.get("popularity"),
            "revenue": 0,  # this endpoint doesn't give revenue
            "avg_rating": item.get("vote_average"),
            "genre_ids": json.dumps(item.get("genre_ids", []))
        }
        movies.append(movie)

    return movies

#----------Connect to Database and Store Data----------
def store_movies_in_db(movie_list, conn):
    cur = conn.cursor()
    for m in movie_list:
        cur.execute(
            """
            INSERT OR IGNORE INTO Movies
            (id, title, release_date, popularity, revenue, avg_rating, genre_ids)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                m.get("id"),
                m.get("title"),
                m.get("release_date"),
                m.get("popularity"),
                m.get("revenue"),
                m.get("avg_rating"),
                m.get("genre_ids"),
            )
        )
    conn.commit()

def fetch_and_store_tmdb_movies(conn, api_key: str = TMDB_API_KEY):
    """
    Fetch 20 movies at a time until we have at least 100 movies.
    """
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM Movies")
    current = cur.fetchone()[0]

    if current >= 100:
        print("Already have at least 100 movies. No more TMDB fetch.")
        return

    next_page = (current // 20) + 1
    print(f"Currently have {current} movies. Fetching TMDB page {next_page}...")
    movies = get_tmdb_data(api_key, page_number=next_page)
    print(f"Retrieved {len(movies)} movies from TMDB.")
    store_movies_in_db(movies, conn)

def calculate_tmdb_genre_counts(conn, output_file="tmdb_genre_counts.txt"):
    """
    Read Movies.genre_ids, convert to names, count occurrences.
    """
    cur = conn.cursor()
    cur.execute("SELECT genre_ids FROM Movies")
    rows = cur.fetchall()

    counts = Counter()

    for (genre_ids_str,) in rows:
        if not genre_ids_str:
            continue
        try:
            id_list = json.loads(genre_ids_str)
        except json.JSONDecodeError:
            id_list = []
        names = get_genre_names(id_list)
        for g in names:
            counts[g] += 1

    sorted_genres = sorted(counts.items(), key=lambda x: x[1], reverse=True)

    with open(output_file, "w") as f:
        for genre, count in sorted_genres:
            f.write(f"{genre}: {count}\n")

    print(f"Wrote TMDB genre counts to {output_file}")

    return [{"genre": g, "count": c} for g, c in sorted_genres]

#----------Visualization----------
def visualize_tmdb_genres(genre_data, top_n=10):
    top = genre_data[:top_n]
    labels = [item["genre"] for item in top]
    counts = [item["count"] for item in top]

    plt.figure(figsize=(10, 6))
    plt.barh(labels, counts)
    plt.xlabel("Number of Movies")
    plt.ylabel("Genre")
    plt.title(f"Top {top_n} TMDB Movie Genres")
    plt.tight_layout()
    plt.show()













#--------------------Main Function (Claire Fuller, Willow Traylor, and Anna Kerhoulas)-------------------------
if __name__ == '__main__':
    # use the DB_PATH constant from the top of the file
    conn = init_database(DB_PATH)

    #----------Spotify (Claire Fuller)----------
    fetch_and_store_spotify_tracks(conn)
    spotify_genre_data = calculate_spotify_genre_popularity(conn)
    visualize_genre_popularity(spotify_genre_data)

    #----------TV Maze (Willow Traylor)----------
    shows = get_tvmaze_data(page=0)
    insert_shows(conn, shows)
    print(f"Inserted {len(shows)} TV shows into the Shows table.")

    #----------TMDB (Anna Kerhoulas)----------
    fetch_and_store_tmdb_movies(conn)
    tmdb_genre_data = calculate_tmdb_genre_counts(conn)
    visualize_tmdb_genres(tmdb_genre_data)

    conn.close()