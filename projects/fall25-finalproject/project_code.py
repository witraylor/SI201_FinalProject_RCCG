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
    # DROP the old Movies table so the new schema can be created
    # cur.execute("DROP TABLE IF EXISTS sqlite_sequence")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Movies (
            id INTEGER PRIMARY KEY,
            title TEXT,
            release_date TEXT,
            popularity REAL,
            revenue INTEGER,
            avg_rating REAL,
            genres TEXT
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

    conn.commit()

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



#-------------------- TMDB API (Anna Kerhoulas) -------------------------
#---------- Initializing API Key and Base Url ----------

TMDB_API_KEY = "6b8a91e2db2dc97ffc2363b4dc8e6298"
TMDB_BASE_URL = "https://api.themoviedb.org/3/discover/movie"

#---------- Change Genre IDs to Genre Names ----------
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
    """
    Convert a list of TMDB genre IDs to genre names using genre_mapping.
    """
    return [genre_mapping.get(g, "Unknown") for g in genre_ids]

#---------- Database Setup Helper (if you need it here) ----------
def init_database(db_name="media_project.db"):
    """
    Create/open a SQLite database and ensure the Movies table exists.
    Adjust schema if your project already defines it differently.
    """
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS Movies (
            id INTEGER PRIMARY KEY,
            title TEXT,
            release_date TEXT,
            popularity REAL,
            revenue INTEGER,
            avg_rating REAL,
            genres TEXT
        )
        """
    )
    conn.commit()
    return conn

#---------- Get Data from TMDB ----------
def get_tmdb_data(api_key: str, page_number: int = 1):
    """
    Fetch one page (20 results) of popular movies from TMDB Discover endpoint.
    Returns a list of movie dicts with genre names already resolved.
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
        genre_ids = item.get("genre_ids", [])
        genre_names = get_genre_names(genre_ids)

        movie = {
            "id": item.get("id"),
            "title": item.get("title"),
            "release_date": item.get("release_date"),
            "popularity": item.get("popularity"),
            "revenue": 0,  # not in this endpoint, so default to 0
            "avg_rating": item.get("vote_average"),
            "genres": ", ".join(genre_names)  # store genre NAMES here
        }
        movies.append(movie)

    return movies

#---------- Store Data in Database ----------
def store_movies_in_db(movie_list, conn):
    """
    Insert a list of movie dictionaries into the Movies table.
    Uses INSERT OR IGNORE to avoid duplicate primary keys.
    """
    cur = conn.cursor()
    for m in movie_list:
        cur.execute(
            """
            INSERT OR IGNORE INTO Movies
            (id, title, release_date, popularity, revenue, avg_rating, genres)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                m.get("id"),
                m.get("title"),
                m.get("release_date"),
                m.get("popularity"),
                m.get("revenue"),
                m.get("avg_rating"),
                m.get("genres"),   # store names, not IDs
            )
        )
    conn.commit()

#---------- Fetch EXACTLY 25 New Movies Per Run ----------
def fetch_and_store_tmdb_movies(conn, api_key: str = TMDB_API_KEY, batch_size: int = 25):
    """
    Fetch TMDB movies and insert EXACTLY `batch_size` NEW movies
    into the Movies table per run (unless TMDB has no more new ones).

    - Reads existing IDs from the DB
    - Fetches pages starting from page 1
    - Skips movies already in the DB
    - Stops after collecting batch_size new movies
    """
    cur = conn.cursor()
    cur.execute("SELECT id FROM Movies")
    existing_ids = {row[0] for row in cur.fetchall() if row[0] is not None}

    new_movies = []
    page = 1

    while len(new_movies) < batch_size:
        movies = get_tmdb_data(api_key, page_number=page)
        if not movies:
            # No more data from TMDB
            break

        for m in movies:
            if m["id"] not in existing_ids:
                new_movies.append(m)
                existing_ids.add(m["id"])
                if len(new_movies) == batch_size:
                    break

        page += 1

    if not new_movies:
        print("No new TMDB movies found to insert.")
        return

    store_movies_in_db(new_movies, conn)
    print(f"Inserted {len(new_movies)} new TMDB movies into the database.")

#---------- Calculate Genre Counts ----------
def calculate_tmdb_genre_counts(conn, output_file="tmdb_genre_counts.txt"):
    """
    Count genres based directly on stored *genre names* in the Movies table.
    Writes counts to a text file and returns a list of dicts:
    [{"genre": <name>, "count": <int>}, ...]
    """
    cur = conn.cursor()
    cur.execute("SELECT genres FROM Movies")
    rows = cur.fetchall()

    counts = Counter()

    for (genres_str,) in rows:
        if not genres_str:
            continue
        names = [g.strip() for g in genres_str.split(",") if g.strip()]
        for g in names:
            counts[g] += 1

    sorted_genres = sorted(counts.items(), key=lambda x: x[1], reverse=True)

    with open(output_file, "w") as f:
        for genre, count in sorted_genres:
            f.write(f"{genre}: {count}\n")

    print(f"Wrote TMDB genre counts to {output_file}")

    return [{"genre": g, "count": c} for g, c in sorted_genres]

#---------- Visualization (Colorful!) ----------
def visualize_tmdb_genres(genre_data, top_n=10):
    """
    Visualize the top N genres as a colorful horizontal bar chart.
    """
    top = genre_data[:top_n]
    labels = [item["genre"] for item in top]
    counts = [item["count"] for item in top]

    plt.figure(figsize=(10, 6))

    # Use a colormap for multiple distinct colors
    colors = plt.cm.tab20(range(len(labels)))
    plt.barh(labels, counts, color=colors)

    plt.xlabel("Number of Movies")
    plt.ylabel("Genre")
    plt.title(f"Top {top_n} TMDB Movie Genres")
    plt.tight_layout()
    plt.show()



# =============== TVMAZE / TV Shows (Willow) =====================
# Fetch 25 shows from TVMAZE API
def get_tvmaze_data(page=0):
    url = f"https://api.tvmaze.com/shows?page={page}"
    response = requests.get(url)

    if response.status_code != 200:
        print("Error: unable to access TVMaze API")
        return []

    shows_data = response.json()[:25]

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

#Repeat get_tvmaze_data function to fetch 25 shows
def fetch_minimum_shows(min_total=25):
    all_shows = []
    seen_ids = set()
    page = 0

    while len(all_shows) < min_total:
        batch = get_tvmaze_data(page)
        if not batch:
            break  

        #filter out duplicates
        for show in batch:
            if show["id"] not in seen_ids:
                seen_ids.add(show["id"])
                all_shows.append(show)

        page += 1  #move to next page

    return all_shows

#Connect to DB and add show data
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

#Visualize comparison of show ratings VS popularity
def visualize_show_rating_vs_weight(conn):
    cur = conn.cursor()

    cur.execute("""
        SELECT avg_rating, weight
        FROM Shows
        WHERE avg_rating IS NOT NULL
          AND weight IS NOT NULL
    """)

    rows = cur.fetchall()

    ratings = []
    weights = []

    for r, w in rows:
        ratings.append(r)
        weights.append(w)

    #plot ratings vs weights
    plt.figure(figsize=(8, 6))
    plt.scatter(weights, ratings)  

    plt.xlabel("Weight")
    plt.ylabel("Average Rating")
    plt.title("Show Average Rating vs Weight")

    plt.tight_layout()
    plt.show()

# ----------Calculate and compare popular genres across media types (Willow)---------

#Find most popular song genre
def most_popular_song_genre(conn):
    cur = conn.cursor()

    cur.execute("""
        SELECT Genres.genre, Songs.popularity
        FROM Genres
        JOIN Songs ON Genres.song_id = Songs.id
        WHERE Songs.popularity IS NOT NULL;
    """)

    genre_sums = {}
    genre_counts = {}

    for genre, pop in cur.fetchall():
        if genre not in genre_sums:
            genre_sums[genre] = 0
            genre_counts[genre] = 0
        genre_sums[genre] += pop
        genre_counts[genre] += 1

    best_genre = None
    best_avg = -1

    for genre in genre_sums:
        avg = genre_sums[genre] / genre_counts[genre]
        if avg > best_avg:
            best_avg = avg
            best_genre = genre

    return best_genre, best_avg

#Find most popular movie genre
def most_popular_movie_genre(conn):
    cur = conn.cursor()

    cur.execute("""
        SELECT popularity, genres
        FROM Movies
        WHERE popularity IS NOT NULL AND genres IS NOT NULL;
    """)

    genre_sums = {}
    genre_counts = {}

    for pop, genre_str in cur.fetchall():
        try:
            pop = float(pop)
        except:
            continue

        #split comma separated lists
        genres = [g.strip() for g in genre_str.split(",") if g.strip()]

        for genre in genres:
            if genre not in genre_sums:
                genre_sums[genre] = 0
                genre_counts[genre] = 0
            genre_sums[genre] += pop
            genre_counts[genre] += 1

    best_genre = None
    best_avg = -1

    for genre in genre_sums:
        avg = genre_sums[genre] / genre_counts[genre]
        if avg > best_avg:
            best_avg = avg
            best_genre = genre

    return best_genre, best_avg

#Find most popular show genre
def most_popular_show_genre(conn):
    cur = conn.cursor()

    cur.execute("""
        SELECT weight, genres
        FROM Shows
        WHERE weight IS NOT NULL AND genres IS NOT NULL;
    """)

    genre_sums = {}
    genre_counts = {}

    for pop, genre_str in cur.fetchall():
        try:
            pop = float(pop)
        except:
            continue

        #Separate comma separated lists
        genres = [g.strip() for g in genre_str.split(",") if g.strip()]

        for genre in genres:
            if genre not in genre_sums:
                genre_sums[genre] = 0
                genre_counts[genre] = 0
            genre_sums[genre] += pop
            genre_counts[genre] += 1

    best_genre = None
    best_avg = -1

    for genre in genre_sums:
        avg = genre_sums[genre] / genre_counts[genre]
        if avg > best_avg:
            best_avg = avg
            best_genre = genre

    return best_genre, best_avg

#Compare popular genres for media types
def find_most_popular_genres(conn):
    return {
        "songs": most_popular_song_genre(conn),
        "movies": most_popular_movie_genre(conn),
        "shows": most_popular_show_genre(conn)
    }





#--------------------Main Function (Claire Fuller, Willow Traylor, and Anna Kerhoulas)-------------------------
if __name__ == '__main__':
    # use the DB_PATH constant from the top of the file
    conn = init_database(DB_PATH)

    #----------Spotify (Claire Fuller)----------
    fetch_and_store_spotify_tracks(conn)
    spotify_genre_data = calculate_spotify_genre_popularity(conn)
    visualize_genre_popularity(spotify_genre_data)

    #----------TMDB (Anna Kerhoulas)----------
    # fetch_and_store_tmdb_movies(conn)
    # tmdb_genre_data = calculate_tmdb_genre_counts(conn)
    # visualize_tmdb_genres(tmdb_genre_data)
    conn = init_database("media_project.db")

    # Each call adds up to 25 NEW movies
    fetch_and_store_tmdb_movies(conn, api_key=TMDB_API_KEY, batch_size=25)

    genre_data = calculate_tmdb_genre_counts(conn)
    visualize_tmdb_genres(genre_data, top_n=10)

        # ----- TVMaze -----
    min_shows = fetch_minimum_shows()
    insert_shows(conn, min_shows)
    print(f"Inserted {len(min_shows)} TV shows into the Shows table.")
    visualize_show_rating_vs_weight(conn)

    print(find_most_popular_genres(conn))

    conn.close()
