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

#--------------------call spotipy api-------------------------
CLIENT_ID = 'be1ea16df5c24ab195ff21e6c8a82cd1'
CLIENT_SECRET = 'fe32b6a7ae92441bb57db162f56e75a3'
DB_PATH = "/Users/clairefuller/Desktop/umich/Si201/SI201_FinalProject_RCCG/projects/fall25-finalproject/media_data.db"
#create an authorized variable to use for future API calls
sp = spotipy.Spotify(
    auth_manager=SpotifyClientCredentials(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET
    )
)

def get_spotify_data(query="year:2020-2025", limit=25, offset=0):
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

#--------------------------initialize ALL table sets-------------------------
def init_database(db_name="media_data.db"):
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


#--------------insert songs into database----------------
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

#-----------spotipy fetching helpers------------------
def fetch_and_store_spotify_tracks(conn):
    cur = conn.cursor()

    # Count how many songs already exist
    cur.execute("SELECT COUNT(*) FROM Songs")
    current_count = cur.fetchone()[0]

    offset = (current_count // 25) * 25 #changed

    # DO NOT fetch beyond 100
    if current_count >= 100:
        print("You already have 100 songs. No more data fetched.")
        return

    print(f"Currently stored: {current_count} songs.")
    print(f"Fetching tracks using offset = {offset}...")

    # Fetch 25 tracks
    query = "year:2020-2025"
    tracks = get_spotify_data(query=query, limit=25, offset=offset)
    print(f"Retrieved {len(tracks)} tracks")

    # Add genres
    tracks = enrich_tracks_with_genres(tracks)

    # Insert into DB
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

#_----------------calculations-----------------
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

#--------------------visualization----------------
def visualize_genre_popularity(data):
    genres = [item["genre"] for item in data]
    avg_popularity = [item["avg_popularity"] for item in data]

    plt.figure(figsize=(12, 7))
    colors = ["skyblue", "magenta", "lightgreen", "violet", "pink", "turquoise"]
    plt.barh(genres, avg_popularity, color=colors[:len(genres)])
    print("hello")

    plt.xlabel("Average Popularity")
    plt.ylabel("Genre")
    plt.title("Average Spotify Popularity by Genre")


    plt.tight_layout()

    plt.show()

#Retrieve TV show data from TVMAZE API, add to the database, and visualize data
#1: Fetch shows
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






if __name__ == '__main__':
    #my_spotipy_query()
    # conn = init_database(db_name= DB_PATH)
    # fetch_and_store_spotify_tracks(conn)
    # calculate_spotify_genre_popularity(conn)

    # genre_data = calculate_spotify_genre_popularity(conn)
    # visualize_genre_popularity(genre_data)
    #TVMAZE api
    print(get_tvmaze_data(page=0))



