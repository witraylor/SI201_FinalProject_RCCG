import json
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import csv
import matplotlib.pyplot as plt
import os
import sqlite3
from typing import List, Dict, Any, Optional
from datetime import datetime


#spotipy ids
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

def get_spotify_data(query="year:2024", limit=25, offset=0):
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

#initialize ALL table sets
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


def fetch_and_store_spotify_tracks(conn):
    cur = conn.cursor()

    # Count how many songs already exist
    cur.execute("SELECT COUNT(*) FROM Songs")
    current_count = cur.fetchone()[0]

    offset = current_count

    # DO NOT fetch beyond 100
    if current_count >= 100:
        print("You already have 100 songs. No more data fetched.")
        return

    print(f"Currently stored: {current_count} songs.")
    print(f"Fetching tracks using offset = {offset}...")

    # Fetch 25 tracks
    if current_count < 50:
        query = "year:2023"
    else:
        query = "year:2024"
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



if __name__ == '__main__':
    my_spotipy_query()
    conn = init_database(db_name= DB_PATH)
    fetch_and_store_spotify_tracks(conn)


# get data from spotify, loop over track. for each track add it SQlite using api. 