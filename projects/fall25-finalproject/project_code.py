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

def get_spotify_data(query="year:2023", limit=25, offset=0):
    # Search for tracks matching query
    results = sp.search(q=query, type="track", limit=limit, offset=offset)
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
            release_date TEXT,
            genre TEXT

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
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Artists (
            id TEXT PRIMARY KEY,
            name TEXT,
            genres TEXT
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
            INSERT OR IGNORE INTO Songs 
            (id, title, artist, album, popularity, release_date, genre)
            VALUES (?, ?, ?, ?, ?, ?, ?);
        """, (
            s.get('id'),
            s.get('name'),
            s.get('artist'),
            s.get('album'),
            s.get('popularity'),
            s.get('release_date'),
            ", ".join(s.get('genres', [])) if s.get('genres') else None
        ))

    conn.commit()

def fetch_and_store_spotify_tracks(conn):
    all_tracks = []

    #2023 tracks
    for index in range(4):
        batch = get_spotify_data(query="year:2023", limit=25, offset=index * 25)
        all_tracks.extend(batch)

    #2024 tracks
    for index in range(4):
        batch = get_spotify_data(query="year:2024", limit=25, offset=index * 25)
        all_tracks.extend(batch)

    print(f"Fetched {len(all_tracks)} raw tracks.")

    #add genres
    all_tracks = enrich_tracks_with_genres(all_tracks)
    print("Added genre metadata from artists.")

    insert_songs(conn, all_tracks)
    print("Tracks inserted into SQLite.")


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
    conn = init_database(db_name= DB_PATH)
    fetch_and_store_spotify_tracks(conn)


# get data from spotify, loop over track. for each track add it SQlite using api. 