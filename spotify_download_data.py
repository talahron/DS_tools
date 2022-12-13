# -*- coding: utf-8 -*-
"""
Created on Mon Dec 12 20:00:48 2022

@author: talah
"""

# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import os
import pandas as pd
from multiprocessing.pool import ThreadPool
import multiprocessing
import tqdm
import warnings

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

warnings.filterwarnings("ignore")


cols = ['track_uri', 'track_name', 'artist_name', 'artist_genres', 'album',
       'track_popularity', 'danceability', 'energy', 'speechiness',
       'instrumentalness', 'liveness', 'duration_ms', 'num_samples',
       'duration', 'sample_md5', 'offset_seconds', 'window_seconds',
       'analysis_sample_rate', 'analysis_channels', 'end_of_fade_in',
       'start_of_fade_out', 'loudness', 'tempo', 'tempo_confidence',
       'time_signature', 'time_signature_confidence', 'key', 'key_confidence',
       'mode', 'mode_confidence', 'codestring', 'code_version',
       'echoprintstring', 'echoprint_version', 'synchstring', 'synch_version',
       'rhythmstring', 'rhythm_version', 'bars_start', 'bars_duration',
       'bars_confidence', 'beats_start', 'beats_duration', 'beats_confidence',
       'sections_start', 'sections_duration', 'sections_confidence',
       'sections_loudness', 'sections_tempo', 'sections_tempo_confidence',
       'sections_key', 'sections_key_confidence', 'sections_mode',
       'sections_mode_confidence', 'sections_time_signature',
       'sections_time_signature_confidence', 'segments_start',
       'segments_duration', 'segments_confidence', 'segments_loudness_start',
       'segments_loudness_max', 'segments_loudness_max_time',
       'segments_loudness_end', 'segments_pitches', 'segments_timbre',
       'tatums_start', 'tatums_duration', 'tatums_confidence', 'res1']



playlists = ['37i9dQZF1EIfBQKzlOLtaI',
              '6CPRxdYySTq7milVUgtsNv',
              '37i9dQZF1DWXWbLEOaHnU3',
              '69gyd1SXiVhvfsyYS078f0',
               '0iJEh2BsvSw8V3lDTkiUK7',
               '0mwI8C8raIZ1bOLLylX1yF',
               '5LYkaLqDJXIDP2giSxqTjv',
               '0mwI8C8raIZ1bOLLylX1yF',
               '75PxWthUYPOskOU1EjGrFX',
               '6bTaXLHYTrjxqtwfNp2q5r',
               '37i9dQZF1EIfBQKzlOLtaI',
               '5PpLEyVEGJWhybBaZoGRQ3',
               '37i9dQZF1DWXWbLEOaHnU3',
               '1WZDQ33EgE3I8Af3B68Dcw',
               '58GME4t0asA7gi32gFgl6Y',
               '205Eig4eQUa3qnnFRhXz5j',
               '0Q3ugz23LAXFg2PvXJ8hMx',
               '4CHZgzk4GxfzKvhJJtqDfl',
               '1WPBvjS7vmgvxuWF5llelU',
               '1ISS7NJiClZjglJfZHuux3',
               '4mUDrSu9PO8wXsjLKRebXW',
               '2TtMJyax92YgclxKG1Zob9',
               '5xhgfsf8F0CUo8x9Ism00j',
               '2YKqhA4jOHjKr6VAhLvsOe',
               '0YsKFBakZickjDpUG989b9',
               '37i9dQZF1DWXWbLEOaHnU3',
               '3J4wydXJ7Z8KBu9VGZZTYD',
               '2XUx9kaW16YsDjNnAq9PgZ',
               '2ayNjR9JAeRKJ4tcdfJpTG',
               '4Unz9KP0oNKsRnfwJxQ3ZE',
               '3Ljwa5tk2n8qgZea7LXPA4',
               '1wwd4STwdFwDvDB0m3SUsw',
               '4g5ZGt6XCqRzryhLDqVsgH',
               '10I9rW4iCeTqlkj323QYA6',
               '46R5frnZKsWcHj5atjr2W1',
               '2acZJHWwm3ujFZFu4Aqy0a',
               '2fFITGJ2BL4VUnA6JHF7Mf',
               '3R19Yz7MiAm25gjldFeGUf',
               '1umocUPKs0v8iHLW6FxmMm',
               '5L7EZO6b7fPncyi90GZMoz',
               '2J18TJEFQvOLZPs2QPr70O',
               '55coPdCl9eMT04JuhIysHU',
               '4LkHkdDNfS79DjRUHzzOm8',
               '4vtOf57odLMNssRUuKFbsC',
               '37i9dQZF1EIdyUAC3QdkKj',
               '4wIA3KVqx2iCb81C3IiP9v',
               '37i9dQZF1EIgFg4ewOkhQ0',
               '0dJiFK6hcSfrfj3bru4shF',
               '7hDrrQD9R3dicZL1apPhHM',
               '53rqUBXOsmuw2ZkS4nNRt4',
               '1yBIftnKngaJk5HnRVjZeC',
               '2apQmKB26kfclH62AW7l8t',
               '25a8lYmjNUErpzwEfqdrGr',
               '2wQv29tyoPG8SkCyphg0iK',
               '6vyJX1ZKbL3FPW1txSgij5',
               '1JM1GoxgcTkyiKLJUJIYiu',
               '3AafeZ3TLEI7H8ymHfY10g',
               '4iNbaZPFwpYrvmsTtqaOgb',
               '4Ls7VJyeialGrkAI5tXUTn',
               '0PoOHqOYKCvJyt5dj7srNf',
               '5qaD3B1Qnqa1Hx6PhXnBKr',
                '6ZNqBjtmRBjXBG2BW0QYCA',
                '0ws3RdUkAPYzCWnIkqJqzm',
                '7iuiJqxb5kRokJfYg83J5P',
               '1Cw61Hauzz6VmvguzdNjoF',
              '6vnIxTc4MUjthxkaI5xKJU',
              '2gIC7sBbgkhLizlXJO2ydr',
              '61zAE6SMADnoD8yTyakSSa',
              '3ZP3oaBGHx1x1OVlb89bzr',
              '2LndZNK3pKXaGw9wK9AYW8',
              '4SMjygaCRgHR0gJquvhz4k',
              '6CPRxdYySTq7milVUgtsNv',
              '37i9dQZF1DWXWbLEOaHnU3',
              '69gyd1SXiVhvfsyYS078f0',
               '0iJEh2BsvSw8V3lDTkiUK7',
               '0mwI8C8raIZ1bOLLylX1yF',
               '5LYkaLqDJXIDP2giSxqTjv',
               '0mwI8C8raIZ1bOLLylX1yF',
               '75PxWthUYPOskOU1EjGrFX',
               '6bTaXLHYTrjxqtwfNp2q5r',
               '37i9dQZF1EIfBQKzlOLtaI',
               '5PpLEyVEGJWhybBaZoGRQ3',
               '37i9dQZF1DWXWbLEOaHnU3',
               '1WZDQ33EgE3I8Af3B68Dcw',
               '58GME4t0asA7gi32gFgl6Y',
               '205Eig4eQUa3qnnFRhXz5j',
               '0Q3ugz23LAXFg2PvXJ8hMx',
               '4CHZgzk4GxfzKvhJJtqDfl',
               '1WPBvjS7vmgvxuWF5llelU',
               '1ISS7NJiClZjglJfZHuux3',
               '4mUDrSu9PO8wXsjLKRebXW',
               '2TtMJyax92YgclxKG1Zob9',
               '5xhgfsf8F0CUo8x9Ism00j',
               '2YKqhA4jOHjKr6VAhLvsOe',
               '0YsKFBakZickjDpUG989b9',
               '37i9dQZF1DWXWbLEOaHnU3',
               '3J4wydXJ7Z8KBu9VGZZTYD',
               '2XUx9kaW16YsDjNnAq9PgZ',
               '2ayNjR9JAeRKJ4tcdfJpTG',
               '4Unz9KP0oNKsRnfwJxQ3ZE',
               '3Ljwa5tk2n8qgZea7LXPA4',
               '1wwd4STwdFwDvDB0m3SUsw',
               '4g5ZGt6XCqRzryhLDqVsgH',
               '10I9rW4iCeTqlkj323QYA6',
               '46R5frnZKsWcHj5atjr2W1',
               '2acZJHWwm3ujFZFu4Aqy0a',
               '2fFITGJ2BL4VUnA6JHF7Mf',
               '3R19Yz7MiAm25gjldFeGUf',
               '1umocUPKs0v8iHLW6FxmMm',
               '5L7EZO6b7fPncyi90GZMoz',
               '2J18TJEFQvOLZPs2QPr70O',
               '55coPdCl9eMT04JuhIysHU',
               '4LkHkdDNfS79DjRUHzzOm8',
               '4vtOf57odLMNssRUuKFbsC',
               '37i9dQZF1EIdyUAC3QdkKj',
               '4wIA3KVqx2iCb81C3IiP9v',
               '37i9dQZF1EIgFg4ewOkhQ0',
               '0dJiFK6hcSfrfj3bru4shF',
               '7hDrrQD9R3dicZL1apPhHM',
               '53rqUBXOsmuw2ZkS4nNRt4',
               '1yBIftnKngaJk5HnRVjZeC',
               '2apQmKB26kfclH62AW7l8t',
               '25a8lYmjNUErpzwEfqdrGr',
               '2wQv29tyoPG8SkCyphg0iK',
               '6vyJX1ZKbL3FPW1txSgij5',
               '1JM1GoxgcTkyiKLJUJIYiu',
               '3AafeZ3TLEI7H8ymHfY10g',
               '4iNbaZPFwpYrvmsTtqaOgb',
               '4Ls7VJyeialGrkAI5tXUTn',
               '0PoOHqOYKCvJyt5dj7srNf',
               '5qaD3B1Qnqa1Hx6PhXnBKr',
                '6ZNqBjtmRBjXBG2BW0QYCA',
                '0ws3RdUkAPYzCWnIkqJqzm',
               '7iuiJqxb5kRokJfYg83J5P',
               '1Cw61Hauzz6VmvguzdNjoF',
              '6vnIxTc4MUjthxkaI5xKJU',
              '2gIC7sBbgkhLizlXJO2ydr',
              '61zAE6SMADnoD8yTyakSSa',
              '3ZP3oaBGHx1x1OVlb89bzr',
              '2LndZNK3pKXaGw9wK9AYW8',
              '4SMjygaCRgHR0gJquvhz4k'
             ]


path = "https://open.spotify.com/playlist"



def extract_data(track):
    try:

        treks = pd.Series(index=cols)
        treks['res1'] = 'Pass'
        
        clean_url = track["track"]["uri"].replace('spotify:track:', '')
        track_uri = track["track"]["uri"]

        #URI
        treks['track_uri'] = track["track"]["uri"]
       
        #Track name
        treks['track_name'] = track["track"]["name"]
      
        #Main Artist
        artist_uri = track["track"]["artists"][0]["uri"]
        artist_info = sp.artist(artist_uri)
      
        #Name, popularity, genre
        treks['artist_name'] = track["track"]["artists"][0]["name"]
        treks['artist_popularity'] = artist_info["popularity"]
        treks['artist_genres'] = artist_info["genres"]
        
        #Album
        treks['album'] = track["track"]["album"]["name"]
      
        # audio features
        audio_info = sp.audio_features(track_uri)[0]
        if audio_info is not None:
            treks['danceability'] = audio_info['danceability']
            treks['energy'] = audio_info['energy']
            treks['speechiness'] = audio_info['speechiness']
            treks['instrumentalness'] =audio_info['instrumentalness']
            treks['liveness'] = audio_info['liveness']
            treks['duration_ms'] = audio_info['duration_ms']

        # audio analysis
        analysis = sp.audio_analysis(track_uri)
        
        treks['timestamp'] = analysis["meta"]["timestamp"]

        for tr in analysis['track']:
            treks[f"track_{tr}"] = analysis['track'][tr]

        for bar in analysis['bars'][0]:
            treks[f"bars_{bar}"] = analysis['bars'][0][bar]

        for beat in analysis['beats'][0]:
            treks[f"beats_{beat}"] = analysis['beats'][0][beat]

        for section in analysis['sections'][0]:
            treks[f"sections_{section}"] = analysis['sections'][0][section]

        for segment in analysis['segments'][0]:
            treks[f"segments_{segment}"] = analysis['segments'][0][segment]

        for tatum in analysis['tatums'][0]:
            treks[f"tatums_{tatum}"] = analysis['tatums'][0][tatum]

        #Popularity of the track
        treks['track_popularity'] = track["track"]["popularity"]
    except Exception as e:
        treks['res1'] = 'Error: {}'.format(e)
        
    return pd.DataFrame(treks).T

def get_data():
    cid = '797f0f2714e54bf181e734ff877bf026'
    secret = '24e32f03473b4bbeb1e2d6345b9cf3ff'
    # Authentication - without user
    client_credentials_manager = SpotifyClientCredentials(client_id=cid, client_secret=secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

    root_dir = r'C:\Users\talah\Desktop\ML\spotify\data'
    final_df = pd.DataFrame(columns=cols)
    slices = 10

    for i, link in enumerate(playlists):
        if i < 110:
            continue
        if i % slices == 0:
            final_df = pd.DataFrame(columns=cols)
        try:

            playlist_link = path + "/" + link
            playlist_URI = playlist_link.split("/")[-1].split("?")[0]

            total_samples = sp.playlist_tracks(playlist_URI)["items"]

            res_list = list()

            use_mp = True
            if use_mp:
                thread_pool_size = multiprocessing.cpu_count()
                pool = ThreadPool(processes=thread_pool_size)

                imap_it = pool.imap(extract_data, total_samples)
                for item_id in tqdm.trange(len(total_samples), desc="Features Extraction"):
                    res_list.append(imap_it.next())

                pool.close()
                pool.join()
            else:
                for track in total_samples:
                    res_list.append(extract_data(track))

            final_df = pd.concat(res_list, axis=0, ignore_index=True)
            final_df['link'] = link

        except Exception as e:
            print(f'error in link {link}: {e}')
            continue

        if i % slices == 0:
            path = os.path.join(root_dir, f'data_{i / slices}.csv')
            final_df.to_csv(path)
    return root_dir

if __name__ == "__main__":
    dir = get_data()
